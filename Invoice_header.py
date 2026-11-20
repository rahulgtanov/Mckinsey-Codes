import sys
import math
import time
from contextlib import contextmanager
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, trim, when, substring, coalesce, broadcast
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    NumericType,
    IntegerType,
    DoubleType,
    LongType,
)

from mck_glue_library.datastores import jdbc_utils
from mck_glue_library.datastores.snowflake_utils import SparkSnowflakeConnector

# --- INIT ---
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Wider shuffles + AQE help when the source returns a lot of partitions.
spark.conf.set("spark.sql.shuffle.partitions", "1000")
spark.conf.set("spark.sql.adaptive.enabled", "true")

TARGET_TABLE = "MCK_PROCUREMENT.PDM_INVOICE_D"

# Existing bookkeeping table in HANA, used exactly as the original job uses it
# (FULL_LOAD_COMPLETED / LAST_MAX_ID / LAST_LOAD_AT). Its structure is NOT
# changed. The incremental watermark is NOT stored here: it is read from the
# target table itself (MAX(UPDATED_AT)), so no table needs a new column.
LOAD_STATE_TABLE = "MCK_PROCUREMENT.GLUE_JOB_LOAD_STATE"
LOAD_NAME = "COUPA_INVOICE_HEADER"

DEFAULT_SECRET_REGION = "us-east-1"

# Target rows per Spark partition (= per JDBC writer task). The mapped frame is
# repartitioned to ceil(total / ROWS_PER_PARTITION) partitions, so each writer
# streams ~this many rows to HANA. No in-memory caching is involved.
ROWS_PER_PARTITION = 500000

# Load mode. Override per run with the optional Glue argument
#   --load_mode AUTO | FULL | INCREMENTAL
#   AUTO        -> INCREMENTAL whenever the target already has data; FULL only
#                  if the target is empty (normal scheduled setting)
#   FULL        -> truncate + reload everything (explicit opt-in only)
#   INCREMENTAL -> upsert changed invoices; errors (never truncates) if the
#                  target is empty
DEFAULT_LOAD_MODE = "AUTO"

# Invoice IDs per DELETE ... WHERE ID IN (...) statement during the upsert.
DELETE_BATCH_SIZE = 1000

# Every incremental run re-reads this much history before the watermark
# (MAX(UPDATED_AT) already in the target). The upsert is idempotent, so overlap is harmless, and it protects
# against late-arriving rows / replication lag in the Coupa data share.
INCREMENTAL_LOOKBACK_HOURS = 24

# Audit columns on the HANA target (previously always written as NULL).
#   H_UPDATED_DATE -> load time of the run that last wrote the row (every insert AND update)
#   H_CREATED_DATE -> load time of the run that FIRST inserted the row. Because the
#                     incremental is delete+append, the existing value is read before
#                     the delete and written back, so an update does not reset it.
# Both use one timestamp per run, taken from the HANA clock (CURRENT_TIMESTAMP).
H_CREATED_COL = "H_CREATED_DATE"
H_UPDATED_COL = "H_UPDATED_DATE"
PRESERVE_H_CREATED_ON_UPDATE = True   # False -> H_CREATED_DATE is also reset on every update

# Before/after report: these target columns are printed for a sample of the
# invoices processed (or for all --test_ids). Columns missing in HANA are skipped.
SNAPSHOT_COLUMNS = [
    "ID", "STATUS", "UPDATED_AT", "INVOICE_NUMBER", "TOTAL", "PAID",
    "PAYMENT_DATE", H_CREATED_COL, H_UPDATED_COL, "emergency",
]
SNAPSHOT_SAMPLE_SIZE = 10

# Test mode (dev/QA): --test_ids 101,102,... runs the incremental delete+append
# for exactly these invoices, regardless of the watermark.
MAX_TEST_IDS = 50

# The query below already emits exactly one row per INVOICE_HEADER_ID (the
# invoice_header CTE groups by it, and every join is de-duplicated with QUALIFY
# or an aggregate). The old Spark-side/QUALIFY dedup is therefore redundant and
# would add a full global window sort. Flip this to True only as a safety net if
# a source view starts fanning out again.
ENFORCE_ONE_ROW_PER_ID = False

DEDUP_QUALIFY = (
    "QUALIFY ROW_NUMBER() OVER "
    "(PARTITION BY INVOICE_HEADER_ID ORDER BY UPDATED_AT DESC, CREATED_AT DESC) = 1"
)

args = getResolvedOptions(
    sys.argv,
    ["sap_secrets_arn", "snowflake_passwords_arn", "sap_url", "coupa_database", "coupa_schema", "sf_warehouse"],
)


def _optional_arg(name: str, default: str) -> str:
    """getResolvedOptions fails on missing args, so only ask for it if passed."""
    if f"--{name}" in sys.argv:
        return getResolvedOptions(sys.argv, [name])[name]
    return default


sap_arn = args["sap_secrets_arn"]
sf_arn = args["snowflake_passwords_arn"]
hana_url = args["sap_url"]
sf_database = args["coupa_database"]
sf_schema = args["coupa_schema"]
sf_warehouse = args["sf_warehouse"]
region = DEFAULT_SECRET_REGION
LOAD_MODE = _optional_arg("load_mode", DEFAULT_LOAD_MODE).strip().upper()


def _parse_test_ids(raw: str):
    ids = sorted({int(x) for x in raw.replace(" ", "").split(",") if x})
    if len(ids) > MAX_TEST_IDS:
        raise ValueError(f"--test_ids accepts at most {MAX_TEST_IDS} IDs (got {len(ids)})")
    return ids


TEST_IDS = _parse_test_ids(_optional_arg("test_ids", ""))

# ---------------------------------------------------------------------------
# Source query.
#
# CTE-based Coupa invoice header query. Mechanical changes vs. the standalone SQL:
#   1. PROD_COUPA_DATA_SHARE.PRESENTATION. -> __COUPA_DB_SCHEMA__ placeholder,
#      so --coupa_database / --coupa_schema drive the qualifier.
#   2. The final SELECT is wrapped in a CTE (coupa_invoice_header_final) and the
#      trailing ORDER BY is dropped, so the job can append its own projection.
#   3. __IH_FILTER_WHERE__ / __IH_FILTER_AND__ placeholders in the CTEs that
#      scan INVOICES_VIEW per invoice (invoice_header, invoice_accounts,
#      payment_terms). Full load: replaced with nothing. Incremental: replaced
#      with "only changed invoice ids", so the filter is applied at the source
#      scan instead of after the whole query has been computed. Everything
#      built from invoice_header (remit-to matching, UI COA, ...) inherits it.
#      entry_office_coa_prefix_map deliberately stays unfiltered: it is a
#      mapping learned from the full invoice history.
# One small logic change is marked inline: a dedup QUALIFY on
# entry_office_coa_lookup, so a duplicate LOOKUP_VALUE_NAME can't fan out rows.
# ---------------------------------------------------------------------------
_COUPA_INVOICE_HEADER_CTE_TEMPLATE = r"""
WITH entry_office_coa_prefix_map AS (
  SELECT OFFICE_PREFIX, UI_ACCOUNT_TYPE_ID
  FROM (
    SELECT
      SPLIT_PART(iv.INVOICE_HEADER_CUSTOM_FIELD_9, '|', 1) AS OFFICE_PREFIX,
      iv.ACCOUNT_TYPE_ID AS UI_ACCOUNT_TYPE_ID,
      ROW_NUMBER() OVER (
        PARTITION BY SPLIT_PART(iv.INVOICE_HEADER_CUSTOM_FIELD_9, '|', 1)
        ORDER BY COUNT(*) DESC, iv.ACCOUNT_TYPE_ID
      ) AS RN
    FROM __COUPA_DB_SCHEMA__INVOICES_VIEW iv
    WHERE NULLIF(TRIM(iv.INVOICE_HEADER_CUSTOM_FIELD_9), '') IS NOT NULL
      AND POSITION('|' IN iv.INVOICE_HEADER_CUSTOM_FIELD_9) > 0
      AND iv.ACCOUNT_TYPE_ID IS NOT NULL
    GROUP BY 1, 2
  )
  WHERE RN = 1
),

entry_office_coa_lookup AS (
  SELECT
    LOOKUP_VALUE_NAME AS ENTRY_OFFICE,
    ACCOUNT_TYPE_ID AS UI_ACCOUNT_TYPE_ID
  FROM __COUPA_DB_SCHEMA__LOOKUPS_VIEW
  WHERE LOOKUP_NAME = 'Entry Office | Ship to State'
    AND ACCOUNT_TYPE_ID IS NOT NULL
  -- Added: guarantees one row per ENTRY_OFFICE so the join below cannot fan out.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LOOKUP_VALUE_NAME ORDER BY ACCOUNT_TYPE_ID
  ) = 1
),

invoice_header AS (
  SELECT
    iv.INVOICE_HEADER_ID,
    MAX(iv.CREATED_AT) AS CREATED_AT,
    MAX(iv.UPDATED_AT) AS UPDATED_AT,
    MAX(iv.STATUS) AS STATUS,
    MAX(iv.INVOICE_DATE) AS INVOICE_DATE,
    MAX(iv.DELIVERY_DATE) AS DELIVERY_DATE,
    MAX(iv.INVOICE_NUMBER) AS INVOICE_NUMBER,
    MAX(iv.INVOICE_HEADER_TAGS) AS INVOICE_HEADER_TAGS,
    MAX(iv.LINE_LEVEL_TAXATION) AS LINE_LEVEL_TAXATION,
    MAX(iv.PAID) AS PAID,
    MAX(iv.PAYMENT_DATE) AS PAYMENT_DATE,
    MAX(iv.PAYMENT_NOTES) AS PAYMENT_NOTES,
    MAX(iv.PAYMENT_CODE) AS PAYMENT_CODE,
    MAX(iv.TOTAL_WITH_TAXES_AMOUNT) AS TOTAL_WITH_TAXES_AMOUNT,
    MAX(iv.CURRENCY_CODE) AS CURRENCY_CODE,
    MAX(iv.ACCOUNTING_TOTAL_CURRENCY_ID) AS ACCOUNTING_TOTAL_CURRENCY_ID,
    MAX(iv.SUPPLIER_ID) AS SUPPLIER_ID,
    MAX(iv.SUPPLIER_NAME) AS SUPPLIER_NAME,
    MAX(iv.CREATED_BY) AS CREATED_BY,
    MAX(iv.UPDATED_BY) AS UPDATED_BY,
    MAX(iv.REMIT_TO_ADDRESS_ID) AS REMIT_TO_ADDRESS_ID,
    MAX(iv.SHIP_TO_ADDRESS_ID) AS SHIP_TO_ADDRESS_ID,
    MAX(iv.SHIP_FROM_ADDRESS_ID) AS SHIP_FROM_ADDRESS_ID,
    MAX(iv.INBOUND_INVOICE_ID) AS INBOUND_INVOICE_ID,
    MAX(iv.DISCOUNT_PAYMENT_DUE_DATE) AS DISCOUNT_PAYMENT_DUE_DATE,
    MAX(iv.DISCOUNT_AMOUNT) AS DISCOUNT_AMOUNT,
    MAX(iv.DOCUMENT_TYPE) AS DOCUMENT_TYPE,
    MAX(iv.ORIGINAL_INVOICE_NUMBER) AS ORIGINAL_INVOICE_NUMBER,
    MAX(iv.ORIGINAL_INVOICE_DATE) AS ORIGINAL_INVOICE_DATE,
    MAX(iv.DOCUMENT_CHANNEL) AS DOCUMENT_CHANNEL,
    MAX(iv.PAYMENT_CHANNEL) AS PAYMENT_CHANNEL,
    MAX(iv.DOCUMENT_FORMAT) AS DOCUMENT_FORMAT,
    MAX(iv.BUYER_TAX_REG_NUMBER) AS BUYER_TAX_REGISTRATION_NUMBER,
    MAX(iv.BUYER_TAX_REG_COUNTRY_REGION) AS BUYER_TAX_REG_COUNTRY_REGION,
    MAX(iv.REQUESTED_BY_NAME) AS REQUESTED_BY_NAME,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_1) AS INVOICE_HEADER_CUSTOM_FIELD_1,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_2) AS INVOICE_HEADER_CUSTOM_FIELD_2,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_3) AS INVOICE_HEADER_CUSTOM_FIELD_3,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_5) AS INVOICE_HEADER_CUSTOM_FIELD_5,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_6) AS INVOICE_HEADER_CUSTOM_FIELD_6,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_7) AS INVOICE_HEADER_CUSTOM_FIELD_7,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_8) AS INVOICE_HEADER_CUSTOM_FIELD_8,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_9) AS ENTRY_OFFICE,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_11) AS INVOICE_HEADER_CUSTOM_FIELD_11,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_12) AS INVOICE_HEADER_CUSTOM_FIELD_12,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_13) AS INVOICE_HEADER_CUSTOM_FIELD_13,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_14) AS INVOICE_HEADER_CUSTOM_FIELD_14,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_15) AS INVOICE_HEADER_CUSTOM_FIELD_15,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_16) AS INVOICE_HEADER_CUSTOM_FIELD_16,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_18) AS INVOICE_HEADER_CUSTOM_FIELD_18,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_19) AS INVOICE_HEADER_CUSTOM_FIELD_19,
    MAX(iv.INVOICE_HEADER_CUSTOM_FIELD_20) AS INVOICE_HEADER_CUSTOM_FIELD_20,
    SUM(ZEROIFNULL(iv.INVOICE_LINE_TOTAL)) AS LINE_SUBTOTAL,
    SUM(ZEROIFNULL(iv.INVOICE_ACCOUNTING_TOTAL)) AS INVOICE_ACCOUNTING_TOTAL
  FROM __COUPA_DB_SCHEMA__INVOICES_VIEW iv
  __IH_FILTER_WHERE__
  GROUP BY iv.INVOICE_HEADER_ID
),
invoice_accounts AS (
  SELECT
    iv.INVOICE_HEADER_ID,
    CAST(COALESCE(a.ACCOUNT_TYPE_ID, iv.ACCOUNT_TYPE_ID) AS VARCHAR) AS ACCOUNT_TYPE_ID,
    at.NAME AS ACCOUNT_TYPE_NAME
  FROM __COUPA_DB_SCHEMA__INVOICES_VIEW iv
  LEFT JOIN __COUPA_DB_SCHEMA__ACCOUNTS_VIEW a
    ON iv.ACCOUNT_ID = a.ACCOUNT_ID
  LEFT JOIN __COUPA_DB_SCHEMA__ACCOUNT_TYPES_VIEW at
    ON COALESCE(a.ACCOUNT_TYPE_ID, iv.ACCOUNT_TYPE_ID) = at.ID
  WHERE iv.LINE_NUM = 1
    __IH_FILTER_AND__
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY iv.INVOICE_HEADER_ID
    ORDER BY iv.INVOICE_LINE_ID
  ) = 1
),
invoice_ui_coa AS (
  SELECT
    ih.INVOICE_HEADER_ID,
    ih.ENTRY_OFFICE,
    SPLIT_PART(ih.ENTRY_OFFICE, '|', 1) AS ENTRY_OFFICE_PREFIX,
    COALESCE(lu.UI_ACCOUNT_TYPE_ID, pm.UI_ACCOUNT_TYPE_ID, bc.ID) AS UI_ACCOUNT_TYPE_ID,
    CASE
      WHEN lu.UI_ACCOUNT_TYPE_ID IS NOT NULL THEN 'ENTRY_OFFICE_LOOKUP'
      WHEN pm.UI_ACCOUNT_TYPE_ID IS NOT NULL THEN 'OFFICE_PREFIX_HISTORY'
      WHEN bc.ID IS NOT NULL THEN 'BUYER_CARD_SUFFIX'
      ELSE 'UNMAPPED'
    END AS UI_COA_MAPPING_SOURCE
  FROM invoice_header ih
  LEFT JOIN entry_office_coa_lookup lu
    ON ih.ENTRY_OFFICE = lu.ENTRY_OFFICE
  LEFT JOIN entry_office_coa_prefix_map pm
    ON SPLIT_PART(ih.ENTRY_OFFICE, '|', 1) = pm.OFFICE_PREFIX
  LEFT JOIN __COUPA_DB_SCHEMA__ACCOUNT_TYPES_VIEW bc
    ON ih.INVOICE_HEADER_CUSTOM_FIELD_1 = 'Buyer Card'
   AND NULLIF(TRIM(ih.INVOICE_HEADER_CUSTOM_FIELD_6), '') IS NOT NULL
   AND bc.NAME = RIGHT(TRIM(ih.INVOICE_HEADER_CUSTOM_FIELD_6), 3)
),
invoice_charges AS (
  SELECT
    INVOICE_HEADER_ID,
    SUM(ZEROIFNULL(MISC_AMOUNT)) AS MISC_AMOUNT,
    SUM(ZEROIFNULL(HANDLING_AMOUNT)) AS HANDLING_AMOUNT,
    SUM(ZEROIFNULL(SHIPPING_AMOUNT)) AS SHIPPING_AMOUNT
  FROM __COUPA_DB_SCHEMA__INVOICE_CHARGES_VIEW
  GROUP BY INVOICE_HEADER_ID
),
invoice_taxes AS (
  SELECT
    INVOICE_HEADER_ID,
    SUM(ZEROIFNULL(AMOUNT)) AS TAX_AMOUNT,
    LISTAGG(DISTINCT CODE, ', ') WITHIN GROUP (ORDER BY CODE) AS TAX_CODE,
    MAX(EXCHANGE_RATE) AS EXCHANGE_RATE
  FROM __COUPA_DB_SCHEMA__INVOICE_TAXES_VIEW
  WHERE TAXABLE_TYPE = 'InvoiceHeader'
  GROUP BY INVOICE_HEADER_ID
),
withholding_taxes AS (
  SELECT
    INVOICE_HEADER_ID,
    SUM(ZEROIFNULL(WITHHOLDING_AMOUNT)) AS WITHHOLDING_TAX_AMOUNT
  FROM __COUPA_DB_SCHEMA__INVOICE_TAXES_VIEW
  WHERE TYPE = 'WithholdingTaxLine'
  GROUP BY INVOICE_HEADER_ID
),
supplier_view AS (
  SELECT
    ID,
    SUPPLIER_NUMBER,
    TRIM(NAME) AS NAME,
    TRIM(DISPLAY_NAME) AS DISPLAY_NAME,
    STATUS
  FROM __COUPA_DB_SCHEMA__SUPPLIERS_VIEW
),
created_by_user AS (
  SELECT
    ID,
    REGEXP_REPLACE(EMAIL, '(_[0-9]+)+@', '@') AS EMAIL,
    EMPLOYEE_NUMBER,
    REGEXP_REPLACE(LOGIN, '(_[0-9]+)+$', '') AS LOGIN
  FROM __COUPA_DB_SCHEMA__USERS_VIEW
),
updated_by_user AS (
  SELECT
    ID,
    REGEXP_REPLACE(EMAIL, '(_[0-9]+)+@', '@') AS EMAIL,
    EMPLOYEE_NUMBER,
    REGEXP_REPLACE(LOGIN, '(_[0-9]+)+$', '') AS LOGIN
  FROM __COUPA_DB_SCHEMA__USERS_VIEW
),
remit_to_address AS (
  SELECT ID, REMIT_TO_CODE, ATTENTION, STREET1, STREET2, CITY, STATE, POSTAL_CODE, COUNTRY_CODE, COUNTRY, NAME
  FROM __COUPA_DB_SCHEMA__ADDRESSES_VIEW
),
supplier_tax_registration AS (
  SELECT
    SUPPLIER_ID,
    COUNTRY_NAME,
    CASE
      WHEN LENGTH(TAX_REGISTRATION_NUMBER) = 9 AND COUNTRY_NAME = 'United States'
      THEN INSERT(TAX_REGISTRATION_NUMBER, 3, 0, '-')
      ELSE TAX_REGISTRATION_NUMBER
    END AS TAX_REGISTRATION_NUMBER
  FROM __COUPA_DB_SCHEMA__SUPPLIER_TAX_REGISTRATIONS_VIEW
  QUALIFY ROW_NUMBER() OVER (PARTITION BY SUPPLIER_ID ORDER BY UPDATED_AT DESC) = 1
),
ship_to_address AS (
  SELECT ID, ATTENTION, STREET1, STREET2, CITY, STATE, POSTAL_CODE, COUNTRY_CODE, COUNTRY, NAME
  FROM __COUPA_DB_SCHEMA__ADDRESSES_VIEW
),
supplier_remit_to_candidates AS (
  SELECT
    ih.INVOICE_HEADER_ID,
    srt.ID AS SUPPLIER_REMIT_TO_ID,
    srt.CONNECT_REMIT_TO_ADDRESS_ID,
    srt.REMIT_TO_CODE,
    srt.NAME,
    srt.STREET1,
    srt.STREET2,
    srt.CITY,
    srt.STATE,
    srt.POSTAL_CODE,
    srt.COUNTRY_NAME AS COUNTRY,
    CASE
      WHEN ih.SHIP_FROM_ADDRESS_ID = srt.ID
       AND NULLIF(TRIM(srt.REMIT_TO_CODE), '') IS NOT NULL THEN 'SHIP_FROM_EXACT'
      WHEN ih.REMIT_TO_ADDRESS_ID = srt.REMIT_TO_ADDRESS_ID THEN 'REMIT_TO_ADDRESS_EXACT'
      WHEN ih.REMIT_TO_ADDRESS_ID = srt.CONNECT_REMIT_TO_ADDRESS_ID THEN 'CONNECT_REMIT_TO_EXACT'
      WHEN pay_av.REMIT_TO_CODE ILIKE '%TRANSFER%'
       AND srt.REMIT_TO_CODE ILIKE '%TRANSFER%' THEN 'PAYMENT_PATTERN_TRANSFER'
      WHEN (pay_av.REMIT_TO_CODE ILIKE '%PCARD%' OR pay_av.REMIT_TO_CODE ILIKE '%AMEX%')
       AND srt.REMIT_TO_CODE = 'SHI-US-RM-1'
       AND ih.INVOICE_DATE < TO_DATE('2024-10-07') THEN 'PCARD_AS_OF_RM1'
      WHEN (pay_av.REMIT_TO_CODE ILIKE '%PCARD%' OR pay_av.REMIT_TO_CODE ILIKE '%AMEX%')
       AND srt.REMIT_TO_CODE = 'SHI-US-RM-CC'
       AND ih.INVOICE_DATE >= TO_DATE('2024-10-07') THEN 'PCARD_AS_OF_RMCC'
      WHEN (pay_av.REMIT_TO_CODE ILIKE '%ACH%'
         OR pay_av.REMIT_TO_CODE ILIKE '%EFT%'
         OR pay_av.REMIT_TO_CODE ILIKE '%CHECK%'
         OR pay_av.REMIT_TO_CODE ILIKE '%WIRE%')
       AND srt.REMIT_TO_CODE = 'SHI-US-RM-1' THEN 'CHECK_ACH_RM1'
      WHEN srt_ship.ID IS NOT NULL
       AND NULLIF(TRIM(srt_ship.REMIT_TO_CODE), '') IS NULL
       AND srt.CAN_BE_REMIT_TO = '1'
       AND NULLIF(TRIM(srt.REMIT_TO_CODE), '') IS NOT NULL THEN 'FALLBACK_CODED_REMIT_TO'
      WHEN srt.CAN_BE_REMIT_TO = '1'
       AND NULLIF(TRIM(srt.REMIT_TO_CODE), '') IS NOT NULL
       AND srt.CREATED_AT <= ih.INVOICE_DATE THEN 'AS_OF_INVOICE_DATE'
      WHEN ih.SHIP_FROM_ADDRESS_ID = srt.ID THEN 'SHIP_FROM_NO_CODE'
    END AS MATCH_METHOD
  FROM invoice_header ih
  LEFT JOIN __COUPA_DB_SCHEMA__ADDRESSES_VIEW pay_av
    ON pay_av.ID = ih.REMIT_TO_ADDRESS_ID
  LEFT JOIN __COUPA_DB_SCHEMA__SUPPLIER_REMIT_TOS_VIEW srt_ship
    ON ih.SHIP_FROM_ADDRESS_ID = srt_ship.ID
   AND ih.SUPPLIER_ID = srt_ship.SUPPLIER_ID
  JOIN __COUPA_DB_SCHEMA__SUPPLIER_REMIT_TOS_VIEW srt
    ON srt.SUPPLIER_ID = ih.SUPPLIER_ID
  WHERE NULLIF(TRIM(srt.REMIT_TO_CODE), '') IS NOT NULL
     OR ih.SHIP_FROM_ADDRESS_ID = srt.ID
),
supplier_remit_to AS (
  SELECT
    INVOICE_HEADER_ID,
    SUPPLIER_REMIT_TO_ID AS ID,
    CONNECT_REMIT_TO_ADDRESS_ID,
    REMIT_TO_CODE,
    NAME,
    STREET1,
    STREET2,
    CITY,
    STATE,
    POSTAL_CODE,
    COUNTRY,
    MATCH_METHOD AS SUPPLIER_REMIT_TO_MATCH_METHOD
  FROM supplier_remit_to_candidates
  WHERE MATCH_METHOD IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY INVOICE_HEADER_ID
    ORDER BY
      CASE MATCH_METHOD
        WHEN 'SHIP_FROM_EXACT' THEN 1
        WHEN 'REMIT_TO_ADDRESS_EXACT' THEN 2
        WHEN 'CONNECT_REMIT_TO_EXACT' THEN 3
        WHEN 'PAYMENT_PATTERN_TRANSFER' THEN 4
        WHEN 'PCARD_AS_OF_RM1' THEN 5
        WHEN 'PCARD_AS_OF_RMCC' THEN 5
        WHEN 'CHECK_ACH_RM1' THEN 6
        WHEN 'FALLBACK_CODED_REMIT_TO' THEN 7
        WHEN 'AS_OF_INVOICE_DATE' THEN 8
        WHEN 'SHIP_FROM_NO_CODE' THEN 9
        ELSE 99
      END,
      SUPPLIER_REMIT_TO_ID
  ) = 1
),
ship_from_address AS (
  SELECT ID, STREET1, STREET2, CITY, STATE, POSTAL_CODE, COUNTRY_NAME, NAME
  FROM __COUPA_DB_SCHEMA__SUPPLIER_REMIT_TOS_VIEW
),
invoice_inbox AS (
  SELECT
    INVOICE_HEADER_ID,
    INBOUND_INVOICE_ID,
    INBOUND_INVOICE_RECEIVED_AT,
    INBOUND_INVOICE_SENDER_EMAIL,
    INVOICE_INBOX_NAME
  FROM __COUPA_DB_SCHEMA__INVOICE_INBOXES_VIEW
  QUALIFY ROW_NUMBER() OVER (PARTITION BY INBOUND_INVOICE_ID ORDER BY INBOUND_INVOICE_RECEIVED_AT DESC) = 1
),

payment_terms AS (
  SELECT
    iv.INVOICE_HEADER_ID,
    MAX(pt.DISCOUNT_RATE) AS DISCOUNT_RATE
  FROM __COUPA_DB_SCHEMA__INVOICES_VIEW iv
  LEFT JOIN __COUPA_DB_SCHEMA__PAYMENT_TERMS_VIEW pt
    ON iv.PAYMENT_TERM_ID = pt.ID
  __IH_FILTER_WHERE__
  GROUP BY iv.INVOICE_HEADER_ID
),
country_code_mapping AS (
  SELECT
    column1 AS COUNTRY_NAME,
    column2 AS SUPPLIER_VAT_COUNTRY_CODE
  FROM VALUES
    ('Brazil', 'BR'),
    ('Germany', 'DE'),
    ('Thailand', 'TH'),
    ('Philippines', 'PH'),
    ('Norway', 'NO'),
    ('Czech Republic', 'CZ'),
    ('Ireland', 'IE'),
    ('Lithuania', 'LT'),
    ('Ghana', 'GH'),
    ('Iceland', 'IS'),
    ('Paraguay', 'PY'),
    ('Serbia', 'RS'),
    ('Guernsey', 'GG'),
    ('Montenegro', 'ME'),
    ('Romania', 'RO'),
    ('Turkey', 'TR'),
    ('Taiwan', 'TW'),
    ('Viet Nam', 'VN'),
    ('Bangladesh', 'BD'),
    ('Ukraine', 'UA'),
    ('Qatar', 'QA'),
    ('Namibia', 'NA'),
    ('Moldova, Republic Of', 'MD'),
    ('United Kingdom', 'GB'),
    ('Netherlands', 'NL'),
    ('Peru', 'PE'),
    ('Kenya', 'KE'),
    ('Slovakia', 'SK'),
    ('Austria', 'AT'),
    ('Malaysia', 'MY'),
    ('Lebanon', 'LB'),
    ('Uruguay', 'UY'),
    ('Kuwait', 'KW'),
    ('Cyprus', 'CY'),
    ('Barbados', 'BB'),
    ('Togo', 'TG'),
    ('Georgia', 'GE'),
    ('Rwanda', 'RW'),
    ('Zambia', 'ZM'),
    ('Turks & Caicos Islands', 'TC'),
    ('Gabon', 'GA'),
    ('Switzerland', 'CH'),
    ('Azerbaijan', 'AZ'),
    ('Poland', 'PL'),
    ('Greece', 'GR'),
    ('Mexico', 'MX'),
    ('Jamaica', 'JM'),
    ('Tunisia', 'TN'),
    ('Madagascar', 'MG'),
    ('Cayman Islands', 'KY'),
    ('Côte D''ivoire', 'CI'),
    ('Tanzania, United Republic Of', 'TZ'),
    ('Antigua & Barbuda', 'AG'),
    ('Jersey', 'JE'),
    ('Armenia', 'AM'),
    ('Colombia', 'CO'),
    ('New Zealand', 'NZ'),
    ('Japan', 'JP'),
    ('Chile', 'CL'),
    ('Finland', 'FI'),
    ('Pakistan', 'PK'),
    ('Guatemala', 'GT'),
    ('Oman', 'OM'),
    ('Morocco', 'MA'),
    ('Russian Federation', 'RU'),
    ('Jordan', 'JO'),
    ('Mauritius', 'MU'),
    ('Honduras', 'HN'),
    ('Myanmar', 'MM'),
    ('United States', 'US'),
    ('China', 'CN'),
    ('Portugal', 'PT'),
    ('Nigeria', 'NG'),
    ('Saudi Arabia', 'SA'),
    ('Egypt', 'EG'),
    ('Indonesia', 'ID'),
    ('Angola', 'AO'),
    ('Korea, Republic Of', 'KR'),
    ('Kazakhstan', 'KZ'),
    ('Costa Rica', 'CR'),
    ('Ethiopia', 'ET'),
    ('Botswana', 'BW'),
    ('Venezuela, Bolivarian Republic Of', 'VE'),
    ('Senegal', 'SN'),
    ('Trinidad & Tobago', 'TT'),
    ('Hungary', 'HU'),
    ('Sri Lanka', 'LK'),
    ('United Arab Emirates', 'AE'),
    ('Luxembourg', 'LU'),
    ('Italy', 'IT'),
    ('Sweden', 'SE'),
    ('Singapore', 'SG'),
    ('Spain', 'ES'),
    ('Puerto Rico', 'PR'),
    ('Hong Kong', 'HK'),
    ('Bermuda', 'BM'),
    ('Argentina', 'AR'),
    ('Malta', 'MT'),
    ('Virgin Islands, British', 'VG'),
    ('Denmark', 'DK'),
    ('El Salvador', 'SV'),
    ('Bahamas', 'BS'),
    ('Monaco', 'MC'),
    ('India', 'IN'),
    ('Panama', 'PA'),
    ('Bahrain', 'BH'),
    ('Israel', 'IL'),
    ('Australia', 'AU'),
    ('Canada', 'CA'),
    ('South Africa', 'ZA'),
    ('Belgium', 'BE'),
    ('France', 'FR'),
    ('Dominican Republic', 'DO'),
    ('Latvia', 'LV'),
    ('Uzbekistan', 'UZ'),
    ('Ecuador', 'EC'),
    ('Bosnia & Herzegovina', 'BA'),
    ('Bolivia, Plurinational State Of', 'BO'),
    ('Palestine, State of', 'PS'),
    ('Croatia', 'HR'),
    ('Estonia', 'EE'),
    ('Bulgaria', 'BG'),
    ('Slovenia', 'SI'),
    ('Sierra Leone', 'SL'),
    ('Zimbabwe', 'ZW'),
    ('North Macedonia, Republic of', 'MK'),
    ('Mongolia', 'MN'),
    ('Greenland', 'GL'),
    ('United States Minor Outlying Islands', 'UM'),
    ('Mozambique', 'MZ'),
    ('American Samoa', 'AS'),
    ('Suriname', 'SR'),
    ('Congo, The Democratic Republic Of The', 'CD'),
    ('Liechtenstein', 'LI'),
    ('Nicaragua', 'NI'),
    ('Benin', 'BJ'),
    ('Algeria', 'DZ')
),

coupa_invoice_header_final AS (
SELECT
  ih.INVOICE_HEADER_ID,
  ih.CREATED_AT,
  ih.UPDATED_AT,
  ih.STATUS,
  ih.INVOICE_DATE,
  ih.DELIVERY_DATE,
  ih.INVOICE_NUMBER,
  ih.INVOICE_HEADER_TAGS,
  IFF(ih.LINE_LEVEL_TAXATION = 1, 'true', 'false') AS LINE_LEVEL_TAXATION,
  ZEROIFNULL(it.TAX_AMOUNT) AS TAX_AMOUNT,
  ih.LINE_SUBTOTAL
    + ZEROIFNULL(ic.MISC_AMOUNT)
    + ZEROIFNULL(ic.HANDLING_AMOUNT)
    + ZEROIFNULL(ic.SHIPPING_AMOUNT) AS TAXABLE_AMOUNT,
  it.TAX_CODE,
  ZEROIFNULL(wt.WITHHOLDING_TAX_AMOUNT) AS WITHHOLDING_TAX_AMOUNT,
  IFF(
    LOWER(ih.STATUS) = 'voided',
    'false',
    IFF(ih.PAID = 1, 'true', 'false')
  ) AS PAID,
  ih.PAYMENT_DATE,
  ih.PAYMENT_NOTES,
  CASE
    WHEN iuc.UI_ACCOUNT_TYPE_ID IS NOT NULL
     AND NULLIF(TRIM(at_ui.NAME), '') IS NOT NULL
    THEN iuc.UI_ACCOUNT_TYPE_ID::VARCHAR
    ELSE ia.ACCOUNT_TYPE_ID
  END AS ACCOUNT_TYPE_ID,
  CASE
    WHEN iuc.UI_ACCOUNT_TYPE_ID IS NOT NULL
     AND NULLIF(TRIM(at_ui.NAME), '') IS NOT NULL
    THEN at_ui.NAME
    ELSE ia.ACCOUNT_TYPE_NAME
  END AS ACCOUNT_TYPE_NAME,
  ih.PAYMENT_CODE,
  ZEROIFNULL(ic.HANDLING_AMOUNT) AS HANDLING_AMOUNT,
  ZEROIFNULL(ic.MISC_AMOUNT) AS MISC_AMOUNT,
  ZEROIFNULL(ic.SHIPPING_AMOUNT) AS SHIPPING_AMOUNT,
  ZEROIFNULL(ih.TOTAL_WITH_TAXES_AMOUNT) AS TOTAL_WITH_TAXES_AMOUNT,
  ih.CURRENCY_CODE,
  ZEROIFNULL(ih.INVOICE_ACCOUNTING_TOTAL) AS INVOICE_ACCOUNTING_TOTAL,
  ih.CURRENCY_CODE AS ACCOUNTING_TOTAL_CURRENCY,
  ih.SUPPLIER_ID,
  TRIM(REGEXP_REPLACE(
    TRIM(COALESCE(
      NULLIF(supplier_view.NAME, ''),
      TRIM(ih.SUPPLIER_NAME)
    )),
    '^ARCHIVE\\s+',
    '',
    1, 0, 'i'
  )) AS SUPPLIER_NAME,
  supplier_view.SUPPLIER_NUMBER,
  created_by_user.EMAIL AS CREATED_BY_EMAIL,
  created_by_user.EMPLOYEE_NUMBER AS CREATED_BY_EMPLOYEE_NUMBER,
  created_by_user.LOGIN AS CREATED_BY_LOGIN,
  CASE
    WHEN updated_by_user.EMAIL = 'aniket_bhoi@external.mckinsey.stage2.com'
    THEN 'sftp_integration@coupa.com'
    ELSE updated_by_user.EMAIL
  END AS UPDATED_BY_EMAIL,
  updated_by_user.EMPLOYEE_NUMBER AS UPDATED_BY_EMPLOYEE_NUMBER,
  CASE
    WHEN updated_by_user.EMAIL = 'aniket_bhoi@external.mckinsey.stage2.com'
    THEN 'sftp_integration@coupa.com'
    ELSE updated_by_user.LOGIN
  END AS UPDATED_BY_LOGIN,
  remit_to_address.REMIT_TO_CODE,
  remit_to_address.ATTENTION AS REMIT_TO_ADDRESS_ATTENTION,
  remit_to_address.STREET1 AS REMIT_TO_ADDRESS1,
  remit_to_address.STREET2 AS REMIT_TO_ADDRESS2,
  remit_to_address.CITY AS REMIT_TO_ADDRESS_CITY,
  remit_to_address.STATE AS REMIT_TO_ADDRESS_STATE,
  remit_to_address.POSTAL_CODE AS REMIT_TO_ADDRESS_POSTAL_CODE,
  remit_to_address.COUNTRY_CODE AS REMIT_TO_ADDRESS_COUNTRY_CODE,
  remit_to_address.COUNTRY AS REMIT_TO_ADDRESS_COUNTRY_NAME,
  remit_to_address.NAME AS REMIT_TO_ADDRESS_NAME,
  REPLACE(supplier_tax_registration.TAX_REGISTRATION_NUMBER, '-', '') AS SUPPLIER_VAT_NUMBER,
  supplier_tax_registration.COUNTRY_NAME AS SUPPLIER_VAT_COUNTRY_NAME,
  country_code_mapping.SUPPLIER_VAT_COUNTRY_CODE,
  ship_to_address.ATTENTION AS SHIP_TO_ADDRESS_ATTENTION,
  ship_to_address.STREET1 AS SHIP_TO_ADDRESS1,
  ship_to_address.STREET2 AS SHIP_TO_ADDRESS2,
  ship_to_address.CITY AS SHIP_TO_ADDRESS_CITY,
  ship_to_address.STATE AS SHIP_TO_ADDRESS_STATE,
  ship_to_address.POSTAL_CODE AS SHIP_TO_ADDRESS_POSTAL_CODE,
  ship_to_address.COUNTRY_CODE AS SHIP_TO_ADDRESS_COUNTRY_CODE,
  ship_to_address.COUNTRY AS SHIP_TO_ADDRESS_COUNTRY_NAME,
  ship_to_address.NAME AS SHIP_TO_ADDRESS_NAME,
  supplier_remit_to.REMIT_TO_CODE AS SUPPLIER_REMIT_TO_CODE,
  supplier_remit_to.NAME AS SUPPLIER_REMIT_TO_NAME,
  supplier_remit_to.STREET1 AS SUPPLIER_REMIT_TO_STREET1,
  supplier_remit_to.STREET2 AS SUPPLIER_REMIT_TO_STREET2,
  supplier_remit_to.CITY AS SUPPLIER_REMIT_TO_CITY,
  supplier_remit_to.STATE AS SUPPLIER_REMIT_TO_STATE,
  supplier_remit_to.POSTAL_CODE AS SUPPLIER_REMIT_TO_POSTAL_CODE,
  supplier_remit_to.COUNTRY AS SUPPLIER_REMIT_TO_COUNTRY_NAME,
  supplier_remit_to.SUPPLIER_REMIT_TO_MATCH_METHOD,
  ih.DISCOUNT_PAYMENT_DUE_DATE,
  IFNULL(
    TO_CHAR(
      ROUND(
        COALESCE(
          TRY_TO_DOUBLE(NULLIF(TRIM(ih.INVOICE_HEADER_CUSTOM_FIELD_11), '')),
          ih.TOTAL_WITH_TAXES_AMOUNT
        ) * ZEROIFNULL(pt.DISCOUNT_RATE) / 100,
      2),
      'FM999999990.00'
    ),
    ''
  ) AS DISCOUNT_AMOUNT,
  CASE
    WHEN ih.DOCUMENT_TYPE = 'InvoiceHeader' OR ih.DOCUMENT_TYPE IS NULL
    THEN 'Invoice'
    ELSE ih.DOCUMENT_TYPE
  END AS DOCUMENT_TYPE,
  ih.ORIGINAL_INVOICE_NUMBER,
  ih.ORIGINAL_INVOICE_DATE,
  it.EXCHANGE_RATE,
  ih.DOCUMENT_CHANNEL,
  ship_from_address.NAME AS SHIP_FROM_ADDRESS_NAME,
  ship_from_address.STREET1 AS SHIP_FROM_ADDRESS_STREE1,
  ship_from_address.STREET2 AS SHIP_FROM_ADDRESS_STREE2,
  ship_from_address.CITY AS SHIP_FROM_ADDRESS_CITY,
  ship_from_address.STATE AS SHIP_FROM_ADDRESS_STATE,
  ship_from_address.POSTAL_CODE AS SHIP_FROM_ADDRESS_POSTAL_CODE,
  ship_from_address.COUNTRY_NAME AS SHIP_FROM_ADDRESS_COUNTRY,
  TRIM(REGEXP_REPLACE(
    TRIM(COALESCE(
      NULLIF(supplier_view.NAME, ''),
      TRIM(ih.SUPPLIER_NAME)
    )),
    '^ARCHIVE\\s+',
    '',
    1, 0, 'i'
  )) AS SHIP_FROM_SUPPLIER_NAME,
  invoice_inbox.INBOUND_INVOICE_RECEIVED_AT,
  invoice_inbox.INBOUND_INVOICE_SENDER_EMAIL,
  invoice_inbox.INVOICE_INBOX_NAME,
  ih.PAYMENT_CHANNEL,
  ih.DOCUMENT_FORMAT,
  ih.INVOICE_HEADER_CUSTOM_FIELD_1 AS PO,
  ih.INVOICE_HEADER_CUSTOM_FIELD_2 AS PLEASE_ENTER_YOUR_NAME,
  ih.INVOICE_HEADER_CUSTOM_FIELD_6 AS BARCODE,
  ih.INVOICE_HEADER_CUSTOM_FIELD_7 AS DESCRIPTION,
  ih.INVOICE_HEADER_CUSTOM_FIELD_8 AS SAP_INVOICE_STATUS,
  IFF(ih.INVOICE_HEADER_CUSTOM_FIELD_13 = 1, 'true', 'false') AS TPI_SUPPLIER,
  IFF(ih.INVOICE_HEADER_CUSTOM_FIELD_5 = 1, 'true', 'false') AS SYSTEM_CALCULATED_TAX_TOLERANCE_FAILURE,
  ih.INVOICE_HEADER_CUSTOM_FIELD_3 AS SYSTEM_CALCULATED_HEADER_TAX,
  ih.INVOICE_HEADER_CUSTOM_FIELD_19 AS REQUESTOR_ACKNOWLEDGEMENT,
  ih.INVOICE_HEADER_CUSTOM_FIELD_16 AS QUERY_REASON,
  ih.INVOICE_HEADER_CUSTOM_FIELD_20 AS OCR_FINDINGS,
  ih.INVOICE_HEADER_CUSTOM_FIELD_11 AS INVOICE_TOTAL,
  IFF(ih.INVOICE_HEADER_CUSTOM_FIELD_14 = 1, 'TRUE', 'FALSE') AS EMERGENCY,
  ih.INVOICE_HEADER_CUSTOM_FIELD_15 AS CHARGE_ID,
  REGEXP_SUBSTR(ih.INVOICE_HEADER_CUSTOM_FIELD_18, '\\(([^)]+)\\)', 1, 1, 'e', 1) AS APPROVER_FMNO,
  ih.BUYER_TAX_REGISTRATION_NUMBER,
  ih.BUYER_TAX_REG_COUNTRY_REGION AS BUYER_TAX_REGISTRATION_COUNTRY_NAME,
  ih.REQUESTED_BY_NAME AS REQUESTER,
  ih.INVOICE_HEADER_CUSTOM_FIELD_12 AS APPROVER_CHARGE_CODE
FROM invoice_header ih
LEFT JOIN invoice_accounts ia
  ON ih.INVOICE_HEADER_ID = ia.INVOICE_HEADER_ID
LEFT JOIN invoice_ui_coa iuc
  ON ih.INVOICE_HEADER_ID = iuc.INVOICE_HEADER_ID
LEFT JOIN __COUPA_DB_SCHEMA__ACCOUNT_TYPES_VIEW at_ui
  ON iuc.UI_ACCOUNT_TYPE_ID = at_ui.ID
LEFT JOIN invoice_charges ic
  ON ih.INVOICE_HEADER_ID = ic.INVOICE_HEADER_ID
LEFT JOIN invoice_taxes it
  ON ih.INVOICE_HEADER_ID = it.INVOICE_HEADER_ID
LEFT JOIN withholding_taxes wt
  ON ih.INVOICE_HEADER_ID = wt.INVOICE_HEADER_ID
LEFT JOIN supplier_view
  ON ih.SUPPLIER_ID = supplier_view.ID
LEFT JOIN created_by_user
  ON ih.CREATED_BY = created_by_user.ID
LEFT JOIN updated_by_user
  ON ih.UPDATED_BY = updated_by_user.ID
LEFT JOIN remit_to_address
  ON ih.REMIT_TO_ADDRESS_ID = remit_to_address.ID
LEFT JOIN supplier_tax_registration
  ON ih.SUPPLIER_ID = supplier_tax_registration.SUPPLIER_ID
LEFT JOIN country_code_mapping
  ON supplier_tax_registration.COUNTRY_NAME = country_code_mapping.COUNTRY_NAME
LEFT JOIN ship_to_address
  ON ih.SHIP_TO_ADDRESS_ID = ship_to_address.ID
LEFT JOIN supplier_remit_to
  ON ih.INVOICE_HEADER_ID = supplier_remit_to.INVOICE_HEADER_ID
LEFT JOIN ship_from_address
  ON ih.SHIP_FROM_ADDRESS_ID = ship_from_address.ID
LEFT JOIN invoice_inbox
  ON ih.INBOUND_INVOICE_ID = invoice_inbox.INBOUND_INVOICE_ID
LEFT JOIN payment_terms pt
  ON ih.INVOICE_HEADER_ID = pt.INVOICE_HEADER_ID
)
""".strip()

_COUPA_QUAL_PREFIX = f"{sf_database}.{sf_schema}."
COUPA_INVOICE_HEADER_CTE_SQL = _COUPA_INVOICE_HEADER_CTE_TEMPLATE.replace(
    "__COUPA_DB_SCHEMA__", _COUPA_QUAL_PREFIX
)
FINAL_CTE_NAME = "coupa_invoice_header_final"


def _changed_invoice_predicate(changed_since: str, last_max_id=None) -> str:
    """SQL predicate on alias `iv` selecting invoices touched since the watermark.

    An invoice counts as changed if ANY of its lines has UPDATED_AT after
    (watermark - lookback). `ID > last_max_id` is a belt-and-braces catch for
    brand-new invoices whose UPDATED_AT might be NULL or skewed.
    """
    since = (
        f"DATEADD(hour, -{int(INCREMENTAL_LOOKBACK_HOURS)}, "
        f"TO_TIMESTAMP_NTZ('{changed_since}'))"
    )
    id_clause = f" OR chg.INVOICE_HEADER_ID > {int(last_max_id)}" if last_max_id else ""
    return (
        "iv.INVOICE_HEADER_ID IN ("
        f"SELECT chg.INVOICE_HEADER_ID FROM {_COUPA_QUAL_PREFIX}INVOICES_VIEW chg "
        f"WHERE chg.UPDATED_AT > {since}{id_clause})"
    )


def _ids_predicate(ids) -> str:
    """SQL predicate on alias `iv` selecting an explicit list of invoice IDs."""
    return "iv.INVOICE_HEADER_ID IN (" + ",".join(str(int(x)) for x in ids) + ")"


def build_sf_query(
    projection: str = "*",
    dedup: bool = False,
    changed_since: str = None,
    last_max_id=None,
    id_filter=None,
) -> str:
    """Attach a projection / optional dedup to the base CTE query.

    id_filter=[...]     -> only these invoice IDs (test mode; takes precedence)
    changed_since=None  -> full extract (filter placeholders removed)
    changed_since=<ts>  -> only invoices changed since <ts> minus the lookback
    """
    sql = COUPA_INVOICE_HEADER_CTE_SQL
    if id_filter:
        pred = _ids_predicate(id_filter)
    elif changed_since is not None:
        pred = _changed_invoice_predicate(changed_since, last_max_id)
    else:
        pred = None
    if pred is None:
        sql = sql.replace("__IH_FILTER_WHERE__", "").replace("__IH_FILTER_AND__", "")
    else:
        sql = (
            sql.replace("__IH_FILTER_WHERE__", f"WHERE {pred}")
               .replace("__IH_FILTER_AND__", f"AND {pred}")
        )
    qualify = DEDUP_QUALIFY if dedup else ""
    return (
        f"{sql}\n"
        f"SELECT {projection}\n"
        f"FROM {FINAL_CTE_NAME}\n"
        f"{qualify}"
    ).strip()


PDM_INVOICE_D_HANA_COLUMN_ORDER = ( "ID","CREATED_AT","UPDATED_AT","STATUS","INTERNAL_NOTE","INVOICE_DATE","DELIVERY_DATE","INVOICE_NUMBER","LINE_LEVEL_TAXATION","SUPPLIER_NOTE","HEADER_TAX_AMOUNT","HEADER_TAX_RATE","HEADER_TAX_CODE","PAID","PAYMENT_DATE","PAYMENT_NOTES","EXPORTED","ACCOUNT_TYPE_ID","ACCOUNT_TYPE_NAME","PAYMENT_TERM","HANDLING_AMOUNT","MISC_AMOUNT","SHIPPING_AMOUNT", "TOTAL","CURRENCY","ACCOUNTING_TOTAL_AMOUNT","ACCOUNTING_TOTAL_CURRENCY","SUPPLIER_ID","SUPPLIER_NAME","SUPPLIER_NUMBER","CREATED_BY_EMAIL",
    "CREATED_BY_EMPLOYEE_NUMBER","CREATED_BY_LOGIN","UPDATED_BY_EMAIL","UPDATED_BY_EMPLOYEE_NUMBER","UPDATED_BY_LOGIN","ATTACHMENT_TEXTS","COMMENTS","REMIT_TO_ADDRESS_CODE","REMIT_TO_ADDRESS_ATTENTION","REMIT_TO_ADDRESS_STREET1","REMIT_TO_ADDRESS_STREET2",
    "REMIT_TO_ADDRESS_CITY","REMIT_TO_ADDRESS_STATE","REMIT_TO_ADDRESS_POSTAL_CODE","REMIT_TO_ADDRESS_COUNTRY_CODE","REMIT_TO_ADDRESS_COUNTRY_NAME","REMIT_TO_ADDRESS_NAME","REMIT_TO_ADDRESS_VAT_NUMBER","REMIT_TO_ADDRESS_VAT_COUNTRY_CODE",
    "REMIT_TO_ADDRESS_VAT_COUNTRY_NAME","SUPPLIER_VAT_NUMBER","SUPPLIER_VAT_COUNTRY_CODE","DOCUMENT_TYPE","ORIGINAL_INVOICE_NUMBER","ORIGINAL_INVOICE_DATE","CANCELED","BILL_TO_ADDRESS_LEGAL_ENTITY_NAME","BILL_TO_ADDRESS_ATTENTION","BILL_TO_ADDRESS_STREET1",
    "BILL_TO_ADDRESS_STREET2","BILL_TO_ADDRESS_CITY","BILL_TO_ADDRESS_STATE","BILL_TO_ADDRESS_POSTAL_CODE","BILL_TO_ADDRESS_COUNTRY_CODE","BILL_TO_ADDRESS_COUNTRY_NAME","BILL_TO_ADDRESS_NAME","SHIP_TO_ADDRESS_ATTENTION","SHIP_TO_ADDRESS_STREET1",
    "SHIP_TO_ADDRESS_STREET2","SHIP_TO_ADDRESS_CITY","SHIP_TO_ADDRESS_STATE","SHIP_TO_ADDRESS_POSTAL_CODE","SHIP_TO_ADDRESS_COUNTRY_CODE","SHIP_TO_ADDRESS_COUNTRY_NAME","SHIP_TO_ADDRESS_NAME","SUPPLIER_REMIT_TO_CODE","SUPPLIER_REMIT_TO_SUPPLIER_NAME",
    "SUPPLIER_REMIT_TO_STREET1","SUPPLIER_REMIT_TO_STREET2","SUPPLIER_REMIT_TO_CITY","SUPPLIER_REMIT_TO_STATE","SUPPLIER_REMIT_TO_POSTAL_CODE","SUPPLIER_REMIT_TO_COUNTRY_CODE","SUPPLIER_REMIT_TO_COUNTRY_NAME","FISCAL_REP_NAME","FISCAL_REP_VAT_ID",
    "FISCAL_REP_ADDRESS_CODE","FISCAL_REP_ADDRESS_STREET_1","FISCAL_REP_ADDRESS_STREET_2","FISCAL_REP_ADDRESS_CITY","FISCAL_REP_ADDRESS_STATE","FISCAL_REP_ADDRESS_POSTAL_CODE","FISCAL_REP_ADDRESS_COUNTRY_CODE","DISCOUNT_DUE_DATE","NET_DUE_DATE","DISCOUNT_AMOUNT",
    "LATE_PAYMENT_PENALTIES","MARGIN_SCHEME","CASH_ACCOUNTING_SCHEME_REFERENCE","EXCHANGE_RATE","EARLY_PAYMENT_PROVISIONS","PRE_PAYMENT_DATE","REVERSE_CHARGE_REFERENCE","DISCOUNT_PERCENT","CREDIT_NOTE_DIFFERENCES_WITH_ORIGINAL_INVOICE","CUSTOMS_DECLARATION_NUMBER",
    "CUSTOMS_OFFICE","CUSTOMS_DECLARATION_DATE","PAYMENT_ORDER_REFERENCE","PAYMENT_ORDER_NUMBER","ADVANCE_PAYMENT_RECEIVED_AMOUNT","SERIES","FOLIO_NUMBER","USE_OF_INVOICE","FORM_OF_PAYMENT","TYPE_OF_RECEIPT","PAYMENT_METHOD","ISSUANCE_PLACE","CONFIRMATION",
    "WITHHOLDING_TAX_OVERRIDE","TYPE_OF_RELATIONSHIP","INVOICE_REFERENCE_NUMBER","GROSS_TOTAL","REGISTERED_COMPANY_REGISTRATION_NUMBER","REGISTERED_COMPANY_PLACE_OF_REGISTRATION","REGISTERED_COMPANY_TYPE","REGISTERED_COMPANY_MANAGING_DIRECTORS",
    "CREDIT_REASON","REGISTERED_COMPANY_PERMIT_NUMBER","REGISTERED_COMPANY_PERMIT_DATE","REGISTERED_COMPANY_LIQUIDATOR_NAME","REGISTERED_COMPANY_SHARE_CAPITAL","TAX_NATURE","REGISTERED_COMPANY_UNIQUE_SHAREHOLDER","REGISTERED_COMPANY_LIABLE_COMPANY",
    "REGISTERED_COMPANY_TAX_REGIME","REGISTERED_COMPANY_LEGAL_STATUS","QST_REGISTRATION_NUMBER","AUTHORISED_PERSON","AUTHORISED_PERSON_ADDRESS","AUTHORISED_PERSON_VAT_ID","DATE_OF_REGISTRATION","SOLE_REGISTRATION_CODE","ORIGIN_CURRENCY_NET",
    "TAXES_IN_ORIGIN_COUNTRY_CURRENCY","ORIGIN_CURRENCY_GROSS","SELF_BILLING_REFERENCE","REGISTERED_COMPANY_LEGAL_TYPE","REGISTERED_COMPANY_REGISTERED_SEAT","REGISTERED_COMPANY_CHAIRMAN_OF_THE_BOARD","REGISTERED_COMPANY_COURT_OF_REGISTRATION",
    "REGISTERED_COMPANY_LIQUIDATION_REMARK","REGISTERED_COMPANY_COMMERCIAL_REGISTER_NUMBER","REGISTERED_COMPANY_LICENSE_NUMBER","REGISTERED_COMPANY_REGISTER_LEGAL_ENTITIES","REGISTERED_COMPANY_NAME","COMPANY_IN_EXAMINERSHIP","COMPANY_IS_BEING_WOUND_UP",
    "COMPANY_RECEIVER_APPOINTED","PREFERENCE_AGREEMENT","FILE_REFERENCE_NUMBER","NUMBER_OF_ISSUED_STOCKS","ISSUED_SHARE_CAPITAL","SUPPLIER_TELEPHONE_NUMBER","PERMANENT_ACCOUNT_NUMBER","DISPUTED_INVOICE_NUMBER","CHANNEL","TAXABLE_AMOUNT","INVOICE_FROM_CODE",
    "INVOICE_FROM_SUPPLIER_NAME","INVOICE_FROM_STREET1","INVOICE_FROM_STREET2","INVOICE_FROM_CITY","INVOICE_FROM_STATE","INVOICE_FROM_POSTAL_CODE","INVOICE_FROM_COUNTRY_CODE","INVOICE_FROM_COUNTRY_NAME","SHIP_FROM_CODE","SHIP_FROM_SUPPLIER_NAME","SHIP_FROM_STREET1",
    "SHIP_FROM_STREET2","SHIP_FROM_CITY","SHIP_FROM_STATE","SHIP_FROM_POSTAL_CODE","SHIP_FROM_COUNTRY_CODE","SHIP_FROM_COUNTRY_NAME","SUPPLIER_TAX_REGISTRATION_NUMBER","SUPPLIER_TAX_REGISTRATION_COUNTRY_CODE","SUPPLIER_TAX_REGISTRATION_COUNTRY_NAME",
    "SUPPLIER_TAX_REGISTRATION_LOCAL","BUYER_TAX_REGISTRATION_NUMBER","BUYER_TAX_REGISTRATION_COUNTRY_CODE","BUYER_TAX_REGISTRATION_COUNTRY_NAME","BUYER_TAX_REGISTRATION_LOCAL","DISPUTE_METHOD","DATE_RECEIVED","SENDER_EMAIL","INBOX_NAME","AMOUNT_DUE",
    "TAX_DUE_TO_SUPPLIER","CUSTOMER_ACCOUNTING_TAX","PAYMENT_CHANNEL","DATE_OF_DISCOVERY_OF_FACTS_DECISIVE_FOR_CORRECTION","PLACE_OF_SUPPLY","SPLIT_PAYMENT_MECHANISM","GROSS_TOTAL_LESS_DISCOUNT","NET_TOTAL_LESS_DISCOUNT","TOTAL_TAXES_LESS_DISCOUNT",
    "CUSTOMER_ACCOUNTING_TAX_LESS_DISCOUNT","AMOUNT_DUE_LESS_DISCOUNT","ENDORSEMENT_ON_INVOICES","NEW_MEANS_OF_TRANSPORT","PLACE_OF_ISSUANCE","AMOUNT_OF_ADVANCE_PAYMENT","TRANSACTION_UUID","TRANSACTION_NOTIFICATION_DATE","SUPPLIER_INVOICE_ISSUER_NAME",
    "SUPPLIER_INVOICE_REVIEWER_NAME","SUPPLIER_PAYMENT_COLLECTOR_NAME","INVOICE_ISSUANCE_TIME","CASH_REGISTER_OPERATOR_ID","MEANS_OF_PAYMENT","UNIQUE_IDENTIFICATION_CODE_OF_CASH_RECEIPT","SECURITY_CODE_OF_ISSUER","STATE_TAX_NUMBER",
    "STATE_TAX_NUMBER_FOR_SUBSTITUTE_TAXPAYER","MUNICIPAL_TAX_NUMBER","SERIAL_CODE_OF_FISCAL_INVOICE","VERIFICATION_CODE","TYPE_OF_DOCUMENT","PROTOCOL_NUMBER","NATURE_OF_OPERATION","TYPE_OF_OPERATION","FREIGHT_TYPE","VEHICLE_LICENSE_PLATE",
    "NATIONAL_ENROLLMENT_OF_CONVEYOR","VOLUME_AMOUNT","VOLUME_GROSS_WEIGHT","VOLUME_LIQUID_WEIGHT","VOLUME_BRAND","VOLUME_TYPE","VOLUME_NUMBERING","SUPPLIER_SITE_ID","SUPPLIER_SITE_NAME","SUPPLIER_SITE_CODE","PAYMENT_AGREEMENT_NOTES","PARENT_INVOICE_HEADER_ID",
    "SOURCE_FILE_NAME","H_CREATED_DATE","H_UPDATED_DATE","H_CREATED_BY","H_UPDATED_BY","ESR_NUMBER","PO","PLEASE_ENTER_YOUR_NAME","BARCODE","DESCRIPTION","SAP_INVOICE_STATUS","TPI_SUPPLIER","TAG_DESCRIPTION","TAG","SYSTEM_CALCULATED_TAX_TOLERANCE_FAILURE","SYSTEM_CALCULATED_HEADER_TAX","REQUESTOR_ACKNOWLEDGEMENT","QUERY_REASON","PAY_GROUP","OCR_FINDINGS","INVOICE_TOTAL","IMAGENOW_ID","FATCA","EXPORTED_TO_AMEX",
    # NOTE: the HANA emergency column is lowercase ("emergency", NVARCHAR(255)); the name is case-sensitive.
    # ED_FOR_APPROVALS is a separate column and is not mapped (loads as NULL).
    "emergency","ED_FOR_APPROVALS","CHARGE_ID","APPROVER_FMNO","APPROVER_CHARGE_CODE",)

PDM_INVOICE_D_HANA_TO_SNOWFLAKE_COLUMN = { "ID": "INVOICE_HEADER_ID","CREATED_AT": "CREATED_AT","UPDATED_AT": "UPDATED_AT","STATUS": "STATUS","INVOICE_DATE": "INVOICE_DATE",
    "DELIVERY_DATE": "DELIVERY_DATE","INVOICE_NUMBER": "INVOICE_NUMBER","LINE_LEVEL_TAXATION": "LINE_LEVEL_TAXATION","HEADER_TAX_AMOUNT": "TAX_AMOUNT",
    # NEW: the query now aggregates tax codes per header, so this is no longer NULL.
    "HEADER_TAX_CODE": "TAX_CODE",
    # CHANGED: TAXABLE_AMOUNT used to be aliased onto INVOICE_ACCOUNTING_TOTAL.
    # The query now computes line subtotal + misc + handling + shipping.
    "TAXABLE_AMOUNT": "TAXABLE_AMOUNT",
    "PAID": "PAID","PAYMENT_DATE": "PAYMENT_DATE","PAYMENT_NOTES": "PAYMENT_NOTES","ACCOUNT_TYPE_ID": "ACCOUNT_TYPE_ID","ACCOUNT_TYPE_NAME": "ACCOUNT_TYPE_NAME","PAYMENT_TERM": "PAYMENT_CODE","HANDLING_AMOUNT": "HANDLING_AMOUNT","MISC_AMOUNT": "MISC_AMOUNT","SHIPPING_AMOUNT": "SHIPPING_AMOUNT","TOTAL": "TOTAL_WITH_TAXES_AMOUNT","CURRENCY": "CURRENCY_CODE","ACCOUNTING_TOTAL_AMOUNT": "INVOICE_ACCOUNTING_TOTAL",
    "ACCOUNTING_TOTAL_CURRENCY": "ACCOUNTING_TOTAL_CURRENCY","SUPPLIER_ID": "SUPPLIER_ID","SUPPLIER_NAME": "SUPPLIER_NAME","SUPPLIER_NUMBER": "SUPPLIER_NUMBER","CREATED_BY_EMAIL": "CREATED_BY_EMAIL","CREATED_BY_EMPLOYEE_NUMBER": "CREATED_BY_EMPLOYEE_NUMBER",
    "CREATED_BY_LOGIN": "CREATED_BY_LOGIN","UPDATED_BY_EMAIL": "UPDATED_BY_EMAIL","UPDATED_BY_EMPLOYEE_NUMBER": "UPDATED_BY_EMPLOYEE_NUMBER","UPDATED_BY_LOGIN": "UPDATED_BY_LOGIN","REMIT_TO_ADDRESS_CODE": "REMIT_TO_CODE",
    "REMIT_TO_ADDRESS_ATTENTION": "REMIT_TO_ADDRESS_ATTENTION","REMIT_TO_ADDRESS_STREET1": "REMIT_TO_ADDRESS1","REMIT_TO_ADDRESS_STREET2": "REMIT_TO_ADDRESS2","REMIT_TO_ADDRESS_CITY": "REMIT_TO_ADDRESS_CITY","REMIT_TO_ADDRESS_STATE": "REMIT_TO_ADDRESS_STATE",
    "REMIT_TO_ADDRESS_POSTAL_CODE": "REMIT_TO_ADDRESS_POSTAL_CODE","REMIT_TO_ADDRESS_COUNTRY_CODE": "REMIT_TO_ADDRESS_COUNTRY_CODE","REMIT_TO_ADDRESS_COUNTRY_NAME": "REMIT_TO_ADDRESS_COUNTRY_NAME","REMIT_TO_ADDRESS_NAME": "REMIT_TO_ADDRESS_NAME",
    "SUPPLIER_VAT_NUMBER": "SUPPLIER_VAT_NUMBER",
    # NEW: country code now derived from the country_code_mapping CTE.
    "SUPPLIER_VAT_COUNTRY_CODE": "SUPPLIER_VAT_COUNTRY_CODE",
    "DOCUMENT_TYPE": "DOCUMENT_TYPE","ORIGINAL_INVOICE_NUMBER": "ORIGINAL_INVOICE_NUMBER","ORIGINAL_INVOICE_DATE": "ORIGINAL_INVOICE_DATE","SHIP_TO_ADDRESS_ATTENTION": "SHIP_TO_ADDRESS_ATTENTION",
    "SHIP_TO_ADDRESS_STREET1": "SHIP_TO_ADDRESS1","SHIP_TO_ADDRESS_STREET2": "SHIP_TO_ADDRESS2","SHIP_TO_ADDRESS_CITY": "SHIP_TO_ADDRESS_CITY","SHIP_TO_ADDRESS_STATE": "SHIP_TO_ADDRESS_STATE","SHIP_TO_ADDRESS_POSTAL_CODE": "SHIP_TO_ADDRESS_POSTAL_CODE",
    "SHIP_TO_ADDRESS_COUNTRY_CODE": "SHIP_TO_ADDRESS_COUNTRY_CODE","SHIP_TO_ADDRESS_COUNTRY_NAME": "SHIP_TO_ADDRESS_COUNTRY_NAME","SHIP_TO_ADDRESS_NAME": "SHIP_TO_ADDRESS_NAME","SUPPLIER_REMIT_TO_CODE": "SUPPLIER_REMIT_TO_CODE",
    "SUPPLIER_REMIT_TO_SUPPLIER_NAME": "SUPPLIER_REMIT_TO_NAME","SUPPLIER_REMIT_TO_STREET1": "SUPPLIER_REMIT_TO_STREET1","SUPPLIER_REMIT_TO_STREET2": "SUPPLIER_REMIT_TO_STREET2","SUPPLIER_REMIT_TO_CITY": "SUPPLIER_REMIT_TO_CITY",
    "SUPPLIER_REMIT_TO_STATE": "SUPPLIER_REMIT_TO_STATE","SUPPLIER_REMIT_TO_POSTAL_CODE": "SUPPLIER_REMIT_TO_POSTAL_CODE","SUPPLIER_REMIT_TO_COUNTRY_NAME": "SUPPLIER_REMIT_TO_COUNTRY_NAME",
    "DISCOUNT_DUE_DATE": "DISCOUNT_PAYMENT_DUE_DATE","DISCOUNT_AMOUNT": "DISCOUNT_AMOUNT","EXCHANGE_RATE": "EXCHANGE_RATE","CHANNEL": "DOCUMENT_CHANNEL",
    # REMOVED: SHIP_FROM_CODE -> SHIP_FROM_LOCATION_CODE. The new query sources
    # ship-from from SUPPLIER_REMIT_TOS_VIEW, which has no LOCATION_CODE.
    "SHIP_FROM_SUPPLIER_NAME": "SHIP_FROM_SUPPLIER_NAME","SHIP_FROM_STREET1": "SHIP_FROM_ADDRESS_STREE1","SHIP_FROM_STREET2": "SHIP_FROM_ADDRESS_STREE2","SHIP_FROM_CITY": "SHIP_FROM_ADDRESS_CITY","SHIP_FROM_STATE": "SHIP_FROM_ADDRESS_STATE",
    "SHIP_FROM_POSTAL_CODE": "SHIP_FROM_ADDRESS_POSTAL_CODE","SHIP_FROM_COUNTRY_NAME": "SHIP_FROM_ADDRESS_COUNTRY","SUPPLIER_TAX_REGISTRATION_NUMBER": "SUPPLIER_VAT_NUMBER",
    "SUPPLIER_TAX_REGISTRATION_COUNTRY_NAME": "SUPPLIER_VAT_COUNTRY_NAME",
    # NEW
    "SUPPLIER_TAX_REGISTRATION_COUNTRY_CODE": "SUPPLIER_VAT_COUNTRY_CODE",
    "BUYER_TAX_REGISTRATION_NUMBER": "BUYER_TAX_REGISTRATION_NUMBER",
    "BUYER_TAX_REGISTRATION_COUNTRY_NAME": "BUYER_TAX_REGISTRATION_COUNTRY_NAME",
    "DATE_RECEIVED": "INBOUND_INVOICE_RECEIVED_AT","SENDER_EMAIL": "INBOUND_INVOICE_SENDER_EMAIL","INBOX_NAME": "INVOICE_INBOX_NAME","PAYMENT_CHANNEL": "PAYMENT_CHANNEL",
    "TYPE_OF_DOCUMENT": "DOCUMENT_FORMAT","PO": "PO","PLEASE_ENTER_YOUR_NAME": "PLEASE_ENTER_YOUR_NAME","BARCODE": "BARCODE","DESCRIPTION": "DESCRIPTION","SAP_INVOICE_STATUS": "SAP_INVOICE_STATUS","TPI_SUPPLIER": "TPI_SUPPLIER",
    "SYSTEM_CALCULATED_TAX_TOLERANCE_FAILURE": "SYSTEM_CALCULATED_TAX_TOLERANCE_FAILURE","SYSTEM_CALCULATED_HEADER_TAX": "SYSTEM_CALCULATED_HEADER_TAX","REQUESTOR_ACKNOWLEDGEMENT": "REQUESTOR_ACKNOWLEDGEMENT","QUERY_REASON": "QUERY_REASON",
    "OCR_FINDINGS": "OCR_FINDINGS","INVOICE_TOTAL": "INVOICE_TOTAL","CHARGE_ID": "CHARGE_ID","APPROVER_FMNO": "APPROVER_FMNO","APPROVER_CHARGE_CODE": "APPROVER_CHARGE_CODE",
    # NEW: Snowflake EMERGENCY (custom field 14, 'TRUE'/'FALSE') -> HANA lowercase "emergency" column.
    "emergency": "EMERGENCY",}

# Columns the query produces on purpose but the HANA target has no home for.
# Listed so the mapping log below doesn't read as an accident.
KNOWN_UNMAPPED_SNOWFLAKE_COLUMNS = {
    "WITHHOLDING_TAX_AMOUNT",          # HANA only has WITHHOLDING_TAX_OVERRIDE
    "SUPPLIER_REMIT_TO_MATCH_METHOD",  # diagnostic on the remit-to match
    "REQUESTER",                       # REQUESTED_BY_NAME — no HANA column
    "INVOICE_HEADER_TAGS",             # HANA TAG / TAG_DESCRIPTION left unmapped
    "SHIP_FROM_ADDRESS_NAME",          # SHIP_FROM_SUPPLIER_NAME carries the name
}


def _hana_null_column(hana_col: str):
    # ID can exceed 32-bit range at scale -> use LongType to avoid overflow->NULL.
    if hana_col in ["ID"]:
        return lit(None).cast(LongType())
    return lit(None).cast(StringType())


def align_snowflake_to_hana(sf_df, hana_order, hana_to_sql):
    """Select the HANA columns (by name) out of the Snowflake frame.

    The source query already returns one row per INVOICE_HEADER_ID, so no Spark
    shuffle is needed here. Anything missing on either side is logged rather
    than silently substituted, so a renamed source column is visible in the
    job log instead of turning into a column of NULLs.
    """
    cols_upper = {c.upper(): c for c in sf_df.columns}
    used_sources = set()
    null_filled = []
    exprs = []
    for h in hana_order:
        sql_name = hana_to_sql.get(h)
        if not sql_name:
            exprs.append(_hana_null_column(h).alias(h))
            continue
        key = sql_name.upper()
        if key in cols_upper:
            used_sources.add(key)
            exprs.append(col(cols_upper[key]).alias(h))
        else:
            null_filled.append(f"{h} <- {sql_name}")
            exprs.append(_hana_null_column(h).alias(h))

    if null_filled:
        print(
            "[MAPPING] Mapped HANA columns with no matching Snowflake column "
            f"(filled with NULL): {null_filled}"
        )
    unused = sorted(
        c for c in cols_upper if c not in used_sources
        and c not in KNOWN_UNMAPPED_SNOWFLAKE_COLUMNS
    )
    if unused:
        print(f"[MAPPING] Snowflake columns not loaded into HANA: {unused}")
    return sf_df.select(exprs)


# --- Small HANA helpers -----------------------------------------------------

def _hana_exec(sql: str):
    """Open a JDBC connection, execute a single statement, close it."""
    conn = jdbc_utils.get_jdbc_connection(
        spark=spark, datastore_url=hana_url, secret_arn=sap_arn, secret_region=region
    )
    try:
        jdbc_utils.exec_query(connection=conn, query=sql)
    finally:
        conn.close()


def _hana_scalar(query: str):
    """Run a single-value query against HANA and return the value (or None)."""
    df = jdbc_utils.jdbc_read(
        datastore=jdbc_utils.DatastoreType.SAP,
        datastore_url=hana_url,
        secret_arn=sap_arn,
        secret_region=region,
        query=query,
    )
    row = df.first()
    return row[0] if row is not None else None


def _hana_truncate_table(target_table: str):
    print(f"Truncating HANA table {target_table} ...")
    _hana_exec(f"TRUNCATE TABLE {target_table}")
    print("Truncate complete.")


# HANA numeric type name -> the Spark type we cast to before the JDBC write.
_HANA_NUMERIC_SPARK_TYPE = {
    "DOUBLE": DoubleType(),
    "REAL": DoubleType(),
    "FLOAT": DoubleType(),
    "INTEGER": IntegerType(),
    "TINYINT": IntegerType(),
    "SMALLINT": IntegerType(),
    "BIGINT": LongType(),
    "DECIMAL": DecimalType(38, 10),
    "SMALLDECIMAL": DecimalType(16, 6),
}

# Character types whose LENGTH in SYS.TABLE_COLUMNS is a hard max we must
# honour — HANA prod columns cannot be widened. LOBs are left alone.
_HANA_CHAR_TYPES = {
    "VARCHAR",
    "NVARCHAR",
    "CHAR",
    "NCHAR",
    "ALPHANUM",
    "SHORTTEXT",
}
_HANA_LOB_TYPES = {"CLOB", "NCLOB", "TEXT", "BLOB"}


def _hana_column_types(schema_table: str) -> dict:
    """Return {COLUMN_NAME: {type, length}} for the given HANA SCHEMA.TABLE.

    LENGTH is the declared character length for VARCHAR/NVARCHAR/CHAR (and
    numeric precision otherwise). Used to clip over-long strings before the
    JDBC write so we never hit SAP [274] inserted value too large for column.
    """
    schema, table = schema_table.replace('"', "").split(".", 1)
    df = jdbc_utils.jdbc_read(
        datastore=jdbc_utils.DatastoreType.SAP,
        datastore_url=hana_url,
        secret_arn=sap_arn,
        secret_region=region,
        query=(
            "SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH FROM SYS.TABLE_COLUMNS "
            f"WHERE SCHEMA_NAME = '{schema}' AND TABLE_NAME = '{table}' "
            "ORDER BY POSITION"
        ),
    )
    types = {}
    for r in df.collect():
        types[str(r[0])] = {
            "type": str(r[1]).upper() if r[1] is not None else "",
            "length": int(r[2]) if r[2] is not None else None,
        }
    return types


def _cast_to_target_types(df, target_types: dict):
    """Align Spark column types to the HANA target so JDBC inserts don't fail."""
    out = df
    for c in df.columns:
        meta = target_types.get(c) or {}
        spark_type = _HANA_NUMERIC_SPARK_TYPE.get(meta.get("type", ""))
        if spark_type is None:
            continue
        if isinstance(df.schema[c].dataType, NumericType):
            out = out.withColumn(c, col(c).cast(spark_type))
            continue
        cleaned = when(trim(col(c).cast("string")) == "", None).otherwise(col(c))
        out = out.withColumn(c, cleaned.cast(spark_type))
    return out


def _truncate_to_hana_lengths(df, target_types: dict):
    """Clip string values to each HANA column's declared max length.

    Example: INVOICE_NUMBER NVARCHAR(75) with an 80-char source value is
    written as the first 75 characters. CLOB/NCLOB columns are not clipped.
    """
    out = df
    clipped = []
    for c in df.columns:
        meta = target_types.get(c) or {}
        hana_type = meta.get("type", "")
        length = meta.get("length")
        if hana_type in _HANA_LOB_TYPES:
            continue
        if hana_type not in _HANA_CHAR_TYPES:
            continue
        if not length or length <= 0:
            continue
        out = out.withColumn(c, substring(col(c).cast("string"), 1, length))
        clipped.append(f"{c} {hana_type}({length})")
    if clipped:
        print(
            f"[TRUNCATE] Clipped {len(clipped)} character column(s) to HANA "
            f"max length: {clipped}"
        )
    else:
        print("[TRUNCATE] No character columns with a declared LENGTH to clip.")
    return out


# --- Audit columns (H_CREATED_DATE / H_UPDATED_DATE) ---------------------------

_HANA_TS_TYPES = {"TIMESTAMP", "SECONDDATE", "LONGDATE"}
_HANA_DATE_TYPES = {"DATE", "DAYDATE"}


def _to_text(v):
    """HANA value -> comparable text ('YYYY-MM-DD HH:MM:SS.ffffff' for timestamps)."""
    if v is None:
        return None
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")
    text = str(v).strip()
    return text or None


def _hana_now_text() -> str:
    """One load timestamp for the whole run, from the HANA server clock."""
    return _to_text(_hana_scalar("SELECT CURRENT_TIMESTAMP FROM DUMMY"))


def _typed_audit_value(expr, hana_type: str):
    """Cast the audit value to whatever type the HANA column actually has."""
    if hana_type in _HANA_TS_TYPES:
        return expr.cast("timestamp")
    if hana_type in _HANA_DATE_TYPES:
        return expr.cast("timestamp").cast("date")
    return expr.cast("string")


def _read_existing_created(ids) -> dict:
    """{ID: H_CREATED_DATE} for IDs already in the target.

    Collected to the driver BEFORE the delete, so the values survive it.
    """
    out = {}
    for i in range(0, len(ids), DELETE_BATCH_SIZE):
        chunk = ids[i:i + DELETE_BATCH_SIZE]
        id_list = ",".join(str(int(x)) for x in chunk)
        df = jdbc_utils.jdbc_read(
            datastore=jdbc_utils.DatastoreType.SAP,
            datastore_url=hana_url,
            secret_arn=sap_arn,
            secret_region=region,
            query=f"SELECT ID, {H_CREATED_COL} FROM {TARGET_TABLE} WHERE ID IN ({id_list})",
        )
        for r in df.collect():
            out[int(r[0])] = _to_text(r[1])
    return out


def _apply_audit_columns(df, target_types: dict, run_ts: str, existing_created=None):
    """Set H_UPDATED_DATE = run time; H_CREATED_DATE = existing value or run time."""
    for c in (H_CREATED_COL, H_UPDATED_COL):
        if c not in df.columns or c not in target_types:
            raise RuntimeError(f"Audit column {c} not found in {TARGET_TABLE}")
    now = lit(run_ts)
    out = df.withColumn(
        H_UPDATED_COL, _typed_audit_value(now, target_types[H_UPDATED_COL]["type"])
    )
    created_type = target_types[H_CREATED_COL]["type"]

    keep = [(k, v) for k, v in (existing_created or {}).items() if v]
    if PRESERVE_H_CREATED_ON_UPDATE and keep:
        lookup = spark.createDataFrame(
            keep,
            StructType([
                StructField("_EX_ID", LongType(), False),
                StructField("_EX_H_CREATED", StringType(), True),
            ]),
        )
        out = (
            out.join(broadcast(lookup), col("ID").cast("long") == col("_EX_ID"), "left")
               .withColumn(
                   H_CREATED_COL,
                   _typed_audit_value(coalesce(col("_EX_H_CREATED"), now), created_type),
               )
               .drop("_EX_ID", "_EX_H_CREATED")
        )
        print(f"[AUDIT] {H_UPDATED_COL}={run_ts} for all rows; {H_CREATED_COL} kept for "
              f"{len(keep)} existing invoice(s), {run_ts} for new ones.")
        return out

    print(f"[AUDIT] {H_UPDATED_COL}={run_ts} and {H_CREATED_COL}={run_ts} for all rows.")
    return out.withColumn(H_CREATED_COL, _typed_audit_value(now, created_type))


# --- Before / after report ---------------------------------------------------

def _snapshot(ids, target_types: dict) -> dict:
    """{ID: {column: text}} for the given IDs, read from the HANA target."""
    if not ids:
        return {}
    cols = [c for c in SNAPSHOT_COLUMNS if c in target_types]
    # Quote each name: HANA upper-cases unquoted identifiers, and the emergency column is lowercase.
    quoted_cols = ", ".join('"' + c + '"' for c in cols)
    df = jdbc_utils.jdbc_read(
        datastore=jdbc_utils.DatastoreType.SAP,
        datastore_url=hana_url,
        secret_arn=sap_arn,
        secret_region=region,
        query=(
            f"SELECT {quoted_cols} FROM {TARGET_TABLE} "
            f"WHERE ID IN ({','.join(str(int(x)) for x in ids)})"
        ),
    )
    return {int(r["ID"]): {c: _to_text(r[c]) for c in cols} for r in df.collect()}


def _report_before_after(ids, before: dict, after: dict, run_ts: str, existing_created: dict):
    """Print per-invoice before/after values and return a list of failed checks."""
    problems = []
    print("==================== BEFORE / AFTER ====================")
    print(f"Run timestamp (expected {H_UPDATED_COL}): {run_ts}")
    for i in ids:
        b, a = before.get(i), after.get(i)
        if a is None:
            status = "MISSING AFTER LOAD"
            problems.append(f"ID {i}: not in target after load")
        elif b is None:
            status = "INSERTED (was not in target)"
        else:
            status = "REPLACED (deleted + re-inserted)"
        print(f"ID {i}: {status}")
        print(f"   before: {b}")
        print(f"   after : {a}")
        if b and a:
            changed = [f"{c}: {b.get(c)!r} -> {a.get(c)!r}" for c in a if b.get(c) != a.get(c)]
            print("   changed: " + ("; ".join(changed) if changed else "(no column changed)"))
        if a:
            # Compare to the second: HANA may store fewer fractional digits.
            if (a.get(H_UPDATED_COL) or "")[:19] != run_ts[:19]:
                problems.append(f"ID {i}: {H_UPDATED_COL}={a.get(H_UPDATED_COL)} expected {run_ts}")
            expected_created = (existing_created.get(i) if PRESERVE_H_CREATED_ON_UPDATE else None) or run_ts
            if (a.get(H_CREATED_COL) or "")[:19] != expected_created[:19]:
                problems.append(f"ID {i}: {H_CREATED_COL}={a.get(H_CREATED_COL)} expected {expected_created}")
    print("CHECKS: " + ("PASS" if not problems else f"FAIL ({len(problems)})"))
    for p in problems:
        print(f"   - {p}")
    print("========================================================")
    return problems


# --- Load-state (existing control table, unchanged structure) ---------------

def _ensure_load_state_table():
    """Create the bookkeeping table only if it doesn't exist (same as original job)."""
    create_sql = f"""
    CREATE COLUMN TABLE {LOAD_STATE_TABLE} (
        LOAD_NAME VARCHAR(128) PRIMARY KEY,
        FULL_LOAD_COMPLETED CHAR(1) DEFAULT 'N',
        LAST_MAX_ID BIGINT,
        LAST_LOAD_AT TIMESTAMP
    )
    """
    try:
        _hana_exec(create_sql)
        print(f"Created control table {LOAD_STATE_TABLE}")
    except Exception as e:
        print(f"Control table {LOAD_STATE_TABLE} already exists (or create skipped): {e}")


def _record_load_state(max_id: int):
    """Same DELETE-then-INSERT upsert as the original job, same columns."""
    _hana_exec(f"DELETE FROM {LOAD_STATE_TABLE} WHERE LOAD_NAME = '{LOAD_NAME}'")
    _hana_exec(
        f"INSERT INTO {LOAD_STATE_TABLE} "
        f"(LOAD_NAME, FULL_LOAD_COMPLETED, LAST_MAX_ID, LAST_LOAD_AT) "
        f"VALUES ('{LOAD_NAME}', 'Y', {int(max_id)}, CURRENT_TIMESTAMP)"
    )


def _get_target_max_id() -> int:
    v = _hana_scalar(f"SELECT COALESCE(MAX(ID), 0) AS MAX_ID FROM {TARGET_TABLE}")
    return int(v) if v is not None else 0


def _get_target_watermark():
    """Latest UPDATED_AT already loaded into the target, as text Snowflake can parse.

    Works whether the HANA column is a TIMESTAMP (comes back as datetime) or a
    character column holding the timestamp text.
    """
    v = _hana_scalar(f"SELECT MAX(UPDATED_AT) FROM {TARGET_TABLE}")
    if v is None:
        return None
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")
    text = str(v).strip()
    return text or None


def _hana_count(table: str) -> int:
    return int(_hana_scalar(f"SELECT COUNT(*) FROM {table}") or 0)


def _hana_duplicate_ids(table: str) -> int:
    """How many surplus rows share an ID (0 = every ID unique). Read-only."""
    return int(_hana_scalar(f"SELECT COUNT(*) - COUNT(DISTINCT ID) FROM {table}") or 0)


def _hana_delete_ids(ids):
    """DELETE the given IDs from the target in batches, on one JDBC connection."""
    conn = jdbc_utils.get_jdbc_connection(
        spark=spark, datastore_url=hana_url, secret_arn=sap_arn, secret_region=region
    )
    try:
        for i in range(0, len(ids), DELETE_BATCH_SIZE):
            chunk = ids[i:i + DELETE_BATCH_SIZE]
            id_list = ",".join(str(int(x)) for x in chunk)
            jdbc_utils.exec_query(
                connection=conn,
                query=f"DELETE FROM {TARGET_TABLE} WHERE ID IN ({id_list})",
            )
        print(f"[UPSERT] Issued DELETE for {len(ids)} invoice ID(s) in "
              f"{math.ceil(len(ids) / DELETE_BATCH_SIZE)} batch(es).")
    finally:
        conn.close()


# --- Lightweight per-phase timing -------------------------------------------

_PHASE_TIMINGS = []


@contextmanager
def _timed(label: str):
    """Measure wall-clock time for a phase and record it for the final summary."""
    print(f"[PERF] >>> {label} ...")
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        _PHASE_TIMINGS.append((label, elapsed))
        print(f"[PERF] <<< {label}: {elapsed:,.1f}s ({elapsed / 60:,.1f} min)")


def _print_phase_summary():
    if not _PHASE_TIMINGS:
        return
    total = sum(s for _, s in _PHASE_TIMINGS)
    print("==================== PERFORMANCE SUMMARY ====================")
    for label, secs in _PHASE_TIMINGS:
        pct = (secs / total * 100) if total else 0
        print(f"  {label:<42} {secs:>8.1f}s  {secs / 60:>6.1f}m  {pct:>5.1f}%")
    print(f"  {'TOTAL':<42} {total:>8.1f}s  {total / 60:>6.1f}m")
    print("============================================================")


# --- Extract / load building blocks ----------------------------------------

def _changed_invoice_ids(sf, changed_since, last_max_id):
    """IDs the incremental query will return (exactly the invoice_header set)."""
    pred = _changed_invoice_predicate(changed_since, last_max_id)
    q = (
        f"SELECT DISTINCT iv.INVOICE_HEADER_ID FROM {_COUPA_QUAL_PREFIX}INVOICES_VIEW iv "
        f"WHERE {pred}"
    )
    rows = sf.read_sql(q, sf_database, sf_schema).collect()
    return sorted(int(r[0]) for r in rows if r[0] is not None)


def _ids_in_snowflake(sf, ids):
    """Which of the given invoice IDs exist in the Coupa source."""
    q = (
        f"SELECT DISTINCT iv.INVOICE_HEADER_ID FROM {_COUPA_QUAL_PREFIX}INVOICES_VIEW iv "
        f"WHERE {_ids_predicate(ids)}"
    )
    rows = sf.read_sql(q, sf_database, sf_schema).collect()
    return sorted(int(r[0]) for r in rows if r[0] is not None)


def _build_mapped(sf, row_estimate, run_ts, changed_since=None, last_max_id=None,
                  id_filter=None, existing_created=None, target_types=None):
    """Lazy Snowflake -> HANA-shaped DataFrame (nothing is read until the write)."""
    data_query = build_sf_query(
        "*", dedup=ENFORCE_ONE_ROW_PER_ID,
        changed_since=changed_since, last_max_id=last_max_id, id_filter=id_filter,
    )
    num_partitions = max(1, math.ceil(row_estimate / ROWS_PER_PARTITION))
    print(
        f"Rows to load: {row_estimate} -> {num_partitions} partition(s) "
        f"of ~{ROWS_PER_PARTITION} rows each."
    )
    sf_df = sf.read_sql(data_query, sf_database, sf_schema)
    # No persist / no cache: each partition is read, mapped, cast and streamed
    # straight to HANA, so executor memory stays flat regardless of row count.
    mapped = align_snowflake_to_hana(
        sf_df, PDM_INVOICE_D_HANA_COLUMN_ORDER, PDM_INVOICE_D_HANA_TO_SNOWFLAKE_COLUMN
    )
    target_types = target_types or _hana_column_types(TARGET_TABLE)
    mapped = _cast_to_target_types(mapped, target_types)
    mapped = _apply_audit_columns(mapped, target_types, run_ts, existing_created)
    # HANA prod columns cannot be widened; clip over-long strings to LENGTH.
    mapped = _truncate_to_hana_lengths(mapped, target_types)
    return mapped.repartition(num_partitions)


def _write_to_hana(df, table: str):
    jdbc_utils.jdbc_write(
        dataframe=df,
        datastore=jdbc_utils.DatastoreType.SAP,
        schema_table_name=table,
        datastore_url=hana_url,
        secret_arn=sap_arn,
        secret_region=region,
        write_mode="append",
    )


def _run_full_load(sf, run_ts: str) -> int:
    # A full load truncates first, so every row is new: H_CREATED_DATE and
    # H_UPDATED_DATE are both set to this run's timestamp.
    print("MODE: FULL LOAD (truncate + reload all rows)")
    count_query = build_sf_query(
        "COUNT(DISTINCT INVOICE_HEADER_ID) AS C" if ENFORCE_ONE_ROW_PER_ID else "COUNT(*) AS C"
    )
    with _timed("Snowflake row count (for partition sizing)"):
        rows = sf.read_sql(count_query, sf_database, sf_schema).collect()[0][0] or 0
    with _timed("Truncate target table"):
        _hana_truncate_table(TARGET_TABLE)
    if rows == 0:
        print("[WARN] Source returned 0 rows; target left empty.")
        return 0
    mapped = _build_mapped(sf, rows, run_ts)
    with _timed("Write to HANA target (JDBC)"):
        _write_to_hana(mapped, TARGET_TABLE)
    return rows


def _run_incremental_load(sf, watermark: str, prev_max_id: int, run_ts: str,
                          test_ids=None) -> int:
    """Upsert = DELETE the changed invoice IDs, then APPEND their fresh rows.

    This is NOT a key-based MERGE/UPSERT: the job issues DELETE ... WHERE ID IN (...)
    and then appends new rows with JDBC (write_mode="append").

    No staging table and no DDL. Order matters: delete first, then append. If
    the job dies in between, those invoices are missing until the next run --
    and the next run re-picks them automatically, because everything left in
    the target has UPDATED_AT <= the old watermark, while the missing invoices
    all have UPDATED_AT after it.

    test_ids=[...] (dev/QA): process exactly these invoices instead of the
    watermark-based change set, so the delete+append can be tested on demand.
    """
    if test_ids:
        print(f"MODE: INCREMENTAL TEST for {len(test_ids)} invoice(s) given in --test_ids "
              f"(watermark ignored): {test_ids}")
        with _timed("Snowflake: check test IDs exist in source"):
            ids = _ids_in_snowflake(sf, test_ids)
        missing = sorted(set(test_ids) - set(ids))
        if missing:
            # Deleting these would remove them from HANA with nothing to reload.
            raise RuntimeError(f"--test_ids not found in Snowflake source, nothing changed: {missing}")
        id_filter = ids
    else:
        print(
            f"MODE: INCREMENTAL UPSERT (invoices with any line UPDATED_AT > "
            f"{watermark} - {INCREMENTAL_LOOKBACK_HOURS}h, or ID > {prev_max_id})"
        )
        with _timed("Snowflake: find changed invoice IDs"):
            ids = _changed_invoice_ids(sf, watermark, prev_max_id)
        id_filter = None
    if not ids:
        print("No changed invoices since the watermark; nothing to upsert.")
        return 0
    new_ids = sum(1 for x in ids if x > prev_max_id)
    print(f"[UPSERT] {len(ids)} changed invoice(s): ~{len(ids) - new_ids} existing "
          f"to replace, ~{new_ids} new (ID > previous MAX(ID) {prev_max_id}).")

    target_types = _hana_column_types(TARGET_TABLE)
    # Report on all test IDs, or on a sample of existing + new invoices.
    if test_ids:
        sample = ids
    else:
        half = SNAPSHOT_SAMPLE_SIZE // 2
        sample = sorted(set(ids[:half] + ids[-half:]))

    # Everything read from HANA about these IDs is collected BEFORE the delete.
    with _timed("Snapshot BEFORE (sample invoices)"):
        before = _snapshot(sample, target_types)
    existing_created = {}
    if PRESERVE_H_CREATED_ON_UPDATE:
        with _timed(f"Read existing {H_CREATED_COL} for changed IDs"):
            existing_created = _read_existing_created(ids)
        print(f"[AUDIT] {len(existing_created)} of {len(ids)} changed invoice(s) already in target.")

    mapped = _build_mapped(
        sf, len(ids), run_ts, changed_since=watermark, last_max_id=prev_max_id,
        id_filter=id_filter, existing_created=existing_created, target_types=target_types,
    )
    with _timed("Upsert step 1: DELETE changed IDs from target"):
        _hana_delete_ids(ids)
    with _timed("Upsert step 2: APPEND fresh rows to target (JDBC)"):
        _write_to_hana(mapped, TARGET_TABLE)

    with _timed("Snapshot AFTER (sample invoices)"):
        after = _snapshot(sample, target_types)
    problems = _report_before_after(sample, before, after, run_ts, existing_created)
    if problems and test_ids:
        raise RuntimeError(f"Incremental test FAILED: {problems}")
    return len(ids)


def _decide_mode(target_has_data: bool):
    requested = LOAD_MODE
    if requested not in ("AUTO", "FULL", "INCREMENTAL"):
        raise ValueError(f"--load_mode must be AUTO, FULL or INCREMENTAL (got {requested!r})")
    if TEST_IDS:
        if requested == "FULL":
            raise ValueError("--test_ids cannot be combined with --load_mode FULL")
        if not target_has_data:
            raise RuntimeError("--test_ids needs a loaded target; run a FULL load first.")
        return "INCREMENTAL", f"test run for {len(TEST_IDS)} --test_ids"
    if requested == "FULL":
        return "FULL", "requested via --load_mode FULL"
    if target_has_data:
        return "INCREMENTAL", "target already loaded"
    if requested == "INCREMENTAL":
        raise RuntimeError(
            "--load_mode INCREMENTAL requested but the target is empty (no UPDATED_AT "
            "watermark). Run once with --load_mode FULL."
        )
    return "FULL", "target table is empty"


# --- Main -------------------------------------------------------------------

def main():
    print(f"Snowflake Coupa qualifier: {_COUPA_QUAL_PREFIX}")
    print(f"Requested load mode: {LOAD_MODE}")

    _ensure_load_state_table()

    with _timed("Read target state (HANA)"):
        before_count = _hana_count(TARGET_TABLE)
        prev_max_id = _get_target_max_id()
        watermark = _get_target_watermark() if before_count else None
    print(f"Target before load: rows={before_count} | MAX(ID)={prev_max_id} | "
          f"MAX(UPDATED_AT)={watermark}")

    mode, reason = _decide_mode(before_count > 0 and watermark is not None)
    print(f"Effective mode: {mode} ({reason})")

    sf = SparkSnowflakeConnector(
        spark_session=spark, secrets_arn=sf_arn, secrets_region=region, warehouse=sf_warehouse
    )

    run_ts = _hana_now_text()
    print(f"Run timestamp for {H_CREATED_COL}/{H_UPDATED_COL}: {run_ts}")

    if mode == "FULL":
        rows_loaded = _run_full_load(sf, run_ts)
    else:
        rows_loaded = _run_incremental_load(sf, watermark, prev_max_id, run_ts, TEST_IDS)

    # Validate by reading back from HANA (read-only queries).
    with _timed("Post-load validation (HANA reads)"):
        target_count = _hana_count(TARGET_TABLE)
        target_dupes = _hana_duplicate_ids(TARGET_TABLE)
        new_max_id = _get_target_max_id()
        new_watermark = _get_target_watermark()
    print(f"Invoices written this run: {rows_loaded}")
    print(f"Target rows: {before_count} -> {target_count} "
          f"(net +{target_count - before_count} new invoices)")
    print(f"Target MAX(ID): {prev_max_id} -> {new_max_id}")
    print(f"Target MAX(UPDATED_AT) (next run's watermark): {new_watermark}")
    print(f"Target duplicate IDs after load: {target_dupes}")

    if target_dupes:
        raise RuntimeError(f"{TARGET_TABLE} has {target_dupes} duplicate ID row(s) after load.")
    if mode == "FULL" and target_count != rows_loaded:
        print(f"[WARN] FULL load: HANA count {target_count} != Snowflake count {rows_loaded}.")

    _record_load_state(new_max_id)
    print(f"Recorded state in {LOAD_STATE_TABLE}: FULL_LOAD_COMPLETED='Y', LAST_MAX_ID={new_max_id}")

    _print_phase_summary()


try:
    main()
except Exception as e:
    print("Error in Coupa invoice header SF→HANA job")
    print(e)
    _print_phase_summary()
    raise
else:
    job.commit()