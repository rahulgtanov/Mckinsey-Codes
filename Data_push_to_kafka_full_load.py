import sys
# from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F 
from functools import reduce as functools_reduce

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from mck_glue_library.datastores.snowflake_utils import SparkSnowflakeConnector
from mck_glue_library.transforms import df_utils
from mck_glue_library.utils import secrets_utils
from mck_glue_library.datastores import jdbc_utils
from mck_glue_library.datastores import rest_api
from mck_glue_library.datastores import kafka_utils
import boto3 
import json 
import requests
from pyspark.sql.functions import md5, concat_ws
import pyspark.sql.functions as fn
from pyspark.sql.types import MapType,StringType,IntegerType
from pyspark.sql import Window
from pyspark.sql.functions import *
import datetime
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

start_date = datetime.strptime("200001", "%Y%m")
end_date = datetime.now()
print(end_date)
def generate_month_list(start_yyyymm, end_date):
    start_date = datetime.strptime(start_yyyymm, "%Y%m")
    months_list = []
    while start_date <= end_date:
        months_list.append(start_date.strftime("%Y%m"))
        start_date += relativedelta(months=1)
    return months_list

months_list = generate_month_list("200001", datetime.now())
dfs=[]
args = getResolvedOptions(sys.argv, ['period','sap_secrets_arn','kafka_secrets_arn','kafka_broker_url'])
period = args['period']
kafka_secrets_arn = args['kafka_secrets_arn']
sap_secret_arn = args['sap_secrets_arn']
kafka_broker_url = args['kafka_broker_url']

client = boto3.client('secretsmanager')

response = client.get_secret_value(
    SecretId='glue-fdm-12790-sap-secrets'
)

database_secrets = json.loads(response['SecretString'])
sapUrl=database_secrets['sapurl']
sapUsername=database_secrets['username']
sapPassword=database_secrets['password']

conn=jdbc_utils.get_jdbc_connection(spark=spark,
datastore_url=sapUrl ,
secret_region = 'us-east-1',                                    
secret_arn= sap_secret_arn )
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dateutil.relativedelta import relativedelta

kafka_topic_read = kafka_utils.kafka_read (
    kafka_bootstrap_servers = kafka_broker_url,
    secrets_arn = kafka_secrets_arn,
    secrets_region = 'us-east-1',
    kafka_topic = 'fct-btphana.cvc-ds-invoice-header' 
)
dfs=[]

CURRENT_MONTH = datetime.now().strftime("%Y%m")
print(CURRENT_MONTH)
import pyspark.sql.functions as fn
from pyspark.sql.types import MapType,StringType,IntegerType
source_df = kafka_topic_read.withColumn('value', fn.col("value").cast(StringType())).select("value","topic","partition","offset","timestamp")
is_kafka_empty = (source_df.limit(1).count() == 0)
print(f"Kafka topic empty: {is_kafka_empty}")

def hana_query_full_load(start_date,end_date):
        query = f"""
        select "INVOICE_ID",
        "INVOICE_NUMBER",
        "ORDER_HEADER_ID",
        "STATUS",
        "SUPPLIER_NAME",
        "LEGACY_SUPPLIER_NUMBER",
        "SUPPLIER_TAX_REGISTRATION_NUMBER",
        "SUPPLIER_TAX_REGISTRATION_COUNTRY_CODE",
        "SUPPLIER_TAX_REGISTRATION_COUNTRY_NAME",
        "SUPPLIER_TAX_REGISTRATION_LOCAL",
        "SUPPLIER_VAT_NUMBER",
        "SUPPLIER_VAT_COUNTRY_CODE",
        "SUPPLIER_TELEPHONE_NUMBER",
        "COMPANY_CODE",
        "ACCOUNTING_TOTAL_CURRENCY",
        "APPROVER_FMNO",
        "BARCODE",
        "BILL_TO_ADDRESS_ATTENTION",
        "BILL_TO_ADDRESS_NAME",
        "BILL_TO_ADDRESS1",
        "BILL_TO_ADDRESS2",
        "BILL_TO_CITY",
        "BILL_TO_STATE",
        "BILL_TO_POSTAL_CODE",
        "BILL_TO_COUNTRY_CODE",
        "BILL_TO_COUNTRY_NAME",
        "BILL_TO_COMPANY_CODE",
        "BUYER_TAX_REGISTRATION_COUNTRY_CODE",
        "BUYER_TAX_REGISTRATION_COUNTRY_NAME",
        "BUYER_TAX_REGISTRATION_LOCAL",
        "BUYER_TAX_REGISTRATION_NUMBER",
        "CANCELED",
        "CHARGE_ID",
        "CURRENCY",
        "DATE_RECEIVED",
        "DELIVERY_DATE",
        "DESCRIPTION",
        "DISPUTE_METHOD",
        "DISPUTED_INVOICE_NUMBER",
        "DOCUMENT_TYPE",
        "EMERGENCY_FLAG",
        "ENTRY_OFFICE_SHIP_TO_STATE",
        "EXCHANGE_RATE",
        "EXPORTED",
        "EXPORTED_TO_AMEX",
        "HEADER_TAX_CODE",
        "IMAGENOW_ID",
        "INVOICE_DATE",
        "INVOICE_FROM_SUPPLIER_NAME",
        "INVOICE_FROM_CODE",
        "INVOICE_FROM_ADDRESS1",
        "INVOICE_FROM_ADDRESS2",
        "INVOICE_FROM_CITY",
        "INVOICE_FROM_STATE",
        "INVOICE_FROM_POSTAL_CODE",
        "INVOICE_FROM_COUNTRY_CODE",
        "INVOICE_FROM_COUNTRY_NAME",
        "INVOICE_REFERENCE_NUMBER",
        "INVOICE_TOTAL",
        "LATE_PAYMENT_PENALTIES",
        "LINE_LEVEL_TAXATION",
        "MEANS_OF_PAYMENT",
        "NET_DUE_DATE",
        "ORIGINAL_INVOICE_DATE",
        "ORIGINAL_INVOICE_NUMBER",
        "PAID",
        "PARENT_INVOICE_HEADER_ID",
        "PAY_GROUP",
        "PAYMENT_CHANNEL",
        "PAYMENT_DATE",
        "PAYMENT_METHOD",
        "PAYMENT_ORDER_NUMBER",
        "PAYMENT_ORDER_REFERENCE",
        "PAYMENT_TERM",
        "PRE_PAYMENT_DATE",
        "QST_REGISTRATION_NUMBER",
        "REMIT_TO_ADDRESS_CODE",
        "REMIT_TO_ADDRESS_ATTENTION",
        "REMIT_TO_ADDRESS_NAME",
        "REMIT_TO_ADDRESS1",
        "REMIT_TO_ADDRESS2",
        "REMIT_TO_CITY",
        "REMIT_TO_STATE",
        "REMIT_TO_POSTAL_CODE",
        "REMIT_TO_COUNTRY_CODE",
        "REMIT_TO_COUNTRY_NAME",
        "REMIT_TO_VAT_COUNTRY_CODE",
        "REMIT_TO_VAT_COUNTRY_NAME",
        "REMIT_TO_VAT_NUMBER",
        "REVERSE_CHARGE_REFERENCE",
        "SAP_INVOICE_STATUS",
        "SENDER_EMAIL",
        "SHIP_FROM_SUPPLIER_NAME",
        "SHIP_FROM_CODE",
        "SHIP_FROM_ADDRESS1",
        "SHIP_FROM_ADDRESS2",
        "SHIP_FROM_CITY",
        "SHIP_FROM_STATE",
        "SHIP_FROM_POSTAL_CODE",
        "SHIP_FROM_COUNTRY_CODE",
        "SHIP_FROM_COUNTRY_NAME",
        "SHIP_TO_ADDRESS_ATTENTION",
        "SHIP_TO_ADDRESS_NAME",
        "SHIP_TO_ADDRESS1",
        "SHIP_TO_ADDRESS2",
        "SHIP_TO_CITY",
        "SHIP_TO_STATE",
        "SHIP_TO_POSTAL_CODE",
        "SHIP_TO_COUNTRY_CODE",
        "SHIP_TO_COUNTRY_NAME",
        "SUPPLIER_REMIT_TO_CODE",
        "SUPPLIER_REMIT_TO_SUPPLIER_NAME",
        "SUPPLIER_REMIT_TO_ADDRESS1",
        "SUPPLIER_REMIT_TO_ADDRESS2",
        "SUPPLIER_REMIT_TO_CITY",
        "SUPPLIER_REMIT_TO_STATE",
        "SUPPLIER_REMIT_TO_POSTAL_CODE",
        "SUPPLIER_REMIT_TO_COUNTRY_CODE",
        "SUPPLIER_REMIT_TO_COUNTRY_NAME",
        "SYSTEM_CALCULATED_HEADER_TAX",
        "SYSTEM_CALCULATED_TAX_TOLERANCE_FAILURE",
        "TAX_NATURE",
        "CREATED_BY_FMNO",
        "CREATED_BY_NAME",
        "CREATED_BY_EMAIL",
        "UPDATED_BY_FMNO",
        "UPDATED_BY_NAME",
        "UPDATED_BY_EMAIL",
        "SRC_CREATE_DATE",
        "SRC_UPDATE_DATE",
        "UPDATE_DATE",
        "SUPPLIER_REMIT_TO_SUPPLIER_ID",
        "SUPPLIER_REMIT_TO_PAYMENT_METHOD",
        "SUPPLIER_REMIT_TO_PAYMENT_TERMS_DESC",
        "SUPPLIER_REMIT_TO_COMPANY_CODE",
        "SUPPLIER_REMIT_TO_PAYMENT_GROUP",
        "SUPPLIER_BP_ID",
        "PLEASE_ENTER_YOUR_NAME",
        "REQUESTER",
        SUM("ACCOUNTING_TOTAL_AMOUNT") AS "ACCOUNTING_TOTAL_AMOUNT",
        SUM("ADVANCE_PAYMENT_RECEIVED_AMOUNT") AS "ADVANCE_PAYMENT_RECEIVED_AMOUNT",
        SUM("AMOUNT_DUE") AS "AMOUNT_DUE",
        SUM("AMOUNT_DUE_LESS_DISCOUNT") AS "AMOUNT_DUE_LESS_DISCOUNT",
        SUM("AMOUNT_OF_ADVANCE_PAYMENT") AS "AMOUNT_OF_ADVANCE_PAYMENT",
        SUM("HEADER_TAX_AMOUNT") AS "HEADER_TAX_AMOUNT",
        SUM("HEADER_TAX_RATE") AS "HEADER_TAX_RATE",
        SUM("MISC_AMOUNT") AS "MISC_AMOUNT",
        SUM("ORIGIN_CURRENCY_GROSS") AS "ORIGIN_CURRENCY_GROSS",
        SUM("ORIGIN_CURRENCY_NET") AS "ORIGIN_CURRENCY_NET",
        SUM("SHIPPING_AMOUNT") AS "SHIPPING_AMOUNT",
        SUM("TAX_DUE_TO_SUPPLIER") AS "TAX_DUE_TO_SUPPLIER",
        SUM("TAXABLE_AMOUNT") AS "TAXABLE_AMOUNT",
        SUM("TAXES_IN_ORIGIN_COUNTRY_CURRENCY") AS "TAXES_IN_ORIGIN_COUNTRY_CURRENCY",
        SUM("TOTAL") AS "TOTAL",
        SUM("WITHHOLDING_TAX_OVERRIDE") AS "WITHHOLDING_TAX_OVERRIDE"
        from 
        S4_REPORTING_CONTAINER.CVC_DS_INVOICE_HEADER 
        (placeholder."$$IP_INVOICE_DATE_FROM$$"=>'{start_date}',
        placeholder."$$IP_INVOICE_DATE_TO$$"=>'{end_date}')
        GROUP BY "INVOICE_ID", "INVOICE_NUMBER", "ORDER_HEADER_ID", "STATUS", "SUPPLIER_NAME", "LEGACY_SUPPLIER_NUMBER", "SUPPLIER_TAX_REGISTRATION_NUMBER", "SUPPLIER_TAX_REGISTRATION_COUNTRY_CODE", "SUPPLIER_TAX_REGISTRATION_COUNTRY_NAME", "SUPPLIER_TAX_REGISTRATION_LOCAL", "SUPPLIER_VAT_NUMBER", "SUPPLIER_VAT_COUNTRY_CODE", "SUPPLIER_TELEPHONE_NUMBER", "COMPANY_CODE", "ACCOUNTING_TOTAL_CURRENCY", "APPROVER_FMNO", "BARCODE", "BILL_TO_ADDRESS_ATTENTION", "BILL_TO_ADDRESS_NAME", "BILL_TO_ADDRESS1", "BILL_TO_ADDRESS2", "BILL_TO_CITY", "BILL_TO_STATE", "BILL_TO_POSTAL_CODE", "BILL_TO_COUNTRY_CODE", "BILL_TO_COUNTRY_NAME", "BILL_TO_COMPANY_CODE", "BUYER_TAX_REGISTRATION_COUNTRY_CODE", "BUYER_TAX_REGISTRATION_COUNTRY_NAME", "BUYER_TAX_REGISTRATION_LOCAL", "BUYER_TAX_REGISTRATION_NUMBER", "CANCELED", "CHARGE_ID", "CURRENCY", "DATE_RECEIVED", "DELIVERY_DATE", "DESCRIPTION", "DISPUTE_METHOD", "DISPUTED_INVOICE_NUMBER", "DOCUMENT_TYPE", "EMERGENCY_FLAG", "ENTRY_OFFICE_SHIP_TO_STATE", "EXCHANGE_RATE", "EXPORTED", "EXPORTED_TO_AMEX", "HEADER_TAX_CODE", "IMAGENOW_ID", "INVOICE_DATE", "INVOICE_FROM_SUPPLIER_NAME", "INVOICE_FROM_CODE", "INVOICE_FROM_ADDRESS1", "INVOICE_FROM_ADDRESS2", "INVOICE_FROM_CITY", "INVOICE_FROM_STATE", "INVOICE_FROM_POSTAL_CODE", "INVOICE_FROM_COUNTRY_CODE", "INVOICE_FROM_COUNTRY_NAME", "INVOICE_REFERENCE_NUMBER", "INVOICE_TOTAL", "LATE_PAYMENT_PENALTIES", "LINE_LEVEL_TAXATION", "MEANS_OF_PAYMENT", "NET_DUE_DATE", "ORIGINAL_INVOICE_DATE", "ORIGINAL_INVOICE_NUMBER", "PAID", "PARENT_INVOICE_HEADER_ID", "PAY_GROUP", "PAYMENT_CHANNEL", "PAYMENT_DATE", "PAYMENT_METHOD", "PAYMENT_ORDER_NUMBER", "PAYMENT_ORDER_REFERENCE", "PAYMENT_TERM", "PRE_PAYMENT_DATE", "QST_REGISTRATION_NUMBER", "REMIT_TO_ADDRESS_CODE", "REMIT_TO_ADDRESS_ATTENTION", "REMIT_TO_ADDRESS_NAME", "REMIT_TO_ADDRESS1", "REMIT_TO_ADDRESS2", "REMIT_TO_CITY", "REMIT_TO_STATE", "REMIT_TO_POSTAL_CODE", "REMIT_TO_COUNTRY_CODE", "REMIT_TO_COUNTRY_NAME", "REMIT_TO_VAT_COUNTRY_CODE", "REMIT_TO_VAT_COUNTRY_NAME", "REMIT_TO_VAT_NUMBER", "REVERSE_CHARGE_REFERENCE", "SAP_INVOICE_STATUS", "SENDER_EMAIL", "SHIP_FROM_SUPPLIER_NAME", "SHIP_FROM_CODE", "SHIP_FROM_ADDRESS1", "SHIP_FROM_ADDRESS2", "SHIP_FROM_CITY", "SHIP_FROM_STATE", "SHIP_FROM_POSTAL_CODE", "SHIP_FROM_COUNTRY_CODE", "SHIP_FROM_COUNTRY_NAME", "SHIP_TO_ADDRESS_ATTENTION", "SHIP_TO_ADDRESS_NAME", "SHIP_TO_ADDRESS1", "SHIP_TO_ADDRESS2", "SHIP_TO_CITY", "SHIP_TO_STATE", "SHIP_TO_POSTAL_CODE", "SHIP_TO_COUNTRY_CODE", "SHIP_TO_COUNTRY_NAME", "SUPPLIER_REMIT_TO_CODE", "SUPPLIER_REMIT_TO_SUPPLIER_NAME", "SUPPLIER_REMIT_TO_ADDRESS1", "SUPPLIER_REMIT_TO_ADDRESS2", "SUPPLIER_REMIT_TO_CITY", "SUPPLIER_REMIT_TO_STATE", "SUPPLIER_REMIT_TO_POSTAL_CODE", "SUPPLIER_REMIT_TO_COUNTRY_CODE", "SUPPLIER_REMIT_TO_COUNTRY_NAME", "SYSTEM_CALCULATED_HEADER_TAX", "SYSTEM_CALCULATED_TAX_TOLERANCE_FAILURE", "TAX_NATURE", "CREATED_BY_FMNO", "CREATED_BY_NAME", "CREATED_BY_EMAIL", "UPDATED_BY_FMNO", "UPDATED_BY_NAME", "UPDATED_BY_EMAIL", "SRC_CREATE_DATE", "SRC_UPDATE_DATE", "UPDATE_DATE", "SUPPLIER_REMIT_TO_SUPPLIER_ID", "SUPPLIER_REMIT_TO_PAYMENT_METHOD", "SUPPLIER_REMIT_TO_PAYMENT_TERMS_DESC", "SUPPLIER_REMIT_TO_COMPANY_CODE", "SUPPLIER_REMIT_TO_PAYMENT_GROUP", "SUPPLIER_BP_ID", "PLEASE_ENTER_YOUR_NAME", "REQUESTER"
        """
        return query

def hana_query_incremental(update_str,start_date,end_date):
        query = f"""
        select "INVOICE_ID",
        "INVOICE_NUMBER",
        "ORDER_HEADER_ID",
        "STATUS",
        "SUPPLIER_NAME",
        "LEGACY_SUPPLIER_NUMBER",
        "SUPPLIER_TAX_REGISTRATION_NUMBER",
        "SUPPLIER_TAX_REGISTRATION_COUNTRY_CODE",
        "SUPPLIER_TAX_REGISTRATION_COUNTRY_NAME",
        "SUPPLIER_TAX_REGISTRATION_LOCAL",
        "SUPPLIER_VAT_NUMBER",
        "SUPPLIER_VAT_COUNTRY_CODE",
        "SUPPLIER_TELEPHONE_NUMBER",
        "COMPANY_CODE",
        "ACCOUNTING_TOTAL_CURRENCY",
        "APPROVER_FMNO",
        "BARCODE",
        "BILL_TO_ADDRESS_ATTENTION",
        "BILL_TO_ADDRESS_NAME",
        "BILL_TO_ADDRESS1",
        "BILL_TO_ADDRESS2",
        "BILL_TO_CITY",
        "BILL_TO_STATE",
        "BILL_TO_POSTAL_CODE",
        "BILL_TO_COUNTRY_CODE",
        "BILL_TO_COUNTRY_NAME",
        "BILL_TO_COMPANY_CODE",
        "BUYER_TAX_REGISTRATION_COUNTRY_CODE",
        "BUYER_TAX_REGISTRATION_COUNTRY_NAME",
        "BUYER_TAX_REGISTRATION_LOCAL",
        "BUYER_TAX_REGISTRATION_NUMBER",
        "CANCELED",
        "CHARGE_ID",
        "CURRENCY",
        "DATE_RECEIVED",
        "DELIVERY_DATE",
        "DESCRIPTION",
        "DISPUTE_METHOD",
        "DISPUTED_INVOICE_NUMBER",
        "DOCUMENT_TYPE",
        "EMERGENCY_FLAG",
        "ENTRY_OFFICE_SHIP_TO_STATE",
        "EXCHANGE_RATE",
        "EXPORTED",
        "EXPORTED_TO_AMEX",
        "HEADER_TAX_CODE",
        "IMAGENOW_ID",
        "INVOICE_DATE",
        "INVOICE_FROM_SUPPLIER_NAME",
        "INVOICE_FROM_CODE",
        "INVOICE_FROM_ADDRESS1",
        "INVOICE_FROM_ADDRESS2",
        "INVOICE_FROM_CITY",
        "INVOICE_FROM_STATE",
        "INVOICE_FROM_POSTAL_CODE",
        "INVOICE_FROM_COUNTRY_CODE",
        "INVOICE_FROM_COUNTRY_NAME",
        "INVOICE_REFERENCE_NUMBER",
        "INVOICE_TOTAL",
        "LATE_PAYMENT_PENALTIES",
        "LINE_LEVEL_TAXATION",
        "MEANS_OF_PAYMENT",
        "NET_DUE_DATE",
        "ORIGINAL_INVOICE_DATE",
        "ORIGINAL_INVOICE_NUMBER",
        "PAID",
        "PARENT_INVOICE_HEADER_ID",
        "PAY_GROUP",
        "PAYMENT_CHANNEL",
        "PAYMENT_DATE",
        "PAYMENT_METHOD",
        "PAYMENT_ORDER_NUMBER",
        "PAYMENT_ORDER_REFERENCE",
        "PAYMENT_TERM",
        "PRE_PAYMENT_DATE",
        "QST_REGISTRATION_NUMBER",
        "REMIT_TO_ADDRESS_CODE",
        "REMIT_TO_ADDRESS_ATTENTION",
        "REMIT_TO_ADDRESS_NAME",
        "REMIT_TO_ADDRESS1",
        "REMIT_TO_ADDRESS2",
        "REMIT_TO_CITY",
        "REMIT_TO_STATE",
        "REMIT_TO_POSTAL_CODE",
        "REMIT_TO_COUNTRY_CODE",
        "REMIT_TO_COUNTRY_NAME",
        "REMIT_TO_VAT_COUNTRY_CODE",
        "REMIT_TO_VAT_COUNTRY_NAME",
        "REMIT_TO_VAT_NUMBER",
        "REVERSE_CHARGE_REFERENCE",
        "SAP_INVOICE_STATUS",
        "SENDER_EMAIL",
        "SHIP_FROM_SUPPLIER_NAME",
        "SHIP_FROM_CODE",
        "SHIP_FROM_ADDRESS1",
        "SHIP_FROM_ADDRESS2",
        "SHIP_FROM_CITY",
        "SHIP_FROM_STATE",
        "SHIP_FROM_POSTAL_CODE",
        "SHIP_FROM_COUNTRY_CODE",
        "SHIP_FROM_COUNTRY_NAME",
        "SHIP_TO_ADDRESS_ATTENTION",
        "SHIP_TO_ADDRESS_NAME",
        "SHIP_TO_ADDRESS1",
        "SHIP_TO_ADDRESS2",
        "SHIP_TO_CITY",
        "SHIP_TO_STATE",
        "SHIP_TO_POSTAL_CODE",
        "SHIP_TO_COUNTRY_CODE",
        "SHIP_TO_COUNTRY_NAME",
        "SUPPLIER_REMIT_TO_CODE",
        "SUPPLIER_REMIT_TO_SUPPLIER_NAME",
        "SUPPLIER_REMIT_TO_ADDRESS1",
        "SUPPLIER_REMIT_TO_ADDRESS2",
        "SUPPLIER_REMIT_TO_CITY",
        "SUPPLIER_REMIT_TO_STATE",
        "SUPPLIER_REMIT_TO_POSTAL_CODE",
        "SUPPLIER_REMIT_TO_COUNTRY_CODE",
        "SUPPLIER_REMIT_TO_COUNTRY_NAME",
        "SYSTEM_CALCULATED_HEADER_TAX",
        "SYSTEM_CALCULATED_TAX_TOLERANCE_FAILURE",
        "TAX_NATURE",
        "CREATED_BY_FMNO",
        "CREATED_BY_NAME",
        "CREATED_BY_EMAIL",
        "UPDATED_BY_FMNO",
        "UPDATED_BY_NAME",
        "UPDATED_BY_EMAIL",
        "SRC_CREATE_DATE",
        "SRC_UPDATE_DATE",
        "UPDATE_DATE",
        "SUPPLIER_REMIT_TO_SUPPLIER_ID",
        "SUPPLIER_REMIT_TO_PAYMENT_METHOD",
        "SUPPLIER_REMIT_TO_PAYMENT_TERMS_DESC",
        "SUPPLIER_REMIT_TO_COMPANY_CODE",
        "SUPPLIER_REMIT_TO_PAYMENT_GROUP",
        "SUPPLIER_BP_ID",
        "PLEASE_ENTER_YOUR_NAME",
        "REQUESTER",
        SUM("ACCOUNTING_TOTAL_AMOUNT") AS "ACCOUNTING_TOTAL_AMOUNT",
        SUM("ADVANCE_PAYMENT_RECEIVED_AMOUNT") AS "ADVANCE_PAYMENT_RECEIVED_AMOUNT",
        SUM("AMOUNT_DUE") AS "AMOUNT_DUE",
        SUM("AMOUNT_DUE_LESS_DISCOUNT") AS "AMOUNT_DUE_LESS_DISCOUNT",
        SUM("AMOUNT_OF_ADVANCE_PAYMENT") AS "AMOUNT_OF_ADVANCE_PAYMENT",
        SUM("HEADER_TAX_AMOUNT") AS "HEADER_TAX_AMOUNT",
        SUM("HEADER_TAX_RATE") AS "HEADER_TAX_RATE",
        SUM("MISC_AMOUNT") AS "MISC_AMOUNT",
        SUM("ORIGIN_CURRENCY_GROSS") AS "ORIGIN_CURRENCY_GROSS",
        SUM("ORIGIN_CURRENCY_NET") AS "ORIGIN_CURRENCY_NET",
        SUM("SHIPPING_AMOUNT") AS "SHIPPING_AMOUNT",
        SUM("TAX_DUE_TO_SUPPLIER") AS "TAX_DUE_TO_SUPPLIER",
        SUM("TAXABLE_AMOUNT") AS "TAXABLE_AMOUNT",
        SUM("TAXES_IN_ORIGIN_COUNTRY_CURRENCY") AS "TAXES_IN_ORIGIN_COUNTRY_CURRENCY",
        SUM("TOTAL") AS "TOTAL",
        SUM("WITHHOLDING_TAX_OVERRIDE") AS "WITHHOLDING_TAX_OVERRIDE"
        from 
        S4_REPORTING_CONTAINER.CVC_DS_INVOICE_HEADER 
        (placeholder."$$IP_INVOICE_DATE_FROM$$"=>'{start_date}',
        placeholder."$$IP_INVOICE_DATE_TO$$"=>'{end_date}')
        where UPDATE_DATE like '{update_str}%'
        GROUP BY "INVOICE_ID", "INVOICE_NUMBER", "ORDER_HEADER_ID", "STATUS", "SUPPLIER_NAME", "LEGACY_SUPPLIER_NUMBER", "SUPPLIER_TAX_REGISTRATION_NUMBER", "SUPPLIER_TAX_REGISTRATION_COUNTRY_CODE", "SUPPLIER_TAX_REGISTRATION_COUNTRY_NAME", "SUPPLIER_TAX_REGISTRATION_LOCAL", "SUPPLIER_VAT_NUMBER", "SUPPLIER_VAT_COUNTRY_CODE", "SUPPLIER_TELEPHONE_NUMBER", "COMPANY_CODE", "ACCOUNTING_TOTAL_CURRENCY", "APPROVER_FMNO", "BARCODE", "BILL_TO_ADDRESS_ATTENTION", "BILL_TO_ADDRESS_NAME", "BILL_TO_ADDRESS1", "BILL_TO_ADDRESS2", "BILL_TO_CITY", "BILL_TO_STATE", "BILL_TO_POSTAL_CODE", "BILL_TO_COUNTRY_CODE", "BILL_TO_COUNTRY_NAME", "BILL_TO_COMPANY_CODE", "BUYER_TAX_REGISTRATION_COUNTRY_CODE", "BUYER_TAX_REGISTRATION_COUNTRY_NAME", "BUYER_TAX_REGISTRATION_LOCAL", "BUYER_TAX_REGISTRATION_NUMBER", "CANCELED", "CHARGE_ID", "CURRENCY", "DATE_RECEIVED", "DELIVERY_DATE", "DESCRIPTION", "DISPUTE_METHOD", "DISPUTED_INVOICE_NUMBER", "DOCUMENT_TYPE", "EMERGENCY_FLAG", "ENTRY_OFFICE_SHIP_TO_STATE", "EXCHANGE_RATE", "EXPORTED", "EXPORTED_TO_AMEX", "HEADER_TAX_CODE", "IMAGENOW_ID", "INVOICE_DATE", "INVOICE_FROM_SUPPLIER_NAME", "INVOICE_FROM_CODE", "INVOICE_FROM_ADDRESS1", "INVOICE_FROM_ADDRESS2", "INVOICE_FROM_CITY", "INVOICE_FROM_STATE", "INVOICE_FROM_POSTAL_CODE", "INVOICE_FROM_COUNTRY_CODE", "INVOICE_FROM_COUNTRY_NAME", "INVOICE_REFERENCE_NUMBER", "INVOICE_TOTAL", "LATE_PAYMENT_PENALTIES", "LINE_LEVEL_TAXATION", "MEANS_OF_PAYMENT", "NET_DUE_DATE", "ORIGINAL_INVOICE_DATE", "ORIGINAL_INVOICE_NUMBER", "PAID", "PARENT_INVOICE_HEADER_ID", "PAY_GROUP", "PAYMENT_CHANNEL", "PAYMENT_DATE", "PAYMENT_METHOD", "PAYMENT_ORDER_NUMBER", "PAYMENT_ORDER_REFERENCE", "PAYMENT_TERM", "PRE_PAYMENT_DATE", "QST_REGISTRATION_NUMBER", "REMIT_TO_ADDRESS_CODE", "REMIT_TO_ADDRESS_ATTENTION", "REMIT_TO_ADDRESS_NAME", "REMIT_TO_ADDRESS1", "REMIT_TO_ADDRESS2", "REMIT_TO_CITY", "REMIT_TO_STATE", "REMIT_TO_POSTAL_CODE", "REMIT_TO_COUNTRY_CODE", "REMIT_TO_COUNTRY_NAME", "REMIT_TO_VAT_COUNTRY_CODE", "REMIT_TO_VAT_COUNTRY_NAME", "REMIT_TO_VAT_NUMBER", "REVERSE_CHARGE_REFERENCE", "SAP_INVOICE_STATUS", "SENDER_EMAIL", "SHIP_FROM_SUPPLIER_NAME", "SHIP_FROM_CODE", "SHIP_FROM_ADDRESS1", "SHIP_FROM_ADDRESS2", "SHIP_FROM_CITY", "SHIP_FROM_STATE", "SHIP_FROM_POSTAL_CODE", "SHIP_FROM_COUNTRY_CODE", "SHIP_FROM_COUNTRY_NAME", "SHIP_TO_ADDRESS_ATTENTION", "SHIP_TO_ADDRESS_NAME", "SHIP_TO_ADDRESS1", "SHIP_TO_ADDRESS2", "SHIP_TO_CITY", "SHIP_TO_STATE", "SHIP_TO_POSTAL_CODE", "SHIP_TO_COUNTRY_CODE", "SHIP_TO_COUNTRY_NAME", "SUPPLIER_REMIT_TO_CODE", "SUPPLIER_REMIT_TO_SUPPLIER_NAME", "SUPPLIER_REMIT_TO_ADDRESS1", "SUPPLIER_REMIT_TO_ADDRESS2", "SUPPLIER_REMIT_TO_CITY", "SUPPLIER_REMIT_TO_STATE", "SUPPLIER_REMIT_TO_POSTAL_CODE", "SUPPLIER_REMIT_TO_COUNTRY_CODE", "SUPPLIER_REMIT_TO_COUNTRY_NAME", "SYSTEM_CALCULATED_HEADER_TAX", "SYSTEM_CALCULATED_TAX_TOLERANCE_FAILURE", "TAX_NATURE", "CREATED_BY_FMNO", "CREATED_BY_NAME", "CREATED_BY_EMAIL", "UPDATED_BY_FMNO", "UPDATED_BY_NAME", "UPDATED_BY_EMAIL", "SRC_CREATE_DATE", "SRC_UPDATE_DATE", "UPDATE_DATE", "SUPPLIER_REMIT_TO_SUPPLIER_ID", "SUPPLIER_REMIT_TO_PAYMENT_METHOD", "SUPPLIER_REMIT_TO_PAYMENT_TERMS_DESC", "SUPPLIER_REMIT_TO_COMPANY_CODE", "SUPPLIER_REMIT_TO_PAYMENT_GROUP", "SUPPLIER_BP_ID", "PLEASE_ENTER_YOUR_NAME", "REQUESTER"
        """
        return query 
        
from datetime import date 
import calendar
dfs= []
start_year=2000
today=date.today()
end_year=today.year

today_str=today.isoformat()
print(today_str)
print(months_list)
START_DATE = "200001"

def replace_nulls_with_empty(df):
    """Replace null values with empty strings for all columns"""
    return df.select([
        F.when(F.col(c).isNull(), '').otherwise(F.col(c)).alias(c) 
        for c in df.columns
    ])

# Determine load type: Full Load if source_df_cnt == 0, else Incremental
# is_full_load = (source_df_cnt == 0)
is_full_load = is_kafka_empty
print(f"Load Type: {'FULL LOAD' if is_full_load else 'INCREMENTAL LOAD'}")

if is_full_load:
    # ============== FULL LOAD - Process month by month==============
    print(f"Full Load: Processing {len(months_list)} months (200001 to {CURRENT_MONTH})")
    
    dfs = []
    
    # Process single month at a time
    for month in months_list:
        try:
            print(f"Processing month: {month}")
            
            query = hana_query_full_load(month, month)
            
            df_hana = jdbc_utils.jdbc_read(
                datastore=jdbc_utils.DatastoreType.SAP,
                datastore_url=sapUrl,
                query=query,
                secret_arn=sap_secret_arn
            )
            
            # Replace nulls with empty strings
            df_hana = replace_nulls_with_empty(df_hana)
            
            dfs.append(df_hana)
            print(f"Month {month} processed successfully")
            
        except Exception as e:
            print(f"Error processing month {month}: {str(e)}")
            continue
    
    # Union all DataFrames at the end
    if dfs:
        print(f"Combining {len(dfs)} DataFrames...")
        df_combined = functools_reduce(lambda a, b: a.unionByName(b), dfs)
        
        final_count = df_combined.count()
        print(f"Full Load - Total records: {final_count}")
        
        ### Kafka write for full load
        kafka_topic = kafka_utils.kafka_write(
            dataframe=df_combined,
            kafka_bootstrap_servers=kafka_broker_url,
            secrets_arn=kafka_secrets_arn,
            secrets_region='us-east-1',
            kafka_topic='fct-btphana.cvc-ds-invoice-header',
        )
        
        print("Full Load completed successfully")
    else:
        print("No data to process for Full Load")

else:
    # ============== INCREMENTAL LOAD ==============
    print(f"Incremental Load: Single query from {START_DATE} to {CURRENT_MONTH}")
    
    # Get the maximum timestamp from Kafka topic to determine last successful load date
    print("Checking last successful load date from Kafka topic timestamp...")
    
    last_kafka_timestamp = source_df.agg(F.max("timestamp")).collect()[0][0]
    
    if last_kafka_timestamp is not None:
        # Convert Kafka timestamp to date
        last_load_date = last_kafka_timestamp.date()
        today_date = date.today()
        
        print(f"Last successful Kafka write timestamp: {last_kafka_timestamp}")
        print(f"Last successful load date: {last_load_date}")
        print(f"Today's date: {today_date}")
        
        # Calculate the number of missed days
        days_diff = (today_date - last_load_date).days
        print(f"Days since last successful load: {days_diff}")
        
        if days_diff > 1:
            # There are missed days - need to backfill
            print(f"SAFEGUARD TRIGGERED: {days_diff - 1} day(s) of data may have been missed!")
            
            # Generate list of dates to process (from day after last load to today)
            dates_to_process = []
            for i in range(1, days_diff + 1):
                process_date = last_load_date + timedelta(days=i)
                dates_to_process.append(process_date.isoformat())
            
            print(f"Dates to process for recovery: {dates_to_process}")
        else:
            # This handles the case where records are updated after the previous day's job run
            yesterday_date = today_date - timedelta(days=1)
            yesterday_str = yesterday_date.isoformat()
            dates_to_process = [yesterday_str, today_str]
            print(f"Including yesterday ({yesterday_str}) + today ({today_str}) to catch late updates")
    else:
        # If no timestamp found, just process today
        dates_to_process = [today_str]
        print(f"No Kafka timestamp found. Processing only today: {today_str}")
    
    print(f"Total dates to process: {len(dates_to_process)}")
    print(f"Date range: {dates_to_process[0]} to {dates_to_process[-1]}")
    
    # ============== Process all dates (including missed ones) ==============
    all_incremental_dfs = []
    
    for update_date_str in dates_to_process:
        print(f"\n--- Processing UPDATE_DATE: {update_date_str} ---")
        
        query = hana_query_incremental(update_date_str, START_DATE, CURRENT_MONTH)
        
        try:
            df_hana_day = jdbc_utils.jdbc_read(
                datastore=jdbc_utils.DatastoreType.SAP,
                datastore_url=sapUrl,
                query=query,
                secret_arn=sap_secret_arn
            )
            
            # Replace nulls with empty strings
            df_hana_day = replace_nulls_with_empty(df_hana_day)
            
            day_count = df_hana_day.count()
            print(f"Records found for {update_date_str}: {day_count}")
            
            if day_count > 0:
                all_incremental_dfs.append(df_hana_day)
                
        except Exception as e:
            print(f"Error processing date {update_date_str}: {str(e)}")
            continue
    
    # Combine all incremental dataframes
    if all_incremental_dfs:
        print(f"\nCombining {len(all_incremental_dfs)} DataFrames from all processed dates...")
        df_hana = functools_reduce(
            lambda a, b: a.unionByName(b), all_incremental_dfs
        )
        
        df_hana.cache()
        hana_count = df_hana.count()
        print(f"\nIncremental Load - Total records from HANA (all dates): {hana_count}")
        from pyspark.sql.functions import col, row_number, monotonically_increasing_id
        
        try:
            if hana_count > 0:
                print("Deduplicating within batch by INVOICE_ID (keeping latest)...")
                w = Window.partitionBy('INVOICE_ID').orderBy(col("UPDATE_DATE").desc())
                df_hana_deduped = df_hana.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
                
                # Cache deduplicated dataframe for validation
                df_hana_deduped.cache()
                final_count = df_hana_deduped.count()
                print(f"Incremental Load - Records after batch deduplication: {final_count}")
               
                if final_count > 0:
                    # Log invoice IDs being processed for recovery purposes
                    invoice_ids_to_process = df_hana_deduped.select("INVOICE_ID").collect()
                    invoice_id_list = [row["INVOICE_ID"] for row in invoice_ids_to_process]
                    print(f"Processing {len(invoice_id_list)} invoice IDs: {invoice_id_list[:10]}{'...' if len(invoice_id_list) > 10 else ''}")
                    
                    try:
                        #### Kafka write for incremental data
                        kafka_topic = kafka_utils.kafka_write(
                            dataframe=df_hana_deduped,
                            kafka_bootstrap_servers=kafka_broker_url,
                            secrets_arn=kafka_secrets_arn,
                            secrets_region='us-east-1',
                            kafka_topic='fct-btphana.cvc-ds-invoice-header',
                        )
                        print(f"Incremental Load completed successfully - {final_count} records written to Kafka")
                        print(f"Date range processed: {dates_to_process[0]} to {dates_to_process[-1]} ({len(dates_to_process)} day(s))")
                        
                    except Exception as kafka_error:
                        # Log failed batch details
                        print(f"ERROR: Kafka write failed after potential partial write!")
                        print(f"Error details: {str(kafka_error)}")
                        print(f"UPDATE_DATE range processed: {dates_to_process[0]} to {dates_to_process[-1]}")
                        print(f"Invoice IDs that may need reprocessing: {invoice_id_list}")
                        print(f"Total records affected: {final_count}")
                        # Re-raise to fail the job
                        raise Exception(f"Kafka write failed - partial data may have been written. Error: {str(kafka_error)}")
                else:
                    print("No new records to load after deduplication")
                
                # Unpersist deduplicated dataframe
                df_hana_deduped.unpersist()
            else:
                print(f"No records found for date range: {dates_to_process[0]} to {dates_to_process[-1]}")
                
        except Exception as e:
            print(f"Incremental Load FAILED: {str(e)}")
            raise
        finally:
            try:
                df_hana.unpersist()
            except Exception:
                pass
    else:
        # No data found - skip processing entirely
        print(f"No data found for any of the processed dates ({dates_to_process[0]} to {dates_to_process[-1]}) - skipping processing")

job.commit()