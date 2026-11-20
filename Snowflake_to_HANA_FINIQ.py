"""
Simple Snowflake -> HANA data transfer for FINIQ party details.

Flow (as requested):
    1) Read the data from Snowflake (the source view).
    2) Map the Snowflake columns to the HANA target columns (by name).
    3) Write/upsert the data into the HANA target table.

The job is fully parameter driven (see Terraform module
`mod_fdm_com_finiq_sf_to_hana`):

    --snowflake_passwords_arn   Secrets Manager ARN for the Snowflake creds.
    --sap_secrets_arn           Secrets Manager ARN for the HANA creds.
    --sap_url                   HANA JDBC url.
    --sf_warehouse              Snowflake warehouse.
    --sf_database               Snowflake source database (e.g. ELM_DATAMART_DEV).
    --sf_schema                 Snowflake source schema   (e.g. CURATED).
    --sf_object                 Snowflake source view     (e.g. VW_PARTY_DETAILS_WITH_TAXONOMY).
    --target_schema_table       HANA target SCHEMA.TABLE  (e.g. MCK_FIN_OTHER.FDM_COM_FINIQ).
    --upsert_keys               Comma separated key column(s)
                                (e.g. CST_LEADER_FMNO,PARTY_ID).
    --dedup_order_by            Column used to keep the newest row per key
                                (newest first, e.g. PARTY_SRC_UPDATE_DATE).

The load strategy is decided automatically (no extra parameter): when the HANA
target is empty (e.g. the very first run) the target is cleared (row delete)
and fully loaded with the deduped Snowflake data; on every subsequent run the
data is upserted by key (delete matching keys + insert), keeping rows for
untouched keys.

The source view and the HANA target use identical column names, so the column
mapping is resolved automatically: the HANA column list (and ordering) is read
from the catalog and each column is pulled from Snowflake by name. Columns that
do not exist in the source are written as NULL instead of failing the job.
"""

import sys
import math
import time
from contextlib import contextmanager

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    when,
    row_number,
    spark_partition_id,
    substring,
    current_timestamp,
)
from pyspark.sql.types import (
    StringType,
    DecimalType,
    NumericType,
    IntegerType,
    LongType,
    DoubleType,
    TimestampType,
    BooleanType,
)

from mck_glue_library.datastores import jdbc_utils
from mck_glue_library.datastores.snowflake_utils import SparkSnowflakeConnector

# --- INIT ---
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

DEFAULT_SECRET_REGION = "us-east-1"

# Max rows pushed to HANA in a single JDBC append. The staging load is sliced
# into disjoint chunks of this size and each chunk is appended in its own pass.
BATCH_SIZE = 500000

# Audit column stamped with the current timestamp on every insert / upsert so
# the HANA target records when each row was last refreshed. Because the load
# rewrites every row it touches (full refresh, or the upsert DELETE + INSERT
# from staging), historical rows also pick up a fresh value on each run.
# Compared case-insensitively against the HANA catalog column names.
LAST_REFRESH_COLUMN = "LAST_REFRESH_DATE"


def _normalize_argv(argv):
    """Strip stray whitespace / invisible chars from option-name tokens.

    Guards against a common Glue console mistake where a parameter Key is
    entered with a leading/trailing space or a non-breaking / zero-width space
    (e.g. "--sf_database "). Glue then passes that exact flag, argparse cannot
    match it, and getResolvedOptions fails with
    "the following arguments are required: --sf_database" even though the
    parameter is clearly set in the console.
    """
    junk = "\u00a0\u200b\ufeff \t\r\n"
    cleaned = []
    for tok in argv:
        if isinstance(tok, str) and tok.lstrip(junk).startswith("--"):
            if "=" in tok:
                key, val = tok.split("=", 1)
                tok = key.strip(junk) + "=" + val
            else:
                tok = tok.strip(junk)
        cleaned.append(tok)
    return cleaned


sys.argv = _normalize_argv(sys.argv)

args = getResolvedOptions(
    sys.argv,
    [
        "snowflake_passwords_arn",
        "sap_secrets_arn",
        "sap_url",
        "sf_warehouse",
        "sf_database",
        "sf_schema",
        "sf_object",
        "target_schema_table",
        "upsert_keys",
        "dedup_order_by",
        "proof_point_arn",
        "email_list",
        "env",
    ],
)

sf_arn = args["snowflake_passwords_arn"]
sap_arn = args["sap_secrets_arn"]
hana_url = args["sap_url"]
sf_warehouse = args["sf_warehouse"]
sf_database = args["sf_database"]
sf_schema = args["sf_schema"]
sf_object = args["sf_object"]
target_table = args["target_schema_table"]
upsert_keys = [k.strip() for k in args["upsert_keys"].split(",") if k.strip()]
dedup_order_by = args["dedup_order_by"].strip()
proof_point_arn = args["proof_point_arn"]
email_list = args["email_list"]
environment = args["env"]
region = DEFAULT_SECRET_REGION


def email_notification(proofpoint_arn, recipient_list, status, jobname, exception, env):
    import smtplib
    from mck_glue_library.utils import email_utils
    from mck_glue_library.utils import secrets_utils

    HOST = "authnz.proofpoint.com"
    PORT = 587
    recipients = recipient_list.split(",")
    sender = "fdmgluejobstatus@mckinsey.com"
    title = "Execution Status in " + env + " of Glue job :" + jobname
    text_body = "Glue job status notification"
    html_body = """
        <!DOCTYPE html>
        <html><body>
        <h2>Job has {} . Please check the logs.</h2>
        <h2>Reason : {} </h2>
        </body></html>
      """.format(status, exception)
    msg = email_utils.create_multipart_message(sender, recipients, title, text_body, html_body)
    proofpoint_creds = secrets_utils.get_secrets(proofpoint_arn)
    mailserver = smtplib.SMTP(HOST, PORT)
    mailserver.starttls()
    mailserver.login(proofpoint_creds["pfpusername"], proofpoint_creds["pfppassword"])
    mailserver.sendmail(sender, recipients, msg.as_string())
    mailserver.quit()

# Staging table used for the key-based upsert (created LIKE the target).
_schema_part, _table_part = target_table.replace('"', "").split(".", 1)
staging_table = f"{_schema_part}.{_table_part}_STG"


# --- HANA type handling -----------------------------------------------------

# HANA type name -> Spark type we cast to before the JDBC write, so the insert
# does not fail on a type mismatch and so empty strings become NULL for
# numeric / date / boolean targets.
_HANA_SPARK_TYPE = {
    "DOUBLE": DoubleType(),
    "REAL": DoubleType(),
    "FLOAT": DoubleType(),
    "INTEGER": IntegerType(),
    "TINYINT": IntegerType(),
    "SMALLINT": IntegerType(),
    "BIGINT": LongType(),
    "DECIMAL": DecimalType(38, 10),
    "SMALLDECIMAL": DecimalType(16, 6),
    "BOOLEAN": BooleanType(),
    "DATE": TimestampType(),
    "TIME": TimestampType(),
    "SECONDDATE": TimestampType(),
    "LONGDATE": TimestampType(),
    "TIMESTAMP": TimestampType(),
}

# HANA character types. Anything mapped to one of these is cast to string and
# truncated to the column's declared length before the JDBC write, so a wider
# source value (e.g. a timestamp serialized to text, or an over-long string)
# cannot trigger "[274]: inserted value too large for column".
_HANA_CHAR_TYPES = {
    "VARCHAR",
    "NVARCHAR",
    "CHAR",
    "NCHAR",
    "SHORTTEXT",
    "ALPHANUM",
}


# --- Lightweight per-phase timing -------------------------------------------

_PHASE_TIMINGS = []


@contextmanager
def _timed(label: str):
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


def _hana_read(query: str):
    return jdbc_utils.jdbc_read(
        datastore=jdbc_utils.DatastoreType.SAP,
        datastore_url=hana_url,
        secret_arn=sap_arn,
        secret_region=region,
        query=query,
    )


def _hana_target_columns(schema_table: str):
    """Return [(COLUMN_NAME, DATA_TYPE_NAME, LENGTH), ...] in target order.

    LENGTH is the declared character length for character columns (and the
    numeric precision otherwise); it is used to truncate over-long string
    values before the JDBC write. It may be None when not applicable.
    """
    schema, table = schema_table.replace('"', "").split(".", 1)
    df = _hana_read(
        "SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH FROM SYS.TABLE_COLUMNS "
        f"WHERE SCHEMA_NAME = '{schema}' AND TABLE_NAME = '{table}' "
        "ORDER BY POSITION"
    )
    return [
        (str(r[0]), str(r[1]).upper(), int(r[2]) if r[2] is not None else None)
        for r in df.collect()
    ]


def _resolve_upsert_keys(keys, hana_cols):
    """Resolve configured keys to the target column casing from HANA."""
    target_by_upper = {c.upper(): c for c, _t, _n in hana_cols}
    resolved = []
    missing = []

    for key in keys:
        target_col = target_by_upper.get(key.upper())
        if target_col is None:
            missing.append(key)
        else:
            resolved.append(target_col)

    if missing:
        available = [c for c, _t, _n in hana_cols]
        raise ValueError(
            f"Configured upsert key(s) not found in HANA target columns: {missing}. "
            f"Available columns: {available}"
        )

    if not resolved:
        raise ValueError("At least one upsert key must be configured.")

    return resolved


def _hana_count(schema_table: str) -> int:
    row = _hana_read(f"SELECT COUNT(*) FROM {schema_table}").first()
    return int(row[0]) if row is not None else 0


# --- Mapping / casting ------------------------------------------------------

def align_snowflake_to_hana(sf_df, hana_cols):
    """Select the HANA columns (by name) out of the Snowflake frame.

    Column names match between the source view and the target table, so this is
    a straight name match. Any HANA column missing from the source is emitted as
    a typed NULL so the column order/shape always matches the target.

    LAST_REFRESH_COLUMN is always stamped with the current timestamp (regardless
    of whether the source view has that column) so every inserted / upserted row
    records when it was last refreshed.
    """
    cols_upper = {c.upper(): c for c in sf_df.columns}
    exprs = []
    missing = []
    for hana_col, hana_type, _length in hana_cols:
        if hana_col.upper() == LAST_REFRESH_COLUMN.upper():
            exprs.append(current_timestamp().alias(hana_col))
            continue
        src = cols_upper.get(hana_col.upper())
        if src is not None:
            exprs.append(col(src).alias(hana_col))
        else:
            spark_type = _HANA_SPARK_TYPE.get(hana_type, StringType())
            exprs.append(lit(None).cast(spark_type).alias(hana_col))
            missing.append(hana_col)
    if missing:
        print(f"WARNING: columns not found in source (written as NULL): {missing}")
    return sf_df.select(exprs)


def cast_to_target_types(df, hana_cols):
    """Cast Spark columns to match the HANA target types.

    For numeric / date / boolean targets, empty or whitespace-only strings
    (which HANA cannot convert) are turned into NULL first, then cast.

    For character targets, the value is cast to string and truncated to the
    column's declared length. This prevents "[274]: inserted value too large
    for column" when the source value is wider than the target, e.g. a
    timestamp serialized to text ("0001-12-30T00:00:00.000+00:00") landing in
    a short NVARCHAR column.
    """
    meta_by_col = {c: (t, n) for c, t, n in hana_cols}
    out = df
    for c in df.columns:
        hana_type, length = meta_by_col.get(c, ("", None))

        if hana_type in _HANA_CHAR_TYPES:
            as_str = col(c).cast("string")
            if length and length > 0:
                as_str = substring(as_str, 1, length)
            out = out.withColumn(c, as_str)
            continue

        spark_type = _HANA_SPARK_TYPE.get(hana_type)
        if spark_type is None:
            continue
        if isinstance(df.schema[c].dataType, NumericType):
            out = out.withColumn(c, col(c).cast(spark_type))
            continue
        cleaned = when(trim(col(c).cast("string")) == "", None).otherwise(col(c))
        out = out.withColumn(c, cleaned.cast(spark_type))
    return out


def dedup_latest(df):
    """Keep the newest row per upsert key (newest = max dedup_order_by)."""
    if not upsert_keys:
        return df
    order_col = dedup_order_by if dedup_order_by in df.columns else upsert_keys[0]
    w = (
        Window.partitionBy(*upsert_keys)
        .orderBy(col(order_col).desc_nulls_last())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        df.withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )


# --- Staging + upsert -------------------------------------------------------

def _prepare_staging():
    """Ensure an empty staging table with the same shape as the target exists.

    On the first run the staging table is created LIKE the target. On later
    runs it already exists, so its rows are simply removed (row delete) instead
    of dropping/recreating it. No DROP or TRUNCATE is issued.

    Note: because the staging table is not recreated each run, a target schema
    change (e.g. a new column) is not automatically reflected here; the staging
    table would need to be adjusted manually in that case.
    """
    try:
        _hana_exec(f"CREATE COLUMN TABLE {staging_table} LIKE {target_table} WITH NO DATA")
        print(f"Created staging table {staging_table} (LIKE {target_table})")
    except Exception as e:
        print(f"Staging table {staging_table} already exists ({e}); clearing its rows.")
        _hana_exec(f"DELETE FROM {staging_table}")
        print(f"Cleared existing rows from staging table {staging_table}")


def _write_in_batches(df, total: int, dest_table: str, batch_size: int = BATCH_SIZE) -> int:
    """Append `df` to `dest_table` in sequential batches of ~batch_size."""
    if total == 0:
        print(f"Nothing to write to {dest_table} — skipping JDBC append.")
        return 0

    num_batches = math.ceil(total / batch_size)
    print(
        f"Writing {total} rows to {dest_table} in {num_batches} "
        f"sequential batch(es) of ~{batch_size}."
    )

    batched = (
        df.repartition(num_batches)
        .withColumn("_batch_id", spark_partition_id())
        .persist()
    )
    written = 0
    try:
        batch_counts = {
            int(r["_batch_id"]): int(r["count"])
            for r in batched.groupBy("_batch_id").count().collect()
        }
        for b in range(num_batches):
            batch_count = batch_counts.get(b, 0)
            if batch_count == 0:
                print(f"  Batch {b + 1}/{num_batches}: 0 rows — skipped.")
                continue
            batch_df = batched.filter(col("_batch_id") == b).drop("_batch_id").coalesce(1)
            print(f"  Batch {b + 1}/{num_batches}: writing {batch_count} rows ...")
            jdbc_utils.jdbc_write(
                dataframe=batch_df,
                datastore=jdbc_utils.DatastoreType.SAP,
                schema_table_name=dest_table,
                datastore_url=hana_url,
                secret_arn=sap_arn,
                secret_region=region,
                write_mode="append",
            )
            written += batch_count
            print(f"  Batch {b + 1}/{num_batches}: done. Cumulative: {written}/{total}.")
    finally:
        batched.unpersist()

    print(f"Completed writing {written} rows to {dest_table} in {num_batches} batch(es).")
    return written


def _full_refresh_load(df, total: int):
    """Replace the whole target with the deduped source.

    Deletes all existing rows from the target and writes the deduped rows
    straight in. This clears any stale keys (rows whose key no longer exists in
    the source), so the target ends up mirroring the source exactly.
    """
    _hana_exec(f"DELETE FROM {target_table}")
    print(f"Cleared target {target_table} for full refresh.")
    _write_in_batches(df, total, target_table)
    print(f"Full refresh complete: wrote {total} rows to {target_table}.")


def _upsert_from_staging(hana_cols):
    """Apply the staging rows to the target: delete matching keys, then insert.

    Done inside HANA so the source data only travels over JDBC once (into the
    staging table). The DELETE + INSERT keeps existing rows for untouched keys.
    """
    key_match = " AND ".join(
        f"(t.{k} = s.{k} OR (t.{k} IS NULL AND s.{k} IS NULL))"
        for k in upsert_keys
    )
    col_list = ", ".join(c for c, _t, _n in hana_cols)

    delete_sql = (
        f"DELETE FROM {target_table} t "
        f"WHERE EXISTS (SELECT 1 FROM {staging_table} s WHERE {key_match})"
    )
    insert_sql = (
        f"INSERT INTO {target_table} ({col_list}) "
        f"SELECT {col_list} FROM {staging_table}"
    )

    print(f"Upserting into {target_table} on key(s): {upsert_keys}")
    _hana_exec(delete_sql)
    print("  Deleted matching keys from target.")
    _hana_exec(insert_sql)
    print("  Inserted staged rows into target.")
    _hana_exec(f"DELETE FROM {staging_table}")
    print("  Cleared staging table rows.")


# --- Main -------------------------------------------------------------------

def _resolve_effective_load_mode() -> str:
    """Decide the load strategy from the current target state (no parameter).

    When the target is empty (e.g. the very first run) a clean full refresh is
    done; once the target holds rows, subsequent runs upsert incrementally.
    """
    existing = _hana_count(target_table)
    mode = "full_refresh" if existing == 0 else "upsert"
    print(f"Target has {existing} row(s) -> {mode}.")
    return mode


def main():
    global upsert_keys

    print(f"Source : {sf_database}.{sf_schema}.{sf_object}")
    print(f"Target : {target_table}")

    hana_cols = _hana_target_columns(target_table)
    upsert_keys = _resolve_upsert_keys(upsert_keys, hana_cols)
    print(f"Keys   : {upsert_keys} | dedup by {dedup_order_by} (newest first)")
    print(f"Resolved {len(hana_cols)} target columns from HANA catalog.")

    # 1) Read from Snowflake.
    sf = SparkSnowflakeConnector(
        spark_session=spark,
        secrets_arn=sf_arn,
        secrets_region=region,
        warehouse=sf_warehouse,
    )
    source_query = f"SELECT * FROM {sf_database}.{sf_schema}.{sf_object}"
    sf_df = sf.read_sql(source_query, sf_database, sf_schema)

    # 2) Map SF -> HANA columns, cast to target types, dedup to newest per key.
    mapped = align_snowflake_to_hana(sf_df, hana_cols)
    mapped = cast_to_target_types(mapped, hana_cols)
    mapped = dedup_latest(mapped).persist()

    try:
        with _timed("Snowflake read + map + dedup (materialize)"):
            rec_count = mapped.count()
        print(f"--- ROWS TO UPSERT (after dedup on {upsert_keys}): {rec_count} ---")

        if rec_count == 0:
            print("Nothing to load — skipping HANA write.")
        else:
            # 3) Write to HANA: full refresh or staging + key-based upsert.
            effective_mode = _resolve_effective_load_mode()
            if effective_mode == "full_refresh":
                with _timed("Full refresh (truncate + load)"):
                    _full_refresh_load(mapped, rec_count)
            else:
                with _timed("Prepare staging table"):
                    _prepare_staging()
                with _timed("Stage rows (JDBC)"):
                    _write_in_batches(mapped, rec_count, staging_table)
                with _timed("Upsert staging -> target"):
                    _upsert_from_staging(hana_cols)
    finally:
        mapped.unpersist()

    with _timed("Post-load validation (HANA count)"):
        target_count = _hana_count(target_table)
    print(f"Target count (HANA) after load: {target_count}")

    _print_phase_summary()


try:
    main()
    email_notification(
        proof_point_arn,
        email_list,
        "Successful",
        "FDM COM FINIQ SF to HANA",
        "",
        environment,
    )
except Exception as e:
    print(f"Error in FINIQ SF->HANA job ({target_table})")
    print(e)
    _print_phase_summary()
    email_notification(
        proof_point_arn,
        email_list,
        "failed",
        "FDM COM FINIQ SF to HANA",
        e,
        environment,
    )
    sys.exit(1)
else:
    job.commit()