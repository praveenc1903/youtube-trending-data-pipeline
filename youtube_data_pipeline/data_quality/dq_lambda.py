"""
Lambda: Data Quality Checks
────────────────────────────
Called by Step Functions after the Silver layer is built.
Validates data quality before allowing the Gold aggregation to proceed.

Checks performed:
  1. Row count — is there enough data?
  2. Null percentage — are critical columns populated?
  3. Schema validation — do expected columns exist?
  4. Value range checks — are numeric values reasonable?
  5. Freshness — is the data recent enough?

Environment Variables:
    GLUE_DB_SILVER          — Silver Glue database (default: yt-pipeline-silver-dev)
    ATHENA_WORKGROUP        — Athena workgroup (default: primary)
    SNS_ALERT_TOPIC_ARN     — SNS topic ARN for alerts (optional)
    DQ_MIN_ROW_COUNT        — Minimum acceptable row count (default: 10)
    DQ_MAX_NULL_PERCENT     — Max acceptable null % on critical cols (default: 5.0)
    DQ_FRESHNESS_HOURS      — Data must be no older than this (default: 48)
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
import awswrangler as wr
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

# ── Config (env-overridable) ─────────────────────────────────────────────────
DEFAULT_DATABASE = os.environ.get("GLUE_DB_SILVER", "yt-pipeline-silver-dev")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")

# ── Thresholds ───────────────────────────────────────────────────────────────
MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
FRESHNESS_HOURS = int(os.environ.get("DQ_FRESHNESS_HOURS", "48"))
MAX_VIEWS = 50_000_000_000  # 50B — sanity ceiling for view counts

# Critical columns per table. Keys MUST match the real Glue table names.
CRITICAL_COLUMNS = {
    "clean_statistics": ["video_id", "title", "channel_title", "views", "region"],
    "reference_data": ["id", "region"],
}


def check_row_count(df: pd.DataFrame, table_name: str) -> dict:
    """Check that table has a minimum number of rows."""
    count = len(df)
    passed = count >= MIN_ROW_COUNT
    return {
        "check": "row_count",
        "table": table_name,
        "value": count,
        "threshold": MIN_ROW_COUNT,
        "passed": passed,
        "message": f"Row count: {count} (min: {MIN_ROW_COUNT})",
    }


def check_null_percentage(df: pd.DataFrame, table_name: str) -> list:
    """Check null percentages for critical columns."""
    results = []
    cols = CRITICAL_COLUMNS.get(table_name, [])

    if not cols:
        results.append({
            "check": "null_pct",
            "table": table_name,
            "passed": False,
            "message": f"No critical columns configured for table '{table_name}' — "
                       f"cannot validate. Add it to CRITICAL_COLUMNS.",
        })
        return results

    for col in cols:
        if col not in df.columns:
            results.append({
                "check": "null_pct",
                "table": table_name,
                "column": col,
                "passed": False,
                "message": f"Column '{col}' missing from table",
            })
            continue

        null_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 100.0
        passed = null_pct <= MAX_NULL_PCT
        results.append({
            "check": "null_pct",
            "table": table_name,
            "column": col,
            "value": round(null_pct, 2),
            "threshold": MAX_NULL_PCT,
            "passed": passed,
            "message": f"{col} null%: {null_pct:.2f}% (max: {MAX_NULL_PCT}%)",
        })

    return results


def check_schema(df: pd.DataFrame, table_name: str) -> dict:
    """Check that expected columns exist."""
    expected = set(CRITICAL_COLUMNS.get(table_name, []))
    actual = set(df.columns)
    missing = expected - actual
    # If we have no expected columns configured, that's a config gap, not a pass.
    passed = len(expected) > 0 and len(missing) == 0
    if not expected:
        message = f"No expected columns configured for '{table_name}'"
    elif missing:
        message = f"Missing columns: {sorted(missing)}"
    else:
        message = "All expected columns present"
    return {
        "check": "schema",
        "table": table_name,
        "missing_columns": sorted(missing),
        "passed": passed,
        "message": message,
    }


def check_value_ranges(df: pd.DataFrame, table_name: str) -> list:
    """Check that numeric values are within reasonable ranges."""
    results = []

    # Only the statistics table has numeric view counts to sanity-check.
    if table_name != "clean_statistics":
        return results

    if "views" in df.columns:
        views = pd.to_numeric(df["views"], errors="coerce")
        negative = int((views < 0).sum())
        extreme = int((views > MAX_VIEWS).sum())
        passed = negative == 0 and extreme == 0
        results.append({
            "check": "value_range",
            "table": table_name,
            "column": "views",
            "negative_count": negative,
            "extreme_count": extreme,
            "passed": passed,
            "message": f"Views: {negative} negative, {extreme} extreme (>{MAX_VIEWS:,})",
        })

    return results


def check_freshness(df: pd.DataFrame, table_name: str) -> dict:
    """Check that data includes recent records."""
    ts_col = None
    if "_processed_at" in df.columns:
        ts_col = "_processed_at"
    elif "_ingestion_timestamp" in df.columns:
        ts_col = "_ingestion_timestamp"

    if ts_col is None:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": "No timestamp column found — skipping freshness check (backfill data)",
        }

    try:
        latest = pd.to_datetime(df[ts_col], errors="coerce").max()
        if pd.isna(latest):
            return {
                "check": "freshness",
                "table": table_name,
                "passed": True,
                "message": f"Timestamp column '{ts_col}' had no parseable values — skipping",
            }
        cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)
        # Normalise tz-naive timestamps to UTC so the comparison is valid.
        if latest.tzinfo is None:
            latest = latest.tz_localize("UTC")
        passed = latest >= cutoff
        return {
            "check": "freshness",
            "table": table_name,
            "latest_record": str(latest),
            "cutoff": str(cutoff),
            "passed": passed,
            "message": f"Latest: {latest}, Cutoff: {cutoff}",
        }
    except Exception as e:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": f"Could not parse timestamps: {e} — skipping",
        }


def lambda_handler(event, context):
    """
    Run data quality checks on Silver layer tables.

    Expected event:
    {
        "layer": "silver",
        "database": "yt-pipeline-silver-dev",
        "tables": ["clean_statistics", "reference_data"]
    }
    """
    database = event.get("database", DEFAULT_DATABASE)
    tables = event.get("tables", ["clean_statistics", "reference_data"])

    all_results = []
    overall_passed = True

    for table_name in tables:
        logger.info(f"Running DQ checks on {database}.{table_name}...")

        try:
            # Read a sample of the data (LIMIT keeps Athena scan cost/time down).
            # Note: Athena needs a query result location set on the workgroup.
            query = f'SELECT * FROM "{table_name}" LIMIT 10000'
            df = wr.athena.read_sql_query(
                sql=query,
                database=database,
                workgroup=ATHENA_WORKGROUP,
                ctas_approach=False,
            )
        except Exception as e:
            logger.error(f"Could not read {table_name}: {e}")
            all_results.append({
                "check": "read_table",
                "table": table_name,
                "passed": False,
                "message": str(e),
            })
            overall_passed = False
            continue

        # Run all checks
        checks = []
        checks.append(check_row_count(df, table_name))
        checks.extend(check_null_percentage(df, table_name))
        checks.append(check_schema(df, table_name))
        checks.extend(check_value_ranges(df, table_name))
        checks.append(check_freshness(df, table_name))

        for check in checks:
            status = "PASS" if check["passed"] else "FAIL"
            logger.info(f"  {check['check']}: {status} — {check['message']}")
            if not check["passed"]:
                overall_passed = False

        all_results.extend(checks)

    # Summary
    passed_count = sum(1 for r in all_results if r["passed"])
    total_count = len(all_results)
    logger.info(
        f"DQ Summary: {passed_count}/{total_count} checks passed. "
        f"Overall: {'PASS' if overall_passed else 'FAIL'}"
    )

    if not overall_passed and SNS_TOPIC:
        failed = [r for r in all_results if not r["passed"]]
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject="[YT Pipeline] Data quality checks FAILED",
            Message=json.dumps(failed, indent=2, default=str),
        )

    return {
        "quality_passed": bool(overall_passed),
        "checks_passed": int(passed_count),
        "checks_total": int(total_count),
        "details": json.loads(json.dumps(all_results, default=str)),
    }