import logging
import json
import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# globals
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app_id = "a0149c"
hbase_table = "A0149C_CASE_EVENTS_PROCESSED"
hive_table = "a0149c_kyc_workflow_events"


def _namespace_resolver(env):
    return {
        "dev01": "BDPD01PUB",
        "sit01": "BDPS01PUB",
        "svp01": "BDPV01PUB",
        "prod": "BDPP01PUB",
    }[env]


def main(opts):
    logger.info("kyc_workflow_events_hbase_to_raw started")

    env = opts.env
    bdp_year, bdp_month, bdp_day = opts.formatted_odate.split("-")
    namespace = _namespace_resolver(env)

    spark = (
        SparkSession.builder.appName("kyc_workflow_events_hbase_to_raw")
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")

    catalog = json.dumps(
        {
            "table": {"namespace": namespace, "name": hbase_table},
            "rowkey": "key",
            "columns": {
                "key": {"cf": "rowkey", "col": "key", "type": "string"},
                "event_name": {"cf": "scp", "col": "en", "type": "string"},
                "topic_partition": {"cf": "scp", "col": "tp", "type": "string"},
                "topic_offset": {"cf": "scp", "col": "to", "type": "string"},
                "offset_time": {"cf": "scp", "col": "ot", "type": "string"},
                "process_datetime": {"cf": "scp", "col": "pd", "type": "string"},
                "event_data": {"cf": "scp", "col": "ev", "type": "string"}
            }
        }
    )

    df = (
        spark.read.format("org.apache.spark.sql.execution.datasources.hbase")
        .options(catalog=catalog)
        .load()
    )

    logger.info("Source dataframe loaded")

    date_regex = "^.*{}-{}-{}.*".format(bdp_year, bdp_month, bdp_day)
    df = df.filter(col("key").rlike(date_regex))

    if df.count() == 0:
        logger.info("No data available for {}".format(opts.formatted_odate))
        sys.exit(0)

    df = df.select("key","event_name","topic_partition","topic_offset","offset_time","process_datetime","event_data")

    load_path = (
        "/{}/landing/{}/{}/processed/bdp_year={}/bdp_month={}/bdp_day={}/bdp_hour=00".format(
            env, app_id, hive_table, bdp_year, bdp_month, bdp_day
        )
    )
    df.repartition(10).write.format("orc").mode("overwrite").save(load_path)

    logger.info("kyc_workflow_events_hbase_to_raw loading completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migration Hbase Extract Job")
    parser.add_argument("env", type=str, help="BDP Runtime Environment")
    parser.add_argument(
        "formatted_odate", type=str, help="BDP Processed Formatted ODate"
    )

    args = parser.parse_args()
    main(args)
