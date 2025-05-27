"""
The 'custom_transform' module encapsulates custom Spark transform logic for a specific pipeline.
"""
import os
from datetime import datetime, timedelta
from ddep.batch_functions.scd2.scd_type2 import perform_scd
from pyspark.sql import functions as f
from pyspark.sql.functions import col, trim, when, upper, round, to_date, lit, concat, count
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, BooleanType, TimestampType, DateType, IntegerType, LongType, DecimalType
from dateutil.relativedelta import relativedelta

from .constants import (
    ActivityAcraamaa,
    HardShipDate,
    MssSecurityLoanRltsp,
    MssTitle,
    TempMipRecov,
    TmpSecuritySale,
    RdeHardship,
    TmpPreprocessRdeclctepsd,
    RDE_PRV_SYS_APPLN_ID,
    B2K_APPLN_KEY_ERD,
    TCS_APPLN_KEY_ERD,
    RDE_APPLN_KEY_ERD,
    CMT_APPN_KEY,
    INFLIS_APPN_KEY,
    PCC_PRV_SYS_APPLN_ID,
    PCC_APPLN_KEY,
    TmpPreprocessPccclectepsd,
    PccAccount,
)
from .constants import (
    CLECT_EPSD_HOST_ID,
    FIN_RLF_ARMT_HOST_ID,
    DDE_ARREARS_REASON,
    DDE_AUTO_LTR_DAYS,
    DDE_ACCT_ID,
    DDE_DTE_COLCTED,
    DDE_CURR_DT_ADD,
    DDE_OL_WRITEOFF_IND,
    DDE_OL_LRD_IND,
    DDE_SUPPRESS_LTR,
    DDE_SUPV_SUPR_DAYS,
    DDE_RECOVERY_STAT_CODE,
    DDE_SCO_PENDING,
    DDE_COLL_ENTRY_REAS,
    DDE_REC_STAT,
    DDE_21D3_LIST_AMNT,
    DDE_HARDSHIP_INFO_EXP_DT,
    DDE_HARDSHIP_LVR,
    DDE_PROM_SCHED_WK_IND,
    DDE_PROM_STAT,
    DDE_PROM_AMT,
    DDE_PROM_DTE,
    DDE_DTE_LAST_REVW_DT,
    DDE_AMT_TOWARDS_PROM,
    DDE_VP_ASSIST_IND,
    LEND_ARMT_HOST_ID,
    DDE_FIN_RLF_ST_DT,
    DDE_FIN_RLF_END_DT,
    ACTY_CD,
    DDE_BANK_ERR_PRESENT,
    DDE_COMMERCIALLY_AVAILABLE_PRESENT,
    DDE_PROM_SCHEDULE,
    DDE_ASSISTANCE_LEVEL,
    DDE_COMPLAINTS_CUST_RELTNS,
    DDE_SGB_FOS,
    DDE_COMMERCIAL,
    SRC_BDP_PARTITION,
    DDE_ACTY_CDS,
    DDE_ARREARS_CODE,
    DDE_DEFER_WRITE_OFF_IND,
    DDE_FINANCIAL_SOLUTION,
    DDE_HARDSHIP_EXP_DATE,
    DDE_HARDSHIP_IND,
    DDE_IL_RECOVERY_AMOUNT,
    DDE_IL_RECOVERY_OUTCOME,
    DDE_LETR_WARNING_DATE,
    DDE_MI_ARRS_REASON,
    DDE_NO_PHONE_CONTACT,
    DDE_ORIG_PROD_SYS,
    DDE_OUTSOURCING_CODE,
    DDE_RECOVERY_CODE,
    DDE_SOFT_CHARGEOFF_IND,
    DDE_SUPRES_AUTO_LTR_IND,
    ClectoutsrcgType,
    PrmsStatType,
    RecovOutcmType,
    RlshpType,
    FinSolType,
    RefDataApplnMap,
    LEFT,
    THRESHOLD_ODATE,
    INFLS_THRESHOLD_ODATE,
    PCC_THRESHOLD_ODATE,
    RDE_THRESHOLD_ODATE,
    ClectEpsdCustRole,
    CLECT_EPSD_PARTY_ROLE_TYPE_CD,
    CUSTOMER_PARTY_ROLE,
    PARTY_SK,
    MIPB2K,
    MIPTCS,
    LIS_KMR_HOST,
    TCS_CUSTID_FORMAT,
    CLECT_RECOV_TYPE_CD,
    RECOVERY_TYPE_CODE,
    PRMS_AMT,
    PRMS_DT,
    PRMS_REVW_DT,
    TOT_PRMS_AMT,
    PRMS_STAT_TYPE_CD,
    PAYMT_PLAN_PERIOD_TYPE_CD,
    B2K_APPLN_KEY,
    PROMISE_AMOUNT_CURRENCY_CODE,
    CASE_SK,
    INNER,
    AUD,
    PRMS_STAT_TYPE,
    PROMISE_SCHEDULE_TYPE,
    PRV_SYS_APPLN_ID,
    TCS_APPLN_KEY,
    AREARS_REASN_TYPE_CD,
    AUTO_LTR_DAY_CNT,
    CLECT_DT,
    CLECT_OUTSRCG_TYPE_CD,
    CREAT_DT,
    DEFER_WRITE_OFF_IND,
    FIN_SOLTN_TYPE_CD,
    ALTN_STRAT_CD,
    DDE_ALT_STRATEGY_IND,
    DDE_APRA,
    ALTERNATE_STRATEGY_CODE,
    LEND_ARMT_SK,
    LITIGN_IND,
    LTR_SUPRSN_IND,
    NO_PH_CONTCT_IND,
    OMIT_FROM_SUPVSRY_QU_DAY_CNT,
    OWNG_LEND_ARMT_APPLN_KEY,
    SOFT_CHRG_OFF_PNDG_IND,
    CLECT_ENTRY_REASN_TYPE_CD,
    CLECT_STAT_TYPE_CD,
    CREDIT_LISTING_AMOUNT_CURRENCY_CODE,
    CRAA_LODG_DT,
    CR_LISTNG_AMT,
    FIN_SOLTN_TYPE,
    Collection_Entry_Reason,
    Collection_outsrcg_type,
    Arears_Reason_Type,
    UNK,
    Collections_Stat_Type,
    FIN_REF_ENTITY_SUBTYPE_NM,
    FIN_REF_ENTITY_STAT_NM,
    ARMT_SK,
    CLECT_EPSD_CASE_SK,
    FIN_RLF_ARMT_SUBTYPE_CD,
    FIN_RLF_ARMT_STAT_CD,
    FIN_RLF_ARMT_STAT_TYPE_CD,
    COMRC_AVAIL_IND,
    FIN_RLF_ST_DT,
    FIN_RLF_END_DT,
    DDE_DUE_DATE,
    DDE_PAYMENT_DUE_DATE,
    DDE_CUST_ID,
    DDE_LITIGATION,
    DDE_LITIGATION_IND,
    DDE_NEXT_DUE_DATE,
    HARDSP_LOAN_VALN_RATIO,
    HARDSP_EXPIR_DT,
    OWNG_LEND_ARMT_APPLN_KEY_CRD,
    OWNG_LEND_ARMT_APPLN_KEY_DDA,
    OWNG_LEND_ARMT_APPLN_KEY_LIS,
    OWNG_LEND_ARMT_APPLN_KEY_INFLIS,
    KMRKEY,
    CLECT_BANK_ERROR_CD,
    CLECT_RECOV_STATUS_CD,
    BANK_ERROR_CODE,
    RECOVERY_STATUS_CODE,
    MIP_IND,
    REF_TYPE_SRC_CD,
    LIT_START_DT,
    MOD_DEBT_ACCOUNT_NUMBER,
    REL_SECURITY_NUMBER,
    TRN,
    VTRN,
    TBLNM,
    SECURITY_NUMBER,
    ACCT_NUM,
    TITLE_REFERENCE,
    PROPTY_ACQUISITION_DT,
    DDE_ACCT_ID,
    DDE_DTE_COLCTED,
    FMT_SECURITY_NUM,
    ACCT_BRND,
    STATE_CD,
    DESCRIPTION,
    STAT_DESC,
    DATE_OF_SALE,
    SALE_SETLMT_DT,
    SALE_AMT,
    LST_VALN_DT,
    CURNT_LOW_VALN_AMT,
    THRESHOLD_ODATE_RDE,
    RDE_APPLN_KEY,
    SECURITY_KEY,
    STREET_ADDR,
    WORKLIST_WORKFLOW_SK,
    EPISODE_START,
    EPISODE_END,
    RAMS_APPLN_KEY,
)
from .constants import (
    DdepSCDColumns,
    TempCredclectsepsd,
    TempDdaclectsepsd,
    TempListclectsepsd,
    Tempb2kclectsepsd,
    RdeCsAccount,
    RdeAcctActivities,
    RdeAccounts,
    RdeHardship,
)
from .constants import PrmsToPay, HarmRef, TempHostId, CollectEpi, Worklist, WorklistPcc
from .constants import TmpPreprocessB2kclctepsd, KmrHostidKeys, ErdData
from .constants import TmpPreprocessLisclctepsd, TmpPreprocessDdaclctepsd, TmpPreprocessCrdclctepsd
from .constants import CMTAccts, FinRlfArmtCMT, TempInflsPreKMR

def get_source_clect_epsd_tcs_df(df):
    # Populate Harmonized column
    target_df = df.alias(CollectEpi.ALIAS.value).withColumn(
        CollectEpi.BANK_ERR_CD.value,
        when(
            (
                (upper(col(CollectEpi.DDE_COMPLAINTS_CUST_RELTNS.value)) == "Y")
                & (upper(col(CollectEpi.DDE_SGB_FOS.value)) != "Y")
            ),
            "I",
        )
        .when(upper(col(CollectEpi.DDE_SGB_FOS.value)) == "Y", "O")
        .otherwise(None),
    )
    return target_df


def create_static_dataframe():
    """
    create a static dataframe used for fdp048 b2k and tcs tables
    :return : schema,data
    """
    schema = StructType(
        [
            StructField("clect_outsrcg_type_cd", StringType(), False),
            StructField("clect_outsrcg_type_desc", StringType(), False),
        ]
    )

    # calling the static method to get the static values for below dataframe
    # constant = Constant()
    # Defining Hardcode values which is required for manual load into
    # fdp048_clect_outsrcg_type REF table
    data = [
        (ClectoutsrcgType.D.value, ClectoutsrcgType.OUTSOURCE_DECLINED.value),
        (ClectoutsrcgType.A.value, ClectoutsrcgType.OUTSOURCE_READY.value),
        (ClectoutsrcgType.O.value, ClectoutsrcgType.OUTSOURCE_CAUSE.value),
        (ClectoutsrcgType.X.value, ClectoutsrcgType.OUTSOURCE_EXCLUDE.value),
        (ClectoutsrcgType.E.value, ClectoutsrcgType.OUTSOURCE_EARLY.value),
    ]
    return schema, data


def create_static_dataframe_prms():
    """
    create a static dataframe used for fdp048 b2k and tcs tables
    :return : schema,data
    """
    schema = StructType(
        [
            StructField("prms_stat_type_cd", StringType(), False),
            StructField("prms_stat_type_desc", StringType(), False),
        ]
    )

    # calling the static method to get the static values for below dataframe
    # constant = Constant()
    # Defining Hardcode values which is required for manual load into
    # fdp048_prms_stat_type REF table
    data = [
        (PrmsStatType.B.value, PrmsStatType.PROMISE_BROKEN.value),
        (PrmsStatType.K.value, PrmsStatType.PROMISE_KEPT.value),
        (PrmsStatType.O.value, PrmsStatType.PROMISE_OPEN.value),
        (PrmsStatType.N.value, PrmsStatType.PROMISE_NO.value),
    ]
    return schema, data


def create_static_dataframe_recov():
    """
    create a static dataframe used for fdp048 b2k and tcs tables
    :return : schema,data
    """
    schema = StructType(
        [
            StructField("recov_outcm_type_cd", StringType(), False),
            StructField("recov_outcm_type_desc", StringType(), False),
        ]
    )

    # calling the static method to get the static values for below dataframe
    # constant = Constant()
    # Defining Hardcode values which is required for manual load into
    # fdp048_recov_outcm_type REF table
    data = [
        (RecovOutcmType.WOFF.value, RecovOutcmType.LTG_SHORTFALL.value),
        (RecovOutcmType.WOF2.value, RecovOutcmType.LIT_SHORTFALL.value),
    ]
    return schema, data


def create_static_dataframe_rlshp_type():
    """
    create a static dataframe used for fdp048 b2k and tcs tables
    :return : schema,data
    """
    schema = StructType(
        [
            StructField("fin_rlf_armt_rlshp_type_cd", StringType(), False),
            StructField("fin_rlf_armt_rlshp_type_desc", StringType(), False),
        ]
    )

    # calling the static method to get the static values for below dataframe
    # constant = Constant()
    # Defining Hardcode values which is required for manual load into
    # fdp048_fin_rlf_armt_rlshp_type REF table
    data = [
        (RlshpType.FRACE.value, RlshpType.RLSHP_DESC.value),
    ]
    return schema, data


def create_static_dataframe_fin_sol_type():
    """
    create a static dataframe used for fdp048 b2k and tcs tables.
    :return : schema, data
    """
    schema = StructType(
        [
            StructField("fin_soltn_type_cd", StringType(), False),
            StructField("fin_soltn_type_desc", StringType(), False),
        ]
    )

    # calling the static method to get the static values for below dataframes
    # constant = Constant()
    # Defining Hardcoded values which is required for manual load into
    # fdp048_fin_soltn_type REF table
    data = [
        (FinSolType.Y.value, FinSolType.ASSIST_APPLICATION.value),
        (FinSolType.C.value, FinSolType.ASSIST_APPLICATION.value),
        (FinSolType.M.value, FinSolType.ASSIST_MONITOR.value),
        (FinSolType.A.value, FinSolType.FS_APPROVED.value),
        (FinSolType.BLANK.value, FinSolType.NO_ASSIST.value),
        (FinSolType.N.value, FinSolType.NO_ASSIST.value),
        (FinSolType.X.value, FinSolType.ASSIST_WRITEOFF.value),
        (FinSolType.W.value, FinSolType.ASSIST_SOFT_CHARGE_OFF.value),
        (FinSolType.A0.value, FinSolType.ASSISTANCE_LEVEL.value),
        (FinSolType.DC.value, FinSolType.FS_DECEASED.value),
        (FinSolType.DS.value, FinSolType.DISASTER_SHORTFALL.value),
    ]
    return schema, data


# creating scd dataframe
def get_scd_target_df(
    framework_context, atlas, config, logger, session, dataframes, df_target, src_system, juris_cntry_cd
):
    """
    Obtain target scd target dataframe by performing perform_scd function
    :param framework_context: Framework context object including '.args' property containing all command line arguments.
    :param atlas: Atlas service instance.
    :param config: Configuration service instance.
    :param logger: Logger from 'logging' module with both Console and File handlers attached.
    :param session: Apache Spark session object.
    :param dataframes: Dictionary of named Spark dataframe loaded from the defined inputs metadata attribute.
    :param df_target: Final dataframe.
    :return: A target scd Dataframe object after perform scd.
    """
    logger.info("Initiating SCD process for Current load")

    scd_df_target = perform_scd(
        framework_context=framework_context,
        atlas=atlas,
        config=config,
        logger=logger,
        session=session,
        dataframes=dataframes,
        input_dataframe=df_target,
        source_system=src_system,
        jurisdiction_country_code=juris_cntry_cd,
        odate=None,
    )
    return scd_df_target


# get SCD dataframe for History
def get_scd_target_hist_df(
    framework_context,
    atlas,
    config,
    logger,
    session,
    dataframes,
    input_df,
    src_system,
    juris_cntry_cd,
    target_name,
    pk_cols=None,
):
    """
    Obtain target scd target dataframe by performing perform_scd function
    :param framework_context: Framework context object including '.args' property containing all command line arguments.
    :param atlas: Atlas service instance.
    :param config: Configuration service instance.
    :param logger: Logger from 'logging' module with both Console and File handlers attached.
    :param session: Apache Spark session object.
    :param dataframes: Dictionary of named Spark dataframe loaded from the defined inputs metadata attribute.
    :param input_df: Final dataframe.
    :param src_system: Source system name.
    :param juris_cntry_cd: Jurisdiction country code.
    :param load_type: Load type.
    :param pk_cols: Primary key columns list
    :return: A target scd Dataframe object after perform scd.
    """

    logger.info("Initiating SCD process for History load")

    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    bdp_partition = SRC_BDP_PARTITION
    clect_epsd_win = Window.partitionBy(*pk_cols).orderBy(bdp_partition)
    non_hash_cols = pk_cols + [bdp_partition]
    hash_cols = sorted([col for col in input_df.columns if col not in non_hash_cols])

    target_scd_df = (
        input_df
        # DDEP start date
        .withColumn(DdepSCDColumns.ST_DT.value, f.col(bdp_partition))
        # DDEP end date
        .withColumn(
            DdepSCDColumns.END_DT.value,
            f.coalesce(
                f.date_sub(f.lead(bdp_partition).over(clect_epsd_win), 1),
                f.lit("2999-12-31").cast("date"),
            ),
        )
        # DDEP record deleted indicator
        .withColumn(DdepSCDColumns.DDEP_RCRD_DELET_IND.value, f.lit("0"))
        # DDEP Hash
        .withColumn(
            DdepSCDColumns.DDEP_HASH.value,
            # hash for all columns except DDPESCD  and bdp_partition column
            f.hash(*hash_cols).cast("string"),
        )
        # DDEP start timestamp
        .withColumn(
            DdepSCDColumns.DDEP_ST_TS.value,
            f.lit(current_timestamp).cast("timestamp"),
        )
        # DDEP end timestamp
        .withColumn(
            DdepSCDColumns.DDEP_END_TS.value,
            f.lit(current_timestamp).cast("timestamp"),
        )
        # Jurisdiction country code
        .withColumn(DdepSCDColumns.JURIS_CNTRY_CD.value, f.lit(juris_cntry_cd))
        # Source system id
        .withColumn(DdepSCDColumns.SRC_SYS_APPLN_ID.value, f.lit(src_system))
        # DDEP active indicator
        .withColumn(
            DdepSCDColumns.ACTIVE_IND.value,
            f.when(f.col(DdepSCDColumns.END_DT.value) == "2999-12-31", "Y").otherwise("N"),
        )
        # drop bdp_partition columns
        .drop(bdp_partition)
    )

    remote_backup_path = os.path.join(
        os.path.abspath("/"),
        framework_context.environment,
        "fdp",
        "fdp048",
        "fdp048_snapshot",
        target_name,
    )

    remote_backup_path_odate = os.path.join(
        remote_backup_path,
        DdepSCDColumns.JURIS_CNTRY_CD.value + "=" + juris_cntry_cd,
        DdepSCDColumns.SRC_SYS_APPLN_ID.value + "=" + src_system,
        framework_context.odate,
    )

    target_scd_df.filter(DdepSCDColumns.ACTIVE_IND.value + " = 'Y'").drop(
        DdepSCDColumns.JURIS_CNTRY_CD.value, DdepSCDColumns.SRC_SYS_APPLN_ID.value, DdepSCDColumns.ACTIVE_IND.value
    ).repartition(20).write.mode("overwrite").orc(
        remote_backup_path_odate + "/" + DdepSCDColumns.ACTIVE_IND.value + "=Y"
    )

    target_scd_df.filter(DdepSCDColumns.ACTIVE_IND.value + " = 'N'").drop(
        DdepSCDColumns.JURIS_CNTRY_CD.value, DdepSCDColumns.SRC_SYS_APPLN_ID.value, DdepSCDColumns.ACTIVE_IND.value
    ).repartition(20).write.mode("overwrite").orc(
        remote_backup_path_odate + "/" + DdepSCDColumns.ACTIVE_IND.value + "=N"
    )

    target_scd_dict = {target_name: target_scd_df}

    return target_scd_dict


def union_dataframes(*dfs):
    """
    Obtain combined zone1 and zone2 source data in dataframe format.
    :param dataframes:
    :param usa_table:
    :param nonusa_table:
    :return df_table
    """

    result = dfs[0]
    for df in dfs[1:]:
        result = result.union(df)

    return result


def get_collections_ref_data(session, df_collections_ref_data, entity_name, code, description):
    df_target1 = df_collections_ref_data.filter(col(RefDataApplnMap.entity_name) == entity_name).select(
        col(RefDataApplnMap.HARMONIZED_CODE).alias(code),
        col(RefDataApplnMap.HARMONIZED_DESC).alias(description),
    )

    df_target1 = df_target1.withColumn(PRV_SYS_APPLN_ID, lit(None))
    new_schema = StructType(
        [
            StructField(code, StringType(), False),
            StructField(description, StringType(), False),
            StructField(PRV_SYS_APPLN_ID, StringType(), True),
        ]
    )
    spark = session.builder.getOrCreate()
    new_row = spark.createDataFrame([("UNK", "Unknown", None)], new_schema)
    df_target2 = df_target1.union(new_row)
    df_target = df_target2.dropDuplicates([code])
    return df_target


# Generate Financial Relief Start Date for Collection Episode
def generate_fin_rlf_st_dt(df, df_activity, fin_rlf_cols_dict, fin_rlf_hrdshp_cds=["A", "C"]):
    """
    Calculate Financial Relief Start Date for input dataframe.
    :param dataframes: df
    :return df_target
    """
    # parse dictionary
    acct_id = fin_rlf_cols_dict["acct_id"]
    clect_curr_dt = fin_rlf_cols_dict["clect_curr_dt"]
    hrdshp = fin_rlf_cols_dict["hrdshp"]
    clect_rec_stat = fin_rlf_cols_dict["clect_rec_stat"]
    colcted_dt = fin_rlf_cols_dict["colcted_dt"]
    fin_rlf_st_dt = fin_rlf_cols_dict["fin_rlf_st_dt"]
    fin_rlf_end_dt = fin_rlf_cols_dict["fin_rlf_end_dt"]
    curr_hrdshp = "curr_hardship_code"
    prv_hrdshp = "prv_hardship_code"
    min_hrdshp_dt = "min_hardship_dt"
    bdp_partition = "bdp_partition"
    acty_acct_id = fin_rlf_cols_dict["acty_acct_id"]
    acty_cmp_dt = fin_rlf_cols_dict["acty_cmp_dt"]
    acty_cmp_time = fin_rlf_cols_dict["acty_cmp_time"]
    acty_acty_cd = fin_rlf_cols_dict["acty_acty_cd"]
    acty_codes = "dde_acty_cds"
    lv_hrdshp_dt = "lv_hrdshp_dt"
    temp_fin_rlf_dt = "temp_fin_rlf_dt"
    rhaa_acty_ind = "rhaa_acivity_ind"
    DDE_ACCT_ID_CLD = "DDE_ACCT_ID_CLD"
    DDE_ACCT_ID_ACT = "DDE_ACCT_ID_ACT"
    litind = fin_rlf_cols_dict["litind"]
    lit_start_dt = fin_rlf_cols_dict["lit_start_dt"]
    prv_litind = "prv_litind"

    clect_epsd_win = Window.partitionBy(f.col(acct_id), f.col(clect_curr_dt))
    acty_win = Window.partitionBy(f.trim(f.col(acty_acct_id)), f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"))
    df_activity = (
        df_activity.withColumn("rn", f.row_number().over(acty_win.orderBy(f.desc(acty_cmp_dt), f.desc(acty_cmp_time))))
        .withColumn(
            acty_codes,
            f.collect_set(f.trim(f.col(acty_acty_cd))).over(acty_win.orderBy(f.col(acty_cmp_dt), f.col(acty_cmp_time))),
        )
        .withColumn(rhaa_acty_ind, f.array_contains(acty_codes, "RHAA"))
        .filter(f.col("rn") == 1)
        .drop("rn")
    )

    # print("\n***** Printing df_activity dataframe *****\n ")
    # df_activity.show(20, truncate=False)
    # print("\n###Count of df_activity DF is {}\n".format(df_activity.count()))

    # preprocess input dataframe
    pre_prc_df = (
        df.withColumn(
            bdp_partition,
            f.from_unixtime(
                f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"
            ),
        )
        .withColumn(curr_hrdshp, f.when(f.trim(f.col(hrdshp)).isin(fin_rlf_hrdshp_cds), "A").otherwise("N"))
        .withColumn(
            prv_hrdshp,
            f.when(
                (f.lead(f.trim(f.col(hrdshp))).over(clect_epsd_win.orderBy(f.desc(bdp_partition)))).isin(
                    fin_rlf_hrdshp_cds
                ),
                "A",
            )
            .when((f.lead(f.trim(f.col(hrdshp))).over(clect_epsd_win.orderBy(f.desc(bdp_partition)))).isNull(), None)
            .otherwise("N"),
        )
        .withColumn(
            min_hrdshp_dt,
            f.min(
                f.when(
                    f.trim(f.col(hrdshp)).isin(fin_rlf_hrdshp_cds),
                    f.when(
                        f.datediff(f.col(bdp_partition), f.lit(THRESHOLD_ODATE)) > 0, f.col(bdp_partition)
                    ).otherwise(None),
                ).otherwise(None)
            ).over(clect_epsd_win),
        )
        .withColumn(
            prv_litind,
            f.when(
                (f.lead(f.trim(f.col(litind))).over(clect_epsd_win.orderBy(f.desc(bdp_partition)))).isNotNull(),
                f.lit("Y"),
            ).otherwise("N"),
        )
        .withColumn(
            lit_start_dt,
            f.when(
                ((f.col(litind) == "Y") & (f.coalesce(f.col(prv_litind), f.lit("N")) != "Y")),
                f.col(bdp_partition),
            )
            .when(
                ((f.col(litind) == "Y") & (f.col(prv_litind) == "Y")),
                f.col(min_hrdshp_dt),
            )
            .otherwise(None),
        )
        .withColumn(
            lv_hrdshp_dt,
            f.collect_list(
                f.when(
                    ((f.col(curr_hrdshp) == "A") & (f.col(prv_hrdshp) != "A")),
                    f.when(
                        f.datediff(f.col(bdp_partition), f.lit(THRESHOLD_ODATE)) > 0, f.col(bdp_partition)
                    ).otherwise(None),
                ).otherwise(None)
            ).over(clect_epsd_win.orderBy(f.asc(bdp_partition))),
        )
    )

    pre_prc_df = pre_prc_df.withColumn(DDE_ACCT_ID_CLD, f.trim(f.col(acct_id)))
    df_activity = df_activity.withColumn(DDE_ACCT_ID_ACT, f.trim(f.col(acty_acct_id)))
    activty_var = "activity1."
    pre_prc_df = (
        pre_prc_df.alias("pre_prc_df1")
        .join(
            df_activity.alias("activity1"),
            (
                (f.col("pre_prc_df1." + DDE_ACCT_ID_CLD) == f.col(activty_var + DDE_ACCT_ID_ACT))
                & (pre_prc_df.bdp_year == df_activity.bdp_year)
                & (pre_prc_df.bdp_month == df_activity.bdp_month)
                & (pre_prc_df.bdp_day == df_activity.bdp_day)
            ),
            LEFT,
        )
        .select("pre_prc_df1.*", activty_var + acty_codes, activty_var + rhaa_acty_ind)
    )

    # calculate fin_rlf_st_dt in target df
    df_target = (
        pre_prc_df.withColumn(
            temp_fin_rlf_dt,
            f.to_date(
                f.when(
                    ((f.col(curr_hrdshp) == "A") & (f.col(prv_hrdshp) != "A") & (f.size(f.col(lv_hrdshp_dt)) <= 1)),
                    f.when(
                        f.datediff(f.col(min_hrdshp_dt), f.lit(THRESHOLD_ODATE)) > 0, *[f.col(lv_hrdshp_dt).getItem(0)]
                    ).otherwise(None),
                )
                .when(
                    (
                        (f.col(curr_hrdshp) == "A")
                        & (f.col(prv_hrdshp) != "A")
                        & (f.coalesce(f.col(rhaa_acty_ind), f.lit(False)) == 1)
                        & (f.size(f.col(lv_hrdshp_dt)) > 1)
                    ),
                    f.when(
                        f.datediff(f.col(min_hrdshp_dt), f.lit(THRESHOLD_ODATE)) > 0,
                        *[f.col(lv_hrdshp_dt).getItem(f.size(f.col(lv_hrdshp_dt)) - 1)]
                    ).otherwise(None),
                )
                .when(
                    (
                        (f.col(curr_hrdshp) == "A")
                        & (f.col(prv_hrdshp) != "A")
                        & (f.coalesce(f.col(rhaa_acty_ind), f.lit(False)) != 1)
                        & (f.size(f.col(lv_hrdshp_dt)) > 1)
                    ),
                    f.when(
                        f.datediff(f.col(min_hrdshp_dt), f.lit(THRESHOLD_ODATE)) > 0,
                        *[f.col(lv_hrdshp_dt).getItem(f.size(f.col(lv_hrdshp_dt)) - 1 - 1)]
                    ).otherwise(None),
                )
            ),
        )
        .withColumn(
            fin_rlf_st_dt,
            f.to_date(
                f.when(
                    ((f.col(curr_hrdshp) == "A") & (f.col(prv_hrdshp) != "A") & (f.size(f.col(lv_hrdshp_dt)) <= 1)),
                    f.when(
                        f.datediff(f.col(min_hrdshp_dt), f.lit(THRESHOLD_ODATE)) > 0, *[f.col(lv_hrdshp_dt).getItem(0)]
                    ).otherwise(None),
                )
                .when(
                    (
                        (f.col(curr_hrdshp) == "A")
                        & (f.col(prv_hrdshp) != "A")
                        & (f.coalesce(f.col(rhaa_acty_ind), f.lit(False)) == 1)
                        & (f.size(f.col(lv_hrdshp_dt)) > 1)
                    ),
                    f.when(
                        f.datediff(f.col(min_hrdshp_dt), f.lit(THRESHOLD_ODATE)) > 0,
                        *[f.col(lv_hrdshp_dt).getItem(f.size(f.col(lv_hrdshp_dt)) - 1)]
                    ).otherwise(None),
                )
                .when(
                    (
                        (f.col(curr_hrdshp) == "A")
                        & (f.col(prv_hrdshp) != "A")
                        & (f.coalesce(f.col(rhaa_acty_ind), f.lit(False)) != 1)
                        & (f.size(f.col(lv_hrdshp_dt)) > 1)
                    ),
                    f.when(
                        f.datediff(f.col(min_hrdshp_dt), f.lit(THRESHOLD_ODATE)) > 0,
                        *[f.col(lv_hrdshp_dt).getItem(f.size(f.col(lv_hrdshp_dt)) - 1 - 1)]
                    ).otherwise(None),
                )
                .when(
                    ((f.col(curr_hrdshp) == "A") & (f.col(prv_hrdshp) == "A")),
                    f.when(
                        f.datediff(f.col(min_hrdshp_dt), f.lit(THRESHOLD_ODATE)) > 0,
                        f.last(temp_fin_rlf_dt, True).over(clect_epsd_win.orderBy(f.asc(bdp_partition))),
                    ).otherwise(None),
                )
                .when(
                    ((f.col(curr_hrdshp) != "A") & (f.col(prv_hrdshp) == "A")),
                    f.when(
                        f.datediff(f.col(min_hrdshp_dt), f.lit(THRESHOLD_ODATE)) > 0,
                        f.last(temp_fin_rlf_dt, True).over(clect_epsd_win.orderBy(f.asc(bdp_partition))),
                    ).otherwise(None),
                )
                .otherwise(None)
            ),
        )
        .withColumn(
            fin_rlf_end_dt,
            f.to_date(
                f.when(
                    (
                        (f.col(fin_rlf_st_dt).isNotNull())
                        & (f.col(curr_hrdshp) != "A")
                        & (f.col(prv_hrdshp) == "A")
                        & (f.trim(f.coalesce(f.col(clect_rec_stat), f.lit(""))) == "")
                    ),
                    f.col(bdp_partition),
                )
                .when(
                    (
                        (
                            (f.col(fin_rlf_st_dt).isNotNull())
                            & (f.col(curr_hrdshp) == "A")
                            & (f.trim(f.coalesce(f.col(clect_rec_stat), f.lit(""))) != "")
                        )
                        | (
                            (f.col(fin_rlf_st_dt).isNotNull())
                            & (f.col(curr_hrdshp) != "A")
                            & (f.col(prv_hrdshp) == "A")
                            & (f.trim(f.coalesce(f.col(clect_rec_stat), f.lit(""))) != "")
                        )
                    ),
                    f.col(colcted_dt),
                )
                .otherwise(None)
            ),
        )
        .drop(curr_hrdshp, prv_hrdshp, min_hrdshp_dt, lv_hrdshp_dt, temp_fin_rlf_dt, rhaa_acty_ind, DDE_ACCT_ID_CLD)
    )
    # print("\n***** Printing df_target dataframe for generate_fin_rlf_st_dt() *****\n ")
    # df_target.show(20, truncate=False)
    # print("\n###Count of df_target DF is {}\n".format(df_target.count()))
    return df_target


def generate_harmonized_code(df, harm_ref_df, col_dict):
    # Parse dict
    src_alias = col_dict["alias"]
    app_id = col_dict["app_id"]
    entity_name = col_dict["entity_name"]
    ref_src_cd = col_dict["ref_src_cd"]
    target_harm_cd = col_dict["target_harm_cd"]
    # Applying Filter for Harmonized data frame on entity name and Application Key
    fil_harm_df = harm_ref_df.alias(HarmRef.ALIAS.value).filter(
        (trim(upper(col(HarmRef.REF_ENTITY_NM.value))) == entity_name.upper())
        & (trim(upper(col(HarmRef.IT_APPLN_KEY.value))) == app_id)
        & (trim(upper(col(HarmRef.ACTIVE_IND.value))) == "Y")
    )
    # Joining input source df and filtered harmonized df on REF_SRC_CD
    merge_df = df.alias(src_alias).join(
        fil_harm_df.alias(HarmRef.ALIAS.value),
        (trim(upper(col(HarmRef.REF_TYPE_SRC_CD.value))) == trim(upper(col(ref_src_cd)))),
        LEFT,
    )
    # Populate Harmonized column
    target_df = merge_df.withColumn(
        target_harm_cd,
        when(col(ref_src_cd).isNull(), None)
        .when(col(HarmRef.REF_TYPE_CD.value).isNull() & col(ref_src_cd).isNotNull(), "UNK")
        .otherwise(col(HarmRef.REF_TYPE_CD.value)),
    )
    return target_df


# dedup of dataframe
def get_dedup_df(df, cols):
    """
    Deduplicate the input spark dataframe on given colum list
    """
    # concat all dedup columns and generate hash
    df = df.withColumn("dedup_hash", f.hash(*cols))

    bdp_cols = ["bdp_year", "bdp_month", "bdp_day"]
    win = Window.partitionBy("dedup_hash").orderBy(f.concat_ws("-", *bdp_cols).asc())

    # get latest record based on bdp partition
    dedup_df = df.withColumn("rn", f.row_number().over(win)).filter(f.col("rn") == 1).drop("rn", "dedup_hash")

    # or drop duplicates on columns
    # dedup_df = df.dropDuplicates(cols, keep="last")

    return dedup_df


def get_target_recov_b2k_df(df_b2k_clects_epsd, df_harm_ref, df_temp_host_id, df_b2k_mip, order_date):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :param df_b2k_clects_epsd:
    :param df_harm_ref:
    :return df_target:
    """
    df_b2k_mip = df_b2k_mip.filter(f.upper(col(MIPB2K.ACCT_BRND.value)).isin(["WESTPAC"]))

    df_b2k_clects_epsd = df_b2k_clects_epsd.withColumn(
        MIPB2K.ACC_ID.value,
        when(
            f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value) == "MSS",
            f.regexp_replace(f.trim(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value)), "^(\\w{2})", ""),
        ).otherwise(f.trim(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value))),
    )

    clect_recov_type_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": B2K_APPLN_KEY,
        "entity_name": RECOVERY_TYPE_CODE,
        "ref_src_cd": CollectEpi.DDE_RECOVERY_CODE.value,
        "target_harm_cd": CLECT_RECOV_TYPE_CD,
    }

    clect_recov_type = generate_harmonized_code(df_b2k_clects_epsd, df_harm_ref, clect_recov_type_dict)
    clect_recov_type_b2k_list = (
        clect_recov_type.select(col(REF_TYPE_SRC_CD)).distinct().rdd.flatMap(lambda x: x).collect()
    )

    clect_recov_stat_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": B2K_APPLN_KEY,
        "entity_name": RECOVERY_STATUS_CODE,
        "ref_src_cd": CollectEpi.DDE_RECOVERY_CODE.value,
        "target_harm_cd": CLECT_RECOV_STATUS_CD,
    }

    df_clect_stat_type = generate_harmonized_code(clect_recov_type, df_harm_ref, clect_recov_stat_dict)

    df_target = (
        df_clect_stat_type.join(
            df_temp_host_id.alias(TempHostId.ALIAS.value),
            (trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value)) == trim(col(CollectEpi.CLECT_EPSD_HOST_ID_CE.value))),
            INNER,
        )
        .join(
            df_b2k_mip,
            (col(MIPB2K.ACC_ID.value) == trim(col(MIPB2K.ACCT_NUM.value)))
            & (col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value) <= col(MIPB2K.PROPTY_ACQUISITION_DT.value)),
            LEFT,
        )
        .filter(col(CollectEpi.DDE_RECOVERY_CODE.value).isin(clect_recov_type_b2k_list))
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            CLECT_RECOV_TYPE_CD,
            CLECT_RECOV_STATUS_CD,
            lit("A0077D:A001F5").alias(PRV_SYS_APPLN_ID),
            col(CollectEpi.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    return df_target


def get_target_recov_tcs_crds_df(df_tcs_clects_epsd, df_harm_ref, df_temp_host_id):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :param df_b2k_clects_epsd:
    :param df_harm_ref:
    :return df_target:
    """
    clect_recov_type_dict_crds = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": RECOVERY_TYPE_CODE,
        "ref_src_cd": CollectEpi.DDE_RECOVERY_STAT_CODE.value,
        "target_harm_cd": CLECT_RECOV_TYPE_CD,
    }

    clect_recov_type_crds = generate_harmonized_code(df_tcs_clects_epsd, df_harm_ref, clect_recov_type_dict_crds)
    clect_recov_type_tcs_list_crds = (
        clect_recov_type_crds.select(col(REF_TYPE_SRC_CD)).distinct().rdd.flatMap(lambda x: x).collect()
    )

    clect_recov_stat_dict_crds = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": RECOVERY_STATUS_CODE,
        "ref_src_cd": CollectEpi.DDE_RECOVERY_STAT_CODE.value,
        "target_harm_cd": CLECT_RECOV_STATUS_CD,
    }

    df_clect_stat_type_crds = generate_harmonized_code(clect_recov_type_crds, df_harm_ref, clect_recov_stat_dict_crds)

    df_target = (
        df_clect_stat_type_crds.join(
            df_temp_host_id.alias(TempHostId.ALIAS.value),
            (trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value)) == trim(col(CollectEpi.CLECT_EPSD_HOST_ID_CE.value))),
            INNER,
        )
        .filter(col(CollectEpi.DDE_RECOVERY_STAT_CODE.value).isin(clect_recov_type_tcs_list_crds))
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            CLECT_RECOV_TYPE_CD,
            CLECT_RECOV_STATUS_CD,
            lit("A0077D:A0030A").alias(PRV_SYS_APPLN_ID),
            col(CollectEpi.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    return df_target


def get_target_recov_tcs_dda_df(df_tcs_clects_epsd, df_harm_ref, df_temp_host_id, df_tcs_mip, order_date):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :param df_b2k_clects_epsd:
    :param df_harm_ref:
    :return df_target:
    """
    df_tcs_mip = df_tcs_mip.filter(
        f.upper(col(MIPTCS.ACCT_BRND_TCS.value)).isin(["ST GEORGE", "BANKSA", "BANK OF MELBOURNE"])
    )

    df_tcs_clects_epsd = df_tcs_clects_epsd.withColumn(
        MIPTCS.ACC_ID_TCS.value,
        f.lpad(f.trim(f.col(TempDdaclectsepsd.DDE_ACCT_ID_DDA.value)), 15, "0"),
    )

    clect_recov_type_dict_dda = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": RECOVERY_TYPE_CODE,
        "ref_src_cd": CollectEpi.DDE_RECOVERY_STAT_CODE.value,
        "target_harm_cd": CLECT_RECOV_TYPE_CD,
    }

    clect_recov_type_dda = generate_harmonized_code(df_tcs_clects_epsd, df_harm_ref, clect_recov_type_dict_dda)
    clect_recov_type_tcs_list_dda = (
        clect_recov_type_dda.select(col(REF_TYPE_SRC_CD)).distinct().rdd.flatMap(lambda x: x).collect()
    )

    clect_recov_stat_dict_dda = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": RECOVERY_STATUS_CODE,
        "ref_src_cd": CollectEpi.DDE_RECOVERY_STAT_CODE.value,
        "target_harm_cd": CLECT_RECOV_STATUS_CD,
    }

    df_clect_stat_type_dda = generate_harmonized_code(clect_recov_type_dda, df_harm_ref, clect_recov_stat_dict_dda)

    df_target = (
        df_clect_stat_type_dda.join(
            df_temp_host_id.alias(TempHostId.ALIAS.value),
            (trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value)) == trim(col(CollectEpi.CLECT_EPSD_HOST_ID_CE.value))),
            INNER,
        )
        .join(
            df_tcs_mip,
            (col(MIPTCS.ACC_ID_TCS.value) == trim(col(MIPTCS.ACCT_NUM_TCS.value)))
            & (col(TempDdaclectsepsd.DDE_CURR_DT_ADD_DDA.value) <= col(MIPB2K.PROPTY_ACQUISITION_DT.value)),
            LEFT,
        )
        .filter(col(CollectEpi.DDE_RECOVERY_STAT_CODE.value).isin(clect_recov_type_tcs_list_dda))
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            CLECT_RECOV_TYPE_CD,
            CLECT_RECOV_STATUS_CD,
            lit("A0077D:A0030A").alias(PRV_SYS_APPLN_ID),
            col(CollectEpi.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    return df_target


def get_target_recov_tcs_lis_df(df_tcs_clects_epsd, df_harm_ref, df_temp_host_id, df_tcs_mip, order_date):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :param df_b2k_clects_epsd:
    :param df_harm_ref:
    :return df_target:
    """
    df_tcs_mip = df_tcs_mip.filter(
        f.upper(col(MIPTCS.ACCT_BRND_TCS.value)).isin(["ST GEORGE", "BANKSA", "BANK OF MELBOURNE"])
    )

    df_tcs_clects_epsd = df_tcs_clects_epsd.withColumn(
        MIPTCS.ACC_ID_TCS.value,
        f.regexp_replace(f.trim(f.col(TempListclectsepsd.DDE_ACCT_ID.value)), LIS_KMR_HOST, "100"),
    )
    # df_tcs_clects_epsd.printSchema()
    # df_tcs_clects_epsd.show(truncate=False)

    clect_recov_type_dict_lis = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": RECOVERY_TYPE_CODE,
        "ref_src_cd": CollectEpi.DDE_RECOVERY_STAT_CODE.value,
        "target_harm_cd": CLECT_RECOV_TYPE_CD,
    }

    clect_recov_type_lis = generate_harmonized_code(df_tcs_clects_epsd, df_harm_ref, clect_recov_type_dict_lis)
    clect_recov_type_tcs_list_lis = (
        clect_recov_type_lis.select(col(REF_TYPE_SRC_CD)).distinct().rdd.flatMap(lambda x: x).collect()
    )
 
   
    clect_recov_stat_dict_lis = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": RECOVERY_STATUS_CODE,
        "ref_src_cd": CollectEpi.DDE_RECOVERY_STAT_CODE.value,
        "target_harm_cd": CLECT_RECOV_STATUS_CD,
    }

    df_clect_stat_type_lis = generate_harmonized_code(clect_recov_type_lis, df_harm_ref, clect_recov_stat_dict_lis)

    # df_clect_stat_type_lis.printSchema()
    # df_clect_stat_type_lis.show(truncate=False)

    df_target = (
        df_clect_stat_type_lis.join(
            df_temp_host_id.alias(TempHostId.ALIAS.value),
            (trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value)) == trim(col(CollectEpi.CLECT_EPSD_HOST_ID_CE.value))),
            INNER,
        )
        .join(
            df_tcs_mip,
            (col(MIPTCS.ACC_ID_TCS.value) == trim(col(MIPTCS.ACCT_NUM_TCS.value)))
            & (col(TempListclectsepsd.DDE_CURR_DT_ADD.value) <= col(MIPB2K.PROPTY_ACQUISITION_DT.value)),
            LEFT,
        )
        .filter(col(CollectEpi.DDE_RECOVERY_STAT_CODE.value).isin(clect_recov_type_tcs_list_lis))
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            CLECT_RECOV_TYPE_CD,
            CLECT_RECOV_STATUS_CD,
            lit("A0077D:A0030A").alias(PRV_SYS_APPLN_ID),
            col(CollectEpi.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    return df_target


def get_crd_target_df(df_crd_clects_epsd, df_activity, odate, is_history_load=False):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :param df_crd_clects_epsd_today:
    :param df_crd_clects_epsd_yday:
    :return df_target:
    """
    column_list = [e.value for e in TempCredclectsepsd]

    dict_dates = {
        "acct_id": TempCredclectsepsd.DDE_ACCT_ID_CRED.value,
        "clect_curr_dt": TempCredclectsepsd.DDE_CURR_DT_ADD_CRED.value,
        "hrdshp": TempCredclectsepsd.DDE_VP_ASSIST_IND_CRED.value,
        "clect_rec_stat": TempCredclectsepsd.DDE_REC_STAT_CRED.value,
        "colcted_dt": TempCredclectsepsd.DDE_DTE_COLCTED_CRED.value,
        "fin_rlf_st_dt": DDE_FIN_RLF_ST_DT,
        "fin_rlf_end_dt": DDE_FIN_RLF_END_DT,
        "acty_acct_id": ActivityAcraamaa.DDE_ACCT_ID.value,
        "acty_cmp_dt": ActivityAcraamaa.DDE_CMP_DATE.value,
        "acty_cmp_time": ActivityAcraamaa.DDE_CMP_TIME.value,
        "acty_acty_cd": ActivityAcraamaa.DDE_ACTY_CD.value,
        "litind": TempCredclectsepsd.DDE_LITIGATION_CRED.value,
        "lit_start_dt": LIT_START_DT,
    }

    if not is_history_load:
        df_fin_st_dt = generate_fin_rlf_st_dt(df_crd_clects_epsd, df_activity, dict_dates, ["H", "X", "D"])
        df_fin_st_dt = df_fin_st_dt.filter(
            f.datediff(f.to_date(f.col("bdp_partition"), "yyyy-MM-dd"), f.lit(odate)) == 0
        )

    else:
        dedup_df_crd = get_dedup_df(df_crd_clects_epsd, column_list)
        df_fin_st_dt = generate_fin_rlf_st_dt(dedup_df_crd, df_activity, dict_dates, ["H", "X", "D"])
        df_fin_st_dt = df_fin_st_dt.filter(
            f.datediff(f.to_date(f.col("bdp_partition"), "yyyy-MM-dd"), f.lit(odate)) >= 0
        )

    df_target = (
        df_fin_st_dt.alias("ODate")
        .select(
            f.concat_ws(
                "",
                f.trim(f.col(TempCredclectsepsd.DDE_ACCT_ID_CRED.value)),
                f.when(
                    f.col(TempCredclectsepsd.DDE_CURR_DT_ADD_CRED.value).isNotNull(),
                    f.from_unixtime(
                        f.unix_timestamp(TempCredclectsepsd.DDE_CURR_DT_ADD_CRED.value, "yyyy-MM-dd"), "yyyyMMdd"
                    ),
                ).otherwise(None),
            ).alias(CLECT_EPSD_HOST_ID),
            f.when(
                f.col("DDE_FIN_RLF_ST_DT").isNotNull(),
                f.concat_ws(
                    "",
                    f.trim(f.col(TempCredclectsepsd.DDE_ACCT_ID_CRED.value)),
                    f.from_unixtime(f.unix_timestamp(DDE_FIN_RLF_ST_DT, "yyyy-MM-dd"), "yyyyMMdd"),
                ),
            )
            .otherwise(None)
            .alias(FIN_RLF_ARMT_HOST_ID),
            f.col(TempCredclectsepsd.DDE_21D3_LIST_AMNT_CRED.value).alias(DDE_21D3_LIST_AMNT),
            f.col(TempCredclectsepsd.DDE_ACCT_ID_CRED.value).alias(DDE_ACCT_ID),
            f.col(TempCredclectsepsd.DDE_AMT_TOWARDS_PROM_CRED.value).alias(DDE_AMT_TOWARDS_PROM),
            f.col(TempCredclectsepsd.DDE_ARREARS_REASON_CRED.value).alias(DDE_ARREARS_REASON),
            f.col(TempCredclectsepsd.DDE_AUTO_LTR_DAYS_CRED.value).alias(DDE_AUTO_LTR_DAYS),
            f.col(TempCredclectsepsd.DDE_COLL_ENTRY_REAS_CRED.value).alias(DDE_COLL_ENTRY_REAS),
            f.col(TempCredclectsepsd.DDE_CURR_DT_ADD_CRED.value).alias(DDE_CURR_DT_ADD),
            f.col(TempCredclectsepsd.DDE_OL_WRITEOFF_IND_CRED.value).alias(DDE_OL_WRITEOFF_IND),
            f.col(TempCredclectsepsd.DDE_DTE_COLCTED_CRED.value).alias(DDE_DTE_COLCTED),
            f.col(TempCredclectsepsd.DDE_DTE_LAST_REVW_DT_CRED.value).alias(DDE_DTE_LAST_REVW_DT),
            f.col(TempCredclectsepsd.DDE_HARDSHIP_INFO_EXP_DT_CRED.value).alias(DDE_HARDSHIP_INFO_EXP_DT),
            f.col(TempCredclectsepsd.DDE_HARDSHIP_LVR_CRED.value).alias(DDE_HARDSHIP_LVR),
            f.col(TempCredclectsepsd.DDE_OL_LRD_IND_CRED.value).alias(DDE_OL_LRD_IND),
            f.col(TempCredclectsepsd.DDE_PROM_AMT_CRED.value).alias(DDE_PROM_AMT),
            f.col(TempCredclectsepsd.DDE_PROM_DTE_CRED.value).alias(DDE_PROM_DTE),
            f.col(TempCredclectsepsd.DDE_PROM_SCHED_WK_IND_CRED.value).alias(DDE_PROM_SCHED_WK_IND),
            f.col(TempCredclectsepsd.DDE_PROM_STAT_CRED.value).alias(DDE_PROM_STAT),
            f.col(TempCredclectsepsd.DDE_REC_STAT_CRED.value).alias(DDE_REC_STAT),
            f.col(TempCredclectsepsd.DDE_RECOVERY_STAT_CODE_CRED.value).alias(DDE_RECOVERY_STAT_CODE),
            f.col(TempCredclectsepsd.DDE_SCO_PENDING_CRED.value).alias(DDE_SCO_PENDING),
            f.col(TempCredclectsepsd.DDE_SUPPRESS_LTR_CRED.value).alias(DDE_SUPPRESS_LTR),
            f.col(TempCredclectsepsd.DDE_SUPV_SUPR_DAYS_CRED.value).alias(DDE_SUPV_SUPR_DAYS),
            f.col(TempCredclectsepsd.DDE_VP_ASSIST_IND_CRED.value).alias(DDE_VP_ASSIST_IND),
            DDE_FIN_RLF_ST_DT,
            DDE_FIN_RLF_END_DT,
            f.trim(f.col(TempCredclectsepsd.DDE_ACCT_ID_CRED.value)).alias(LEND_ARMT_HOST_ID),
            f.col(TempCredclectsepsd.DDE_PROM_SCHEDULE_CRED.value).alias(DDE_PROM_SCHEDULE),
            f.col(TempCredclectsepsd.DDE_ASSISTANCE_LEVEL_CRED.value).alias(DDE_ASSISTANCE_LEVEL),
            f.to_date(f.col("bdp_partition"), "yyyy-MM-dd").alias(SRC_BDP_PARTITION),
            f.col(TempCredclectsepsd.DDE_COMPLAINTS_CUST_RELTNS_CRED.value).alias(DDE_COMPLAINTS_CUST_RELTNS),
            f.col(TempCredclectsepsd.DDE_SGB_FOS_CRED.value).alias(DDE_SGB_FOS),
            f.col(TempCredclectsepsd.DDE_COMMERCIAL_CRED.value).alias(DDE_COMMERCIAL),
            f.col(DDE_ACTY_CDS).alias(ACTY_CD),
            f.col(TempCredclectsepsd.DDE_DUE_DATE_CRED.value).alias(DDE_DUE_DATE),
            f.lpad(
                f.substring(
                    f.trim(f.regexp_extract(f.col(TempCredclectsepsd.DDE_CUST_ID_CRED.value), TCS_CUSTID_FORMAT, 1)),
                    -9,
                    9,
                ),
                15,
                "0",
            ).alias(DDE_CUST_ID),
            f.col(TempCredclectsepsd.DDE_LITIGATION_CRED.value).alias(DDE_LITIGATION),
            f.col(TempCredclectsepsd.DDE_APRA_CRED.value).alias(DDE_APRA),
            LIT_START_DT,
        )
        .dropDuplicates()
    )
    return df_target


def get_dda_target_df(df_dda_clects_epsd, df_activity, odate, is_history_load=False):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :param df_crd_clects_epsd_today:
    :param df_crd_clects_epsd_yday:
    :return df_target:
    """
    column_list = [e.value for e in TempDdaclectsepsd]

    dict_dates = {
        "acct_id": TempDdaclectsepsd.DDE_ACCT_ID_DDA.value,
        "clect_curr_dt": TempDdaclectsepsd.DDE_CURR_DT_ADD_DDA.value,
        "hrdshp": TempDdaclectsepsd.DDE_ARREARS_CODE_DDA.value,
        "clect_rec_stat": TempDdaclectsepsd.DDE_REC_STAT_DDA.value,
        "colcted_dt": TempDdaclectsepsd.DDE_DTE_COLCTED_DDA.value,
        "fin_rlf_st_dt": DDE_FIN_RLF_ST_DT,
        "fin_rlf_end_dt": DDE_FIN_RLF_END_DT,
        "acty_acct_id": ActivityAcraamaa.DDE_ACCT_ID.value,
        "acty_cmp_dt": ActivityAcraamaa.DDE_CMP_DATE.value,
        "acty_cmp_time": ActivityAcraamaa.DDE_CMP_TIME.value,
        "acty_acty_cd": ActivityAcraamaa.DDE_ACTY_CD.value,
        "litind": TempDdaclectsepsd.DDE_LITIGATION_DDA.value,
        "lit_start_dt": LIT_START_DT,
    }

    if not is_history_load:
        df_fin_st_dt = generate_fin_rlf_st_dt(df_dda_clects_epsd, df_activity, dict_dates, ["A", "I"])
        df_fin_st_dt = df_fin_st_dt.filter(
            f.datediff(f.to_date(f.col("bdp_partition"), "yyyy-MM-dd"), f.lit(odate)) == 0
        )

    else:
        dedup_df_dda = get_dedup_df(df_dda_clects_epsd, column_list)
        df_fin_st_dt = generate_fin_rlf_st_dt(dedup_df_dda, df_activity, dict_dates, ["A", "I"])
        df_fin_st_dt = df_fin_st_dt.filter(
            f.datediff(f.to_date(f.col("bdp_partition"), "yyyy-MM-dd"), f.lit(odate)) >= 0
        )

    df_target = (
        df_fin_st_dt.alias("ODate")
        .select(
            f.concat_ws(
                "",
                f.trim(f.col(TempDdaclectsepsd.DDE_ACCT_ID_DDA.value)),
                f.from_unixtime(
                    f.unix_timestamp(TempDdaclectsepsd.DDE_CURR_DT_ADD_DDA.value, "yyyy-MM-dd"), "yyyyMMdd"
                ),
            ).alias(CLECT_EPSD_HOST_ID),
            f.when(
                f.col("DDE_FIN_RLF_ST_DT").isNotNull(),
                f.concat_ws(
                    "",
                    f.trim(f.col(TempDdaclectsepsd.DDE_ACCT_ID_DDA.value)),
                    f.from_unixtime(f.unix_timestamp(DDE_FIN_RLF_ST_DT, "yyyy-MM-dd"), "yyyyMMdd"),
                ),
            )
            .otherwise(None)
            .alias(FIN_RLF_ARMT_HOST_ID),
            f.col(TempDdaclectsepsd.DDE_21D3_LIST_AMNT_DDA.value).alias(DDE_21D3_LIST_AMNT),
            f.col(TempDdaclectsepsd.DDE_ACCT_ID_DDA.value).alias(DDE_ACCT_ID),
            f.col(TempDdaclectsepsd.DDE_AMT_TOWARDS_PROM_DDA.value).alias(DDE_AMT_TOWARDS_PROM),
            f.col(TempDdaclectsepsd.DDE_ARREARS_REASON_DDA.value).alias(DDE_ARREARS_REASON),
            f.col(TempDdaclectsepsd.DDE_AUTO_LTR_DAYS_DDA.value).alias(DDE_AUTO_LTR_DAYS),
            f.col(TempDdaclectsepsd.DDE_COLL_ENTRY_REAS_DDA.value).alias(DDE_COLL_ENTRY_REAS),
            f.col(TempDdaclectsepsd.DDE_CURR_DT_ADD_DDA.value).alias(DDE_CURR_DT_ADD),
            f.col(TempDdaclectsepsd.DDE_OL_WRITEOFF_IND_DDA.value).alias(DDE_OL_WRITEOFF_IND),
            f.col(TempDdaclectsepsd.DDE_DTE_COLCTED_DDA.value).alias(DDE_DTE_COLCTED),
            f.col(TempDdaclectsepsd.DDE_DTE_LAST_REVW_DT_DDA.value).alias(DDE_DTE_LAST_REVW_DT),
            f.col(TempDdaclectsepsd.DDE_HARDSHIP_INFO_EXP_DT_DDA.value).alias(DDE_HARDSHIP_INFO_EXP_DT),
            f.col(TempDdaclectsepsd.DDE_HARDSHIP_LVR_DDA.value).alias(DDE_HARDSHIP_LVR),
            f.col(TempDdaclectsepsd.DDE_OL_LRD_IND_DDA.value).alias(DDE_OL_LRD_IND),
            f.col(TempDdaclectsepsd.DDE_PROM_AMT_DDA.value).alias(DDE_PROM_AMT),
            f.col(TempDdaclectsepsd.DDE_PROM_DTE_DDA.value).alias(DDE_PROM_DTE),
            f.col(TempDdaclectsepsd.DDE_PROM_SCHED_WK_IND_DDA.value).alias(DDE_PROM_SCHED_WK_IND),
            f.col(TempDdaclectsepsd.DDE_PROM_STAT_DDA.value).alias(DDE_PROM_STAT),
            f.col(TempDdaclectsepsd.DDE_REC_STAT_DDA.value).alias(DDE_REC_STAT),
            f.col(TempDdaclectsepsd.DDE_SCO_PENDING_DDA.value).alias(DDE_SCO_PENDING),
            f.col(TempDdaclectsepsd.DDE_SUPPRESS_LTR_DDA.value).alias(DDE_SUPPRESS_LTR),
            f.col(TempDdaclectsepsd.DDE_SUPV_SUPR_DAYS_DDA.value).alias(DDE_SUPV_SUPR_DAYS),
            f.col(TempDdaclectsepsd.DDE_ARREARS_CODE_DDA.value).alias(DDE_ARREARS_CODE),
            DDE_FIN_RLF_ST_DT,
            DDE_FIN_RLF_END_DT,
            f.lpad(f.trim(f.col(TempDdaclectsepsd.DDE_ACCT_ID_DDA.value)), 15, "0").alias(LEND_ARMT_HOST_ID),
            f.col(TempDdaclectsepsd.DDE_PROM_SCHEDULE_DDA.value).alias(DDE_PROM_SCHEDULE),
            f.col(TempDdaclectsepsd.DDE_ASSISTANCE_LEVEL_DDA.value).alias(DDE_ASSISTANCE_LEVEL),
            f.to_date(f.col("bdp_partition"), "yyyy-MM-dd").alias(SRC_BDP_PARTITION),
            f.col(TempDdaclectsepsd.DDE_COMPLAINTS_CUST_RELTNS_DDA.value).alias(DDE_COMPLAINTS_CUST_RELTNS),
            f.col(TempDdaclectsepsd.DDE_SGB_FOS_DDA.value).alias(DDE_SGB_FOS),
            f.col(TempDdaclectsepsd.DDE_COMMERCIAL_DDA.value).alias(DDE_COMMERCIAL),
            f.col(DDE_ACTY_CDS).alias(ACTY_CD),
            f.col(TempDdaclectsepsd.DDE_DUE_DATE_DDA.value).alias(DDE_DUE_DATE),
            f.lpad(
                f.substring(
                    f.trim(f.regexp_extract(f.col(TempDdaclectsepsd.DDE_CUST_ID_DDA.value), TCS_CUSTID_FORMAT, 1)),
                    -9,
                    9,
                ),
                15,
                "0",
            ).alias(DDE_CUST_ID),
            f.col(TempDdaclectsepsd.DDE_RECOVERY_STAT_CODE_DDA.value).alias(DDE_RECOVERY_STAT_CODE),
            f.col(TempDdaclectsepsd.DDE_LITIGATION_DDA.value).alias(DDE_LITIGATION),
            f.col(TempDdaclectsepsd.DDE_APRA_DDA.value).alias(DDE_APRA),
            LIT_START_DT,
        )
        .dropDuplicates()
    )

    return df_target


def get_lis_target_df(df_lis_clects_epsd, df_activity, a002f4_acct_loan_df, odate, is_history_load=False):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :return df_target:
    """
    column_list = [e.value for e in TempListclectsepsd]

    dict_dates = {
        "acct_id": TempListclectsepsd.DDE_ACCT_ID.value,
        "clect_curr_dt": TempListclectsepsd.DDE_CURR_DT_ADD.value,
        "hrdshp": TempListclectsepsd.DDE_ARREARS_CODE.value,
        "clect_rec_stat": TempListclectsepsd.DDE_REC_STAT.value,
        "colcted_dt": TempListclectsepsd.DDE_DTE_COLCTED.value,
        "fin_rlf_st_dt": DDE_FIN_RLF_ST_DT,
        "fin_rlf_end_dt": DDE_FIN_RLF_END_DT,
        "acty_acct_id": ActivityAcraamaa.DDE_ACCT_ID.value,
        "acty_cmp_dt": ActivityAcraamaa.DDE_CMP_DATE.value,
        "acty_cmp_time": ActivityAcraamaa.DDE_CMP_TIME.value,
        "acty_acty_cd": ActivityAcraamaa.DDE_ACTY_CD.value,
        "litind": TempListclectsepsd.DDE_LITIGATION.value,
        "lit_start_dt": LIT_START_DT,
    }

    a002f4_acct_loan_df = a002f4_acct_loan_df.filter(
        f.datediff(
            f.from_unixtime(
                f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"
            ),
            f.lit(THRESHOLD_ODATE),
        )
        >= 0
    )
    column_list_a002f4 = [e.value.split(".")[1] for e in HardShipDate if e.name != "ALIAS"]
    a002f4_acct_loan_df = a002f4_acct_loan_df.select(column_list_a002f4)

    ##print("## a002f4_acct_loan_df ##")
    ##a002f4_acct_loan_df.show(20,truncate=False)

    if not is_history_load:
        df_fin_st_dt = generate_fin_rlf_st_dt(df_lis_clects_epsd, df_activity, dict_dates, ["A"])
        df_fin_st_dt = df_fin_st_dt.filter(
            f.datediff(f.to_date(f.col("bdp_partition"), "yyyy-MM-dd"), f.lit(odate)) == 0
        )
        ##print("## df_fin_st_dt first ##")
        ##df_fin_st_dt.show(20,truncate=False)
    else:
        dedup_df_lis = get_dedup_df(df_lis_clects_epsd, column_list)
        df_fin_st_dt = generate_fin_rlf_st_dt(dedup_df_lis, df_activity, dict_dates, ["A"])
        df_fin_st_dt = df_fin_st_dt.filter(
            f.datediff(f.to_date(f.col("bdp_partition"), "yyyy-MM-dd"), f.lit(odate)) >= 0
        )
        ##print("## df_fin_st_dt second ##")
        ##df_fin_st_dt.show(20,truncate=False)

    df_target = (
        df_fin_st_dt.alias("ODate")
        .join(
            a002f4_acct_loan_df.alias(HardShipDate.ALIAS.value),
            (
                (
                    f.regexp_replace(f.trim(f.col(TempListclectsepsd.DDE_ACCT_ID.value)), LIS_KMR_HOST, "100")
                    == f.col(HardShipDate.ACCT_NUM_A002F4.value)
                )
                & (df_fin_st_dt.bdp_year == f.col(HardShipDate.BDP_YEAR_A002F4.value))
                & (df_fin_st_dt.bdp_month == f.col(HardShipDate.BDP_MONTH_A002F4.value))
                & (df_fin_st_dt.bdp_day == f.col(HardShipDate.BDP_DAY_A002F4.value))
            ),
            LEFT,
        )
        .select(
            f.concat_ws(
                "",
                f.trim(f.col(TempListclectsepsd.DDE_ACCT_ID.value)),
                f.from_unixtime(f.unix_timestamp(TempListclectsepsd.DDE_CURR_DT_ADD.value, "yyyy-MM-dd"), "yyyyMMdd"),
            ).alias(CLECT_EPSD_HOST_ID),
            f.when(
                f.col("DDE_FIN_RLF_ST_DT").isNotNull(),
                f.concat_ws(
                    "",
                    f.trim(f.col(TempListclectsepsd.DDE_ACCT_ID.value)),
                    f.from_unixtime(f.unix_timestamp(DDE_FIN_RLF_ST_DT, "yyyy-MM-dd"), "yyyyMMdd"),
                ),
            )
            .otherwise(None)
            .alias(FIN_RLF_ARMT_HOST_ID),
            f.col(TempListclectsepsd.DDE_ARREARS_REASON.value).alias(DDE_ARREARS_REASON),
            f.col(TempListclectsepsd.DDE_AUTO_LTR_DAYS.value).alias(DDE_AUTO_LTR_DAYS),
            f.col(TempListclectsepsd.DDE_ACCT_ID.value).alias(DDE_ACCT_ID),
            f.col(TempListclectsepsd.DDE_DTE_COLCTED.value).alias(DDE_DTE_COLCTED),
            f.col(TempListclectsepsd.DDE_CURR_DT_ADD.value).alias(DDE_CURR_DT_ADD),
            f.col(TempListclectsepsd.DDE_OL_WRITEOFF_IND.value).alias(DDE_OL_WRITEOFF_IND),
            f.col(TempListclectsepsd.DDE_OL_LRD_IND.value).alias(DDE_OL_LRD_IND),
            f.col(TempListclectsepsd.DDE_SUPPRESS_LTR.value).alias(DDE_SUPPRESS_LTR),
            f.col(TempListclectsepsd.DDE_SUPV_SUPR_DAYS.value).alias(DDE_SUPV_SUPR_DAYS),
            f.col(TempListclectsepsd.DDE_RECOVERY_STAT_CODE.value).alias(DDE_RECOVERY_STAT_CODE),
            f.col(TempListclectsepsd.DDE_SCO_PENDING.value).alias(DDE_SCO_PENDING),
            f.col(TempListclectsepsd.DDE_COLL_ENTRY_REAS.value).alias(DDE_COLL_ENTRY_REAS),
            f.col(TempListclectsepsd.DDE_REC_STAT.value).alias(DDE_REC_STAT),
            f.col(TempListclectsepsd.DDE_21D3_LIST_AMNT.value).alias(DDE_21D3_LIST_AMNT),
            f.col(TempListclectsepsd.DDE_HARDSHIP_INFO_EXP_DT.value).alias(DDE_HARDSHIP_INFO_EXP_DT),
            f.col(TempListclectsepsd.DDE_HARDSHIP_LVR.value).alias(DDE_HARDSHIP_LVR),
            f.col(TempListclectsepsd.DDE_PROM_SCHED_WK_IND.value).alias(DDE_PROM_SCHED_WK_IND),
            f.col(TempListclectsepsd.DDE_PROM_STAT.value).alias(DDE_PROM_STAT),
            f.col(TempListclectsepsd.DDE_PROM_AMT.value).alias(DDE_PROM_AMT),
            f.col(TempListclectsepsd.DDE_PROM_DTE.value).alias(DDE_PROM_DTE),
            f.col(TempListclectsepsd.DDE_DTE_LAST_REVW_DT.value).alias(DDE_DTE_LAST_REVW_DT),
            f.col(TempListclectsepsd.DDE_AMT_TOWARDS_PROM.value).alias(DDE_AMT_TOWARDS_PROM),
            f.col(TempListclectsepsd.DDE_ARREARS_CODE.value).alias(DDE_ARREARS_CODE),
            DDE_FIN_RLF_ST_DT,
            DDE_FIN_RLF_END_DT,
            f.regexp_replace(f.trim(f.col(TempListclectsepsd.DDE_ACCT_ID.value)), LIS_KMR_HOST, "100").alias(
                LEND_ARMT_HOST_ID
            ),
            f.col(TempListclectsepsd.DDE_PROM_SCHEDULE.value).alias(DDE_PROM_SCHEDULE),
            f.col(TempListclectsepsd.DDE_ASSISTANCE_LEVEL.value).alias(DDE_ASSISTANCE_LEVEL),
            f.to_date(f.col("bdp_partition"), "yyyy-MM-dd").alias(SRC_BDP_PARTITION),
            f.col(TempListclectsepsd.DDE_COMPLAINTS_CUST_RELTNS.value).alias(DDE_COMPLAINTS_CUST_RELTNS),
            f.col(TempListclectsepsd.DDE_SGB_FOS.value).alias(DDE_SGB_FOS),
            f.col(TempListclectsepsd.DDE_COMMERCIAL.value).alias(DDE_COMMERCIAL),
            f.col(DDE_ACTY_CDS).alias(ACTY_CD),
            f.col(HardShipDate.NEXT_REPAY_DUE_DATE_A002F4.value).alias(DDE_PAYMENT_DUE_DATE),
            f.lpad(
                f.substring(
                    f.trim(f.regexp_extract(f.col(TempListclectsepsd.DDE_CUST_ID.value), TCS_CUSTID_FORMAT, 1)), -9, 9
                ),
                15,
                "0",
            ).alias(DDE_CUST_ID),
            f.col(TempListclectsepsd.DDE_LITIGATION.value).alias(DDE_LITIGATION),
            f.col(TempListclectsepsd.DDE_APRA.value).alias(DDE_APRA),
            LIT_START_DT,
        )
        .dropDuplicates()
    )

    ##print("## LIS - TCS - Target DF - Schema")
    ##df_target.printSchema()
    ##df_target.show(20,False)
    return df_target


def get_b2k_target_df(df_b2k_clects_epsd, df_activity, odate, is_history_load=False):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :return df_target:
    """
    column_list = [e.value for e in Tempb2kclectsepsd]

    dict_dates = {
        "acct_id": Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value,
        "clect_curr_dt": Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value,
        "hrdshp": Tempb2kclectsepsd.DDE_HARDSHIP_IND_B2K.value,
        "clect_rec_stat": Tempb2kclectsepsd.DDE_REC_STAT_B2K.value,
        "colcted_dt": Tempb2kclectsepsd.DDE_DTE_COLCTED_B2K.value,
        "fin_rlf_st_dt": DDE_FIN_RLF_ST_DT,
        "fin_rlf_end_dt": DDE_FIN_RLF_END_DT,
        "acty_acct_id": ActivityAcraamaa.DDE_ACCT_ID.value,
        "acty_cmp_dt": ActivityAcraamaa.DDE_CMP_DATE.value,
        "acty_cmp_time": ActivityAcraamaa.DDE_CMP_TIME.value,
        "acty_acty_cd": ActivityAcraamaa.DDE_ACTY_CD.value,
        "litind": Tempb2kclectsepsd.DDE_LITIGATION_IND_B2K.value,
        "lit_start_dt": LIT_START_DT,
    }

    if not is_history_load:
        df_fin_st_dt = generate_fin_rlf_st_dt(df_b2k_clects_epsd, df_activity, dict_dates, ["A", "C", "I"])
        # print("\n***** Printing df_fin_st_dt dataframe after applying generate_fin_rlf_st_dt() *****\n ")
        # df_fin_st_dt.show(20, truncate=False)
        # print("\n###Count of df_fin_st_dt DF is {}\n".format(df_fin_st_dt.count()))
        df_fin_st_dt = df_fin_st_dt.filter(
            (f.datediff(f.to_date(f.col("bdp_partition"), "yyyy-MM-dd"), f.lit(odate)) == 0)
            | (
                (f.datediff(f.lit(odate), f.to_date(f.col("bdp_partition"), "yyyy-MM-dd")) <= 6)
                & (f.datediff(f.lit(odate), f.to_date(f.col("bdp_partition"), "yyyy-MM-dd")) > 0)
                & (f.datediff(f.to_date(f.col("dde_dte_colcted"), "yyyy-MM-dd"), f.lit(odate)) >= 0)
            )
        )
        # print("\n***** Printing df_fin_st_dt dataframe after applying filter condition *****\n ")
        # df_fin_st_dt.show(20, truncate=False)
        # print("\n###Count of df_fin_st_dt DF is {}\n".format(df_fin_st_dt.count()))
    else:
        dedup_df_b2k = get_dedup_df(df_b2k_clects_epsd, column_list)
        df_fin_st_dt = generate_fin_rlf_st_dt(dedup_df_b2k, df_activity, dict_dates, ["A", "C", "I"])
        df_fin_st_dt = df_fin_st_dt.filter(
            f.datediff(f.to_date(f.col("bdp_partition"), "yyyy-MM-dd"), f.lit(odate)) >= 0
        )

    df_target = (
        df_fin_st_dt.alias("ODate")
        .select(
            f.concat_ws(
                "",
                f.trim(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value)),
                f.when(
                    f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value).isNotNull(),
                    f.from_unixtime(
                        f.unix_timestamp(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value, "yyyy-MM-dd"), "yyyyMMdd"
                    ),
                ).otherwise(None),
            ).alias(CLECT_EPSD_HOST_ID),
            f.when(
                f.col("DDE_FIN_RLF_ST_DT").isNotNull(),
                f.concat_ws(
                    "",
                    f.trim(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value)),
                    f.from_unixtime(f.unix_timestamp(DDE_FIN_RLF_ST_DT, "yyyy-MM-dd"), "yyyyMMdd"),
                ),
            )
            .otherwise(None)
            .alias(FIN_RLF_ARMT_HOST_ID),
            f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value).alias(DDE_ACCT_ID),
            f.col(Tempb2kclectsepsd.DDE_AMT_TOWARDS_PROM_B2K.value).alias(DDE_AMT_TOWARDS_PROM),
            f.col(Tempb2kclectsepsd.DDE_AUTO_LTR_DAYS_B2K.value).alias(DDE_AUTO_LTR_DAYS),
            f.col(Tempb2kclectsepsd.DDE_COLL_ENTRY_REAS_B2K.value).alias(DDE_COLL_ENTRY_REAS),
            f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value).alias(DDE_CURR_DT_ADD),
            f.col(Tempb2kclectsepsd.DDE_DEFER_WRITE_OFF_IND_B2K.value).alias(DDE_DEFER_WRITE_OFF_IND),
            f.col(Tempb2kclectsepsd.DDE_DTE_COLCTED_B2K.value).alias(DDE_DTE_COLCTED),
            f.col(Tempb2kclectsepsd.DDE_DTE_LAST_REVW_DT_B2K.value).alias(DDE_DTE_LAST_REVW_DT),
            f.col(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value).alias(DDE_FINANCIAL_SOLUTION),
            f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value).alias(DDE_HARDSHIP_EXP_DATE),
            f.col(Tempb2kclectsepsd.DDE_HARDSHIP_IND_B2K.value).alias(DDE_HARDSHIP_IND),
            f.col(Tempb2kclectsepsd.DDE_HARDSHIP_LVR_B2K.value).alias(DDE_HARDSHIP_LVR),
            f.col(Tempb2kclectsepsd.DDE_IL_RECOVERY_AMOUNT_B2K.value).alias(DDE_IL_RECOVERY_AMOUNT),
            f.col(Tempb2kclectsepsd.DDE_IL_RECOVERY_OUTCOME_B2K.value).alias(DDE_IL_RECOVERY_OUTCOME),
            f.col(Tempb2kclectsepsd.DDE_LETR_WARNING_DATE_B2K.value).alias(DDE_LETR_WARNING_DATE),
            f.col(Tempb2kclectsepsd.DDE_MI_ARRS_REASON_B2K.value).alias(DDE_MI_ARRS_REASON),
            f.col(Tempb2kclectsepsd.DDE_NO_PHONE_CONTACT_B2K.value).alias(DDE_NO_PHONE_CONTACT),
            f.col(Tempb2kclectsepsd.DDE_OL_LRD_IND_B2K.value).alias(DDE_OL_LRD_IND),
            f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value).alias(DDE_ORIG_PROD_SYS),
            f.col(Tempb2kclectsepsd.DDE_OUTSOURCING_CODE_B2K.value).alias(DDE_OUTSOURCING_CODE),
            f.col(Tempb2kclectsepsd.DDE_PROM_AMT_B2K.value).alias(DDE_PROM_AMT),
            f.col(Tempb2kclectsepsd.DDE_PROM_DTE_B2K.value).alias(DDE_PROM_DTE),
            f.col(Tempb2kclectsepsd.DDE_PROM_SCHEDULE_B2K.value).alias(DDE_PROM_SCHEDULE),
            f.col(Tempb2kclectsepsd.DDE_PROM_STAT_B2K.value).alias(DDE_PROM_STAT),
            f.col(Tempb2kclectsepsd.DDE_REC_STAT_B2K.value).alias(DDE_REC_STAT),
            f.col(Tempb2kclectsepsd.DDE_RECOVERY_CODE_B2K.value).alias(DDE_RECOVERY_CODE),
            f.col(Tempb2kclectsepsd.DDE_SOFT_CHARGEOFF_IND_B2K.value).alias(DDE_SOFT_CHARGEOFF_IND),
            f.col(Tempb2kclectsepsd.DDE_SUPRES_AUTO_LTR_IND_B2K.value).alias(DDE_SUPRES_AUTO_LTR_IND),
            f.col(Tempb2kclectsepsd.DDE_SUPV_SUPR_DAYS_B2K.value).alias(DDE_SUPV_SUPR_DAYS),
            DDE_FIN_RLF_ST_DT,
            DDE_FIN_RLF_END_DT,
            f.when(
                f.upper(f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value)).isin(["ICB", "TBK"]),
                f.trim(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value)),
            )
            .when(
                f.upper(f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value)) == "INF",
                f.regexp_replace(f.trim(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value)), "-", ""),
            )
            .when(
                f.upper(f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value)) == "MSS",
                f.regexp_replace(f.trim(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value)), "^(\\w{2})", ""),
            )
            .otherwise(f.trim(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value)))
            .alias(LEND_ARMT_HOST_ID),
            f.to_date(f.col("bdp_partition"), "yyyy-MM-dd").alias(SRC_BDP_PARTITION),
            f.col(Tempb2kclectsepsd.DDE_BANK_ERR_PRESENT_B2K.value).alias(DDE_BANK_ERR_PRESENT),
            f.col(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value).alias(DDE_COMMERCIALLY_AVAILABLE_PRESENT),
            f.col(DDE_ACTY_CDS).alias(ACTY_CD),
            f.col(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value).alias(DDE_NEXT_DUE_DATE),
            f.col(Tempb2kclectsepsd.DDE_CUST_ID_B2K.value).alias(DDE_CUST_ID),
            f.col(Tempb2kclectsepsd.DDE_LITIGATION_IND_B2K.value).alias(DDE_LITIGATION_IND),
            f.col(Tempb2kclectsepsd.DDE_ALT_STRATEGY_IND_B2K.value).alias(DDE_ALT_STRATEGY_IND),
            LIT_START_DT,
        )
        .dropDuplicates()
    )
    # print("\n***** Printing df_target dataframe after applying all custom logic *****\n ")
    # df_target.show(20, truncate=False)
    # print("\n###Count of df_target DF is {}\n".format(df_target.count()))
    return df_target


def get_target_prms_b2k_data(df_b2k_prms_data, df_harm_ref, df_temp_host_id):
    """
    Obtain the final transformed dataframe with target fields from temp_fdp048_kmr_preprocessing_b2k_clects_epsd
    :param df_b2k_prms_data: Dataframe containing df_b2k_prms_data table data.
    :return: dataframe with all the prms_to_pay related columns
    """

    df_b2k_filter = df_b2k_prms_data.alias(PrmsToPay.ALIAS.value).filter(
        (col(PrmsToPay.DDE_DTE_COLCTED.value).isNull())
        & (col(PrmsToPay.DDE_ORIG_PROD_SYS.value).isin(["ICB", "INF", "MSS", "TBK"]))
        & (col(PrmsToPay.DDE_PROM_DTE.value).isNotNull())
    )

    prms_stat_type_dict = {
        "alias": PrmsToPay.ALIAS.value,
        "app_id": B2K_APPLN_KEY,
        "entity_name": PRMS_STAT_TYPE,
        "ref_src_cd": PrmsToPay.DDE_PROM_STAT.value,
        "target_harm_cd": PRMS_STAT_TYPE_CD,
    }

    df_prms_stat_type = generate_harmonized_code(df_b2k_filter, df_harm_ref, prms_stat_type_dict)

    prms_pymt_plan_dict = {
        "alias": PrmsToPay.ALIAS.value,
        "app_id": B2K_APPLN_KEY,
        "entity_name": PROMISE_SCHEDULE_TYPE,
        "ref_src_cd": PrmsToPay.DDE_PROM_SCHEDULE.value,
        "target_harm_cd": PAYMT_PLAN_PERIOD_TYPE_CD,
    }
    df_prms_pymt_plan = generate_harmonized_code(df_prms_stat_type, df_harm_ref, prms_pymt_plan_dict)

    df_target = (
        df_prms_pymt_plan.join(
            df_temp_host_id.alias(TempHostId.ALIAS.value),
            (trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value)) == trim(col(PrmsToPay.CLECT_EPSD_HOST_ID_PTP.value))),
            INNER,
        )
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            PRMS_STAT_TYPE_CD,
            PAYMT_PLAN_PERIOD_TYPE_CD,
            to_date(col(PrmsToPay.DDE_PROM_DTE.value), "YYYY-MM-DD").alias(PRMS_DT),
            round(col(PrmsToPay.DDE_PROM_AMT.value), 2).alias(PRMS_AMT),
            to_date(col(PrmsToPay.DDE_DTE_LAST_REVW_DT.value), "YYYY-MM-DD").alias(PRMS_REVW_DT),
            col(PrmsToPay.DDE_AMT_TOWARDS_PROM.value).alias(TOT_PRMS_AMT),
            lit(AUD).alias(PROMISE_AMOUNT_CURRENCY_CODE),
            lit("A0077D:A001F5").alias(PRV_SYS_APPLN_ID),
            col(PrmsToPay.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )

    return df_target


def get_target_clect_epsd_tcs_df(
    df_tcs_temp, df_harm_ref, df_temp_host_id, code, df_fdp059_armt_clect_stat_type, odate
):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :param df_tcs_clects_epsd:
    :param df_harm_ref:
    :return df_target:
    """
    clect_bank_error_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": BANK_ERROR_CODE,
        "ref_src_cd": CollectEpi.BANK_ERR_CD.value,
        "target_harm_cd": CLECT_BANK_ERROR_CD,
    }

    clect_bank_error_type = generate_harmonized_code(df_tcs_temp, df_harm_ref, clect_bank_error_dict)

    altn_strat_cd_tcs_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": ALTERNATE_STRATEGY_CODE,
        "ref_src_cd": CollectEpi.DDE_APRA.value,
        "target_harm_cd": ALTN_STRAT_CD,
    }

    altn_strat_cd_tcs_type = generate_harmonized_code(clect_bank_error_type, df_harm_ref, altn_strat_cd_tcs_dict)

    df_tcs_clects_epsd = altn_strat_cd_tcs_type.withColumn(
        "dde_rec_stat",
        when(trim(col("dde_rec_stat")).isNull(), lit(None))
        .when(trim(col("dde_rec_stat")) == "", lit("A"))
        .otherwise(trim(col("dde_rec_stat"))),
    )

    df_clect_stat_type = df_tcs_clects_epsd.join(
        df_fdp059_armt_clect_stat_type, trim(col("dde_rec_stat")) == trim(col("armt_clect_stat_type_cd")), LEFT
    ).withColumn(
        CLECT_STAT_TYPE_CD,
        when(trim(col("armt_clect_stat_type_cd")).isNull(), lit("UNK")).otherwise(trim(col("armt_clect_stat_type_cd"))),
    )

    fin_soltn_type_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": FIN_SOLTN_TYPE,
        "ref_src_cd": CollectEpi.DDE_ASSISTANCE_LEVEL.value,
        "target_harm_cd": FIN_SOLTN_TYPE_CD,
    }

    df_fin_soltn_type = generate_harmonized_code(df_clect_stat_type, df_harm_ref, fin_soltn_type_dict)

    clect_entry_type_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": Collection_Entry_Reason,
        "ref_src_cd": CollectEpi.DDE_COLL_ENTRY_REAS.value,
        "target_harm_cd": CLECT_ENTRY_REASN_TYPE_CD,
    }

    df_clect_entry_type = generate_harmonized_code(df_fin_soltn_type, df_harm_ref, clect_entry_type_dict)

    arears_type_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": Arears_Reason_Type,
        "ref_src_cd": CollectEpi.DDE_ARREARS_REASON.value,
        "target_harm_cd": AREARS_REASN_TYPE_CD,
    }

    df_arears_reasn_type = generate_harmonized_code(df_clect_entry_type, df_harm_ref, arears_type_dict)
    win = Window.partitionBy(CollectEpi.DDE_ACCT_ID.value, CollectEpi.DDE_CURR_DT_ADD.value)

    df_target = (
        df_arears_reasn_type.join(
            df_temp_host_id.alias(TempHostId.ALIAS.value),
            (trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value)) == trim(col(CollectEpi.CLECT_EPSD_HOST_ID_CE.value))),
            INNER,
        )
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            col(TempHostId.LEND_ARMT_SK.value).alias(LEND_ARMT_SK),
            lit(code).alias(OWNG_LEND_ARMT_APPLN_KEY),
            FIN_SOLTN_TYPE_CD,
            CLECT_ENTRY_REASN_TYPE_CD,
            lit(UNK).alias(CLECT_OUTSRCG_TYPE_CD),
            AREARS_REASN_TYPE_CD,
            to_date(col(CollectEpi.DDE_CURR_DT_ADD.value), "YYYY-MM-DD").alias(CREAT_DT),
            when(trim(col(CollectEpi.DDE_SCO_PENDING.value)) == "", "N")
            .when(trim(col(CollectEpi.DDE_SCO_PENDING.value)) == "N", "N")
            .when(trim(col(CollectEpi.DDE_SCO_PENDING.value)) == "Y", "Y")
            .otherwise(None)
            .alias(SOFT_CHRG_OFF_PNDG_IND),
            to_date(col(CollectEpi.DDE_DTE_COLCTED.value), "YYYY-MM-DD").alias(CLECT_DT),
            when(trim(col(CollectEpi.DDE_OL_WRITEOFF_IND.value)) == "", "N")
            .when(trim(col(CollectEpi.DDE_OL_WRITEOFF_IND.value)) == "N", "N")
            .when(trim(col(CollectEpi.DDE_OL_WRITEOFF_IND.value)) == "Y", "Y")
            .otherwise(None)
            .alias(DEFER_WRITE_OFF_IND),
            when(trim(col(CollectEpi.DDE_SUPPRESS_LTR.value)) == "", "N")
            .when(trim(col(CollectEpi.DDE_SUPPRESS_LTR.value)) == "N", "N")
            .when(trim(col(CollectEpi.DDE_SUPPRESS_LTR.value)) == "Y", "Y")
            .otherwise(None)
            .alias(LTR_SUPRSN_IND),
            lit(None).cast("date").alias(CRAA_LODG_DT),
            lit(None).cast("string").alias(NO_PH_CONTCT_IND),
            col(CollectEpi.DDE_AUTO_LTR_DAYS.value).alias(AUTO_LTR_DAY_CNT),
            col(CollectEpi.DDE_SUPV_SUPR_DAYS.value).alias(OMIT_FROM_SUPVSRY_QU_DAY_CNT),
            when(f.coalesce(f.trim(f.col(CollectEpi.DDE_LITIGATION.value)), f.lit("")) == "", "N")
            .when(f.trim(f.col(CollectEpi.DDE_LITIGATION.value)) == "N", "N")
            .when(f.trim(f.col(CollectEpi.DDE_LITIGATION.value)) == "Y", "Y")
            .otherwise(None)
            .alias(LITIGN_IND),
            lit(None).cast("int").alias(CR_LISTNG_AMT),
            lit(AUD).alias(CREDIT_LISTING_AMOUNT_CURRENCY_CODE),
            CLECT_STAT_TYPE_CD,
            CLECT_BANK_ERROR_CD,
            col(CollectEpi.TCS_LIT_START_DT.value).alias(LIT_START_DT),
            lit(None).cast("bigint").alias("worklist_workflow_sk"),
            lit("A0077D:A0030A").alias(PRV_SYS_APPLN_ID),
            col(CollectEpi.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
            ALTN_STRAT_CD,
        )
        .dropDuplicates()
    )
    return df_target


def df_target_b2k_df(df_b2k_clects_epsd_kmr_key, df_temp_clects_epsd, df_harm, df_harm_erd):
    """
    Obtain the final transformed dataframe with target fields of compose temp table columns and KMR
    :param df_b2k_clects_epsd_today: Dataframe containing current odate table data.
    :return: dataframe with all the customer transaction related columns and KMR columns
    """

    # to filter infolease data
    df_temp_clects_epsd = df_temp_clects_epsd.\
        filter(f.trim(f.upper(f.col(TmpPreprocessB2kclctepsd.DDE_ORIG_PROD_SYS_B2K.value))).isin('ICB', 'MSS', 'TBK'))

    b2k = "B2kclctepsd."
    fin_rlf_armt_subtype_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": B2K_APPLN_KEY,
        "entity_name": FIN_REF_ENTITY_SUBTYPE_NM,
        "ref_src_cd": CollectEpi.DDE_MI_ARRS_REASON.value,
        "target_harm_cd": FIN_RLF_ARMT_SUBTYPE_CD,
    }

    df_temp_clects_epsd = generate_harmonized_code(df_temp_clects_epsd, df_harm, fin_rlf_armt_subtype_dict)

    fin_rlf_armt_stat_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": B2K_APPLN_KEY_ERD,
        "ref_src_cd": CollectEpi.DDE_HARDSHIP_IND.value,
        "ref_erd_cd": ErdData.FIN_RLF_ARMT_STAT_TYPE_SRC_CD.value,
        "erd_tgt_cd": ErdData.FIN_RLF_ARMT_STAT_TYPE_CD.value,
        "target_harm_cd": FIN_RLF_ARMT_STAT_CD,
    }
    df_temp_clects_epsd = generate_harmonized_code_erd(df_temp_clects_epsd, df_harm_erd, fin_rlf_armt_stat_dict)
    df_target = (
        df_b2k_clects_epsd_kmr_key.alias("Kmrkey")
        .join(
            df_temp_clects_epsd.alias("B2kclctepsd").filter(
                f.col(b2k + TmpPreprocessB2kclctepsd.DDE_FIN_RLF_ST_DT_B2K.value).isNotNull()
            ),
            (
                f.col("Kmrkey." + KmrHostidKeys.CLECT_EPSD_HOST_ID.value)
                == f.col(b2k + TmpPreprocessB2kclctepsd.CLECT_EPSD_HOST_ID_B2K.value)
            ),
            INNER,
        )
        .select(
            f.col(KmrHostidKeys.ARMT_SK.value).alias(ARMT_SK),
            f.col(KmrHostidKeys.LEND_ARMT_SK.value).alias(LEND_ARMT_SK),
            f.col(KmrHostidKeys.CASE_SK.value).alias(CLECT_EPSD_CASE_SK),
            FIN_RLF_ARMT_SUBTYPE_CD,
            f.col(b2k + TmpPreprocessB2kclctepsd.FIN_RLF_ARMT_STAT_CD.value).alias(FIN_RLF_ARMT_STAT_TYPE_CD),
            f.when(
                (
                    (
                        f.coalesce(
                            f.trim(f.col(b2k + TmpPreprocessB2kclctepsd.DDE_COMMERCIALLY_AVAILABLE_PRESENT_B2K.value)),
                            f.lit(""),
                        )
                        == ""
                    )
                    | (f.upper(f.trim(f.col(b2k + TmpPreprocessB2kclctepsd.DDE_OL_LRD_IND_B2K.value))) == "N")
                ),
                "N",
            )
            .when(
                f.upper(f.trim(f.col(b2k + TmpPreprocessB2kclctepsd.DDE_COMMERCIALLY_AVAILABLE_PRESENT_B2K.value)))
                == "Y",
                "Y",
            )
            .otherwise(None)
            .alias(COMRC_AVAIL_IND),
            f.when(f.col(TmpPreprocessB2kclctepsd.DDE_ORIG_PROD_SYS_B2K.value) == "ICB", f.lit("A0077D:A0010E"))
            .when(f.col(TmpPreprocessB2kclctepsd.DDE_ORIG_PROD_SYS_B2K.value) == "INF", f.lit("A0077D:A0018E"))
            .when(f.col(TmpPreprocessB2kclctepsd.DDE_ORIG_PROD_SYS_B2K.value) == "MSS", f.lit("A0077D:A0019B"))
            .when(f.col(TmpPreprocessB2kclctepsd.DDE_ORIG_PROD_SYS_B2K.value) == "TBK", f.lit("A0077D:A00004"))
            .alias(OWNG_LEND_ARMT_APPLN_KEY),
            f.col(b2k + TmpPreprocessB2kclctepsd.DDE_FIN_RLF_ST_DT_B2K.value).alias(FIN_RLF_ST_DT),
            f.col(b2k + TmpPreprocessB2kclctepsd.FIN_RLF_END_DT_B2K.value).alias(FIN_RLF_END_DT),
            f.col(TmpPreprocessB2kclctepsd.DDE_HARDSHIP_LVR_B2K.value).alias(HARDSP_LOAN_VALN_RATIO),
            f.when(
                (
                    f.datediff(
                        f.col(b2k + TmpPreprocessB2kclctepsd.DDE_FIN_RLF_ST_DT_B2K.value),
                        f.col(TmpPreprocessB2kclctepsd.DDE_HARDSHIP_EXP_DATE_B2K.value),
                    )
                    > 0
                ),
                f.col(TmpPreprocessB2kclctepsd.DDE_NEXT_DUE_DATE_B2K.value),
            )
            .otherwise(f.col(TmpPreprocessB2kclctepsd.DDE_HARDSHIP_EXP_DATE_B2K.value))
            .alias(HARDSP_EXPIR_DT),
            f.lit(B2K_APPLN_KEY).alias(PRV_SYS_APPLN_ID),
            f.col(TmpPreprocessB2kclctepsd.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    return df_target


def get_target_prms_tcs_crds_data(df_tcs_crds, df_harm_ref, df_temp_host_id_crds):
    """
    Obtain the final transformed dataframe with target fields from temp_fdp048_kmr_preprocessing_b2k_clects_epsd
    :param df_tcs_crds: Dataframe containing df_tcs_crds table data.
    :return: dataframe with all the prms_to_pay related columns
    """

    df_crds_filter = df_tcs_crds.alias(PrmsToPay.ALIAS.value).filter((col(PrmsToPay.DDE_PROM_DTE.value).isNotNull()))
    prms_stat_type_dict = {
        "alias": PrmsToPay.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": PRMS_STAT_TYPE,
        "ref_src_cd": PrmsToPay.DDE_PROM_STAT.value,
        "target_harm_cd": PRMS_STAT_TYPE_CD,
    }

    df_prms_stat_type = generate_harmonized_code(df_crds_filter, df_harm_ref, prms_stat_type_dict)
    prms_pymt_plan_dict = {
        "alias": PrmsToPay.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": PROMISE_SCHEDULE_TYPE,
        "ref_src_cd": PrmsToPay.DDE_PROM_SCHEDULE.value,
        "target_harm_cd": PAYMT_PLAN_PERIOD_TYPE_CD,
    }
    df_prms_pymt_plan = generate_harmonized_code(df_prms_stat_type, df_harm_ref, prms_pymt_plan_dict)

    df_target = (
        df_prms_pymt_plan.join(
            df_temp_host_id_crds.alias(TempHostId.ALIAS.value),
            (trim(col(PrmsToPay.CLECT_EPSD_HOST_ID_PTP.value)) == trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value))),
            INNER,
        )
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            PRMS_STAT_TYPE_CD,
            PAYMT_PLAN_PERIOD_TYPE_CD,
            to_date(col(PrmsToPay.DDE_PROM_DTE.value), "YYYY-MM-DD").alias(PRMS_DT),
            col(PrmsToPay.DDE_PROM_AMT.value).alias(PRMS_AMT),
            to_date(col(PrmsToPay.DDE_DTE_LAST_REVW_DT.value), "YYYY-MM-DD").alias(PRMS_REVW_DT),
            col(PrmsToPay.DDE_AMT_TOWARDS_PROM.value).alias(TOT_PRMS_AMT),
            lit(AUD).alias(PROMISE_AMOUNT_CURRENCY_CODE),
            lit("A0077D:A0030A").alias(PRV_SYS_APPLN_ID),
            col(PrmsToPay.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    return df_target


def get_target_prms_tcs_dda_data(df_tcs_dda, df_harm_ref, df_temp_host_id_dda):
    """
    Obtain the final transformed dataframe with target fields from temp_fdp048_kmr_preprocessing_b2k_clects_epsd
    :param df_tcs_crds: Dataframe containing df_tcs_crds table data.
    :return: dataframe with all the prms_to_pay related columns
    """

    df_dda_filter = df_tcs_dda.alias(PrmsToPay.ALIAS.value).filter((col(PrmsToPay.DDE_PROM_DTE.value).isNotNull()))

    prms_stat_type_dict = {
        "alias": PrmsToPay.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": PRMS_STAT_TYPE,
        "ref_src_cd": PrmsToPay.DDE_PROM_STAT.value,
        "target_harm_cd": PRMS_STAT_TYPE_CD,
    }

    df_prms_stat_type = generate_harmonized_code(df_dda_filter, df_harm_ref, prms_stat_type_dict)

    prms_pymt_plan_dict = {
        "alias": PrmsToPay.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": PROMISE_SCHEDULE_TYPE,
        "ref_src_cd": PrmsToPay.DDE_PROM_SCHEDULE.value,
        "target_harm_cd": PAYMT_PLAN_PERIOD_TYPE_CD,
    }
    df_prms_pymt_plan = generate_harmonized_code(df_prms_stat_type, df_harm_ref, prms_pymt_plan_dict)
    df_target = (
        df_prms_pymt_plan.join(
            df_temp_host_id_dda.alias(TempHostId.ALIAS.value),
            (trim(col(PrmsToPay.CLECT_EPSD_HOST_ID_PTP.value)) == trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value))),
            INNER,
        )
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            PRMS_STAT_TYPE_CD,
            PAYMT_PLAN_PERIOD_TYPE_CD,
            to_date(col(PrmsToPay.DDE_PROM_DTE.value), "YYYY-MM-DD").alias(PRMS_DT),
            col(PrmsToPay.DDE_PROM_AMT.value).alias(PRMS_AMT),
            to_date(col(PrmsToPay.DDE_DTE_LAST_REVW_DT.value), "YYYY-MM-DD").alias(PRMS_REVW_DT),
            col(PrmsToPay.DDE_AMT_TOWARDS_PROM.value).alias(TOT_PRMS_AMT),
            lit(AUD).alias(PROMISE_AMOUNT_CURRENCY_CODE),
            lit("A0077D:A0030A").alias(PRV_SYS_APPLN_ID),
            col(PrmsToPay.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    return df_target


def get_target_prms_tcs_lis_data(df_tcs_lis, df_harm_ref, df_temp_host_id_lis):
    """
    Obtain the final transformed dataframe with target fields from temp_fdp048_kmr_preprocessing_b2k_clects_epsd
    :param df_tcs_crds: Dataframe containing df_tcs_crds table data.
    :return: dataframe with all the prms_to_pay related columns
    """

    df_lis_filter = df_tcs_lis.alias(PrmsToPay.ALIAS.value).filter((col(PrmsToPay.DDE_PROM_DTE.value).isNotNull()))

    prms_stat_type_dict = {
        "alias": PrmsToPay.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": PRMS_STAT_TYPE,
        "ref_src_cd": PrmsToPay.DDE_PROM_STAT.value,
        "target_harm_cd": PRMS_STAT_TYPE_CD,
    }

    df_prms_stat_type = generate_harmonized_code(df_lis_filter, df_harm_ref, prms_stat_type_dict)

    prms_pymt_plan_dict = {
        "alias": PrmsToPay.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": PROMISE_SCHEDULE_TYPE,
        "ref_src_cd": PrmsToPay.DDE_PROM_SCHEDULE.value,
        "target_harm_cd": PAYMT_PLAN_PERIOD_TYPE_CD,
    }
    df_prms_pymt_plan = generate_harmonized_code(df_prms_stat_type, df_harm_ref, prms_pymt_plan_dict)

    df_target = (
        df_prms_pymt_plan.join(
            df_temp_host_id_lis.alias(TempHostId.ALIAS.value),
            (trim(col(PrmsToPay.CLECT_EPSD_HOST_ID_PTP.value)) == trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value))),
            INNER,
        )
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            PRMS_STAT_TYPE_CD,
            PAYMT_PLAN_PERIOD_TYPE_CD,
            to_date(col(PrmsToPay.DDE_PROM_DTE.value), "YYYY-MM-DD").alias(PRMS_DT),
            col(PrmsToPay.DDE_PROM_AMT.value).alias(PRMS_AMT),
            to_date(col(PrmsToPay.DDE_DTE_LAST_REVW_DT.value), "YYYY-MM-DD").alias(PRMS_REVW_DT),
            col(PrmsToPay.DDE_AMT_TOWARDS_PROM.value).alias(TOT_PRMS_AMT),
            lit(AUD).alias(PROMISE_AMOUNT_CURRENCY_CODE),
            lit("A0077D:A0030A").alias(PRV_SYS_APPLN_ID),
            col(PrmsToPay.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    return df_target


def get_target_clect_epsd_b2k_df(
    df_b2k_clects_epsd, df_harm_ref, df_temp_host_id, df_fdp059_armt_clect_stat_type, odate
):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :param df_b2k_clects_epsd:
    :param df_harm_ref:
    :return df_target:
    """

    clect_bank_error_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": B2K_APPLN_KEY,
        "entity_name": BANK_ERROR_CODE,
        "ref_src_cd": CollectEpi.DDE_BANK_ERR_PRESENT.value,
        "target_harm_cd": CLECT_BANK_ERROR_CD,
    }

    clect_bank_error_type = generate_harmonized_code(df_b2k_clects_epsd, df_harm_ref, clect_bank_error_dict)

    altn_strat_cd_b2k_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": B2K_APPLN_KEY,
        "entity_name": ALTERNATE_STRATEGY_CODE,
        "ref_src_cd": CollectEpi.DDE_ALT_STRATEGY_IND.value,
        "target_harm_cd": ALTN_STRAT_CD,
    }

    altn_strat_cd_b2k_type = generate_harmonized_code(clect_bank_error_type, df_harm_ref, altn_strat_cd_b2k_dict)

    df_b2k_clects_epsd = altn_strat_cd_b2k_type.withColumn(
        "dde_rec_stat",
        when(trim(col("dde_rec_stat")).isNull(), lit(None))
        .when(trim(col("dde_rec_stat")) == "", lit("A"))
        .otherwise(trim(col("dde_rec_stat"))),
    )

    df_clect_stat_type = df_b2k_clects_epsd.join(
        df_fdp059_armt_clect_stat_type, trim(col("dde_rec_stat")) == trim(col("armt_clect_stat_type_cd")), LEFT
    ).withColumn(
        CLECT_STAT_TYPE_CD,
        when(trim(col("armt_clect_stat_type_cd")).isNull(), lit("UNK")).otherwise(trim(col("armt_clect_stat_type_cd"))),
    )

    fin_soltn_type_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": B2K_APPLN_KEY,
        "entity_name": FIN_SOLTN_TYPE,
        "ref_src_cd": CollectEpi.DDE_FINANCIAL_SOLUTION.value,
        "target_harm_cd": FIN_SOLTN_TYPE_CD,
    }

    df_fin_soltn_type = generate_harmonized_code(df_clect_stat_type, df_harm_ref, fin_soltn_type_dict)

    clect_entry_type_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": B2K_APPLN_KEY,
        "entity_name": Collection_Entry_Reason,
        "ref_src_cd": CollectEpi.DDE_COLL_ENTRY_REAS.value,
        "target_harm_cd": CLECT_ENTRY_REASN_TYPE_CD,
    }

    df_clect_entry_type = generate_harmonized_code(df_fin_soltn_type, df_harm_ref, clect_entry_type_dict)

    clect_outsrcg_type_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": B2K_APPLN_KEY,
        "entity_name": Collection_outsrcg_type,
        "ref_src_cd": CollectEpi.DDE_OUTSOURCING_CODE.value,
        "target_harm_cd": CLECT_OUTSRCG_TYPE_CD,
    }

    df_prms_stat_type = generate_harmonized_code(df_clect_entry_type, df_harm_ref, clect_outsrcg_type_dict)

    arears_type_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": B2K_APPLN_KEY,
        "entity_name": Arears_Reason_Type,
        "ref_src_cd": CollectEpi.DDE_MI_ARRS_REASON.value,
        "target_harm_cd": AREARS_REASN_TYPE_CD,
    }

    df_arears_reasn_type = generate_harmonized_code(df_prms_stat_type, df_harm_ref, arears_type_dict)
    win = Window.partitionBy(CollectEpi.DDE_ACCT_ID.value, CollectEpi.DDE_CURR_DT_ADD.value)

    df_target = (
        df_arears_reasn_type.join(
            df_temp_host_id.alias(TempHostId.ALIAS.value),
            (trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value)) == trim(col(CollectEpi.CLECT_EPSD_HOST_ID_CE.value))),
            INNER,
        )
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            col(TempHostId.LEND_ARMT_SK.value).alias(LEND_ARMT_SK),
            when(col(CollectEpi.DDE_ORIG_PROD_SYS.value) == "ICB", "A0077D:A0010E")
            .when(col(CollectEpi.DDE_ORIG_PROD_SYS.value) == "INF", "A0077D:A0018E")
            .when(col(CollectEpi.DDE_ORIG_PROD_SYS.value) == "MSS", "A0077D:A0019B")
            .when(col(CollectEpi.DDE_ORIG_PROD_SYS.value) == "TBK", "A0077D:A00004")
            .alias(OWNG_LEND_ARMT_APPLN_KEY),
            FIN_SOLTN_TYPE_CD,
            CLECT_ENTRY_REASN_TYPE_CD,
            CLECT_OUTSRCG_TYPE_CD,
            AREARS_REASN_TYPE_CD,
            # concat_ws(
            #   "|", trim(col(CollectEpi.DDE_ACCT_ID.value)), to_date(col(CollectEpi.DDE_CURR_DT_ADD.value), "yyyyMMdd")
            # ).alias(CLECT_EPSD_HOST_ID),
            to_date(col(CollectEpi.DDE_CURR_DT_ADD.value), "YYYY-MM-DD").alias(CREAT_DT),
            lit(None).cast("string").alias(SOFT_CHRG_OFF_PNDG_IND),
            to_date(col(CollectEpi.DDE_DTE_COLCTED.value), "YYYY-MM-DD").alias(CLECT_DT),
            when(trim(col(CollectEpi.DDE_DEFER_WRITE_OFF_IND.value)) == "", "N")
            .when(trim(col(CollectEpi.DDE_DEFER_WRITE_OFF_IND.value)) == "N", "N")
            .when(trim(col(CollectEpi.DDE_DEFER_WRITE_OFF_IND.value)) == "Y", "Y")
            .otherwise(None)
            .alias(DEFER_WRITE_OFF_IND),
            when(trim(col(CollectEpi.DDE_SUPRES_AUTO_LTR_IND.value)) == "", "N")
            .when(trim(col(CollectEpi.DDE_SUPRES_AUTO_LTR_IND.value)) == "N", "N")
            .when(trim(col(CollectEpi.DDE_SUPRES_AUTO_LTR_IND.value)) == "Y", "Y")
            .otherwise(None)
            .alias(LTR_SUPRSN_IND),
            # to_date(col(CollectEpi.DDE_LETR_WARNING_DATE.value), "YYYY-MM-DD").alias(CRAA_LODG_DT),
            lit(None).cast("date").alias(CRAA_LODG_DT),
            when(trim(col(CollectEpi.DDE_NO_PHONE_CONTACT.value)) == "", "N")
            .when(trim(col(CollectEpi.DDE_NO_PHONE_CONTACT.value)) == "N", "N")
            .when(trim(col(CollectEpi.DDE_NO_PHONE_CONTACT.value)) == "Y", "Y")
            .otherwise(None)
            .alias(NO_PH_CONTCT_IND),
            col(CollectEpi.DDE_AUTO_LTR_DAYS.value).alias(AUTO_LTR_DAY_CNT),
            col(CollectEpi.DDE_SUPV_SUPR_DAYS.value).alias(OMIT_FROM_SUPVSRY_QU_DAY_CNT),
            when(f.coalesce(f.trim(f.col(CollectEpi.DDE_LITIGATION_IND.value)), f.lit("")) == "", "N")
            .when(f.trim(f.col(CollectEpi.DDE_LITIGATION_IND.value)) == "N", "N")
            .when(f.trim(f.col(CollectEpi.DDE_LITIGATION_IND.value)) == "Y", "Y")
            .otherwise(None)
            .alias(LITIGN_IND),
            lit(None).cast("decimal(15,2)").alias(CR_LISTNG_AMT),
            # round(col(CollectEpi.DDE_IL_RECOVERY_AMOUNT.value), 2).alias(RECOV_AMT),
            lit(AUD).alias(CREDIT_LISTING_AMOUNT_CURRENCY_CODE),
            # lit(AUD).alias(RECOVERY_AMOUNT_CURRENCY_CODE),
            CLECT_STAT_TYPE_CD,
            CLECT_BANK_ERROR_CD,
            col(CollectEpi.B2K_LIT_START_DT.value).alias(LIT_START_DT),
            lit(None).cast("bigint").alias("worklist_workflow_sk"),
            lit("A0077D:A001F5").alias(PRV_SYS_APPLN_ID),
            col(CollectEpi.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
            ALTN_STRAT_CD,
        )
        .dropDuplicates()
    )
    return df_target


def df_target_crd_df(df_crd_clects_epsd_kmr_key, df_temp_clects_epsd, df_harm, df_harm_erd):
    """
    Obtain the final transformed dataframe with target fields of compose temp table columns and KMR
    :param df_crd_clects_epsd_today: Dataframe containing current odate table data.
    :param df_crd_clects_epsd_yday: Dataframe containing previous odate table data.
    :return: dataframe with all the customer transaction related columns and KMR columns
    """
    crd = "Crdclctepsd."
    kmr_crd = KMRKEY + "."
    fin_rlf_armt_subtype_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": FIN_REF_ENTITY_SUBTYPE_NM,
        "ref_src_cd": CollectEpi.DDE_ARREARS_REASON.value,
        "target_harm_cd": FIN_RLF_ARMT_SUBTYPE_CD,
    }
    df_temp_clects_epsd = generate_harmonized_code(df_temp_clects_epsd, df_harm, fin_rlf_armt_subtype_dict)
    fin_rlf_armt_stat_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY_ERD,
        "ref_src_cd": CollectEpi.DDE_VP_ASSIST_IND.value,
        "ref_erd_cd": ErdData.FIN_RLF_ARMT_STAT_TYPE_SRC_CD.value,
        "erd_tgt_cd": ErdData.FIN_RLF_ARMT_STAT_TYPE_CD.value,
        "target_harm_cd": FIN_RLF_ARMT_STAT_CD,
    }
    df_temp_clects_epsd = generate_harmonized_code_erd(df_temp_clects_epsd, df_harm_erd, fin_rlf_armt_stat_dict)
    df_target = (
        df_crd_clects_epsd_kmr_key.alias("Kmrkey")
        .join(
            df_temp_clects_epsd.alias("Crdclctepsd").filter(
                f.col(crd + TmpPreprocessCrdclctepsd.DDE_FIN_RLF_ST_DT_CRD.value).isNotNull()
            ),
            (
                f.col(kmr_crd + KmrHostidKeys.CLECT_EPSD_HOST_ID.value)
                == f.col(crd + TmpPreprocessCrdclctepsd.CLECT_EPSD_HOST_ID_CRD.value)
            ),
            INNER,
        )
        .select(
            f.col(KmrHostidKeys.ARMT_SK.value).alias(ARMT_SK),
            f.col(KmrHostidKeys.LEND_ARMT_SK.value).alias(LEND_ARMT_SK),
            f.col(KmrHostidKeys.CASE_SK.value).alias(CLECT_EPSD_CASE_SK),
            FIN_RLF_ARMT_SUBTYPE_CD,
            f.col(crd + TmpPreprocessCrdclctepsd.FIN_RLF_ARMT_STAT_CD.value).alias(FIN_RLF_ARMT_STAT_TYPE_CD),
            f.when(
                (
                    (f.coalesce(f.trim(f.col(TmpPreprocessCrdclctepsd.DDE_COMMERCIAL_CRED.value)), f.lit("")) == "")
                    | (f.upper(f.trim(f.col(crd + TmpPreprocessCrdclctepsd.DDE_COMMERCIAL_CRED.value))) == "N")
                ),
                "N",
            )
            .when(f.upper(f.trim(f.col(TmpPreprocessCrdclctepsd.DDE_COMMERCIAL_CRED.value))) == "Y", "Y")
            .otherwise(None)
            .alias(COMRC_AVAIL_IND),
            f.lit(OWNG_LEND_ARMT_APPLN_KEY_CRD).alias(OWNG_LEND_ARMT_APPLN_KEY),
            f.col(crd + TmpPreprocessCrdclctepsd.DDE_FIN_RLF_ST_DT_CRD.value).alias(FIN_RLF_ST_DT),
            f.col(crd + TmpPreprocessCrdclctepsd.FIN_RLF_END_DT_CRD.value).alias(FIN_RLF_END_DT),
            f.col(TmpPreprocessCrdclctepsd.DDE_HARDSHIP_LVR_CRED.value).alias(HARDSP_LOAN_VALN_RATIO),
            f.when(
                (
                    f.datediff(
                        f.col(crd + TmpPreprocessCrdclctepsd.DDE_FIN_RLF_ST_DT_CRD.value),
                        f.col(TmpPreprocessCrdclctepsd.DDE_HARDSHIP_INFO_EXP_DT_CRED.value),
                    )
                    > 0
                ),
                f.col(TmpPreprocessCrdclctepsd.DDE_DUE_DATE_CRED.value),
            )
            .otherwise(f.col(TmpPreprocessCrdclctepsd.DDE_HARDSHIP_INFO_EXP_DT_CRED.value))
            .alias(HARDSP_EXPIR_DT),
            f.lit(TCS_APPLN_KEY).alias(PRV_SYS_APPLN_ID),
            f.col(TmpPreprocessCrdclctepsd.SRC_BDP_PARTITION_CRD.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    return df_target


def df_target_dda_df(df_dda_clects_epsd_kmr_key, df_temp_clects_epsd, df_harm, df_harm_erd):
    """
    Obtain the final transformed dataframe with target fields of compose temp table columns and KMR
    :param df_dda_clects__epsd_today: Dataframe containing current odate table data.
    :param df_dda_clects__epsd_yday: Dataframe containing previous odate table data.
    :return: dataframe with all the customer transaction related columns and KMR columns
    """
    dda = "Ddaclctepsd."
    kmr_dda = KMRKEY + "."
    fin_rlf_armt_subtype_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": FIN_REF_ENTITY_SUBTYPE_NM,
        "ref_src_cd": CollectEpi.DDE_ARREARS_REASON.value,
        "target_harm_cd": FIN_RLF_ARMT_SUBTYPE_CD,
    }
    df_temp_clects_epsd = generate_harmonized_code(df_temp_clects_epsd, df_harm, fin_rlf_armt_subtype_dict)
    fin_rlf_armt_stat_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY_ERD,
        "ref_src_cd": CollectEpi.DDE_ARREARS_CODE.value,
        "ref_erd_cd": ErdData.FIN_RLF_ARMT_STAT_TYPE_SRC_CD.value,
        "erd_tgt_cd": ErdData.FIN_RLF_ARMT_STAT_TYPE_CD.value,
        "target_harm_cd": FIN_RLF_ARMT_STAT_CD,
    }
    df_temp_clects_epsd = generate_harmonized_code_erd(df_temp_clects_epsd, df_harm_erd, fin_rlf_armt_stat_dict)
    df_target = (
        df_dda_clects_epsd_kmr_key.alias("Kmrkey")
        .join(
            df_temp_clects_epsd.alias("Ddaclctepsd").filter(
                f.col(dda + TmpPreprocessDdaclctepsd.DDE_FIN_RLF_ST_DT_DDA.value).isNotNull()
            ),
            (
                f.col(kmr_dda + KmrHostidKeys.CLECT_EPSD_HOST_ID.value)
                == f.col(dda + TmpPreprocessDdaclctepsd.CLECT_EPSD_HOST_ID_DDA.value)
            ),
            INNER,
        )
        .select(
            f.col(KmrHostidKeys.ARMT_SK.value).alias(ARMT_SK),
            f.col(KmrHostidKeys.LEND_ARMT_SK.value).alias(LEND_ARMT_SK),
            f.col(KmrHostidKeys.CASE_SK.value).alias(CLECT_EPSD_CASE_SK),
            FIN_RLF_ARMT_SUBTYPE_CD,
            f.col(dda + TmpPreprocessDdaclctepsd.FIN_RLF_ARMT_STAT_CD.value).alias(FIN_RLF_ARMT_STAT_TYPE_CD),
            f.when(
                (
                    (f.coalesce(f.trim(f.col(TmpPreprocessDdaclctepsd.DDE_COMMERCIAL_DDA.value)), f.lit("")) == "")
                    | (f.upper(f.trim(f.col(dda + TmpPreprocessDdaclctepsd.DDE_COMMERCIAL_DDA.value))) == "N")
                ),
                "N",
            )
            .when(f.upper(f.trim(f.col(TmpPreprocessDdaclctepsd.DDE_COMMERCIAL_DDA.value))) == "Y", "Y")
            .otherwise(None)
            .alias(COMRC_AVAIL_IND),
            f.lit(OWNG_LEND_ARMT_APPLN_KEY_DDA).alias(OWNG_LEND_ARMT_APPLN_KEY),
            f.col(dda + TmpPreprocessDdaclctepsd.DDE_FIN_RLF_ST_DT_DDA.value).alias(FIN_RLF_ST_DT),
            f.col(dda + TmpPreprocessDdaclctepsd.FIN_RLF_END_DT_DDA.value).alias(FIN_RLF_END_DT),
            f.col(TmpPreprocessDdaclctepsd.DDE_HARDSHIP_LVR_DDA.value).alias(HARDSP_LOAN_VALN_RATIO),
            f.when(
                (
                    f.datediff(
                        f.col(dda + TmpPreprocessDdaclctepsd.DDE_FIN_RLF_ST_DT_DDA.value),
                        f.col(TmpPreprocessDdaclctepsd.DDE_HARDSHIP_INFO_EXP_DT_DDA.value),
                    )
                    > 0
                ),
                f.col(TmpPreprocessDdaclctepsd.DDE_DUE_DATE_DDA.value),
            )
            .otherwise(f.col(TmpPreprocessDdaclctepsd.DDE_HARDSHIP_INFO_EXP_DT_DDA.value))
            .alias(HARDSP_EXPIR_DT),
            f.lit(TCS_APPLN_KEY).alias(PRV_SYS_APPLN_ID),
            f.col(TmpPreprocessDdaclctepsd.SRC_BDP_PARTITION_DDA.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    return df_target


def df_target_lis_df(df_lis_clects_epsd_kmr_key, df_temp_clects_epsd, df_harm, df_harm_erd):
    """
    Obtain the final transformed dataframe with target fields of compose temp table columns and KMR
    :param df_lis_clects__epsd_today: Dataframe containing current odate table data.
    :param df_lis_clects__epsd_yday: Dataframe containing previous odate table data.
    :return: dataframe with all the customer transaction related columns and KMR columns
    """
    lis = "Lisclctepsd."
    kmr_lis = KMRKEY + "."
    fin_rlf_armt_subtype_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY,
        "entity_name": FIN_REF_ENTITY_SUBTYPE_NM,
        "ref_src_cd": CollectEpi.DDE_ARREARS_REASON.value,
        "target_harm_cd": FIN_RLF_ARMT_SUBTYPE_CD,
    }
    df_temp_clects_epsd = generate_harmonized_code(df_temp_clects_epsd, df_harm, fin_rlf_armt_subtype_dict)
    fin_rlf_armt_stat_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": TCS_APPLN_KEY_ERD,
        "ref_src_cd": CollectEpi.DDE_ARREARS_CODE.value,
        "ref_erd_cd": ErdData.FIN_RLF_ARMT_STAT_TYPE_SRC_CD.value,
        "erd_tgt_cd": ErdData.FIN_RLF_ARMT_STAT_TYPE_CD.value,
        "target_harm_cd": FIN_RLF_ARMT_STAT_CD,
    }
    df_temp_clects_epsd = generate_harmonized_code_erd(df_temp_clects_epsd, df_harm_erd, fin_rlf_armt_stat_dict)
    df_target = (
        df_lis_clects_epsd_kmr_key.alias("Kmrkey")
        .join(
            df_temp_clects_epsd.alias("Lisclctepsd").filter(
                f.col(lis + TmpPreprocessLisclctepsd.DDE_FIN_RLF_ST_DT_LIS.value).isNotNull()
            ),
            (
                f.col(kmr_lis + KmrHostidKeys.CLECT_EPSD_HOST_ID.value)
                == f.col(lis + TmpPreprocessLisclctepsd.CLECT_EPSD_HOST_ID_LIS.value)
            ),
            INNER,
        )
        .select(
            f.col(KmrHostidKeys.ARMT_SK.value).alias(ARMT_SK),
            f.col(KmrHostidKeys.LEND_ARMT_SK.value).alias(LEND_ARMT_SK),
            f.col(KmrHostidKeys.CASE_SK.value).alias(CLECT_EPSD_CASE_SK),
            FIN_RLF_ARMT_SUBTYPE_CD,
            f.col(lis + TmpPreprocessLisclctepsd.FIN_RLF_ARMT_STAT_CD.value).alias(FIN_RLF_ARMT_STAT_TYPE_CD),
            f.when(
                (
                    (f.coalesce(f.trim(f.col(TmpPreprocessLisclctepsd.DDE_COMMERCIAL_LIS.value)), f.lit("")) == "")
                    | (f.upper(f.trim(f.col(lis + TmpPreprocessLisclctepsd.DDE_COMMERCIAL_LIS.value))) == "N")
                    | (f.upper(f.trim(f.col(TmpPreprocessLisclctepsd.DDE_COMMERCIAL_LIS.value))) != "Y")
                ),
                "N",
            )
            .when(f.upper(f.trim(f.col(TmpPreprocessLisclctepsd.DDE_COMMERCIAL_LIS.value))) == "Y", "Y")
            .otherwise(None)
            .alias(COMRC_AVAIL_IND),
            f.lit(OWNG_LEND_ARMT_APPLN_KEY_LIS).alias(OWNG_LEND_ARMT_APPLN_KEY),
            f.col(lis + TmpPreprocessLisclctepsd.DDE_FIN_RLF_ST_DT_LIS.value).alias(FIN_RLF_ST_DT),
            f.col(lis + TmpPreprocessLisclctepsd.FIN_RLF_END_DT_LIS.value).alias(FIN_RLF_END_DT),
            f.col(TmpPreprocessLisclctepsd.DDE_HARDSHIP_LVR_LIS.value).alias(HARDSP_LOAN_VALN_RATIO),
            f.when(
                (
                    f.datediff(
                        f.col(lis + TmpPreprocessLisclctepsd.DDE_FIN_RLF_ST_DT_LIS.value),
                        f.col(TmpPreprocessLisclctepsd.DDE_HARDSHIP_INFO_EXP_DT_LIS.value),
                    )
                    > 0
                ),
                f.col(TmpPreprocessLisclctepsd.DDE_PAYMENT_DUE_DATE_LIS.value),
            )
            .otherwise(f.col(TmpPreprocessLisclctepsd.DDE_HARDSHIP_INFO_EXP_DT_LIS.value))
            .alias(HARDSP_EXPIR_DT),
            f.lit(TCS_APPLN_KEY).alias(PRV_SYS_APPLN_ID),
            f.col(TmpPreprocessLisclctepsd.SRC_BDP_PARTITION_LIS.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    return df_target


def get_target_cust_role_data(app_id, df_cust_role_data, df_temp_host_id):
    """
    Get the transformed dataframe with target fields from temp_fdp048_prekmr_hostids_b2k_clects_epsd
    :param app_id: APPLICATION ID of the Providing System
    :param df_temp_host_id: Dataframe containing df_temp_host_id table data with Case SK and Party SK.
    :param df_b2k_cust_role_data: Dataframe containing df_b2k_cust_role_data table data.
    :return: dataframe with all the clect_epsd_cust_role related columns
    """

    df_filter = df_cust_role_data.alias(ClectEpsdCustRole.ALIAS.value)
    if app_id == B2K_APPLN_KEY:
        df_filter = df_cust_role_data.alias(ClectEpsdCustRole.ALIAS.value).filter(
            (col(ClectEpsdCustRole.DDE_ORIG_PROD_SYS.value).isin(["ICB", "INF", "MSS", "TBK"]))
        )

    df_target = (
        df_filter.join(
            df_temp_host_id.alias(TempHostId.ALIAS.value),
            (trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value)) == trim(col(PrmsToPay.CLECT_EPSD_HOST_ID_PTP.value))),
            INNER,
        )
        .select(
            col(TempHostId.PARTY_SK.value).alias(PARTY_SK),
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            lit(CUSTOMER_PARTY_ROLE).alias(CLECT_EPSD_PARTY_ROLE_TYPE_CD),
            lit(app_id).alias(PRV_SYS_APPLN_ID),
            col(ClectEpsdCustRole.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )

    return df_target


def get_mss_acct_trf(
    df_fdp048_temp_b2k,
    df_fdp048_temp_mip,
):

    df_exploded_temp1 = (
        df_fdp048_temp_mip.alias(TempMipRecov.ALIAS.value).filter((f.col(TempMipRecov.ACCT_NUM.value).like("%&%"))
                                  & (f.upper(f.col(TempMipRecov.ACCT_BRND.value)) == "WESTPAC"))
        .select(
            f.explode(f.split(f.col(TempMipRecov.ACCT_NUM.value),'&')).alias(ACCT_NUM),
            f.col(TempMipRecov.SECURITY_KEY.value).alias(SECURITY_KEY),
            f.col(TempMipRecov.PROPTY_ACQUISITION_DT.value).alias(PROPTY_ACQUISITION_DT),
            f.col(TempMipRecov.STREET_ADDR.value).alias(STREET_ADDR),
            f.col(TempMipRecov.TITLE_REFERENCE.value).alias(TITLE_REFERENCE),
            f.col(TempMipRecov.ACCT_BRND.value).alias(ACCT_BRND),
            f.col(TempMipRecov.STATE_CD.value).alias(STATE_CD),
            f.col(TempMipRecov.STAT_DESC.value).alias(STAT_DESC),
            f.col(TempMipRecov.DATE_OF_SALE.value).alias(DATE_OF_SALE),
            f.col(TempMipRecov.SALE_SETLMT_DT.value).alias(SALE_SETLMT_DT),
            f.col(TempMipRecov.SALE_AMT.value).alias(SALE_AMT),
            f.col(TempMipRecov.LST_VALN_DT.value).alias(LST_VALN_DT),
            f.col(TempMipRecov.CURNT_LOW_VALN_AMT.value).alias(CURNT_LOW_VALN_AMT),
        )
    )
    df_wo_explode_temp1 = (
        df_fdp048_temp_mip.alias(TempMipRecov.ALIAS.value).filter(~(f.col(TempMipRecov.ACCT_NUM.value).like("%&%"))
                                  & (f.upper(f.col(TempMipRecov.ACCT_BRND.value)) == "WESTPAC"))
        .select(
            f.col(TempMipRecov.ACCT_NUM.value).alias(ACCT_NUM),
            f.col(TempMipRecov.SECURITY_KEY.value).alias(SECURITY_KEY),
            f.col(TempMipRecov.PROPTY_ACQUISITION_DT.value).alias(PROPTY_ACQUISITION_DT),
            f.col(TempMipRecov.STREET_ADDR.value).alias(STREET_ADDR),
            f.col(TempMipRecov.TITLE_REFERENCE.value).alias(TITLE_REFERENCE),
            f.col(TempMipRecov.ACCT_BRND.value).alias(ACCT_BRND),
            f.col(TempMipRecov.STATE_CD.value).alias(STATE_CD),
            f.col(TempMipRecov.STAT_DESC.value).alias(STAT_DESC),
            f.col(TempMipRecov.DATE_OF_SALE.value).alias(DATE_OF_SALE),
            f.col(TempMipRecov.SALE_SETLMT_DT.value).alias(SALE_SETLMT_DT),
            f.col(TempMipRecov.SALE_AMT.value).alias(SALE_AMT),
            f.col(TempMipRecov.LST_VALN_DT.value).alias(LST_VALN_DT),
            f.col(TempMipRecov.CURNT_LOW_VALN_AMT.value).alias(CURNT_LOW_VALN_AMT),
        )
    )
    df_mip_explode = df_wo_explode_temp1.union(df_exploded_temp1)
    df_mip_explode = df_mip_explode.alias(TempMipRecov.ALIAS.value).filter(f.col(TempMipRecov.SECURITY_KEY.value).isNotNull())
    
    df_tmp_security_mss = (
        df_fdp048_temp_b2k.filter(f.trim(f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value)) == "MSS")
        .join(
            df_mip_explode.alias(TempMipRecov.ALIAS.value),
            (
                (
                    f.trim(df_fdp048_temp_b2k[Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value])
                    == f.trim(f.col(TempMipRecov.ACCT_NUM.value))
                )
                & (
                    f.trim(df_fdp048_temp_b2k[Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value])
                    <= f.trim(f.col(TempMipRecov.PROPTY_ACQUISITION_DT.value))
                )
            ),
            INNER,
        )
        .select(
            f.lpad(f.trim(f.col(ACCT_NUM).cast("string")), 20, "0").alias(ACCT_NUM),
            f.col(SECURITY_KEY).alias(FMT_SECURITY_NUM),
            f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value).alias(DDE_CURR_DT_ADD),
            f.col(TITLE_REFERENCE),
            f.col(PROPTY_ACQUISITION_DT),
            f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value).alias(DDE_ACCT_ID),
            f.col(Tempb2kclectsepsd.DDE_DTE_COLCTED_B2K.value).alias(DDE_DTE_COLCTED),
            f.col(ACCT_BRND),
            f.col(STATE_CD),
            f.col(STAT_DESC),
            f.col(DATE_OF_SALE),
            f.col(SALE_SETLMT_DT),
            f.col(SALE_AMT),
            f.col(LST_VALN_DT),
            f.col(CURNT_LOW_VALN_AMT),
            f.col(CLECT_EPSD_HOST_ID),
        ).distinct()
    )


    df_tmp_security_mss_not_null = df_tmp_security_mss.filter(f.col(FMT_SECURITY_NUM).isNotNull()).select(
        f.col(FMT_SECURITY_NUM),
        f.col(PROPTY_ACQUISITION_DT),
        f.col(DDE_ACCT_ID),
        f.col(DDE_DTE_COLCTED),
        f.col(STAT_DESC),
        f.col(DATE_OF_SALE),
        f.col(SALE_SETLMT_DT),
        f.col(SALE_AMT),
        f.col(LST_VALN_DT),
        f.col(CURNT_LOW_VALN_AMT),
        f.col(CLECT_EPSD_HOST_ID),
    )

    df_tmp_security_mss_error = df_tmp_security_mss.filter(f.col(FMT_SECURITY_NUM).isNull()).select(
        f.col(ACCT_BRND),
        f.col(ACCT_NUM),
        f.col(TITLE_REFERENCE),
        f.col(STATE_CD),
        lit("No Valid Title Reference for MSS").alias(DESCRIPTION),
    )

    return df_tmp_security_mss_not_null, df_tmp_security_mss_error


def get_rde_wrklist_target_df(df_a009a8_rde_worklists,
                              odate,
                              is_history_load=False):
    """
    Obtain all the target columns for rams worklist after applying the odate logic
    :return df_wrklist_target:
    """
    df_a009a8_rde_worklists = df_a009a8_rde_worklists.withColumn(
        "bdp_partition_wrklist",
        f.from_unixtime(
            f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"
        ),
    )

    df_rde_wrklist_target = df_a009a8_rde_worklists.\
        filter(f.col("bdp_partition_wrklist") == odate).\
        select(f.col("id").alias("id"),
               f.col("name").alias("name"),
               f.col("description").alias("description"))

    return df_rde_wrklist_target


def get_target_wrklist_rde_df(df_temp_rde_wrklist, odate, is_history_load=False):
    """
    Obtain all the target columns for df_temp_rde_wrklist after applying the provided logic
    :return df_target:
    """
    df_temp_rde_wrklist = (

        df_temp_rde_wrklist.alias(Worklist.ALIAS.value)
                 .select(f.col(Worklist.WRKFLW_SK.value),
                         f.col(Worklist.WRKLIST_NM.value),
                         f.col(Worklist.WRKLIST_DESC.value),
                         f.lit(RDE_PRV_SYS_APPLN_ID).alias(PRV_SYS_APPLN_ID)
    ).dropDuplicates()
                 )
    return df_temp_rde_wrklist

def get_rde_target_df(
    df_a009a8_rde_cs_account,
    df_a009a8_rde_accountactivities,
    df_a009a8_rde_accounts,
    df_a009a8_rde_hardship,
    odate,
    is_history_load=False,
):
    """
    Obtain all the target columns for get_rde_target_df after applying the provided logic
    :return df_target:
    """
    df_a009a8_rde_cs_account = df_a009a8_rde_cs_account.withColumn(
        "bdp_partition_cs",
        f.from_unixtime(
            f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"
        ),
    )

    df_a009a8_rde_accountactivities = df_a009a8_rde_accountactivities.withColumn(
        "datetime_activities",
        f.from_unixtime(f.unix_timestamp(f.col("datetime"), "yyyy-M-d"), "yyyy-MM-dd"),
    ).withColumn(
        "bdp_partition_activities",
        f.from_unixtime(
            f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"
        ),
    )

   
    df_a009a8_rde_hardship = df_a009a8_rde_hardship.withColumn(
        "bdp_partition_hrdsp",
        f.from_unixtime(
            f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"
        ),
    )

    print("\n###Results for df_a009a8_rde_hardship DF is {}\n")
    

    prevdate = odate - timedelta(days=1)
    clect_epsd_win = Window.partitionBy(f.col("reference"))
    df_target = (
        df_a009a8_rde_cs_account.join(
            df_a009a8_rde_accountactivities,
            (f.col(RdeCsAccount.ACCOUNTS1.value) == f.col(RdeAcctActivities.ACCOUNTID.value))
            & (f.col("bdp_partition_cs") == odate)
            & (f.col("bdp_partition_activities") <= odate)
            & (f.col("childid").isin(["505", "506"])),
            "left",
        )
        .join(
            df_a009a8_rde_accounts,
            (f.col(RdeCsAccount.ACCOUNTS1.value) == df_a009a8_rde_accounts["id"])
            & (f.col("bdp_partition_cs") == odate),
            "left",
        )
        .join(
            df_a009a8_rde_hardship.alias("a"),
            (df_a009a8_rde_cs_account["accounts1"] == f.col("a.accounts1"))
            & (f.col("bdp_partition_cs") == odate)
            & (f.col("a.bdp_partition_hrdsp") == odate),
            "left",
        )
        .join(
            df_a009a8_rde_hardship.alias("b"),
            (df_a009a8_rde_cs_account["accounts1"] == f.col("b.accounts1"))
            & (f.col("bdp_partition_cs") == odate)
            & (f.col("b.bdp_partition_hrdsp") == prevdate),
            "left",
        )
        .select(
            "datetime",
            "reference",
            f.col("a.status").alias("cur_status"),
            f.col("b.status").alias("pre_status"),
            f.col("a.SERVICEABILITYENDDT").alias("SERVICEABILITYENDDT"),
            f.col("a.assistance_end_date").alias("assistance_end_date"),
            f.col("a.status_update_date").alias("status_update_date"),
            f.col("a.approve_date").alias("cur_approve_date"),
            f.col("b.approve_date").alias("pre_approve_date"),
            f.col("a.treatment_status").alias("treatment_status"),
            f.col("a.disaster_flag").alias("disaster_flag"),
            f.col("a.HARDSHIP_REASON").alias("HARDSHIP_REASON"),
            f.col("worklistid"),
            f.col("childid"),
            f.col("bdp_partition_cs"),
            f.col("a.noncommercial"),
            f.col("treatment_code"),
        )
    )
    print("\n###Results for df_target DF is {}\n")



    df_target1 = df_target.select(
        when(
            f.col("childid") == "505",
            f.max(
                f.from_unixtime(f.unix_timestamp(f.col(RdeAcctActivities.DATETIME.value), "yyyy-MM-dd"), "yyyy-MM-dd")
            ).over(clect_epsd_win),
        )
        .otherwise(None)
        .alias("episode_start"),
        when(
            f.col("childid") == "506",
            f.max(
                f.from_unixtime(f.unix_timestamp(f.col(RdeAcctActivities.DATETIME.value), "yyyy-MM-dd"), "yyyy-MM-dd")
            ).over(clect_epsd_win),
        )
        .otherwise(None)
        .alias("episode_end"),
        when(f.upper(f.col("cur_status")) == "HARDSHIP APPROVED", f.date_format(f.col("cur_approve_date"),"yyyy-MM-dd"))
        .when(
            (f.upper(f.col("cur_status")) != "HARDSHIP APPROVED")
            & (f.upper(f.col("pre_status")) == "HARDSHIP APPROVED"),
            f.date_format(f.col("pre_approve_date"),"yyyy-MM-dd"),
        )
        .otherwise(None)
        .alias("fin_rlf_st_dt"),
        when(
            (f.upper(f.col("cur_status")) == "HARDSHIP APPROVED") & (f.col("SERVICEABILITYENDDT").isNotNull()),
            f.date_format(f.col("SERVICEABILITYENDDT"),"yyyy-MM-dd"),
        )
        .when(
            (f.upper(f.col("cur_status")) == "HARDSHIP APPROVED")
            & (f.col("SERVICEABILITYENDDT").isNull())
            & (f.col("assistance_end_date").isNotNull()),
            f.date_format(f.col("assistance_end_date"),"yyyy-MM-dd"),
        )
        .when(
            (f.upper(f.col("cur_status")) != "HARDSHIP APPROVED")
            & (f.upper(f.col("pre_status")) == "HARDSHIP APPROVED"),
            f.date_format(f.col("status_update_date"),"yyyy-MM-dd"),
        )
        .otherwise(None)
        .alias("fin_rlf_end_dt"),
        when(f.upper(f.col("cur_status")) == "HARDSHIP APPROVED", f.col("cur_status"))
        .when(
            (f.upper(f.col("cur_status")) != "HARDSHIP APPROVED")
            & (f.upper(f.col("pre_status")) == "HARDSHIP APPROVED"),
            f.col("pre_status"),
        )
        .otherwise(None)
        .alias("fin_rlf_armt_stat_type_cd"),
        when(f.col("treatment_status") == "Moratorium", f.lit("NIL Repayment"))
        .when(
            f.col("treatment_status").isin(
                ["Hardship Arrangement", "Hardship Monitoring", "Hardship Arrangement Broken"]
            ),
            f.lit("Reduced Payment"),
        )
        .when(
            f.col("treatment_status").isin(
                ["Serviceability", "Serviceability Monitoring", "Serviceability Completed", "Serviceability Broken"]
            ),
            f.lit("Serviceability"),
        )
        .when(
            (
                f.col("treatment_status").isin(
                    ["Serviceability", "Serviceability Monitoring", "Serviceability Completed", "Serviceability Broken"]
                )
            )
            & (f.col("disaster_flag") == "Y"),
            f.lit("Serviceability"),
        )
        .when((f.col("treatment_status") == "Moratorium") & (f.col("disaster_flag") == "Y"), f.lit("NIL Repayment"))
        .otherwise("UNK")
        .alias("fin_soltn_type_cd"),
        when(f.col("hardship_reason").isNotNull(), f.col("hardship_reason"))
        .otherwise("Missing Payment")
        .alias("arears_reasn_type_cd"),
        when(f.col("worklistid").isin(["63", "64", "65"]), f.lit("Y")).otherwise("N").alias("litign_ind_rams"),
        when(f.col("worklistid").isin(["24", "25"]), f.lit("BEPST")).otherwise("NOBER").alias("bank_error_invstgn_cd"),
        f.lpad(f.trim(f.col("reference")), 10, "0").alias("clect_epsd_lend_host_id"),
        f.col("reference"),
        f.col("bdp_partition_cs"),
        f.col("worklistid"),
        f.col("treatment_code"),
        f.col("noncommercial"),
        f.col("hardship_reason"),
    ).withColumn(
        "clect_epsd_host_id",
        f.concat_ws(
            "",
            f.lpad(f.trim(f.col("reference")), 20, "0"),
            f.date_format(f.col("episode_start"),"yyyyMMdd")
        ),
    )

    clect_epsd_win1 = Window.partitionBy(f.col("clect_epsd_host_id")).orderBy("bdp_partition_cs")
    clect_epsd_win2 = Window.partitionBy(f.col("clect_epsd_host_id"), f.col("litign_ind_rams")).orderBy(
        "bdp_partition_cs"
    )
    print("\n###Results for clect_epsd_win1 DF is {}\n")
    #df_target1.withColumn("test1", f.row_number().over(clect_epsd_win1)).show(30,truncate=False)
    print("\n###Results for clect_epsd_win2 DF is {}\n")
    #df_target1.withColumn("test2", f.row_number().over(clect_epsd_win2)).show(30,truncate=False)

    df_target2 = df_target1.withColumn(
        "grp", f.row_number().over(clect_epsd_win1) - f.row_number().over(clect_epsd_win2)
    ).withColumn(
        "episode_end", when(f.col("episode_end") > f.col("episode_start"), f.col("episode_end")).otherwise(None)
    )

    print("\n###Results for df_target2 DF is {}\n")


    clect_epsd_win3 = Window.partitionBy(f.col("grp"), f.col("clect_epsd_host_id"))
    df_target3 = df_target2.withColumn(
        "lit_st_dt",
        when(
            f.col("litign_ind_rams") == "Y",
            f.min(when(f.col("litign_ind_rams") == "Y", f.col("bdp_partition_cs"))).over(clect_epsd_win3),
        ),
    ).filter(f.col("episode_start").isNotNull())

    print("\n###Results for df_target3 DF is {}\n")


    df_target4 = df_target3.select(
        "clect_epsd_host_id",
        "episode_start",
        "episode_end",
        "fin_soltn_type_cd",
        "clect_epsd_lend_host_id",
        "litign_ind_rams",
        "lit_st_dt",
        "bank_error_invstgn_cd",
        "arears_reasn_type_cd",
        "noncommercial",
        "fin_rlf_armt_stat_type_cd",
        f.col("hardship_reason").alias("fin_rlf_armt_subtype_cd"),
        "fin_rlf_st_dt",
        "fin_rlf_end_dt",
        f.concat_ws(
            "",
            f.lpad(f.trim(f.col("reference")), 20, "0"),
            f.date_format(f.col("fin_rlf_st_dt"),"yyyyMMdd")
        ).alias("fin_rlf_armt_host_id"),
        "worklistid",
        "reference",
        "treatment_code",
        f.col("bdp_partition_cs").alias("src_bdp_partition"),
    ).dropDuplicates()

    #print("\n###Results for df_target4 DF is {}\n")
    return df_target4



def generate_lit_st_dt(df_target):
    clect_epsd_win_lit = Window.partitionBy(
        f.col("reference"), f.col("datetime_activities"), f.col(RdeAcctActivities.CHILDID.value)
    )
    df_target = df_target.filter(f.coalesce(f.col(RdeAcctActivities.CHILDID.value), f.lit(0)) != 506)
    df_target2 = (
        df_target.withColumn("curr_hrdshp", f.when(f.col("litign_ind_rams") == "Y", "Y").otherwise("N"))
        .withColumn(
            "prv_hrdshp",
            f.lead(f.col("litign_ind_rams")).over(clect_epsd_win_lit.orderBy(f.desc("bdp_partition_activities"))),
        )
        .withColumn(
            "min_hrdshp_dt",
            f.min(f.when(f.col("litign_ind_rams") == "Y", f.col("bdp_partition_activities")).otherwise(None)).over(
                clect_epsd_win_lit
            ),
        )
    )
    df_target3 = df_target2.withColumn(
        "lit_st_dt",
        f.to_date(
            f.when(
                ((f.col("curr_hrdshp") == "Y") & (f.coalesce(f.col("prv_hrdshp"), f.lit("N")) != "Y")),
                f.col("bdp_partition_activities"),
            )
            .when(((f.col("curr_hrdshp") == "Y") & (f.col("prv_hrdshp") == "Y")), f.col("min_hrdshp_dt"))
            .otherwise(None)
        ),
    )
    return df_target3


def get_target_clect_epsd_rde_df(
    df_b2k_clects_epsd, df_harm_ref, df_temp_host_id, df_fdp059_armt_clect_stat_type_map, odate
):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :param df_b2k_clects_epsd:
    :param df_harm_ref:
    :return df_target:
    """
    df_rde_clects_epsd = df_b2k_clects_epsd.withColumn(
        "treatment_code",
        when(trim(col("treatment_code")).isNull(), lit(None)).otherwise(trim(col("treatment_code"))),
    )

    df_clect_stat_type = df_rde_clects_epsd.join(
        df_fdp059_armt_clect_stat_type_map,
        trim(col("treatment_code")) == trim(col("armt_clect_stat_type_src_cd")),
        LEFT,
    ).withColumn(
        CLECT_STAT_TYPE_CD,
        when(trim(col("armt_clect_stat_type_cd")).isNull(), lit("UNK")).otherwise(
            trim(col("armt_clect_stat_type_cd"))
        ),
    )

    fin_soltn_type_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": RAMS_APPLN_KEY,
        "entity_name": FIN_SOLTN_TYPE,
        "ref_src_cd": CollectEpi.RDE_FINANCIAL_SOLUTION.value,
        "target_harm_cd": FIN_SOLTN_TYPE_CD,
    }

    df_fin_soltn_type = generate_harmonized_code(df_clect_stat_type, df_harm_ref, fin_soltn_type_dict)

    arears_type_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": RAMS_APPLN_KEY,
        "entity_name": Arears_Reason_Type,
        "ref_src_cd": CollectEpi.RDE_MI_ARRS_REASON.value,
        "target_harm_cd": AREARS_REASN_TYPE_CD,
    }

    df_arears_reasn_type = generate_harmonized_code(df_fin_soltn_type, df_harm_ref, arears_type_dict)
    win = Window.partitionBy(CollectEpi.DDE_ACCT_ID.value, CollectEpi.DDE_CURR_DT_ADD.value)

    df_target = (
        df_arears_reasn_type.join(
            df_temp_host_id.alias(TempHostId.ALIAS.value),
            (trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value)) == trim(col(CollectEpi.CLECT_EPSD_HOST_ID_CE.value))),
            INNER,
        )
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            col(TempHostId.LEND_ARMT_SK.value).alias(LEND_ARMT_SK),
            lit("A0077D:A0075C").cast("string").alias(OWNG_LEND_ARMT_APPLN_KEY),
            FIN_SOLTN_TYPE_CD,
            lit(None).cast("string").alias(CLECT_ENTRY_REASN_TYPE_CD),
            lit(None).cast("string").alias(CLECT_OUTSRCG_TYPE_CD),
            AREARS_REASN_TYPE_CD,
            # concat_ws(
            #   "|", trim(col(CollectEpi.DDE_ACCT_ID.value)), to_date(col(CollectEpi.DDE_CURR_DT_ADD.value), "yyyyMMdd")
            # ).alias(CLECT_EPSD_HOST_ID),
            #lit(None).cast("date").alias(CREAT_DT),
            col(CollectEpi.RDE_EPISODE_START.value).alias(CREAT_DT), 
            lit(None).cast("string").alias(SOFT_CHRG_OFF_PNDG_IND),
            col(CollectEpi.RDE_EPISODE_END.value).alias(CLECT_DT),
            lit(None).cast("string").alias(DEFER_WRITE_OFF_IND),
            lit(None).cast("string").alias(LTR_SUPRSN_IND),
            # to_date(col(CollectEpi.DDE_LETR_WARNING_DATE.value), "YYYY-MM-DD").alias(CRAA_LODG_DT),
            lit(None).cast("date").alias(CRAA_LODG_DT),
            lit(None).cast("string").alias(NO_PH_CONTCT_IND),
            lit(None).cast("int").alias(AUTO_LTR_DAY_CNT),
            lit(None).cast("int").alias(OMIT_FROM_SUPVSRY_QU_DAY_CNT),
            col(CollectEpi.RDE_LITIGATION.value).alias(LITIGN_IND),
            lit(None).cast(DecimalType(15, 2)).alias(CR_LISTNG_AMT),
            # round(col(CollectEpi.DDE_IL_RECOVERY_AMOUNT.value), 2).alias(RECOV_AMT),
            lit("AUD").cast("string").alias(CREDIT_LISTING_AMOUNT_CURRENCY_CODE),
            # lit(AUD).alias(RECOVERY_AMOUNT_CURRENCY_CODE),
            CLECT_STAT_TYPE_CD,
            col(CollectEpi.RDE_BANK_ERR_PRESENT.value).alias(CLECT_BANK_ERROR_CD),
            col(CollectEpi.RDE_LIT_START_DT.value).alias(LIT_START_DT),
            #col(CollectEpi.WORKLIST_WORKFLOW_SK.value).alias("worklist_workflow_sk"),
            col(TempHostId.wrkflw_sk.value).cast("string").alias("worklist_workflow_sk"),
            lit("A0077D:A00845").alias(PRV_SYS_APPLN_ID),
            lit("").alias(ALTN_STRAT_CD),
            col(CollectEpi.SRC_BDP_PARTITION.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    df_target.groupBy(df_target.case_sk).agg(count("*").alias("cnt")).filter(f.col("cnt")>1).show(20, truncate=False)

    return df_target


def df_target_rde_df(df_rde_clects_epsd_kmr_key, df_temp_clects_epsd, df_harm, df_harm_erd):
    """
    Obtain the final transformed dataframe with target fields of compose temp table columns and KMR
    :param df_rde_clects_epsd_kmr_key: Dataframe containing current odate table data.
    :return: dataframe with all the customer transaction related columns and KMR columns
    """
    rde = "Rdeclctepsd."
    fin_rlf_armt_subtype_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": RDE_APPLN_KEY,
        "entity_name": FIN_REF_ENTITY_SUBTYPE_NM,
        "ref_src_cd": CollectEpi.RDE_FIN_RLF_ARMT_SUBTYPE_CD.value,
        "target_harm_cd": FIN_RLF_ARMT_SUBTYPE_CD,
    }

    df_temp_clects_epsd = generate_harmonized_code(df_temp_clects_epsd, df_harm, fin_rlf_armt_subtype_dict)

    # print (" printing from df_temp_clects_epsd using temp_fdp048_kmr_preprocessing_b2k_clects_epsd,fdp048_ref_data_appln_map,fin_rlf_armt_subtype_dict ")
    # df_temp_clects_epsd.show()

    fin_rlf_armt_stat_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": RDE_APPLN_KEY_ERD,
        "ref_src_cd": CollectEpi.FIN_RLF_ARMT_STAT_TYPE_CD.value,
        "ref_erd_cd": ErdData.FIN_RLF_ARMT_STAT_TYPE_SRC_CD.value,
        "erd_tgt_cd": ErdData.FIN_RLF_ARMT_STAT_TYPE_CD.value,
        "target_harm_cd": FIN_RLF_ARMT_STAT_CD,
    }

    df_temp_clects_epsd = generate_harmonized_code_erd(df_temp_clects_epsd, df_harm_erd, fin_rlf_armt_stat_dict)

    # print (" printing from df_temp_clects_epsd using df_temp_clects_epsd,fdp059_fin_rlf_armt_stat_type_appln_map,fin_rlf_armt_stat_dict ")
    # df_temp_clects_epsd.show()

    df_target = (
        df_rde_clects_epsd_kmr_key.alias(TempHostId.ALIAS.value)
        .filter(f.col(TempHostId.FIN_RLF_ST_DT.value).isNotNull())
        .join(
            df_temp_clects_epsd.alias("Rdeclctepsd").filter(
                f.col(rde + TmpPreprocessRdeclctepsd.FIN_RLF_ST_DT_RDE.value).isNotNull()
            ),
            (
                (
                    f.col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value)
                    == f.col(rde + TmpPreprocessRdeclctepsd.CLECT_EPSD_HOST_ID_RDE.value)
                )
                & (
                    f.col(TempHostId.SRC_BDP_PARTITION.value)
                    == f.col(rde + TmpPreprocessRdeclctepsd.SRC_BDP_PARTITION_RDE.value)
                )
            ),
            INNER,
        )
        .select(
            f.col(TempHostId.ARMT_SK.value).alias(ARMT_SK),
            f.col(TempHostId.LEND_ARMT_SK.value).alias(LEND_ARMT_SK),
            f.col(TempHostId.CASE_SK.value).alias(CLECT_EPSD_CASE_SK),
            FIN_RLF_ARMT_SUBTYPE_CD,
            f.col(rde + TmpPreprocessRdeclctepsd.FIN_RLF_ARMT_STAT_CD.value).alias(FIN_RLF_ARMT_STAT_TYPE_CD),
            f.when(f.upper(f.trim(f.col(rde + TmpPreprocessRdeclctepsd.NONCOMMERCIAL_RDE.value))) == "Y", "N")
            .when(f.upper(f.trim(f.col(rde + TmpPreprocessRdeclctepsd.NONCOMMERCIAL_RDE.value))) == "N", "Y")
            .otherwise(None)
            .alias(COMRC_AVAIL_IND),
            f.lit(RDE_APPLN_KEY).alias(OWNG_LEND_ARMT_APPLN_KEY),
            f.col(rde + TmpPreprocessRdeclctepsd.FIN_RLF_ST_DT_RDE.value).alias(FIN_RLF_ST_DT),
            f.col(rde + TmpPreprocessRdeclctepsd.FIN_RLF_END_DT_RDE.value).alias(FIN_RLF_END_DT),
            lit(None).cast(DecimalType(12, 4)).alias(HARDSP_LOAN_VALN_RATIO),
            lit(None).cast(DateType()).alias(HARDSP_EXPIR_DT),
            f.lit(RDE_PRV_SYS_APPLN_ID).alias(PRV_SYS_APPLN_ID),
            f.col(rde + TmpPreprocessRdeclctepsd.SRC_BDP_PARTITION_RDE.value).alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )
    # print ("printing from df_target")
    # df_target.show()
    return df_target


def get_target_recov_rde_df(df_rde_clects_epsd, df_harm_ref, df_temp_host_id, df_rde_mip, order_date):
    """
    Obtain all the target columns for fin_tran_gl_entry after applying the provided logic
    :param df_b2k_clects_epsd:
    :param df_harm_ref:
    :return df_target:
    """

    
    df_mip_temp1 = (df_rde_mip.alias(TempMipRecov.ALIAS.value)
        .filter((f.col(TempMipRecov.ACCT_NUM.value).like("%&%"))
                                & (f.upper(f.col(TempMipRecov.ACCT_BRND.value)) == "RAMS") )
        .select(f.explode(f.split(f.col(MIPB2K.ACCT_NUM.value), '&')).alias(ACCT_NUM),
                                f.col(MIPB2K.SECURITY_KEY.value).alias(SECURITY_KEY),
                                f.col(MIPB2K.PROPTY_ACQUISITION_DT.value).alias(PROPTY_ACQUISITION_DT),
                                f.col(MIPB2K.STAT_DESC.value).alias(STAT_DESC))
                        )
    df_mip_temp2 = (df_rde_mip.alias(TempMipRecov.ALIAS.value).filter(
                                  ~(f.col(TempMipRecov.ACCT_NUM.value).like("%&%"))
                                  & (f.upper(f.col(TempMipRecov.ACCT_BRND.value)) == "RAMS")
                                  & (f.col(TempMipRecov.SECURITY_KEY.value).isNotNull()))
                                  .select(
                                  f.col(MIPB2K.ACCT_NUM.value).alias(ACCT_NUM),
                                  f.col(MIPB2K.SECURITY_KEY.value).alias(SECURITY_KEY),
                                  f.col(MIPB2K.PROPTY_ACQUISITION_DT.value).alias(PROPTY_ACQUISITION_DT),
                                  f.col(MIPB2K.STAT_DESC.value).alias(STAT_DESC))
    )
    df_mip_rde = df_mip_temp1.union(df_mip_temp2)
    df_mip_rde = df_mip_rde.withColumn(MIPB2K.STAT_DESC.value, f.concat_ws("", col(MIPB2K.STAT_DESC.value)))

    df_target = (
        df_rde_clects_epsd.join(
            df_temp_host_id.alias(TempHostId.ALIAS.value),
            (trim(col(TempHostId.CLECT_EPSD_HOST_ID_TEMP.value)) == trim(df_rde_clects_epsd["clect_epsd_host_id"]))
            & (df_rde_clects_epsd["worklistid"].isin([60, 63, 64, 65])),
            INNER,
        )
        .join(
            df_mip_rde,
            trim(df_rde_clects_epsd["reference"]) == trim(col(MIPB2K.ACCT_NUM.value)),
            LEFT,
        )
        .select(
            col(TempHostId.CASE_SK.value).alias(CASE_SK),
            when(
                ((df_rde_clects_epsd["worklistid"].isin([64, 65])) & (col(MIPB2K.STAT_DESC.value).isNotNull())),
                col(MIPB2K.STAT_DESC.value),
            )
            .when(
                ((df_rde_clects_epsd["worklistid"] == 64) & (col(MIPB2K.STAT_DESC.value).isNull())),
                lit("Ready for Legal"),
            )
            .when(
                ((df_rde_clects_epsd["worklistid"] == 65) & (col(MIPB2K.STAT_DESC.value).isNull())),
                lit("Ready for Write Off"),
            )
            .when(
                ((df_rde_clects_epsd["worklistid"] == 60)),
                lit("Recoveries Review"),
            )
            .when(
                ((df_rde_clects_epsd["worklistid"] == 63)),
                lit("Internal Recoveries"),
            )
            .otherwise("UNK")
            .alias("recov_stat_cd"),
            df_rde_clects_epsd["src_bdp_partition"].alias(SRC_BDP_PARTITION),
        )
        .dropDuplicates()
    )

    clect_recov_type_dict = {
        "alias": CollectEpi.ALIAS.value,
        "app_id": RDE_APPLN_KEY,
        "entity_name": RECOVERY_STATUS_CODE,
        "ref_src_cd": CollectEpi.RDE_STAT_CD.value,
        "target_harm_cd": CLECT_RECOV_STATUS_CD,
    }

    clect_recov_type = generate_harmonized_code(df_target, df_harm_ref, clect_recov_type_dict)
    return clect_recov_type.select(
        f.col(CASE_SK),
        lit(None).cast("string").alias(CLECT_RECOV_TYPE_CD),
        f.col(CLECT_RECOV_STATUS_CD),
        lit("A0077D:A00845").alias(PRV_SYS_APPLN_ID),
        f.col(SRC_BDP_PARTITION),
    )


def generate_harmonized_code_erd(df, harm_erd_df, col_dict):
    # Parse dict
    src_alias = col_dict["alias"]
    erd_app_id = col_dict["app_id"]
    ref_src_cd = col_dict["ref_src_cd"]
    ref_erd_cd = col_dict["ref_erd_cd"]
    erd_tgt_cd = col_dict["erd_tgt_cd"]
    target_harm_cd = col_dict["target_harm_cd"]
    # Applying Filter for Harmonized data frame on entity name and Application Key
    erd_harm_df = harm_erd_df.alias(ErdData.ALIAS.value).filter(
        trim(upper(col(ErdData.IT_APPLN_RSRC_KEY.value))) == erd_app_id
    )
    # Joining input source df and filtered harmonized erd df on REF_SRC_CD
    merge_df = df.alias(src_alias).join(
        erd_harm_df.alias(ErdData.ALIAS.value),
        trim(upper(f.col(ref_erd_cd))) == trim(upper(f.col(ref_src_cd))),
        LEFT,
    )
    # Populate Harmonized column
    target_df = merge_df.withColumn(
        target_harm_cd,
        when(col(ref_src_cd).isNull(), None)
        .when(col(erd_tgt_cd).isNull() & col(ref_src_cd).isNotNull(), "UNK")
        .otherwise(col(erd_tgt_cd)),
    )
    return target_df


def get_clect_epsd_cmt_preproc_df(odate, col1_cmt_accnts_df):

    account_id_zero = "account_id_zero"
    account_id = "account_id"
    stg_df1 = col1_cmt_accnts_df.alias(CMTAccts.ALIAS.value).\
        filter((f.col(CMTAccts.ASSIST_HARDSHIP_START_DT_1.value).isNotNull())).\
        select(CMTAccts.CASE_ID.value,
                CMTAccts.ACCOUNT_ID.value,
                CMTAccts.BRAND.value,
                CMTAccts.PRODUCT_TYPE.value,
                CMTAccts.ASSIST_HARDSHIP_START_DT_1.value,
                CMTAccts.WORK_LIST_NAME.value,
                CMTAccts.DATE_LAST_MOVED_WORKLIST.value,
                CMTAccts.ASSIST_REASON.value).\
        dropDuplicates()

    #logger.info("Applying filter on Brand & Product")
    stg_df2 = stg_df1.alias(CMTAccts.ALIAS.value).filter(
        (
                (f.trim(f.col(CMTAccts.BRAND.value)) == 'Capital Finance') &
                (f.trim(f.col(CMTAccts.PRODUCT_TYPE.value)).isin('Auto Finance', 'Commercial Loan',
                                                                 'Secured Personal Loan',
                                                                 'Unsecured Personal Loan', 'Equipment Finance',
                                                                 'Commercial Hire Purchase', 'Novated Lease',
                                                                 'Goods Mortgage'))
        )
        |
        (
                (f.trim(f.col(CMTAccts.BRAND.value)) == 'St George') &
                (f.trim(f.col(CMTAccts.PRODUCT_TYPE.value)).isin('Equipment Finance', 'Commercial Loan',
                                                                 'Secured Personal Loan',
                                                                 'Commercial Hire Purchase', 'Unsecured Personal Loan'))
        )
    ).withColumn(account_id_zero,
                 f.when(f.substring(f.col(CMTAccts.ACCOUNT_ID.value), 0, 3).isin('520', '521', '550', '551', '571'),
                        f.concat_ws("", f.lit("0"), f.col(CMTAccts.ACCOUNT_ID.value))).otherwise(
                     f.col(CMTAccts.ACCOUNT_ID.value)))

    stg_df3 = stg_df2.alias(CMTAccts.ALIAS.value).\
        select(CMTAccts.CASE_ID.value,
               stg_df2.account_id_zero.alias(account_id),
               CMTAccts.BRAND.value,
               CMTAccts.PRODUCT_TYPE.value,
               CMTAccts.ASSIST_HARDSHIP_START_DT_1.value,
               CMTAccts.WORK_LIST_NAME.value,
               CMTAccts.DATE_LAST_MOVED_WORKLIST.value,
               CMTAccts.ASSIST_REASON.value
               )

    # logger.info("Deriving  Columns - APP_ID - Completed")

    return stg_df3


def get_erd_cmt_df(stat_app_map_df):

    target_df = stat_app_map_df.alias(ErdData.ALIAS.value).\
        filter((upper(trim(f.col(ErdData.IT_APPLN_RSRC_KEY.value))) == 'A00D41')). \
        select(ErdData.IT_APPLN_RSRC_KEY.value,
               ErdData.FIN_RLF_ARMT_STAT_TYPE_CD.value,
               ErdData.FIN_RLF_ARMT_STAT_TYPE_SRC_CD.value)

    return target_df


def get_hrm_cmt_df(data_appln_map_df):

    targ_df = data_appln_map_df.alias(HarmRef.ALIAS.value) \
        .filter((upper(trim(f.col(HarmRef.REF_ENTITY_NM.value))) == 'FINANCIAL RELIEF ARRANGEMENT SUBTYPE') & (
            upper(trim(f.col(HarmRef.IT_APPLN_KEY.value))) == CMT_APPN_KEY))\
        .select(HarmRef.REF_TYPE_CD.value,
                HarmRef.REF_TYPE_SRC_CD.value,
                HarmRef.REF_ENTITY_NM.value,
                HarmRef.IT_APPLN_KEY.value)

    return targ_df


def get_clect_epsd_cmt_df(cmt_prekmr_df, data_app_map_df, stat_app_map_df, odate):

    # fetch direct mapping columns
    stg_df1 = cmt_prekmr_df.alias(CMTAccts.ALIAS.value).\
        withColumn(FinRlfArmtCMT.FIN_RLF_END_DT.value,
                   when(upper(f.col(CMTAccts.WORK_LIST_NAME.value)).isin('DECLINE', 'WITHDRAWN', 'COMPLETE'),
                        f.col(CMTAccts.DATE_LAST_MOVED_WORKLIST.value))
                   .otherwise(None)). \
        withColumn(FinRlfArmtCMT.OWNG_LEND_ARMT_APPLN_KEY.value,
                   when(f.substring(f.col(CMTAccts.ACCOUNT_ID.value), 0, 3).isin('008', '009', '083', '084'), 'A0077D:A002F5')
                   .when(f.substring(f.col(CMTAccts.ACCOUNT_ID.value), 0, 4).isin('0520', '0521', '0550', '0551', '0571'), 'A0077D:A0018E')).\
        withColumn(FinRlfArmtCMT.FIN_RLF_ST_DT.value, f.col(CMTAccts.ASSIST_HARDSHIP_START_DT_1.value))\
        .dropDuplicates()

    # apply joins & columns with null
    stg_df2 = stg_df1.alias(CMTAccts.ALIAS.value) \
        .join(data_app_map_df.alias(HarmRef.ALIAS.value),
              f.col(CMTAccts.ASSIST_REASON.value) == f.col(HarmRef.REF_TYPE_SRC_CD.value), "left") \
        .join(stat_app_map_df.alias(ErdData.ALIAS.value),
              f.col(CMTAccts.WORK_LIST_NAME.value) == f.col(ErdData.FIN_RLF_ARMT_STAT_TYPE_SRC_CD.value), 'left'). \
        withColumn(FinRlfArmtCMT.FIN_RLF_ARMT_SUBTYPE_CD.value,
                   when(f.col(CMTAccts.ASSIST_REASON.value).isNull(), None)
                   .when((f.col(HarmRef.REF_TYPE_CD.value).isNull() & f.col(CMTAccts.ASSIST_REASON.value).isNotNull()), "UNK")
                   .otherwise(f.col(HarmRef.REF_TYPE_CD.value))). \
        withColumn(FinRlfArmtCMT.FIN_RLF_ARMT_STAT_TYPE_CD.value,
                   when(f.col(ErdData.FIN_RLF_ARMT_STAT_TYPE_CD.value).isNull(), "UNK")
                   .otherwise(f.col(ErdData.FIN_RLF_ARMT_STAT_TYPE_CD.value))). \
        withColumn(FinRlfArmtCMT.CLECT_EPSD_CASE_SK.value, lit(None)).\
        withColumn(FinRlfArmtCMT.COMRC_AVAIL_IND.value, lit(None)).\
        withColumn(FinRlfArmtCMT.HARDSP_LOAN_VALN_RATIO.value, lit(None)).\
        withColumn(FinRlfArmtCMT.HARDSP_EXPIR_DT.value, lit(None)). \
        withColumn(SRC_BDP_PARTITION, f.to_date(f.lit(odate), "yyyy-MM-dd")). \
        dropDuplicates()

    # logger.info("get final df - applying transformation logic - completed")

    # logger.info("get final df - prepare final df- start")

    # typecasting & final df
    stg_df3 = stg_df2.select(
        stg_df2.armt_sk.cast(LongType()).alias(FinRlfArmtCMT.ARMT_SK.value),
        stg_df2.lend_armt_sk.cast(LongType()).alias(FinRlfArmtCMT.LEND_ARMT_SK.value),
        col(FinRlfArmtCMT.CLECT_EPSD_CASE_SK.value).cast(LongType()).alias(FinRlfArmtCMT.CLECT_EPSD_CASE_SK.value),
        stg_df2.fin_rlf_armt_subtype_cd.cast(StringType()).alias(FinRlfArmtCMT.FIN_RLF_ARMT_SUBTYPE_CD.value),
        stg_df2.fin_rlf_armt_stat_type_cd.cast(StringType()).alias(FinRlfArmtCMT.FIN_RLF_ARMT_STAT_TYPE_CD.value),
        col(FinRlfArmtCMT.COMRC_AVAIL_IND.value).cast(StringType()).alias(FinRlfArmtCMT.COMRC_AVAIL_IND.value),
        stg_df2.owng_lend_armt_appln_key.cast(StringType()).alias(FinRlfArmtCMT.OWNG_LEND_ARMT_APPLN_KEY.value),
        stg_df2.assist_hardship_start_dt_1.cast(DateType()).alias(FinRlfArmtCMT.FIN_RLF_ST_DT.value),
        stg_df2.fin_rlf_end_dt.cast(DateType()).alias(FinRlfArmtCMT.FIN_RLF_END_DT.value),
        col(FinRlfArmtCMT.HARDSP_LOAN_VALN_RATIO.value).cast(DecimalType(38, 12)).alias(FinRlfArmtCMT.HARDSP_LOAN_VALN_RATIO.value),
        col(FinRlfArmtCMT.HARDSP_EXPIR_DT.value).cast(DateType()).alias(FinRlfArmtCMT.HARDSP_EXPIR_DT.value),
        lit(CMT_APPN_KEY).cast(StringType()).alias(FinRlfArmtCMT.PRV_SYS_APPLN_ID.value),
        col(SRC_BDP_PARTITION).cast(DateType()).alias(SRC_BDP_PARTITION)
    ).dropDuplicates()

    # stg_df3.show(100, False)
    # logger.info("get final df - prepare final df- completed")
    return stg_df3


def get_b2k_infolease_df(df_b2k_clects_epsd, df_activity, df_kmrpp, odate):

    print("declare variables")
    acty_cmp_dt = "dde_cmp_date"
    acty_cmp_time = "dde_cmp_time"
    acty_acty_cd = "dde_acty_cd"
    DDEP_APPL = "dde_appl"
    DDE_REVIEW_OUTCOME_IND = "dde_review_outcome_ind"
    acct_id = "dde_acct_id"
    acty_acct_id = "acty_acct_id"

    bdp_year = "bdp_year"
    bdp_month = "bdp_month"
    bdp_day = "bdp_day"
    bdp_part_b2k  = "bdp_part_b2k"
    bdp_part_acty = "bdp_part_acty"
    hardship_start = "hardship_start"
    tmp_hardship_start = "tmp_hardship_start"

    der_armt_sk = "der_armt_sk"
    der_case_sk = "der_case_sk"
    der_lend_armt_sk = "der_lend_armt_sk"
    fin_rlf_st_dt = "fin_rlf_st_dt"
    fin_rlf_end_dt = "fin_rlf_end_dt"
    hardsp_loan_valn_ratio = "hardsp_loan_valn_ratio"
    hardsp_expir_dt = "hardsp_expir_dt"
    comrc_avail_ind = "comrc_avail_ind"
    infls_acct_id = "infls_acct_id"
    odate_col = "odate_col"
    fltr_dt_col = "fltr_dt_col"
    prev_fin_end_dt = "prev_fin_end_dt"
    prev_der_armt_sk = "prev_der_armt_sk"

    fltr_dt = odate - relativedelta(days=6)

    print(fltr_dt)

    print("fetching required cols - acct")
    """ Fetch required columns"""
    b2k_df = df_b2k_clects_epsd \
        .select(f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value),
                f.trim(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value)).alias(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_IND_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_REC_STAT_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_DTE_COLCTED_B2K.value),
                f.col(DDEP_APPL),
                f.col(Tempb2kclectsepsd.DDE_MI_ARRS_REASON_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value),
                f.col(DDE_REVIEW_OUTCOME_IND),
                f.col(Tempb2kclectsepsd.DDE_REC_STAT_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_LVR_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value),
                f.col(bdp_year),
                f.col(bdp_month),
                f.col(bdp_day)
                ) \
        .withColumn(bdp_part_b2k, f.to_date(f.concat_ws("-", f.col(bdp_year), f.col(bdp_month), f.col(bdp_day))))\
        .withColumn(odate_col, f.lit(odate)).withColumn(fltr_dt_col, f.lit(fltr_dt))

    b2k_df.show(10, False)

    print("fetching required cols - acty")
    activity_df = df_activity.select(
        f.trim(f.col(acct_id)).alias(acty_acct_id),
        f.col(acty_cmp_dt), f.col(acty_cmp_time),
        f.col(acty_acty_cd),
        f.col(bdp_year),
        f.col(bdp_month),
        f.col(bdp_day),
        f.to_date(f.concat_ws("-", f.col(bdp_year), f.col(bdp_month), f.col(bdp_day))).alias(bdp_part_acty)
    )

    activity_df.count()
    activity_df.show(10, False)

    prev_df = df_kmrpp \
        .filter((f.col(fin_rlf_end_dt).isNotNull())
                ) \
        .select(f.col(der_armt_sk).alias(prev_der_armt_sk),
                f.col(fin_rlf_end_dt).alias(prev_fin_end_dt))
    prev_df.show(1000, False)

    print("acty -  filter")
    print(odate)
    activity_df.show(100, False)
    acty_df = activity_df\
        .filter( (f.trim(f.col(acty_acty_cd)) == 'RHAA') & (f.to_date(f.col(bdp_part_acty)) <= odate) )\
        .groupby(f.col(acty_acct_id)).agg(f.max(f.col(bdp_part_acty)).alias(tmp_hardship_start))\
        .select(f.col(acty_acct_id), f.col(tmp_hardship_start)).dropDuplicates()
    activity_df.count()
    activity_df.show(10, False)

    print("acct - filter 1")
    b2k_fltr1_df = b2k_df. \
        filter(
        (to_date(f.col(bdp_part_b2k)) == f.to_date(f.col(odate_col))) &
        (f.upper(f.trim(f.col(DDEP_APPL))) == 'WEF') &
        (f.upper(f.trim(f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value))).isin('INF'))
    )
    b2k_fltr1_df.count()
    b2k_fltr1_df.show(10, False)
    print("acct - filter 2")

    b2k_fltr2_df = b2k_df. \
        filter(
        (f.to_date(f.col(bdp_part_b2k)).between(f.to_date(f.col(fltr_dt_col)), f.to_date(f.col(odate_col))))  &
        (f.upper(f.trim(f.col(DDEP_APPL))) == 'WEF') &
        (f.upper(f.trim(f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value))).isin('INF'))
        & (f.col(Tempb2kclectsepsd.DDE_DTE_COLCTED_B2K.value).isNotNull())
        & (f.to_date(f.col(Tempb2kclectsepsd.DDE_DTE_COLCTED_B2K.value)) >= odate)
    )
    b2k_fltr2_df.count()
    b2k_fltr2_df.show(10, False)
    print("b2k - union")
    b2k_unn_df = b2k_fltr1_df.union(b2k_fltr2_df).dropDuplicates()
    b2k_unn_df.count()
    b2k_unn_df.show(10, False)

    print("acct - acty - join")
    acct_df = b2k_unn_df.join(acty_df,
                              f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value) == f.col(acty_acct_id),
                              "inner"
                              )\
        .withColumn(hardship_start,
                    f.when(
                        (f.to_date(f.col(tmp_hardship_start)) >=
                         f.to_date(f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value))),
                        f.to_date(f.col(tmp_hardship_start))
                    )
                    .otherwise(f.lit(None))
                    )\
        .filter(f.col(hardship_start).isNotNull())\
        .drop(acty_acct_id, tmp_hardship_start)
    acct_df.count()
    acct_df.show(10, False)

    print("derive columns - infolease")
    inflz_df = acct_df\
        .withColumn(der_armt_sk,
                    f.concat_ws("",
                                f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value),
                                f.date_format(f.to_date(f.col(hardship_start)), "yyyyMMdd")
                                )
                    )\
        .withColumn(der_case_sk,
                    f.concat_ws("",
                                f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value),
                                f.date_format(f.to_date(f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value)), "yyyyMMdd")
                                )
                    )\
        .withColumn(der_lend_armt_sk,
                    f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value)
                    )\
        .withColumn(fin_rlf_st_dt, f.col(hardship_start))\
        .withColumn(fin_rlf_end_dt,
                    f.when(
                        (
                                (
                                        (f.trim(f.col(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value)) == '') &
                                        (f.trim(f.col(DDE_REVIEW_OUTCOME_IND)) == '')
                                ) |
                                (
                                        f.upper(f.col(Tempb2kclectsepsd.DDE_REC_STAT_B2K.value)) == 'I'
                                )
                        ) &
                        (
                                f.col(bdp_part_b2k) > f.col(hardship_start)
                        ),
                        f.col(bdp_part_b2k)
                    )
                    .otherwise(f.lit(None))
                    )\
        .withColumn(hardsp_loan_valn_ratio, f.col(Tempb2kclectsepsd.DDE_HARDSHIP_LVR_B2K.value))\
        .withColumn(hardsp_expir_dt,
                    f.when(
                        f.datediff(fin_rlf_st_dt, f.to_date(f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value))) > 0,
                        f.col(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value))
                    .otherwise(f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value))
                    )\
        .withColumn(comrc_avail_ind, f.when(f.upper(f.trim(f.col(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value))).isin('', 'N'), 'N')
                    .when(f.upper(f.trim(f.col(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value))).isin('Y'), 'Y').otherwise(f.lit(None)))
    inflz_df.count()
    inflz_df.show(10, False)

    print("filter out the records which are already existing with fin_rlf_end_dt")

    inflz_dly_df = inflz_df \
        .join(prev_df, f.col(der_armt_sk) == f.col(prev_der_armt_sk), "left") \
        .filter(f.col(prev_der_armt_sk).isNull())
    inflz_dly_df.select(f.col(der_armt_sk), f.col(fin_rlf_end_dt)).show(500, False)
    print("Count from left join - {}".format(inflz_dly_df.count()))

    print("prepare - final df")
    final_df = inflz_dly_df.select(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value).alias(infls_acct_id),
                               f.col(der_armt_sk),
                               f.col(der_case_sk),
                               f.col(der_lend_armt_sk),
                               f.col(fin_rlf_st_dt),
                               f.col(fin_rlf_end_dt),
                               f.col(hardsp_loan_valn_ratio),
                               f.col(hardsp_expir_dt),
                               f.col(comrc_avail_ind),
                               f.col(Tempb2kclectsepsd.DDE_MI_ARRS_REASON_B2K.value),
                               f.col(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value),
                               f.col(DDE_REVIEW_OUTCOME_IND)
                               ).dropDuplicates()
    print("final df - prepared")
    final_df.count()
    final_df.show(10, False)

    return  final_df


def get_b2k_infolease_target_df(df_prekmr, df_hrm, df_erd, df_self_fin_armt, df_clect_epsd, odate):

    ref_entity_nm = "ref_entity_nm"
    it_appln_rsrc_key = "it_appln_rsrc_key"
    ref_type_cd = "ref_type_cd"
    ref_type_src_cd = "ref_type_src_cd"
    fin_rlf_armt_stat_type_src_cd = "fin_rlf_armt_stat_type_src_cd"
    inf_stat_type_src_cd ="inf_stat_type_src_cd"
    dde_mi_arrs_reason= "dde_mi_arrs_reason"
    der_fin_rlf_armt_stat_type_cd = "der_fin_rlf_armt_stat_type_cd"
    fin_rlf_armt_stat_type_cd = "fin_rlf_armt_stat_type_cd"
    fin_rlf_armt_subtype_cd = "fin_rlf_armt_subtype_cd"
    odate_col = "odate_col"
    prev_armt_sk = "prev_armt_sk"
    prev_fin_end_dt = "prev_fin_end_dt"

    fin_rlf_end_dt = "fin_rlf_end_dt"

    prev_dt = odate - relativedelta(days=1)

    print("previous date")
    print(prev_dt)
    df_self_fin_armt.show(100, False)
    df_self_fin_armt.count()

    df2 = df_self_fin_armt.filter((f.col(FinRlfArmtCMT.OWNG_LEND_ARMT_APPLN_KEY.value) == OWNG_LEND_ARMT_APPLN_KEY_INFLIS) )
    df2.show(100, False)
    df2.count()

    df3 = df2.filter((f.col(fin_rlf_end_dt).isNotNull()))
    df3.show(100, False)
    df3.count()

    df_self_fin_armt.select(f.to_date(f.col("st_dt"))).distinct().show()

    prev_day_df = df_self_fin_armt\
        .filter((f.col(FinRlfArmtCMT.OWNG_LEND_ARMT_APPLN_KEY.value) == OWNG_LEND_ARMT_APPLN_KEY_INFLIS) &
                (f.col(fin_rlf_end_dt).isNotNull())
                )\
        .select(f.col(FinRlfArmtCMT.ARMT_SK.value).alias(prev_armt_sk),
                f.col(FinRlfArmtCMT.FIN_RLF_END_DT.value).alias(prev_fin_end_dt))

    prev_day_df.show(100, False)

    prev_day_fltr_df = df_prekmr\
        .join(prev_day_df, f.col(FinRlfArmtCMT.ARMT_SK.value) == f.col(prev_armt_sk), "left")\
        .filter(f.col(prev_armt_sk).isNull())

    prev_day_fltr_df.show(100, False)

    df_hrm_fltr = df_hrm.filter((f.upper(f.trim(f.col(ref_entity_nm))) == 'FINANCIAL RELIEF ARRANGEMENT SUBTYPE')
                                & (f.col(it_appln_rsrc_key) == INFLIS_APPN_KEY)).select(ref_entity_nm, ref_type_cd, ref_type_src_cd, it_appln_rsrc_key)

    df_erd_fltr = df_erd.filter((f.upper(f.trim(f.col(it_appln_rsrc_key))) == 'A001F5')).select(it_appln_rsrc_key, fin_rlf_armt_stat_type_cd, fin_rlf_armt_stat_type_src_cd)

    df_clect_epsd = df_clect_epsd.select(f.col(CASE_SK).alias("case_sk_clect_cpsd"))

    acct_df = prev_day_fltr_df.withColumn(inf_stat_type_src_cd,
                                   f.when( ((f.trim(f.upper(f.col(TempInflsPreKMR.DDE_FINANCIAL_SOLUTION.value))) == 'Y' ) &  (f.trim(f.upper(f.col(TempInflsPreKMR.DDE_REVIEW_OUTCOME_IND.value))) == 'BH' ) ), 'A' )
                                   .when( ((f.trim(f.upper(f.col(TempInflsPreKMR.DDE_FINANCIAL_SOLUTION.value))) == 'Y' ) &  (f.trim(f.upper(f.col(TempInflsPreKMR.DDE_REVIEW_OUTCOME_IND.value))) == 'BR' )), 'R')
                                   .when( ((f.trim(f.upper(f.col(TempInflsPreKMR.DDE_FINANCIAL_SOLUTION.value))) == 'Y' ) &  (f.trim(f.upper(f.col(TempInflsPreKMR.DDE_REVIEW_OUTCOME_IND.value))) == 'BS' )), 'SERV' )
                                   .when( ((f.trim(f.upper(f.col(TempInflsPreKMR.DDE_FINANCIAL_SOLUTION.value))) == 'Y' ) &  (f.trim(f.upper(f.col(TempInflsPreKMR.DDE_REVIEW_OUTCOME_IND.value))) == 'BD' )), 'D' )
                                   .when(((f.trim(f.upper(f.col(TempInflsPreKMR.DDE_FINANCIAL_SOLUTION.value))) == '') & (f.trim(f.upper(f.col(TempInflsPreKMR.DDE_REVIEW_OUTCOME_IND.value))) == '')), 'EXT')
                                   .otherwise(f.lit(None))
                                   )

    b2k_df = acct_df.alias("acct")\
        .join(df_hrm_fltr.alias("hrm"), f.col("acct." + dde_mi_arrs_reason) == f.col("hrm." + ref_type_src_cd), "left")\
        .join(df_erd_fltr.alias("erd"), f.col("acct." + inf_stat_type_src_cd) == f.col("erd."  + fin_rlf_armt_stat_type_src_cd), "left" )\
        .join(df_clect_epsd, f.col(TempInflsPreKMR.CLECT_EPSD_CASE_SK.value) == f.col("case_sk_clect_cpsd"), "left")

    infls_df =  b2k_df\
        .withColumn(der_fin_rlf_armt_stat_type_cd,
                    f.when( (f.col(fin_rlf_armt_stat_type_cd).isNull()), f.lit("UNK"))
                    .otherwise(f.col(fin_rlf_armt_stat_type_cd))
                    )\
        .withColumn(fin_rlf_armt_subtype_cd,
                    f.when( (f.col(dde_mi_arrs_reason).isNull()), f.lit(None))
                    .when( ( (f.col(dde_mi_arrs_reason).isNotNull())  & (f.col(ref_type_cd).isNull()) ), f.lit("UNK"))
                    .otherwise(f.col(ref_type_cd))
                    )\
        .withColumn(odate_col, f.lit(odate))

    final_df = infls_df.select(
        f.col(TempInflsPreKMR.ARMT_SK.value).cast(LongType()).alias(FinRlfArmtCMT.ARMT_SK.value),
        f.col(TempInflsPreKMR.LEND_ARMT_SK.value).cast(LongType()).alias(FinRlfArmtCMT.LEND_ARMT_SK.value),
        f.col(TempInflsPreKMR.CLECT_EPSD_CASE_SK.value).cast(LongType()).alias(FinRlfArmtCMT.CLECT_EPSD_CASE_SK.value),
        f.col(fin_rlf_armt_subtype_cd).alias(FinRlfArmtCMT.FIN_RLF_ARMT_SUBTYPE_CD.value),
        f.col(der_fin_rlf_armt_stat_type_cd).alias(FinRlfArmtCMT.FIN_RLF_ARMT_STAT_TYPE_CD.value),
        col(TempInflsPreKMR.COMRC_AVAIL_IND.value).cast(StringType()).alias(FinRlfArmtCMT.COMRC_AVAIL_IND.value),
        f.lit(OWNG_LEND_ARMT_APPLN_KEY_INFLIS).cast(StringType()).alias(FinRlfArmtCMT.OWNG_LEND_ARMT_APPLN_KEY.value),
        f.col(TempInflsPreKMR.FIN_RLF_ST_DT.value).cast(DateType()).alias(FinRlfArmtCMT.FIN_RLF_ST_DT.value),
        f.col(TempInflsPreKMR.FIN_RLF_END_DT.value).cast(DateType()).alias(FinRlfArmtCMT.FIN_RLF_END_DT.value),
        f.col(TempInflsPreKMR.HARDSP_LOAN_VALN_RATIO.value).cast(DecimalType(38, 12)).alias(
            FinRlfArmtCMT.HARDSP_LOAN_VALN_RATIO.value),
        f.col(TempInflsPreKMR.HARDSP_EXPIR_DT.value).cast(DateType()).alias(FinRlfArmtCMT.HARDSP_EXPIR_DT.value),
        f.lit(INFLIS_APPN_KEY).cast(StringType()).alias(FinRlfArmtCMT.PRV_SYS_APPLN_ID.value),
        f.to_date(f.col(odate_col)).cast(DateType()).alias(SRC_BDP_PARTITION)
    ).dropDuplicates()

    final_df.show(100, False)

    return final_df


def get_b2k_infolease_history_df(df_b2k_clects_epsd, df_activity,  odate):

    print("declare variables")
    acty_cmp_dt = "dde_cmp_date"
    acty_cmp_time = "dde_cmp_time"
    acty_acty_cd = "dde_acty_cd"
    DDEP_APPL = "dde_appl"
    DDE_REVIEW_OUTCOME_IND = "dde_review_outcome_ind"
    acct_id = "dde_acct_id"
    acty_acct_id = "acty_acct_id"
    hardsship_ind = "hardsship_ind"
    prev_hardsship_ind = "prev_hardsship_ind"
    nxt_bdp_part = "nxt_bdp_part"
    hrdshp_chg = "hrdshp_chg"
    bdp_year = "bdp_year"
    bdp_month = "bdp_month"
    bdp_day = "bdp_day"
    bdp_part_b2k  = "bdp_part_b2k"
    bdp_part_acty = "bdp_part_acty"
    nxt_dde_hardship_lvr = "nxt_dde_hardship_lvr"
    nxt_dde_hardship_exp_date = "nxt_dde_hardship_exp_date"
    nxt_dde_next_due_date = "nxt_dde_next_due_date"
    nxt_dde_commercially_available = "nxt_dde_commercially_available"
    nxt_dde_mi_arrs_reason = "nxt_dde_mi_arrs_reason"
    nxt_dde_financial_solution = "nxt_dde_financial_solution"
    nxt_dde_review_outcome_ind = "nxt_dde_review_outcome_ind"
    get_nxt = "get_nxt"

    der_armt_sk = "der_armt_sk"
    der_case_sk = "der_case_sk"
    der_lend_armt_sk = "der_lend_armt_sk"
    fin_rlf_st_dt = "fin_rlf_st_dt"
    fin_rlf_end_dt = "fin_rlf_end_dt"
    hardsp_loan_valn_ratio = "hardsp_loan_valn_ratio"
    hardsp_expir_dt = "hardsp_expir_dt"
    comrc_avail_ind = "comrc_avail_ind"
    infls_acct_id = "infls_acct_id"
    threshold_date_col = "threshold_date_col"


    print("fetching required cols - acct")
    """ Fetch required columns"""
    b2k_df = df_b2k_clects_epsd \
        .select(f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value),
                f.trim(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value)).alias(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_IND_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_REC_STAT_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_DTE_COLCTED_B2K.value),
                f.col(DDEP_APPL),
                f.col(Tempb2kclectsepsd.DDE_MI_ARRS_REASON_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value),
                f.col(DDE_REVIEW_OUTCOME_IND),
                f.col(Tempb2kclectsepsd.DDE_REC_STAT_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_LVR_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value),
                f.col(bdp_year),
                f.col(bdp_month),
                f.col(bdp_day)
                ) \
        .withColumn(bdp_part_b2k, f.to_date(f.concat_ws("-", f.col(bdp_year), f.col(bdp_month), f.col(bdp_day))))\
        .withColumn(threshold_date_col, f.lit(INFLS_THRESHOLD_ODATE))\
        .filter((to_date(f.col(bdp_part_b2k)) >= f.to_date(f.col(threshold_date_col))))

    b2k_df.show(10, False)

    print("fetching required cols - acty")
    activity_df = df_activity\
        .filter( (f.trim(f.col(acty_acty_cd)) =='RHAA'))\
        .select(
        f.trim(f.col(acct_id)).alias(acty_acct_id),
        f.col(acty_cmp_dt), f.col(acty_cmp_time),
        f.col(acty_acty_cd),
        f.col(bdp_year),
        f.col(bdp_month),
        f.col(bdp_day),
        f.to_date(f.concat_ws("-", f.col(bdp_year), f.col(bdp_month), f.col(bdp_day))).alias(bdp_part_acty)
    ).dropDuplicates()

    activity_df.count()
    activity_df.show(10, False)

    print("acct - fin_rlf_st_dt")
    b2k_st_dt_df = b2k_df. \
        filter(
        (f.upper(f.trim(f.col(DDEP_APPL))) == 'WEF') &
        (f.upper(f.trim(f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value))).isin('INF'))
        & (f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value).isNotNull())
    )

    b2k_st_dt_df.count()
    b2k_st_dt_df.show(10, False)

    acct_win1 = Window\
        .partitionBy(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value), f.col(bdp_part_b2k))\
        .orderBy(f.col(bdp_part_b2k).desc())

    print("acct - fin_rlf_end_dt")
    b2k_end_dt_df = b2k_df. \
        filter(
        (f.upper(f.trim(f.col(DDEP_APPL))) == 'WEF') &
        (f.upper(f.trim(f.col(Tempb2kclectsepsd.DDE_ORIG_PROD_SYS_B2K.value))).isin('INF')) &
        (f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value).isNotNull()) &
        (((f.trim(f.col(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value)) == '') &
          (f.trim(f.col(DDE_REVIEW_OUTCOME_IND)) == '')) |
         (f.upper(f.col(Tempb2kclectsepsd.DDE_REC_STAT_B2K.value)) == 'I'))
    )\
        .select(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_LVR_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_MI_ARRS_REASON_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value),
                f.col(DDE_REVIEW_OUTCOME_IND),
                f.lit('X').alias(hardsship_ind),
                f.col(bdp_part_b2k))\
        .withColumn("rn", f.row_number().over(acct_win1))\
        .filter(f.col("rn") == 1)

    b2k_end_dt_df.count()
    b2k_end_dt_df.show(10, False)

    print("acct - acty - join")
    inf_st_dt_df = b2k_st_dt_df.join(activity_df,
                               ((f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value) == f.col(acty_acct_id))
                                & (f.col(bdp_part_b2k) == f.col(bdp_part_acty))),
                              "inner"
                              ) \
        .select(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_LVR_B2K.value),
                f.lit('A').alias(hardsship_ind),
                f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_MI_ARRS_REASON_B2K.value),
                f.col(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value),
                f.col(DDE_REVIEW_OUTCOME_IND),
                f.col(bdp_part_b2k))\
        .withColumn("rn", f.row_number().over(acct_win1))\
        .filter(f.col("rn") == 1)

    inf_st_dt_df.count()
    inf_st_dt_df.show(10, False)

    inf_union_df = inf_st_dt_df.union(b2k_end_dt_df)
    inf_union_df.show(10, False)

    hrdshp_ind_win = Window\
        .partitionBy(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value),
                     f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value))\
        .orderBy(f.col(bdp_part_b2k).asc(), f.col(hardsship_ind).asc())
    hrdshp_df = inf_union_df\
        .withColumn(prev_hardsship_ind, f.lag(f.col(hardsship_ind)).over(hrdshp_ind_win))
    hrdshp_df.show(100, False)

    print("derive hardship change -  hrdshp_chg")
    hrdshp_chg_df = hrdshp_df.withColumn(hrdshp_chg, f.concat_ws("->", f.coalesce(f.col(prev_hardsship_ind), f.lit('')), f.col(hardsship_ind) ))
    hrdshp_chg_df.show(100, False)

    print("filter on - hrdshp_chg")
    hrdshp_chg_fltr_df = hrdshp_chg_df.filter( ((f.col(hrdshp_chg) == f.lit("->A")) |
                                                (f.col(hrdshp_chg) == f.lit("A->X")) |
                                                (f.col(hrdshp_chg) == f.lit("X->A")) |
                                                (f.col(hrdshp_chg) == f.lit("A->A")))
                                               )
    hrdshp_chg_fltr_df.show(100, False)

    nxt_bdp_part_win = Window\
        .partitionBy(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value),
                     f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value))\
        .orderBy(f.col(bdp_part_b2k).asc(), f.col(hardsship_ind).asc())

    print("derive - next partition values for other cols")

    nxt_bdp_part_df = hrdshp_chg_fltr_df\
        .withColumn(nxt_bdp_part, f.lead(f.col(bdp_part_b2k)).over(nxt_bdp_part_win)) \
        .withColumn(nxt_dde_hardship_lvr, f.lead(f.col(Tempb2kclectsepsd.DDE_HARDSHIP_LVR_B2K.value)).over(nxt_bdp_part_win)) \
        .withColumn(nxt_dde_hardship_exp_date, f.lead(f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value)).over(nxt_bdp_part_win)) \
        .withColumn(nxt_dde_next_due_date, f.lead(f.col(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value)).over(nxt_bdp_part_win)) \
        .withColumn(nxt_dde_commercially_available, f.lead(f.col(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value)).over(nxt_bdp_part_win))\
        .withColumn(nxt_dde_mi_arrs_reason, f.lead(f.col(Tempb2kclectsepsd.DDE_MI_ARRS_REASON_B2K.value)).over(nxt_bdp_part_win)) \
        .withColumn(nxt_dde_financial_solution, f.lead(f.col(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value)).over(nxt_bdp_part_win)) \
        .withColumn(nxt_dde_review_outcome_ind, f.lead(f.col(DDE_REVIEW_OUTCOME_IND)).over(nxt_bdp_part_win))

    nxt_bdp_part_df.show(100, False)

    print("Assign -  values for other cols")
    curr_acct_df = nxt_bdp_part_df \
        .withColumn(fin_rlf_st_dt, f.when((f.col(hardsship_ind) == 'A'), f.col(bdp_part_b2k)).otherwise(f.lit(None))) \
        .withColumn(fin_rlf_end_dt, f.when((f.col(hrdshp_chg) != 'A->X'), f.col(nxt_bdp_part)).otherwise(f.lit(None))) \
        .withColumn(get_nxt, f.when((f.col(hrdshp_chg) != 'A->X'), f.lit(1)).otherwise(f.lit(None))) \
        .withColumn(Tempb2kclectsepsd.DDE_HARDSHIP_LVR_B2K.value,
                    f.when((f.col(get_nxt) == 1), f.col(nxt_dde_hardship_lvr)).otherwise(
                        f.col(Tempb2kclectsepsd.DDE_HARDSHIP_LVR_B2K.value))) \
        .withColumn(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value,
                    f.when((f.col(get_nxt) == 1), f.col(nxt_dde_hardship_exp_date)).otherwise(
                        f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value))) \
        .withColumn(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value,
                    f.when((f.col(get_nxt) == 1), f.col(nxt_dde_next_due_date)).otherwise(
                        f.col(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value))) \
        .withColumn(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value,
                    f.when((f.col(get_nxt) == 1), f.col(nxt_dde_commercially_available)).otherwise(
                        f.col(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value))) \
        .withColumn(Tempb2kclectsepsd.DDE_MI_ARRS_REASON_B2K.value,
                    f.when((f.col(get_nxt) == 1), f.col(nxt_dde_mi_arrs_reason)).otherwise(
                        f.col(Tempb2kclectsepsd.DDE_MI_ARRS_REASON_B2K.value))) \
        .withColumn(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value,
                    f.when((f.col(get_nxt) == 1), f.col(nxt_dde_financial_solution)).otherwise(
                        f.col(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value))) \
        .withColumn(DDE_REVIEW_OUTCOME_IND,
                    f.when((f.col(get_nxt) == 1), f.col(nxt_dde_review_outcome_ind)).otherwise(
                        f.col(DDE_REVIEW_OUTCOME_IND))) \
        .filter(f.col(fin_rlf_st_dt).isNotNull())

    curr_acct_df.show(100, False)

    print("derive columns - infolease")
    inflz_df = curr_acct_df\
        .withColumn(der_armt_sk,
                    f.concat_ws("",
                                f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value),
                                f.date_format(f.to_date(f.col(fin_rlf_st_dt)), "yyyyMMdd")
                                )
                    )\
        .withColumn(der_case_sk,
                    f.concat_ws("",
                                f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value),
                                f.date_format(f.to_date(f.col(Tempb2kclectsepsd.DDE_CURR_DT_ADD_B2K.value)), "yyyyMMdd")
                                )
                    )\
        .withColumn(der_lend_armt_sk,
                    f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value)
                    )\
        .withColumn(hardsp_loan_valn_ratio, f.col(Tempb2kclectsepsd.DDE_HARDSHIP_LVR_B2K.value))\
        .withColumn(hardsp_expir_dt,
                    f.when(
                        f.datediff(fin_rlf_st_dt, f.to_date(f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value))) > 0,
                        f.col(Tempb2kclectsepsd.DDE_NEXT_DUE_DATE_B2K.value))
                    .otherwise(f.col(Tempb2kclectsepsd.DDE_HARDSHIP_EXP_DATE_B2K.value))
                    )\
        .withColumn(comrc_avail_ind, f.when(f.upper(f.trim(f.col(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value))).isin('', 'N'), 'N')
                    .when(f.upper(f.trim(f.col(Tempb2kclectsepsd.DDE_COMMERCIALLY_AVAILABLE_B2K.value))).isin('Y'), 'Y').otherwise(f.lit(None)))

    inflz_df.count()
    inflz_df.show(10, False)

    print("prepare - final df")
    final_df = inflz_df.select(f.col(Tempb2kclectsepsd.DDE_ACCT_ID_B2K.value).alias(infls_acct_id),
                               f.col(der_armt_sk),
                               f.col(der_case_sk),
                               f.col(der_lend_armt_sk),
                               f.col(fin_rlf_st_dt),
                               f.col(fin_rlf_end_dt),
                               f.col(hardsp_loan_valn_ratio),
                               f.col(hardsp_expir_dt),
                               f.col(comrc_avail_ind),
                               f.col(Tempb2kclectsepsd.DDE_MI_ARRS_REASON_B2K.value),
                               f.col(Tempb2kclectsepsd.DDE_FINANCIAL_SOLUTION_B2K.value),
                               f.col(DDE_REVIEW_OUTCOME_IND)
                               ).dropDuplicates()
    print("final df - prepared")
    final_df.count()
    final_df.show(10, False)
    return  final_df


def get_pcc_target_df(
        df_a0103f_pcc_accounts,
        df_a0103f_pcc_debt_episode,
        df_a0103f_accounts,
        df_a0103f_pcc_hardship,
        df_a0103f_pcc_card_accounts,
        odate,
        is_history_load=False, ):
    """
    Obtain all the target columns for get_pcc_target_df after applying the provided logic
    :return df_target:
    """

    df_a0103f_pcc_accounts = df_a0103f_pcc_accounts.filter(
        upper(trim(f.col(PccAccount.PRODUCT_TYPE.value))) == "BUSINESS CARD"
    )

    df_a0103f_pcc_accounts = df_a0103f_pcc_accounts.withColumn(
        "bdp_partition_pcc_acct",
        f.from_unixtime(
            f.unix_timestamp(
                f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"
            ),
            "yyyy-MM-dd",
        ),
    ).withColumn(
        "accounts1_pcc",
        f.col("accounts1"
              ),
    )

    df_a0103f_pcc_debt_episode = df_a0103f_pcc_debt_episode.withColumn(
        "bdp_partition_epsd",
        f.from_unixtime(
            f.unix_timestamp(
                f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"
            ),
            "yyyy-MM-dd",
        ),
    ).withColumn(
        "accounts1_debt",
        f.col("accounts1"
              ),
    )

    df_a0103f_accounts = df_a0103f_accounts.withColumn(
        "bdp_partition_acct",
        f.from_unixtime(
            f.unix_timestamp(
                f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"
            ),
            "yyyy-MM-dd",
        ),
    )

    df_a0103f_pcc_hardship = df_a0103f_pcc_hardship.withColumn(
        "bdp_partition_hardship",
        f.from_unixtime(
            f.unix_timestamp(
                f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"
            ),
            "yyyy-MM-dd",
        ),
    ).withColumn(
        "accounts1_hardship",
        f.col("accounts1"
              ),
    )

    df_a0103f_pcc_card_accounts = df_a0103f_pcc_card_accounts.withColumn(
        "bdp_partition_pcc_card_acct",
        f.from_unixtime(
            f.unix_timestamp(
                f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"
            ),
            "yyyy-MM-dd",
        ),
    ).withColumn(
        "accounts1_card",
        f.col("accounts1"
              ),
    )

    # get max bdp_partition (excluding odate)
    hardship_prev_df = df_a0103f_pcc_hardship \
        .withColumn("col_odate", f.lit(odate)) \
        .filter(f.col("bdp_partition_hardship") < f.to_date(f.col("col_odate"))) \
        .select(f.max(f.col("bdp_partition_hardship")).alias("hard_prev_part"))

    prev_debt_epsd_df = df_a0103f_pcc_debt_episode \
        .withColumn("col_odate", f.lit(odate)) \
        .filter(f.col("bdp_partition_epsd") < f.to_date(f.col("col_odate"))) \
        .select(f.max(f.col("bdp_partition_epsd")).alias("debt_prev_part"))

    # hardship_prev_df.show(100, False)
    # sprev_debt_epsd_df.show(100, False)

    print("Count from hardship_prev_df max - {}".format(hardship_prev_df.count()))
    print("Count from prev_debt_epsd_df max - {}".format(prev_debt_epsd_df.count()))

    # fetch records from max bdp_partition as previous day data - both hardship and debt episode
    hardship_prev_df = df_a0103f_pcc_hardship \
        .join(hardship_prev_df, f.col("bdp_partition_hardship") == f.col("hard_prev_part"), "inner")

    # hardship_prev_df.show(100, False)

    prev_debt_epsd_df = df_a0103f_pcc_debt_episode \
        .join(prev_debt_epsd_df, f.col("bdp_partition_epsd") == f.col("debt_prev_part"), "inner")

    # prev_debt_epsd_df.show(100, False)

    print("Count from hardship_prev_df - {}".format(hardship_prev_df.count()))
    print("Count from prev_debt_epsd_df - {}".format(prev_debt_epsd_df.count()))

    # ndays = 1
    # if(odate.strftime("%A") == "Monday"): ndays = 3
    # prevodate = odate - timedelta(days=ndays)

    df_target = (
        df_a0103f_pcc_accounts.join(
            df_a0103f_pcc_debt_episode.alias("current_debt_epsd"),
            (f.col("accounts1_pcc") == f.col("current_debt_epsd.accounts1_debt"))
            & (f.col("bdp_partition_pcc_acct") == odate)
            & (f.col("current_debt_epsd.bdp_partition_epsd") == odate),
            "left",
        )
            .join(
            df_a0103f_pcc_hardship.alias("hardship_curr"),
            (f.col("accounts1_pcc") == f.col("hardship_curr.accounts1_hardship"))
            & (f.col("bdp_partition_pcc_acct") == odate)
            & (f.col("hardship_curr.bdp_partition_hardship") == odate),
            "left",
        )
            .join(
            df_a0103f_pcc_card_accounts.alias("card_accounts"),
            (f.col("accounts1_pcc") == f.col("card_accounts.accounts1_card"))
            & (f.col("bdp_partition_pcc_acct") == odate)
            & (f.col("bdp_partition_pcc_card_acct") == odate),
            "left",
        )
            .join(
            hardship_prev_df.alias("hardship_prev"),
            (f.col("accounts1_pcc") == f.col("hardship_prev.accounts1_hardship"))
            & (f.col("bdp_partition_pcc_acct") == odate),
            "left",
        )
            .join(
            prev_debt_epsd_df.alias("prev_debt_epsd"),
            (f.col("accounts1_pcc") == f.col("prev_debt_epsd.accounts1_debt"))
            & (f.col("bdp_partition_pcc_acct") == odate),
            "left",
        )
            .join(
            df_a0103f_accounts.alias("a0103f_acc"),
            (f.col("accounts1_pcc") == (f.col("a0103f_acc.ID")))
            & (f.col("bdp_partition_pcc_acct") == odate)
            & (f.col("bdp_partition_acct") == odate),
            "left",
        )
            .select(
            df_a0103f_pcc_accounts.accounts1_pcc.alias("accounts1_pcc"),
            df_a0103f_pcc_accounts.cardnumber.alias("cardnumber"),
            df_a0103f_pcc_accounts.product_type.alias("product_type"),
            f.col("current_debt_epsd.startepisodedate").alias("curr_startepisodedate"),
            f.col("current_debt_epsd.endepisodedate").alias("curr_endepisodedate"),
            f.col("prev_debt_epsd.startepisodedate").alias("prev_startepisodedate"),
            f.col("hardship_curr.ASSISTSOLUTIONDATE").alias("curr_assistsolutiondate"),
            f.col("hardship_curr.ASSISTSOLUTIONENDDATE").alias("curr_assistsolutionenddate"),
            f.col("hardship_prev.ASSISTSOLUTIONDATE").alias("prev_assistsolutiondate"),
            f.col("hardship_curr.ENDDATE").alias("enddate"),
            f.col("hardship_curr.ASSISTAPRAINDICATOR").alias("assistapraindicator"),
            df_a0103f_pcc_card_accounts.chargeoffreasoncode.alias(
                "chargeoffreasoncode"
            ),
            df_a0103f_accounts.worklistid.alias("worklistid"),
            when(df_a0103f_accounts.worklistid.isin(["84"]), f.lit("Y"))
                .otherwise(f.lit("N"))
                .alias("litign_ind"),
            f.col("hardship_curr.commercialflag").alias("comrc_avail_ind"),
            f.col("hardship_curr.status").alias("status"),
            f.col("bdp_partition_pcc_acct").alias("bdp_partition_pcc_acct"),
        )
    )
    print("\n ###Select criteria for df_target is completed after the first join ")


    df_target1 = df_target.select(
        f.col("cardnumber").alias("clect_epsd_lend_host_id"),
        when(
            f.col("curr_endepisodedate").isNotNull()
            & f.col("curr_startepisodedate").isNull(),
            f.col("prev_startepisodedate"),
        )
            .otherwise(f.col("curr_startepisodedate"))
            .alias("creat_dt"),
        when(
            f.col("enddate").isNotNull()
            & f.col("curr_assistsolutiondate").isNull()
            & f.col("prev_assistsolutiondate").isNotNull(),
            f.col("prev_assistsolutiondate"),
        )
            .otherwise(f.col("curr_assistsolutiondate"))
            .alias("fin_rlf_st_dt"),
        f.col("enddate").alias("fin_rlf_end_dt"),
        when(f.col("assistapraindicator") == "D", f.lit("D"))
            .otherwise(f.lit("H"))
            .alias("fin_rlf_armt_subtype_cd"),
        f.col("curr_endepisodedate").alias("clect_dt"),
        when(f.col("chargeoffreasoncode") == "SC", f.lit("Y"))
            .otherwise(f.lit("N"))
            .alias("soft_chrg_off_pndg_ind"),
        f.col("litign_ind"),
        f.col("curr_assistsolutionenddate").alias("hardsp_expir_dt"),
        f.col("comrc_avail_ind"),
        when(f.col("status") == "E", f.lit("HEXIT"))
            .when(f.col("status") == "S", f.lit("HAPRD"))
            .when(f.col("status") == "D", f.lit("HDCLN"))
            .when(f.col("status") == "P", f.lit("HREQ"))
            .when(f.col("status") == "W", f.lit("HWITHD"))
            .when(f.col("status") == "H", f.lit("SERV"))
            .otherwise(f.lit("UNK"))
            .alias("fin_rlf_armt_stat_type_cd"),
        f.col("bdp_partition_pcc_acct").alias("bdp_partition_pcc_acct"),
        f.col("worklistid"),
    )

    print("\n ###transform output after applying the decode logic ")

    df_target2 = df_target1.withColumn(
        "clect_epsd_host_id",
        f.concat_ws(
            "", f.col("clect_epsd_lend_host_id"), f.date_format(f.col("creat_dt"), "yyyyMMdd")
        ),
    ).alias("clect_epsd_host_id")

    clect_epsd_window1 = Window.partitionBy(f.col("clect_epsd_host_id")).orderBy(
        "bdp_partition_pcc_acct"
    )
    clect_epsd_window2 = Window.partitionBy(
        f.col("clect_epsd_host_id"), f.col("litign_ind")
    ).orderBy("bdp_partition_pcc_acct")
    print("\n ###transform output after applying the  logic for clect_epsd_host_id")

    df_target3 = df_target2.withColumn(
        "grp",
        f.row_number().over(clect_epsd_window1)
        - f.row_number().over(clect_epsd_window2),
    )
    print("\n ###transform output after applying the  logic for grp")

    clect_epsd_window3 = Window.partitionBy(f.col("grp"), f.col("clect_epsd_host_id"))

    df_target4 = df_target3.withColumn(
        "lit_st_dt",
        when(
            f.col("litign_ind") == "Y",
            f.min(
                when(f.col("litign_ind") == "Y", f.col("bdp_partition_pcc_acct"))
            ).over(clect_epsd_window3),
        ),
    ).filter((f.col("fin_rlf_st_dt").isNotNull()) | (f.col("creat_dt").isNotNull()))

    print("\n ###transform output after applying the  logic for litign indicator")

    df_target5 = df_target4.select(
        "clect_epsd_host_id",
        "creat_dt",
        "clect_dt",
        "soft_chrg_off_pndg_ind",
        "comrc_avail_ind",
        "fin_rlf_st_dt",
        "fin_rlf_end_dt",
        lit(None).cast("string").alias("fin_soltn_type_cd"),
        "clect_epsd_lend_host_id",
        "litign_ind",
        "lit_st_dt",
        lit(None).cast("string").alias("bank_error_invstgn_cd"),
        lit(None).cast("string").alias("arears_reasn_type_cd"),
        "fin_rlf_armt_stat_type_cd",
        "hardsp_expir_dt",
        f.concat_ws(
            "",
            f.trim(f.col("clect_epsd_lend_host_id")),
            f.date_format(f.col("fin_rlf_st_dt"), "yyyyMMdd"),
        ).alias("fin_rlf_armt_host_id"),
        "worklistid",
        f.col("fin_rlf_armt_subtype_cd").alias("fin_rlf_armt_subtype_cd"),
        f.col("bdp_partition_pcc_acct").alias("src_bdp_partition")
    ).dropDuplicates()
    print("\n ###transform output after applying the  logic for final target")

    return df_target5


def df_target_pcc_df(df_pcc_clects_epsd_kmr_key, df_temp_clects_epsd):
    """
    Obtain the final transformed dataframe with target fields of compose temp table columns and KMR
    :param df_temp_pcc_kmr: Dataframe containing current odate table data.
    :return: dataframe with all the PCC related columns and KMR columns
    """

    pcc = "pccclectepsd."
    kmrkey = "kmrkey."

    df_target = (
        df_pcc_clects_epsd_kmr_key.alias("kmrkey")
            .filter(f.col(kmrkey + "fin_rlf_st_dt").isNotNull())
            .join(
            df_temp_clects_epsd.alias("pccclectepsd").filter(
                f.col(pcc + "fin_rlf_st_dt").isNotNull()
            ),
            (
                    (
                            f.col(kmrkey + "fin_rlf_armt_host_id")
                            == f.col(pcc + "fin_rlf_armt_host_id")
                    )
                    & (
                            f.col(kmrkey + "src_bdp_partition")
                            == f.col(pcc + "src_bdp_partition")
                    )
            ),
            INNER,
        )
            .select(
            f.col(kmrkey + ARMT_SK).cast(LongType()).alias(ARMT_SK),
            f.col(kmrkey + LEND_ARMT_SK).cast(LongType()).alias(LEND_ARMT_SK),
            f.col(kmrkey + CASE_SK).cast(LongType()).alias(CLECT_EPSD_CASE_SK),
            f.col(pcc + FIN_RLF_ARMT_SUBTYPE_CD).cast(StringType()).alias(FIN_RLF_ARMT_SUBTYPE_CD),
            f.col(pcc + FIN_RLF_ARMT_STAT_TYPE_CD).cast(StringType()).alias(FIN_RLF_ARMT_STAT_TYPE_CD),
            f.col(pcc + COMRC_AVAIL_IND).cast(StringType()).alias(COMRC_AVAIL_IND),
            f.lit(PCC_APPLN_KEY).cast(StringType()).alias(OWNG_LEND_ARMT_APPLN_KEY),
            f.col(pcc + FIN_RLF_ST_DT).cast(DateType()).alias(FIN_RLF_ST_DT),
            f.col(pcc + FIN_RLF_END_DT).cast(DateType()).alias(FIN_RLF_END_DT),
            lit(None).cast(DecimalType(38, 12)).alias(HARDSP_LOAN_VALN_RATIO),
            f.col(pcc + HARDSP_EXPIR_DT).cast(DateType()).alias(HARDSP_EXPIR_DT),
            f.lit(PCC_PRV_SYS_APPLN_ID).cast(StringType()).alias(PRV_SYS_APPLN_ID),
            f.col(pcc + SRC_BDP_PARTITION).cast(DateType()).alias(SRC_BDP_PARTITION),
        ).dropDuplicates()
    )
    return df_target



def get_target_wrklist_pcc_df(df_temp_pcc_wrklist, odate, is_history_load=False):
    """
    Obtain all the target columns for df_temp_pcc_wrklist after applying the provided logic
    :return df_target:
    """
    df_temp_pcc_wrklist = (

        df_temp_pcc_wrklist.alias(WorklistPcc.ALIAS.value)
                 .select(f.col(WorklistPcc.WRKFLW_SK_PCC.value).alias("wrkflw_sk"),
                         f.col(WorklistPcc.WRKLIST_NM.value),
                         f.col(WorklistPcc.WRKLIST_DESC.value),
                         f.lit(PCC_PRV_SYS_APPLN_ID).alias(PRV_SYS_APPLN_ID)
    ).dropDuplicates()
                 )
    return df_temp_pcc_wrklist

def get_pcc_wrklist_target_df(df_a0103f_worklists,
                              odate,
                              is_history_load=False):
    """
    Obtain all the target columns for PCC worklist after applying the odate logic
    :return df_pcc_wrklist_target:
    """
    df_a0103f_worklists = df_a0103f_worklists.withColumn(
        "bdp_partition_wrklist",
        f.from_unixtime(
            f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"
        ),
    )

    df_pcc_wrklist_target = df_a0103f_worklists.\
        filter(f.col("bdp_partition_wrklist") == odate).\
        select(f.trim(f.col("id")).cast("int").alias("id"),
               f.col("name").alias("name"),
               f.col("description").alias("description"))

    return df_pcc_wrklist_target
	
def get_target_clect_epsd_pcc_df(
     df_temp_clects_epsd, df_pcc_clects_epsd_kmr_key, odate, is_history_load=False):
    """
    Obtain all the target columns for fdp048_clect_epsd for pcc after applying the provided logic
    :param df_pcc_clects_epsd:
    :param df_temp_host_id:
    :param odate
    :return df_target:
    """

    if is_history_load:
        df_temp_clects_epsd = df_temp_clects_epsd.filter(f.col("fin_rlf_st_dt").isNull())
        df_pcc_clects_epsd_kmr_key = df_pcc_clects_epsd_kmr_key.filter(f.col("fin_rlf_st_dt").isNull())

    pcc = "pccclectepsd."
    kmrkey = "kmrkey."

    df_target = df_pcc_clects_epsd_kmr_key.alias("kmrkey").filter(f.col(kmrkey + "creat_dt").isNotNull())\
        .join(
        df_temp_clects_epsd.alias("pccclectepsd").filter(f.col(pcc + "creat_dt").isNotNull()),
        (
            (f.col(kmrkey + "clect_epsd_host_id") == f.col(pcc + "clect_epsd_host_id"))
            &
            (f.col(kmrkey + "src_bdp_partition") == f.col(pcc + "src_bdp_partition"))
        ), INNER,)\
        .select(
            f.col(kmrkey + CASE_SK).alias(CASE_SK),
            f.col(kmrkey + LEND_ARMT_SK).alias(LEND_ARMT_SK),
            f.lit(PCC_APPLN_KEY).cast(StringType()).alias(OWNG_LEND_ARMT_APPLN_KEY),
            f.lit(None).cast(StringType()).alias(FIN_SOLTN_TYPE_CD),
            f.lit(None).cast(StringType()).alias(CLECT_ENTRY_REASN_TYPE_CD),
            f.lit(None).cast(StringType()).alias(CLECT_OUTSRCG_TYPE_CD),
            f.lit(None).cast(StringType()).alias(AREARS_REASN_TYPE_CD),
            to_date(f.col(pcc + CREAT_DT), "YYYY-MM-DD").alias(CREAT_DT),
            f.col(pcc + SOFT_CHRG_OFF_PNDG_IND).cast(StringType()).alias(SOFT_CHRG_OFF_PNDG_IND),
            to_date(f.col(pcc + CLECT_DT), "YYYY-MM-DD").alias(CLECT_DT),
            f.lit(None).cast(StringType()).alias(DEFER_WRITE_OFF_IND),
            f.lit(None).cast(StringType()).alias(LTR_SUPRSN_IND),
            f.lit(None).cast(StringType()).alias(CRAA_LODG_DT),
            f.lit(None).cast(StringType()).alias(NO_PH_CONTCT_IND),
            f.lit(None).cast(StringType()).alias(AUTO_LTR_DAY_CNT),
            f.lit(None).cast(StringType()).alias(OMIT_FROM_SUPVSRY_QU_DAY_CNT),
            f.col(pcc + "litign_ind").alias(LITIGN_IND),
            f.lit(None).cast("decimal(15,2)").alias(CR_LISTNG_AMT),
            f.lit(None).cast(StringType()).alias(CREDIT_LISTING_AMOUNT_CURRENCY_CODE),
            f.lit(None).cast(StringType()).alias(CLECT_STAT_TYPE_CD),
            f.lit(None).cast(StringType()).alias(CLECT_BANK_ERROR_CD),
            f.col(pcc + "lit_st_dt").alias(LIT_START_DT),
            f.col(kmrkey + "wrkflw_sk_pcc").alias(WORKLIST_WORKFLOW_SK),
            f.lit(None).cast(StringType()).alias(ALTN_STRAT_CD),
            f.lit(PCC_PRV_SYS_APPLN_ID).cast(StringType()).alias(PRV_SYS_APPLN_ID),
            f.col(pcc + SRC_BDP_PARTITION).alias(SRC_BDP_PARTITION),

        )\
        .dropDuplicates()

    return df_target



def cast_fin_rlf_armt_final_df(target_df):
    final_df = target_df.select(
        f.col(FinRlfArmtCMT.ARMT_SK.value).cast(LongType()).alias(FinRlfArmtCMT.ARMT_SK.value),
        f.col(FinRlfArmtCMT.LEND_ARMT_SK.value).cast(LongType()).alias(FinRlfArmtCMT.LEND_ARMT_SK.value),
        f.col(FinRlfArmtCMT.CLECT_EPSD_CASE_SK.value).cast(LongType()).alias(FinRlfArmtCMT.CLECT_EPSD_CASE_SK.value),
        f.col(FinRlfArmtCMT.FIN_RLF_ARMT_SUBTYPE_CD.value).alias(FinRlfArmtCMT.FIN_RLF_ARMT_SUBTYPE_CD.value),
        f.col(FinRlfArmtCMT.FIN_RLF_ARMT_STAT_TYPE_CD.value).alias(FinRlfArmtCMT.FIN_RLF_ARMT_STAT_TYPE_CD.value),
        f.col(FinRlfArmtCMT.COMRC_AVAIL_IND.value).cast(StringType()).alias(FinRlfArmtCMT.COMRC_AVAIL_IND.value),
        f.col(FinRlfArmtCMT.OWNG_LEND_ARMT_APPLN_KEY.value).cast(StringType()).alias(FinRlfArmtCMT.OWNG_LEND_ARMT_APPLN_KEY.value),
        f.col(FinRlfArmtCMT.FIN_RLF_ST_DT.value).cast(DateType()).alias(FinRlfArmtCMT.FIN_RLF_ST_DT.value),
        f.col(FinRlfArmtCMT.FIN_RLF_END_DT.value).cast(DateType()).alias(FinRlfArmtCMT.FIN_RLF_END_DT.value),
        f.col(FinRlfArmtCMT.HARDSP_LOAN_VALN_RATIO.value).cast(DecimalType(38, 12)).alias(
            FinRlfArmtCMT.HARDSP_LOAN_VALN_RATIO.value),
        f.col(FinRlfArmtCMT.HARDSP_EXPIR_DT.value).cast(DateType()).alias(FinRlfArmtCMT.HARDSP_EXPIR_DT.value),
        f.col(FinRlfArmtCMT.PRV_SYS_APPLN_ID.value).cast(StringType()).alias(FinRlfArmtCMT.PRV_SYS_APPLN_ID.value)
    ).dropDuplicates()

    final_df.printSchema()

    return final_df




def cast_clect_epsd_final_df(target_df):

    final_df = target_df.select(
        f.col("case_sk").cast(LongType()).alias("case_sk"),
        f.col("lend_armt_sk").cast(LongType()).alias("lend_armt_sk"),
        f.col("owng_lend_armt_appln_key").cast(StringType()).alias("owng_lend_armt_appln_key"),
        f.col("fin_soltn_type_cd").cast(StringType()).alias("fin_soltn_type_cd"),
        f.col("clect_entry_reasn_type_cd").cast(StringType()).alias("clect_entry_reasn_type_cd"),
        f.col("clect_outsrcg_type_cd").cast(StringType()).alias("clect_outsrcg_type_cd"),
        f.col("arears_reasn_type_cd").cast(StringType()).alias("arears_reasn_type_cd"),
        f.col("creat_dt").cast(DateType()).alias("creat_dt"),
        f.col("soft_chrg_off_pndg_ind").cast(StringType()).alias("soft_chrg_off_pndg_ind"),
        f.col("clect_dt").cast(DateType()).alias("clect_dt"),
        f.col("defer_write_off_ind").cast(StringType()).alias("defer_write_off_ind"),
        f.col("ltr_suprsn_ind").cast(StringType()).alias("ltr_suprsn_ind"),
        f.col("craa_lodg_dt").cast(DateType()).alias("craa_lodg_dt"),
        f.col("no_ph_contct_ind").cast(StringType()).alias("no_ph_contct_ind"),
        f.col("auto_ltr_day_cnt").cast(IntegerType()).alias("auto_ltr_day_cnt"),
        f.col("omit_from_supvsry_qu_day_cnt").cast(IntegerType()).alias("omit_from_supvsry_qu_day_cnt"),
        f.col("litign_ind").cast(StringType()).alias("litign_ind"),
        f.col("cr_listng_amt").cast(DecimalType(15, 2)).alias("cr_listng_amt"),
        f.col("cr_listing_amt_crncy_cd").cast(StringType()).alias("cr_listing_amt_crncy_cd"),
        f.col("armt_clect_stat_type_cd").cast(StringType()).alias("armt_clect_stat_type_cd"),
        f.col("bank_error_invstgn_cd").cast(StringType()).alias("bank_error_invstgn_cd"),
        f.col("litign_st_dt").cast(DateType()).alias("litign_st_dt"),
        f.col("worklist_workflow_sk").cast(LongType()).alias("worklist_workflow_sk"),
        f.col("altn_strat_cd").cast(StringType()).alias("altn_strat_cd"),
        f.col("prv_sys_appln_id").cast(StringType()).alias("prv_sys_appln_id"),
        f.col(SRC_BDP_PARTITION).alias(SRC_BDP_PARTITION)
    ).dropDuplicates()

    final_df.printSchema()

    return final_df


def cast_recov_final_df(target_df):
    final_df = target_df.select(
        f.col("case_sk").cast(LongType()).alias("case_sk"),
        f.col("recov_type_cd").cast(StringType()).alias("recov_type_cd"),
        f.col("recov_stat_cd").cast(StringType()).alias("recov_stat_cd"),
        f.col("prv_sys_appln_id").cast(StringType()).alias("prv_sys_appln_id"),
        f.col(SRC_BDP_PARTITION)
    ).dropDuplicates()

    final_df.printSchema()

    return final_df


def cast_recov_sec_sale_final_df(target_df):
    final_df = target_df.select(
        f.col("secrty_rsrc_sk").cast(LongType()).alias("secrty_rsrc_sk"),
        f.col("case_sk").cast(LongType()).alias("case_sk"),
        f.col("prv_sys_appln_id").cast(StringType()).alias("prv_sys_appln_id")
    )

    final_df.printSchema()

    return final_df


def cast_security_sale_final_df(target_df):

    final_df = target_df.select(
        f.col("secrty_rsrc_sk").cast(LongType()).alias("secrty_rsrc_sk"),
        f.col("secrty_sale_stat_cd").cast(StringType()).alias("secrty_sale_stat_cd"),
        f.col("secrty_sale_dt").cast(DateType()).alias("secrty_sale_dt"),
        f.col("secrty_setl_dt").cast(DateType()).alias("secrty_setl_dt"),
        f.col("secrty_acq_dt").cast(DateType()).alias("secrty_acq_dt"),
        f.col("sale_amt").cast(DecimalType(38, 12)).alias("sale_amt"),
        f.col("sale_amt_crncy_cd").cast(StringType()).alias("sale_amt_crncy_cd"),
        f.col("secrty_valn_dt").cast(DateType()).alias("secrty_valn_dt"),
        f.col("secrty_valn_min_amt").cast(DecimalType(38, 12)).alias("secrty_valn_min_amt"),
        f.col("secrty_valn_crncy_cd").cast(StringType()).alias("secrty_valn_crncy_cd"),
        f.col("prv_sys_appln_id").cast(StringType()).alias("prv_sys_appln_id")
    )

    final_df.printSchema()

    return final_df


def get_rde_history_df(
        df_a009a8_rde_cs_account,
        df_a009a8_rde_accountactivities,
        df_a009a8_rde_accounts,
        df_a009a8_rde_hardship
):
    """
    Obtain all the target columns for get_rde_target_df after applying the provided logic
    :return df_target:
    """
    df_a009a8_rde_cs_account = df_a009a8_rde_cs_account.withColumn(
        "bdp_partition_cs",
        f.from_unixtime(
            f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"
        ),
    ).withColumn("threshold_date_col_cs", f.lit(RDE_THRESHOLD_ODATE))\
        .filter(
        (
                to_date(f.col("bdp_partition_cs"))
                >=
                f.to_date(f.col("threshold_date_col_cs"))
         )
    ).select("reference", "accounts1", "treatment_code", "bdp_partition_cs")\
        .withColumn("cs_accounts1", f.col("accounts1"))\
        .drop("accounts1")

    df_a009a8_rde_accountactivities = df_a009a8_rde_accountactivities.withColumn(
        "datetime_activities",
        f.from_unixtime(f.unix_timestamp(f.col("datetime"), "yyyy-M-d"), "yyyy-MM-dd"),
    ).withColumn(
        "bdp_partition_activities",
        f.from_unixtime(
            f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"
        ),
    ).withColumn("threshold_date_col_act", f.lit(RDE_THRESHOLD_ODATE))\
        .filter((to_date(f.col("bdp_partition_activities")) >= f.to_date(f.col("threshold_date_col_act"))) & (f.col("childid").isin(["505", "506"])))\
        .withColumn("rnum", f.row_number().over(Window.partitionBy(f.col("accountid"), f.col("childid"), f.col("datetime_activities")).orderBy(f.col("datetime").desc())))\
        .filter("rnum ==  1")\
        .select("accountid", "childid", "datetime", "bdp_partition_activities")

    df_a009a8_rde_accounts = df_a009a8_rde_accounts \
        .withColumn("bdp_partition_accts",
                    f.from_unixtime(f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"))\
        .withColumn("threshold_date_col_accts", f.lit(RDE_THRESHOLD_ODATE))\
        .filter((to_date(f.col("bdp_partition_accts")) >= f.to_date(f.col("threshold_date_col_accts"))))\
        .select("id", "worklistid", "bdp_partition_accts")

    print("\n###Results for df_a009a8_rde_hardship DF is {}\n")

    df_a009a8_rde_hardship = df_a009a8_rde_hardship\
        .withColumn("bdp_partition_hrdsp",
        f.from_unixtime(f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"))\
        .withColumn("threshold_date_col_hrdsp", f.lit(RDE_THRESHOLD_ODATE))\
        .filter((to_date(f.col("bdp_partition_hrdsp")) >= f.to_date(f.col("threshold_date_col_hrdsp")))) \
        .select("accounts1", "status", "approve_date", "serviceabilityenddt", "assistance_end_date",
                "status_update_date", "treatment_status", "disaster_flag", "hardship_reason", "noncommercial",
                "bdp_partition_hrdsp")

    print("\n###Results for df_a009a8_rde_hardship DF is {}\n")

    clect_win = Window.partitionBy(f.col("accountid")).orderBy(f.col("datetime").asc(), f.col("clect_ind").asc())

    # Deriving 'clect_epsd_start' and 'clect_epsd_end'
    clect_acty_df = df_a009a8_rde_accountactivities\
        .withColumn("clect_ord_dt", f.date_format(f.col("datetime"), "yyyy-MM-dd"))\
        .withColumn("clect_ind", f.when(f.col("childid") == '505', f.lit('A')).when(f.col("childid") == '506', f.lit('X')))\
        .withColumn("prev_clect_ind", f.lag(f.col("clect_ind")).over(clect_win)) \
        .withColumn("clect_ind_change", f.concat_ws("->", f.coalesce(f.col("prev_clect_ind"), f.lit('')), f.col("clect_ind")))\
        .filter(f.col("clect_ind_change").isin('->A', 'A->X', 'X->A'))\
        .withColumn("nxt_clect_ord_dt", f.lead(f.col("clect_ord_dt")).over(clect_win))\
        .withColumn("nxt_bdp_part_acty", f.lead(f.col("bdp_partition_activities")).over(clect_win))\
        .withColumn("episode_start", f.when(f.col("clect_ind") == 'A', f.col("clect_ord_dt")).otherwise(f.lit(None)))\
        .withColumn("episode_end", f.when(f.col("clect_ind") != 'A->X', f.col("nxt_clect_ord_dt")).otherwise(f.lit(None)))\
        .filter(f.col("episode_start").isNotNull())\
        .withColumn("join_part_acty", f.when(f.col("episode_end").isNotNull(), f.col("nxt_bdp_part_acty")).when(f.col("episode_end").isNull(), f.col("bdp_partition_activities")))\
        .select(f.col("accountid"),
                f.col("episode_start").cast("date"),
                f.col("episode_end").cast("date"),
                f.col("join_part_acty").cast("date")
                )

    # Derive fin_rlf_st_dt and fin_rlf_end_dt - from rde_hardship table
    fin_rlf_win1 = Window.partitionBy(f.col("accounts1"), f.col("approve_date")).orderBy(f.col("bdp_partition_hrdsp").desc())

    # fetch fin_rlf_st_dt - hardship_ind - 'A'
    fin_rlf_armt_a_df = df_a009a8_rde_hardship\
        .filter((f.trim(f.upper(f.col("status"))) == "HARDSHIP APPROVED") & (f.col("approve_date").isNotNull()))\
        .withColumn("rnum", f.row_number().over(fin_rlf_win1))\
        .filter(f.col("rnum") == 1)\
        .withColumn("fin_rlf_st_dt", f.date_format(f.col("approve_date"), "yyyy-MM-dd")) \
        .withColumn("tmp_fin_rlf_end_dt",
                    f.when(f.col("serviceabilityenddt").isNotNull(), f.col("serviceabilityenddt")).when(
                        f.col("serviceabilityenddt").isNull() & f.col("assistance_end_date").isNotNull(),
                        f.col("assistance_end_date")))\
        .withColumn("hrdshp_ind", f.lit('A'))\
        .select(f.col("accounts1"),  f.col("status"), f.col("treatment_status"), f.col("disaster_flag"), f.col("hardship_reason"), f.col("noncommercial"), f.col("fin_rlf_st_dt"), f.col("tmp_fin_rlf_end_dt"), f.col("hrdshp_ind"), f.col("bdp_partition_hrdsp"))

    # fetch fin_rlf_end_dt - hardship_ind - 'X'
    fin_rlf_armt_x_df = df_a009a8_rde_hardship\
        .filter((f.trim(f.upper(f.col("status"))) != "HARDSHIP APPROVED") & (f.col("status_update_date").isNotNull()))\
        .withColumn("fin_rlf_st_dt", f.lit(None)) \
        .withColumn("tmp_fin_rlf_end_dt",f.col("status_update_date"))\
        .withColumn("hrdshp_ind", f.lit('X'))\
        .select(f.col("accounts1"), f.col("status"), f.col("treatment_status"), f.col("disaster_flag"), f.col("hardship_reason"), f.col("noncommercial"), f.col("fin_rlf_st_dt"), f.col("tmp_fin_rlf_end_dt"), f.col("hrdshp_ind"), f.col("bdp_partition_hrdsp"))

    # combine hrdshp start & end - dataframes
    fin_rlf_armt_df = fin_rlf_armt_a_df.union(fin_rlf_armt_x_df)

    # 1. find - hrdshp end for each hrdshp start - i.e. find fin_rlf_end_dt for fin_rlf_st_dt    # 2. Derive other columns
    fin_rlf_win2 = Window.partitionBy(f.col("accounts1")).orderBy(f.col("bdp_partition_hrdsp").asc(), f.col("hrdshp_ind").asc())

    rde_hrdshp_df = fin_rlf_armt_df\
        .withColumn("prev_hardshp_ind", f.lag(f.col("hrdshp_ind")).over(fin_rlf_win2))\
        .withColumn("hardship_change", f.concat_ws("->", f.coalesce(f.col("prev_hardshp_ind"), f.lit('')), f.col("hrdshp_ind")))\
        .filter(f.col("hardship_change").isin('->A', 'A->X', 'X->A', 'A->A'))\
        .withColumn("nxt_hrdshp_dt", f.lead(f.col("tmp_fin_rlf_end_dt")).over(fin_rlf_win2)) \
        .withColumn("nxt_fin_rlf_st_dt", f.lead(f.col("fin_rlf_st_dt")).over(fin_rlf_win2)) \
        .withColumn("nxt_bdp_part_hshp", f.lead(f.col("bdp_partition_hrdsp")).over(fin_rlf_win2))\
        .withColumn("nxt_hardshp_ind", f.lead(f.col("hrdshp_ind")).over(fin_rlf_win2))\
        .withColumn("nxt_treatment_status", f.lead(f.col("treatment_status")).over(fin_rlf_win2))\
        .withColumn("nxt_disaster_flag", f.lead(f.col("disaster_flag")).over(fin_rlf_win2))\
        .withColumn("nxt_hardship_reason", f.lead(f.col("hardship_reason")).over(fin_rlf_win2)) \
        .withColumn("nxt_noncommercial", f.lead(f.col("noncommercial")).over(fin_rlf_win2)) \
        .withColumn("get_nxt", f.when((f.col("hardship_change") != 'A->X') & (f.col("nxt_hardshp_ind") == 'X'), 1).when((f.col("hardship_change") != 'A->X') & (f.col("tmp_fin_rlf_end_dt").isNull()), 1).otherwise(0))\
        .withColumn("fin_rlf_end_dt",f.when((f.col("hardship_change") != 'A->X') & (f.col("nxt_hardshp_ind") == 'X'),f.col("nxt_hrdshp_dt")).when((f.col("hardship_change") != 'A->X') & (f.col("tmp_fin_rlf_end_dt").isNull()),f.col("nxt_fin_rlf_st_dt")).otherwise(f.col("tmp_fin_rlf_end_dt"))) \
        .withColumn("treatment_status", f.when(f.col("get_nxt") == 1, f.col("nxt_treatment_status")).otherwise(f.col("treatment_status")))\
        .withColumn("disaster_flag", f.when(f.col("get_nxt") == 1, f.col("nxt_disaster_flag")).otherwise(f.col("disaster_flag")))\
        .withColumn("hardship_reason", f.when(f.col("get_nxt") == 1, f.col("nxt_hardship_reason")).otherwise(f.col("hardship_reason")))\
        .withColumn("noncommercial", f.when(f.col("get_nxt") == 1, f.col("nxt_noncommercial")).otherwise(f.col("noncommercial")))\
        .withColumn("join_part_hshp", f.when(f.col("get_nxt") == 1, f.col("nxt_bdp_part_hshp")).otherwise(f.col("bdp_partition_hrdsp")))\
        .withColumn("date_dup", f.lit("9999-12-31"))\
        .filter(f.col("fin_rlf_st_dt").isNotNull())\
        .withColumn("fin_rlf_armt_stat_type_cd", f.col("status"))\
        .withColumn("fin_soltn_type_cd", f.when(f.col("treatment_status") == "Moratorium", f.lit("NIL Repayment"))
                    .when(f.col("treatment_status").isin(["Hardship Arrangement", "Hardship Monitoring", "Hardship Arrangement Broken"]),f.lit("Reduced Payment"))
                    .when(f.col("treatment_status").isin(["Serviceability", "Serviceability Monitoring", "Serviceability Completed", "Serviceability Broken"]),f.lit("Serviceability"))
                    .when((f.col("treatment_status").isin(["Serviceability", "Serviceability Monitoring", "Serviceability Completed", "Serviceability Broken"]))& (f.col("disaster_flag") == "Y"), f.lit("Serviceability"))
                    .when((f.col("treatment_status") == "Moratorium") & (f.col("disaster_flag") == "Y"), f.lit("NIL Repayment")).otherwise("UNK"))\
        .withColumn("arears_reasn_type_cd", f.when(f.col("hardship_reason").isNotNull(), f.col("hardship_reason")).otherwise("Missing Payment"))\
        .withColumn("fin_rlf_armt_subtype_cd", f.col("hardship_reason"))\
        .select(f.col("accounts1").alias("accounts1_hshp"),
            f.col("fin_rlf_st_dt").cast("date"),
            f.col("fin_rlf_end_dt").cast("date"),
            f.col("fin_rlf_armt_stat_type_cd"),
            f.col("fin_soltn_type_cd"),
            f.col("arears_reasn_type_cd"),
            f.col("fin_rlf_armt_subtype_cd"),
            f.col("noncommercial"),
            f.col("join_part_hshp").cast("date"),
            f.col("date_dup").cast("date")
            )

    # Derive clect_epsd_host_id for fin_rlf data - join with rde_cs_accounts table and get 'reference' # Join with rde_activities to get episode_start # Dervive clect_epsd_host_id - from above 'reference' and 'episode_start'

    fin_rlf_df = df_a009a8_rde_cs_account \
    .join(rde_hrdshp_df,
          (f.col("cs_accounts1") == f.col("accounts1_hshp")) & (f.col("bdp_partition_cs") == f.col("join_part_hshp")), "inner") \
    .join(clect_acty_df,
          (f.col("cs_accounts1") == f.col("accountid")) & (f.col("fin_rlf_st_dt") >= f.col("episode_start")) & (f.col(
              "fin_rlf_st_dt") <= f.coalesce(f.col("episode_end"), f.col("date_dup"))), "left")\
        .withColumn("fin_rlf_armt_host_id", f.concat_ws("", f.lpad(f.trim(f.col("reference")), 20, "0"), f.date_format( f.col("fin_rlf_st_dt"), "yyyyMMdd")))\
        .withColumn("clect_epsd_lend_host_id", f.lpad(f.trim(f.col("reference")), 10, "0"))\
        .withColumn("clect_epsd_host_id",
                    f.concat_ws("", f.lpad(f.trim(f.col("reference")), 20, "0"),
                                f.date_format( f.coalesce( f.col("episode_start"), f.col("fin_rlf_st_dt")), "yyyyMMdd")
                                )
                    )\
        .withColumn("litign_ind_rams", f.lit(None))\
        .withColumn("lit_st_dt", f.lit(None))\
        .withColumn("bank_error_invstgn_cd", f.lit(None))\
        .withColumn("worklistid", f.lit(None))

    # Direct hardships from fin_rlf_df - which will be considered as collection episodes
    clect_act_drct_df = fin_rlf_df\
        .filter(f.col("episode_start").isNull())\
        .select(f.col("accounts1_hshp").alias("accountid"),
                f.col("fin_rlf_st_dt").alias("episode_start").cast("date"),
                f.col("fin_rlf_end_dt").alias("episode_end").cast("date"),
                f.col("join_part_hshp").alias("join_part_acty").cast("date")
                ).dropDuplicates()

    # Union - actual hardships from rde_activities and direct harships from rde_hardship table
    clect_acty_epsd_df = clect_acty_df.union(clect_act_drct_df)

    # For collection episodes data - Join - b/w Collection Episodes derived (from rde_activities & rde_hardship) above and rde_cs table and rde_accounts table

    # Join for records - with episode_end_dt -  episode_end_dt will be latest partition

    clect_rnum_win = Window.partitionBy(f.col("cs_accounts1"), f.col("episode_start")).orderBy(f.col("bdp_partition_cs").desc(), f.col("join_part_acty").desc(), f.col("bdp_partition_accts").desc(), f.col("bdp_partition_hrdsp").desc())
    clect_epsd_1_df = df_a009a8_rde_cs_account \
        .join(clect_acty_epsd_df.filter(f.col("episode_end").isNotNull()),
          (f.col("cs_accounts1") == f.col("accountid")) & (f.col("bdp_partition_cs") == f.col("join_part_acty")), "left")\
        .join(df_a009a8_rde_accounts,
          (f.col("cs_accounts1") == f.col("id")) & (f.col("bdp_partition_cs") == f.col("bdp_partition_accts")), "left") \
        .join(df_a009a8_rde_hardship,
          (df_a009a8_rde_cs_account["cs_accounts1"] == f.col("accounts1")) & (f.col("bdp_partition_cs") == f.col("bdp_partition_hrdsp")), "left")\
        .filter(f.col("episode_start").isNotNull())\
        .withColumn("clect_rnum1", f.row_number().over(clect_rnum_win)).filter(f.col("clect_rnum1") == 1)

    # Join for records - without episode_end_dt -  get data from latest partition (from other table)
    clect_epsd_2_df = df_a009a8_rde_cs_account\
        .join(clect_acty_epsd_df.filter(f.col("episode_end").isNull()),
          (f.col("cs_accounts1") == f.col("accountid")) & (f.col("bdp_partition_cs") == f.col("join_part_acty")), "left")\
        .join(df_a009a8_rde_accounts,
          (f.col("cs_accounts1") == f.col("id")) & (f.col("bdp_partition_cs") == f.col("bdp_partition_accts")), "left") \
        .join(df_a009a8_rde_hardship,
          (df_a009a8_rde_cs_account["cs_accounts1"] == f.col("accounts1")) & (f.col("bdp_partition_cs") == f.col("bdp_partition_hrdsp")), "left")\
        .filter(f.col("episode_start").isNotNull())\
        .withColumn("clect_rnum2", f.row_number().over(clect_rnum_win)).filter(f.col("clect_rnum2") == 1)

    # Union - above two dataframes
    clect_epsd_df = clect_epsd_1_df.union(clect_epsd_2_df)\
        .select(
            "reference",
            "episode_start",
            "episode_end",
            f.col("status"),
            f.col("SERVICEABILITYENDDT").alias("SERVICEABILITYENDDT"),
            f.col("assistance_end_date").alias("assistance_end_date"),
            f.col("status_update_date"),
            f.col("approve_date"),
            f.col("treatment_status").alias("treatment_status"),
            f.col("disaster_flag").alias("disaster_flag"),
            f.col("HARDSHIP_REASON").alias("HARDSHIP_REASON"),
            f.col("worklistid"),
            f.col("bdp_partition_cs"),
            f.col("noncommercial"),
            f.col("treatment_code"),
        ).dropDuplicates()

    # for Collection Episodes - after join with reuired tables - derive  required columns
    clect_epsd_df  = clect_epsd_df\
        .withColumn("fin_rlf_armt_stat_type_cd", f.col("status"))\
        .withColumn("fin_soltn_type_cd", f.when(f.col("treatment_status") == "Moratorium", f.lit("NIL Repayment"))
                    .when(f.col("treatment_status").isin(["Hardship Arrangement", "Hardship Monitoring", "Hardship Arrangement Broken"]),f.lit("Reduced Payment"))
                    .when(f.col("treatment_status").isin(["Serviceability", "Serviceability Monitoring", "Serviceability Completed", "Serviceability Broken"]),f.lit("Serviceability"))
                    .when((f.col("treatment_status").isin(["Serviceability", "Serviceability Monitoring", "Serviceability Completed", "Serviceability Broken"]))& (f.col("disaster_flag") == "Y"), f.lit("Serviceability"))
                    .when((f.col("treatment_status") == "Moratorium") & (f.col("disaster_flag") == "Y"), f.lit("NIL Repayment")).otherwise("UNK"))\
        .withColumn("arears_reasn_type_cd", f.when(f.col("hardship_reason").isNotNull(), f.col("hardship_reason")).otherwise("Missing Payment"))\
        .withColumn("fin_rlf_armt_subtype_cd", f.col("hardship_reason"))\
        .withColumn("litign_ind_rams", f.when(f.col("worklistid").isin(["63", "64", "65"]), f.lit("Y")).otherwise("N"))\
        .withColumn("bank_error_invstgn_cd", f.when(f.col("worklistid").isin(["24", "25"]), f.lit("BEPST")).otherwise("NOBER"))\
        .withColumn("clect_epsd_lend_host_id", f.lpad(f.trim(f.col("reference")), 10, "0"))\
        .withColumn("clect_epsd_host_id", f.concat_ws("", f.lpad(f.trim(f.col("reference")), 20, "0"), f.date_format(f.col("episode_start"), "yyyyMMdd")))\
        .withColumn("fin_rlf_st_dt", f.lit(None)) \
        .withColumn("fin_rlf_end_dt", f.lit(None))\
        .withColumn("fin_rlf_armt_host_id", f.lit(None))

    # for Collection Episodes - Derive Litigation_St_Dt
    clect_epsd_win1 = Window.partitionBy(f.col("clect_epsd_host_id")).orderBy("bdp_partition_cs")
    clect_epsd_win2 = Window.partitionBy(f.col("clect_epsd_host_id"), f.col("litign_ind_rams")).orderBy("bdp_partition_cs")

    clect_epsd_lit_dt_1_df = clect_epsd_df\
        .withColumn("grp", f.row_number().over(clect_epsd_win1) - f.row_number().over(clect_epsd_win2))

    clect_epsd_win3 = Window.partitionBy(f.col("grp"), f.col("clect_epsd_host_id"))

    clect_epsd_lit_dt_2_df = clect_epsd_lit_dt_1_df\
        .withColumn("lit_st_dt",
                    f. when(f.col("litign_ind_rams") == "Y", f.min(when(f.col("litign_ind_rams") == "Y", f.col("bdp_partition_cs"))).over(clect_epsd_win3),))\
        .filter(f.col("episode_start").isNotNull())

    # Final Df - Collection Episode
    clect_epsd_final_df = clect_epsd_lit_dt_2_df\
        .select(
        f.col("clect_epsd_host_id").cast("string"),
        f.col("episode_start").cast("date"),
        f.col("episode_end").cast("date"),
        f.col("fin_soltn_type_cd").cast("string"),
        f.col("clect_epsd_lend_host_id").cast("string"),
        f.col("litign_ind_rams").cast("string"),
        f.col("lit_st_dt").cast("date"),
        f.col("bank_error_invstgn_cd").cast("string"),
        f.col("arears_reasn_type_cd").cast("string"),
        f.col("fin_rlf_st_dt").cast("date"),
        f.col("fin_rlf_end_dt").cast("date"),
        f.col("fin_rlf_armt_host_id").cast("string"),
        f.col("noncommercial").cast("string"),
        f.col("fin_rlf_armt_stat_type_cd").cast("string"),
        f.col("fin_rlf_armt_subtype_cd").cast("integer"),
        f.col("worklistid").cast("integer"),
        f.col("reference").cast("string"),
        f.col("treatment_code").cast("integer"),
        f.lit(RDE_THRESHOLD_ODATE).cast("date").alias(SRC_BDP_PARTITION)
                ).dropDuplicates()

    # Final DF - Fin_Rlf_Armt
    fin_rlf_final_df = fin_rlf_df\
        .select(
        f.col("clect_epsd_host_id").cast("string"),
        f.col("episode_start").cast("date"),
        f.col("episode_end").cast("date"),
        f.col("fin_soltn_type_cd").cast("string"),
        f.col("clect_epsd_lend_host_id").cast("string"),
        f.col("litign_ind_rams").cast("string"),
        f.col("lit_st_dt").cast("date"),
        f.col("bank_error_invstgn_cd").cast("string"),
        f.col("arears_reasn_type_cd").cast("string"),
        f.col("fin_rlf_st_dt").cast("date"),
        f.col("fin_rlf_end_dt").cast("date"),
        f.col("fin_rlf_armt_host_id").cast("string"),
        f.col("noncommercial").cast("string"),
        f.col("fin_rlf_armt_stat_type_cd").cast("string"),
        f.col("fin_rlf_armt_subtype_cd").cast("integer"),
        f.col("worklistid").cast("integer"),
        f.col("reference").cast("string"),
        f.col("treatment_code").cast("integer"),
        f.lit(RDE_THRESHOLD_ODATE).cast("date").alias(SRC_BDP_PARTITION)
                ).dropDuplicates()
    # Final DF
    final_df = clect_epsd_final_df.union(fin_rlf_final_df)

    print("\n End of rde history load\n")
    return final_df


def get_pcc_kmr_preproc_history_target_df(
        df_a0103f_pcc_accounts,
        df_a0103f_pcc_debt_episode,
        df_a0103f_accounts,
        df_a0103f_pcc_hardship,
        df_a0103f_pcc_card_accounts,
        odate,
        is_history_load=False, ):
    """
    Obtain all the target columns for PCC after applying the provided logic
    :return df_target:
    """
    df_a0103f_pcc_accounts = df_a0103f_pcc_accounts.withColumn(
        "bdp_part_pcc_acct",
        f.from_unixtime(
            f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"
        ),
    ).withColumn("threshold_date_col_acct", f.lit(PCC_THRESHOLD_ODATE)) \
        .filter(
        (
                to_date(f.col("bdp_part_pcc_acct"))
                >=
                f.to_date(f.col("threshold_date_col_acct"))
        )
    ).select("accounts1", "cardnumber", "product_type", "bdp_part_pcc_acct") \
        .withColumn("pcc_accounts1", f.col("accounts1")) \
        .drop("accounts1")

    df_a0103f_pcc_debt_episode = df_a0103f_pcc_debt_episode.withColumn(
        "bdp_part_epsd",
        f.from_unixtime(
            f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"), "yyyy-MM-dd"
        ),
    ).withColumn("threshold_date_col_debt", f.lit(PCC_THRESHOLD_ODATE)) \
        .filter(to_date(f.col("bdp_part_epsd")) >= f.to_date(f.col("threshold_date_col_debt"))) \
        .select("accounts1", "startepisodedate", "endepisodedate", "bdp_part_epsd").withColumn(
        "accounts1_debt",
        f.col("accounts1"
              ),
    ).drop("accounts1")

    df_a0103f_accounts = df_a0103f_accounts \
        .withColumn("bdp_part_accts",
                    f.from_unixtime(f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"),
                                    "yyyy-MM-dd")) \
        .withColumn("threshold_date_col_accts", f.lit(PCC_THRESHOLD_ODATE)) \
        .filter((to_date(f.col("bdp_part_accts")) >= f.to_date(f.col("threshold_date_col_accts")))) \
        .withColumn("litign_ind", f.when(f.col("worklistid").isin(["84"]), f.lit("Y")).otherwise(f.lit("N"))) \
        .select("id", "worklistid", "bdp_part_accts", "litign_ind")

    df_a0103f_pcc_hardship = df_a0103f_pcc_hardship \
        .withColumn("bdp_part_hrdshp",
                    f.from_unixtime(f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"),
                                    "yyyy-MM-dd")) \
        .withColumn("threshold_date_col_hrdsp", f.lit(PCC_THRESHOLD_ODATE)) \
        .filter((to_date(f.col("bdp_part_hrdshp")) >= f.to_date(f.col("threshold_date_col_hrdsp")))) \
        .select("accounts1", "assistsolutiondate", "assistsolutionenddate", "enddate", "assistapraindicator",
                "commercialflag",
                "status", "bdp_part_hrdshp").withColumn(
        "accounts1_hrdshp",
        f.col("accounts1"
              ),
    )

    df_a0103f_pcc_card_accounts = df_a0103f_pcc_card_accounts \
        .withColumn("bdp_partition_pcc_card_acct",
                    f.from_unixtime(f.unix_timestamp(f.concat_ws("-", "bdp_year", "bdp_month", "bdp_day"), "yyyy-M-d"),
                                    "yyyy-MM-dd", ), ) \
        .select("accounts1", "chargeoffreasoncode", "bdp_partition_pcc_card_acct") \
        .withColumn("accounts1_card", f.col("accounts1")) \
        .withColumn("soft_chrg_off_pndg_ind",
                    f.when(f.col("chargeoffreasoncode") == "SC", f.lit("Y")).otherwise(f.lit("N"))) \
        .drop("accounts1")

    clect_win1 = Window.partitionBy(f.col("accounts1_debt"), f.col("startepisodedate")).orderBy(
        f.col("bdp_part_epsd").desc())
    clect_pcc_a_df = df_a0103f_pcc_debt_episode \
        .filter(f.col("startepisodedate").isNotNull()) \
        .withColumn("rnum", f.row_number().over(clect_win1)) \
        .filter(f.col("rnum") == 1)

    clect_win2 = Window.partitionBy(f.col("accounts1_debt"), f.col("endepisodedate")).orderBy(
        f.col("bdp_part_epsd").desc())
    clect_pcc_x_df = df_a0103f_pcc_debt_episode \
        .filter(f.col("endepisodedate").isNotNull()) \
        .withColumn("rnum", f.row_number().over(clect_win2)) \
        .filter(f.col("rnum") == 1)

    clect_pcc_un_df = clect_pcc_a_df.union(clect_pcc_x_df)

    clect_win3 = Window.partitionBy(f.col("accounts1_debt")).orderBy(f.col("bdp_part_epsd").asc(),
                                                                     f.col("clect_ind").asc())

    # Deriving 'clect_epsd_start' and 'clect_epsd_end'
    clect_pcc_df = clect_pcc_un_df \
        .withColumn("clect_ord_dt", f.when(f.col("startepisodedate").isNotNull(), f.col("startepisodedate")).when(
        f.col("endepisodedate").isNotNull(), f.col("endepisodedate"))) \
        .withColumn("clect_ind",
                    f.when(f.col("startepisodedate").isNotNull(), f.lit('A')).when(f.col("endepisodedate").isNotNull(),
                                                                                   f.lit('X'))) \
        .withColumn("prev_clect_ind", f.lag(f.col("clect_ind")).over(clect_win3)) \
        .withColumn("clect_ind_change",
                    f.concat_ws("->", f.coalesce(f.col("prev_clect_ind"), f.lit('')), f.col("clect_ind"))) \
        .filter(f.col("clect_ind_change").isin('->A', 'A->X', 'X->A')) \
        .withColumn("nxt_clect_ord_dt", f.lead(f.col("clect_ord_dt")).over(clect_win3)) \
        .withColumn("nxt_bdp_part_acty", f.lead(f.col("bdp_part_epsd")).over(clect_win3)) \
        .withColumn("episode_start", f.when(f.col("clect_ind") == 'A', f.col("clect_ord_dt")).otherwise(f.lit(None))) \
        .withColumn("episode_end",
                    f.when(f.col("clect_ind_change") != 'A->X', f.col("nxt_clect_ord_dt")).otherwise(f.lit(None))) \
        .filter(f.col("episode_start").isNotNull()) \
        .withColumn("join_part_epsd", f.when(f.col("episode_end").isNotNull(), f.col("nxt_bdp_part_acty")).when(
        f.col("episode_end").isNull(), f.col("bdp_part_epsd"))) \
        .select(f.col("accounts1_debt"),
                f.col("episode_start").cast("date"),
                f.col("episode_end").cast("date"),
                f.col("join_part_epsd").cast("date")
                )

    # Derive fin_rlf_st_dt and fin_rlf_end_dt - from pcc_hardship table
    fin_rlf_win1 = Window.partitionBy(f.col("accounts1_hrdshp"), f.col("assistsolutiondate")).orderBy(
        f.col("bdp_part_hrdshp").desc())

    # fetch fin_rlf_st_dt - hardship_ind - 'A'
    fin_rlf_armt_a_df = df_a0103f_pcc_hardship \
        .filter(f.col("assistsolutiondate").isNotNull()) \
        .withColumn("rnum", f.row_number().over(fin_rlf_win1)) \
        .filter(f.col("rnum") == 1) \
        .withColumn("hrdshp_ord_dt", f.date_format(f.col("assistsolutiondate"), "yyyy-MM-dd")) \
        .withColumn("hrdshp_ind", f.lit('A')) \
        .select("accounts1_hrdshp", "assistsolutiondate", "assistsolutionenddate", "enddate", "assistapraindicator",
                "commercialflag",
                "status", "bdp_part_hrdshp", "hrdshp_ind", "hrdshp_ord_dt")

    # fetch fin_rlf_end_dt - hardship_ind - 'X'
    fin_rlf_armt_x_df = df_a0103f_pcc_hardship \
        .filter(f.col("enddate").isNotNull()) \
        .withColumn("hrdshp_ord_dt", f.col("enddate")) \
        .withColumn("hrdshp_ind", f.lit('X')) \
        .select("accounts1_hrdshp", "assistsolutiondate", "assistsolutionenddate", "enddate", "assistapraindicator",
                "commercialflag",
                "status", "bdp_part_hrdshp", "hrdshp_ind", "hrdshp_ord_dt")

    # combine hrdshp start & end - dataframes
    fin_rlf_armt_df = fin_rlf_armt_a_df.union(fin_rlf_armt_x_df)

    # 1. find - hrdshp end for each hrdshp start - i.e. find fin_rlf_end_dt for fin_rlf_st_dt    # 2. Derive other columns
    fin_rlf_win2 = Window.partitionBy(f.col("accounts1_hrdshp")).orderBy(f.col("bdp_part_hrdshp").asc(),
                                                                         f.col("hrdshp_ind").asc())

    pcc_hrdshp_df = fin_rlf_armt_df \
        .withColumn("prev_hardshp_ind", f.lag(f.col("hrdshp_ind")).over(fin_rlf_win2)) \
        .withColumn("hardship_change",
                    f.concat_ws("->", f.coalesce(f.col("prev_hardshp_ind"), f.lit('')), f.col("hrdshp_ind"))) \
        .filter(f.col("hardship_change").isin('->A', 'A->X', 'X->A', 'A->A')) \
        .withColumn("nxt_hrdshp_ord_dt", f.lead(f.col("hrdshp_ord_dt")).over(fin_rlf_win2)) \
        .withColumn("fin_rlf_st_dt", f.when(f.col("hrdshp_ind") == 'A', f.col("hrdshp_ord_dt")).otherwise(f.lit(None))) \
        .withColumn("fin_rlf_end_dt",
                    f.when(f.col("hardship_change") != 'A->X', f.col("nxt_hrdshp_ord_dt")).otherwise(f.lit(None)))\
 \
    .withColumn("nxt_bdp_part_hshp", f.lead(f.col("bdp_part_hrdshp")).over(fin_rlf_win2)) \
        .withColumn("nxt_hardshp_ind", f.lead(f.col("hrdshp_ind")).over(fin_rlf_win2)) \
        .withColumn("nxt_assistsolutionenddate", f.lead(f.col("assistsolutionenddate")).over(fin_rlf_win2)) \
        .withColumn("nxt_assistapraindicator", f.lead(f.col("assistapraindicator")).over(fin_rlf_win2)) \
        .withColumn("nxt_commercialflag", f.lead(f.col("commercialflag")).over(fin_rlf_win2)) \
        .withColumn("nxt_status", f.lead(f.col("status")).over(fin_rlf_win2)) \
        .withColumn("get_nxt", f.when((f.col("hardship_change") != 'A->X') & (f.col("nxt_hardshp_ind") == 'X'), 1).when(
        (f.col("hardship_change") != 'A->X'), 0)) \
        .withColumn("assistsolutionenddate",
                    f.when(f.col("get_nxt") == 1, f.col("nxt_assistsolutionenddate")).otherwise(
                        f.col("assistsolutionenddate"))) \
        .withColumn("assistapraindicator", f.when(f.col("get_nxt") == 1, f.col("nxt_assistapraindicator")).otherwise(
        f.col("assistapraindicator"))) \
        .withColumn("commercialflag",
                    f.when(f.col("get_nxt") == 1, f.col("nxt_commercialflag")).otherwise(f.col("commercialflag"))) \
        .withColumn("status", f.when(f.col("get_nxt") == 1, f.col("nxt_status")).otherwise(f.col("status"))) \
        .withColumn("join_part_hshp",
                    f.when(f.col("get_nxt") == 1, f.col("nxt_bdp_part_hshp")).otherwise(f.col("bdp_part_hrdshp"))) \
        .withColumn("date_dup", f.lit("9999-12-31")) \
        .filter(f.col("fin_rlf_st_dt").isNotNull()) \
        .withColumn("fin_rlf_armt_subtype_cd",
                    f.when(f.col("assistapraindicator") == "D", f.lit("D")).otherwise(f.lit("H"))) \
        .withColumn("hardsp_expir_dt", f.col("assistsolutionenddate")) \
        .withColumn("comrc_avail_ind", f.col("commercialflag")) \
        .withColumn("fin_rlf_armt_stat_type_cd", f.when(f.col("status") == "E", f.lit("HEXIT"))
                    .when(f.col("status") == "S", f.lit("HAPRD"))
                    .when(f.col("status") == "D", f.lit("HDCLN"))
                    .when(f.col("status") == "P", f.lit("HREQ"))
                    .when(f.col("status") == "W", f.lit("HWITHD"))
                    .when(f.col("status") == "H", f.lit("SERV"))
                    .otherwise(f.lit("UNK"))) \
        .select(f.col("accounts1_hrdshp"),
                f.col("fin_rlf_st_dt").cast("date"),
                f.col("fin_rlf_end_dt").cast("date"),
                f.col("fin_rlf_armt_stat_type_cd"),
                f.col("fin_rlf_armt_subtype_cd"),
                f.col("hardsp_expir_dt"),
                f.col("comrc_avail_ind"),
                f.col("join_part_hshp").cast("date"),
                f.col("date_dup").cast("date")
                )

    # Derive clect_epsd_host_id for fin_rlf data

    fin_rlf_df = df_a0103f_pcc_accounts \
        .join(pcc_hrdshp_df,
              (f.col("pcc_accounts1") == f.col("accounts1_hrdshp")) & (
                          f.col("bdp_part_pcc_acct") == f.col("join_part_hshp")), "inner") \
        .join(clect_pcc_df,
              (f.col("pcc_accounts1") == f.col("accounts1_debt")) & (
                          f.col("fin_rlf_st_dt") >= f.col("episode_start")) & (f.col(
                  "fin_rlf_st_dt") <= f.coalesce(f.col("episode_end"), f.col("date_dup"))), "left") \
        .withColumn("fin_rlf_armt_host_id",
                    f.concat_ws("", f.trim(f.col("cardnumber")), f.date_format(f.col("fin_rlf_st_dt"), "yyyyMMdd"))) \
        .withColumn("clect_epsd_lend_host_id", f.trim(f.col("cardnumber"))) \
        .withColumn("clect_epsd_host_id",
                    f.concat_ws("", f.trim(f.col("cardnumber")),
                                f.date_format(f.coalesce(f.col("episode_start"), f.col("fin_rlf_st_dt")), "yyyyMMdd")
                                )
                    ) \
        .withColumn("litign_ind", f.lit(None)) \
        .withColumn("lit_st_dt", f.lit(None)) \
        .withColumn("bank_error_invstgn_cd", f.lit(None)) \
        .withColumn("arears_reasn_type_cd", f.lit(None)) \
        .withColumn("worklistid", f.lit(None)) \
        .withColumn("soft_chrg_off_pndg_ind", f.lit(None)) \
        .withColumn("fin_soltn_type_cd", f.lit(None))

    # For collection episodes data

    clect_rnum_win = Window.partitionBy(f.col("pcc_accounts1"), f.col("episode_start")).orderBy(
        f.col("bdp_part_pcc_acct").desc(), f.col("join_part_epsd").desc(), f.col("bdp_part_accts").desc(),
        f.col("bdp_partition_pcc_card_acct").desc())
    clect_epsd_jn_df = clect_pcc_df \
        .join(df_a0103f_pcc_accounts,
              (f.col("accounts1_debt") == f.col("pcc_accounts1")) & (
                          f.col("join_part_epsd") == f.col("bdp_part_pcc_acct")), "left") \
        .join(df_a0103f_accounts,
              (f.col("pcc_accounts1") == f.col("id")) & (f.col("join_part_epsd") == f.col("bdp_part_accts")), "left") \
        .join(df_a0103f_pcc_card_accounts,
              (f.col("pcc_accounts1") == f.col("accounts1_card")) & (
                          f.col("join_part_epsd") == f.col("bdp_partition_pcc_card_acct")), "left") \
        .filter(f.col("episode_start").isNotNull()) \
        .withColumn("clect_rnum1", f.row_number().over(clect_rnum_win)).filter(f.col("clect_rnum1") == 1)

    # Select reuired cols from above table
    clect_epsd_cols_df = clect_epsd_jn_df \
        .select(
        f.col("cardnumber").alias("clect_epsd_lend_host_id"),
        f.col("episode_start").alias("creat_dt"),
        f.col("episode_end").alias("clect_dt"),
        f.col("soft_chrg_off_pndg_ind").alias("soft_chrg_off_pndg_ind"),
        f.col("worklistid"),
        f.col("bdp_part_pcc_acct"),
        f.col("litign_ind"), ).withColumn("clect_epsd_host_id", f.concat_ws("", f.col("clect_epsd_lend_host_id"),
                                                                            f.date_format(f.col("creat_dt"),
                                                                                          "yyyyMMdd"))).dropDuplicates()
    clect_epsd_window1 = Window.partitionBy(f.col("clect_epsd_host_id")).orderBy("bdp_part_pcc_acct")
    clect_epsd_window2 = Window.partitionBy(f.col("clect_epsd_host_id"), f.col("litign_ind")).orderBy(
        "bdp_part_pcc_acct")

    clect_epsd_grp_df = clect_epsd_cols_df.withColumn("grp",
                                                      f.row_number().over(clect_epsd_window1) - f.row_number().over(
                                                          clect_epsd_window2), )

    clect_epsd_window3 = Window.partitionBy(f.col("grp"), f.col("clect_epsd_host_id"))

    clect_epsd_lit_df = clect_epsd_grp_df \
        .withColumn("lit_st_dt", when(f.col("litign_ind") == "Y",
                                      f.min(when(f.col("litign_ind") == "Y", f.col("bdp_part_pcc_acct"))).over(
                                          clect_epsd_window3), ), ).filter(f.col("creat_dt").isNotNull())

    clect_epsd_df = clect_epsd_lit_df.select(
        f.col("clect_epsd_host_id"),
        f.col("creat_dt"),
        f.col("clect_dt"),
        f.col("soft_chrg_off_pndg_ind"),
        f.lit(None).cast("string").alias("comrc_avail_ind"),
        f.lit(None).cast("date").alias("fin_rlf_st_dt"),
        f.lit(None).cast("date").alias("fin_rlf_end_dt"),
        f.lit(None).cast("string").alias("fin_soltn_type_cd"),
        f.col("clect_epsd_lend_host_id"),
        f.col("litign_ind"),
        f.col("lit_st_dt").cast("date"),
        f.lit(None).cast("string").alias("bank_error_invstgn_cd"),
        f.lit(None).cast("string").alias("arears_reasn_type_cd"),
        f.lit(None).cast("string").alias("fin_rlf_armt_stat_type_cd"),
        f.lit(None).cast("date").alias("hardsp_expir_dt"),
        f.lit(None).cast("string").alias("fin_rlf_armt_host_id"),
        f.col("worklistid"),
        f.lit(None).cast("string").alias("fin_rlf_armt_subtype_cd")
    ).dropDuplicates()

    fin_rlf_final_df = fin_rlf_df.select(
        f.col("clect_epsd_host_id"),
        f.col("episode_start").alias("creat_dt"),
        f.col("episode_end").alias("clect_dt"),
        f.col("soft_chrg_off_pndg_ind"),
        f.col("comrc_avail_ind"),
        f.col("fin_rlf_st_dt"),
        f.col("fin_rlf_end_dt"),
        f.col("fin_soltn_type_cd"),
        f.col("clect_epsd_lend_host_id"),
        f.col("litign_ind"),
        f.col("lit_st_dt").cast("date"),
        f.col("bank_error_invstgn_cd"),
        f.col("arears_reasn_type_cd"),
        f.col("fin_rlf_armt_stat_type_cd"),
        f.col("hardsp_expir_dt"),
        f.col("fin_rlf_armt_host_id"),
        f.col("worklistid"),
        f.col("fin_rlf_armt_subtype_cd")
    )

    final_df = clect_epsd_df.union(fin_rlf_final_df).withColumn(SRC_BDP_PARTITION, f.to_date(f.lit(PCC_THRESHOLD_ODATE)).cast(DateType()))

    print("pcc - history completed")

    return final_df
