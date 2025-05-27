"""
The 'custom_transform' module encapsulates custom Spark transform logic for a specific pipeline.
"""
from ddep.batch_functions.misc.utils import get_fw_process
from common.functions import get_scd_target_df, get_target_clect_epsd_rde_df
from ddep.batch_functions.date.utils import log_order_date
from common.constants import (
    JURIS_CNTRY_CD_AU, PARTY_SK, CASE_SK
)


# Deriving and selecting all the columns for fin_tran_gl_entry using all the dataframes
def get_target_df(df_b2k_clects_epsd, df_harm_ref, df_temp_host_id, df_erd, odate):
    return get_target_clect_epsd_rde_df(df_b2k_clects_epsd, df_harm_ref, df_temp_host_id, df_erd, odate)


def transform(framework_context, atlas, config, logger, session, _context, dataframes):
    """
    :param framework_context: Framework context object including '.args' property containing all
    command line arguments.
    :param atlas: Atlas service instance.
    :param config: Configuration service instance.
    :param logger: Logger from 'logging' module with both Console and File handlers attached.
    :param session: Apache Spark session object.
    :param _context: Apache Spark context object.
    :param dataframes: Dictionary of named Spark dataframe loaded from the defined inputs
    metadata attribute.
    :return: A single Dataframe object after the transform has been completed.
    """

    logger.debug("Retrieving input and output dataset names from atlas metadata")
    process = get_fw_process(atlas, config, framework_context)
    odate = log_order_date(framework_context, logger)

    # Getting all inbound table names from atlas metadata
    logger.info("Collection episode transformation started")
    df_rde_clects_epsd = dataframes[process.entity.attributes.bdp_object_info.inputs[0].name]
    df_harm_ref = dataframes[process.entity.attributes.bdp_object_info.inputs[1].name]
    df_temp_host_id = dataframes[process.entity.attributes.bdp_object_info.inputs[2].name]
    df_erd = dataframes[process.entity.attributes.bdp_object_info.inputs[3].name]

    fdp048_clect_epsd = process.entity.attributes.bdp_object_info.outputs[0].name
    pk_cols = [CASE_SK]

    logger.info("Final DF Created for " + fdp048_clect_epsd)
    # Getting all the target columns in target dataframe for clects_epsd by applying final transformation
    df_target = get_target_df(df_rde_clects_epsd, df_harm_ref, df_temp_host_id, df_erd, odate)
    df_scd_target = get_scd_target_df(
        framework_context,
        atlas,
        config,
        logger,
        session,
        dataframes,
        df_target,
        "A00845",
        JURIS_CNTRY_CD_AU,
        fdp048_clect_epsd,
        pk_cols

    )
    return df_scd_target

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

"""
The 'custom_transform' module encapsulates custom Spark transform logic for a specific pipeline.
"""
from ddep.batch_functions.misc.utils import get_fw_process
from common.functions import get_scd_target_hist_df, get_target_clect_epsd_pcc_df, cast_clect_epsd_final_df
from ddep.batch_functions.date.utils import log_order_date
from common.constants import (
    JURIS_CNTRY_CD_AU,  PARTY_SK, CASE_SK
)


# Deriving and selecting all the columns for fin_tran_gl_entry using all the dataframes
def get_target_df(df_pcc_clects_epsd, df_temp_host_id, odate):
    return get_target_clect_epsd_pcc_df(df_pcc_clects_epsd, df_temp_host_id, odate, True)


def transform(framework_context, atlas, config, logger, session, _context, dataframes):
    """
    :param framework_context: Framework context object including '.args' property containing all
    command line arguments.
    :param atlas: Atlas service instance.
    :param config: Configuration service instance.
    :param logger: Logger from 'logging' module with both Console and File handlers attached.
    :param session: Apache Spark session object.
    :param _context: Apache Spark context object.
    :param dataframes: Dictionary of named Spark dataframe loaded from the defined inputs
    metadata attribute.
    :return: A single Dataframe object after the transform has been completed.
    """

    logger.debug("Retrieving input and output dataset names from atlas metadata")
    process = get_fw_process(atlas, config, framework_context)
    odate = log_order_date(framework_context, logger)

    # Getting all inbound table names from atlas metadata
    logger.info("Collection episode transformation started")
    df_pcc_clects_epsd = dataframes[process.entity.attributes.bdp_object_info.inputs[0].name]
    df_temp_host_id = dataframes[process.entity.attributes.bdp_object_info.inputs[1].name]
    
    fdp048_clect_epsd = process.entity.attributes.bdp_object_info.outputs[0].name

    logger.info("Final DF Created for " + fdp048_clect_epsd)
    # Getting all the target columns in target dataframe for clects_epsd by applying final transformation
    df_target_tmp = get_target_df(df_pcc_clects_epsd, df_temp_host_id, odate)

    df_target = cast_clect_epsd_final_df(df_target_tmp)

    pk_cols = [CASE_SK]

    df_scd_target = get_scd_target_hist_df(
        framework_context,
        atlas,
        config,
        logger,
        session,
        dataframes,
        df_target,
        "A0103F",
        JURIS_CNTRY_CD_AU,
        fdp048_clect_epsd,
        pk_cols,
    )
    return df_scd_target
