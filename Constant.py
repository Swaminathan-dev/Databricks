from __future__ import absolute_import
from enum import Enum

__all__ = (
    "ClectoutsrcgType",
    "TempListclectsepsd",
    "TempCredclectsepsd",
    "TempDdaclectsepsd",
    "CollectEpi",
    "HarmRef",
    "RecovOutcmType",
    "RecovType",
    "Tempb2kclectsepsd",
    "RlshpType",
    "FinSolType",
    "RefDataAppln",
    "ClectEntry",
    "RefDataApplnMap",
    "FinRlfArmtSubtype",
    "FinRlfArmt",
    "TemphostidKmrkey",
    "TmpPreprocessCrdclctepsd",
    "KmrHostidKeys",
    "TempHostId",
    "TmpPreprocessB2kclctepsd",
    "TmpPreprocessDdaclctepsd",
    "TmpPreprocessLisclctepsd",
    "DdepSCDColumns",
    "ActivityAcraamaa",
    "TempMipRecov",
    "HardShipDate",
    "PublishRamsHardship",
    "A002f4SecMast",
    "A002f4AcctLoanSecur",
    "TempMipDda",
    "TempMipLis",
    "RdeSecurity",
    "RdeTitleDeedDetail",
    "RdeLoanSecurity",
    "RdeLoanVariation",
    "RdeAccountBalance",
    "RdeAccount",
    "RdeCustomerAccount",
    "RdeCsAccount",
    "RdeTmpIntScrty",
    "SecrtySale",
    "ErdData",
)
KMRKEY = "Kmrkey"
CLECT_EPSD_HOST_ID_ALL = "clect_epsd_host_id"
CODE = "code"
ICB_CD = "A0010E"
INF_CD = "A0018E"
MSS_CD = "A0019B"
TBK_CD = "A00004"
LIS_CD = "A002F4"
DDA_CD = "A002FB"
CRDS_CD = "A001FC"
JURIS_CNTRY_CD_AU = "AUS"
CLECT_EPSD_HOST_ID = "clect_epsd_host_id"
FIN_RLF_ARMT_HOST_ID = "fin_rlf_armt_host_id"
DDE_ARREARS_REASON = "dde_arrears_reason"
DDE_AUTO_LTR_DAYS = "dde_auto_ltr_days"
DDE_ACCT_ID = "dde_acct_id"
DDE_DTE_COLCTED = "dde_dte_colcted"
DDE_CURR_DT_ADD = "dde_curr_dt_add"
DDE_OL_WRITEOFF_IND = "dde_ol_writeoff_ind"
DDE_OL_LRD_IND = "dde_ol_lrd_ind"
DDE_SUPPRESS_LTR = "dde_suppress_ltr"
DDE_SUPV_SUPR_DAYS = "dde_supv_supr_days"
DDE_RECOVERY_STAT_CODE = "dde_recovery_stat_code"
DDE_SCO_PENDING = "dde_sco_pending"
DDE_COLL_ENTRY_REAS = "dde_coll_entry_reas"
DDE_REC_STAT = "dde_rec_stat"
DDE_21D3_LIST_AMNT = "dde_21d3_list_amnt"
DDE_HARDSHIP_INFO_EXP_DT = "dde_hardship_info_exp_dt"
DDE_HARDSHIP_LVR = "dde_hardship_lvr"
DDE_PROM_SCHED_WK_IND = "dde_prom_sched_wk_ind"
DDE_PROM_STAT = "dde_prom_stat"
DDE_PROM_AMT = "dde_prom_amt"
DDE_PROM_DTE = "dde_prom_dte"
DDE_DTE_LAST_REVW_DT = "dde_dte_last_revw_dt"
DDE_AMT_TOWARDS_PROM = "dde_amt_towards_prom"
DDE_ARREARS_CODE = "dde_arrears_code"
DDE_VP_ASSIST_IND = "dde_vp_assist_ind"
DDE_PROM_SCHEDULE = "dde_prom_schedule"
DDE_ASSISTANCE_LEVEL = "dde_assistance_level"
DDE_COMPLAINTS_CUST_RELTNS = "dde_complaints_cust_reltns"
DDE_SGB_FOS = "dde_sgb_fos"
DDE_COMMERCIAL = "dde_commercial"
DDE_DUE_DATE = "dde_due_date"
DDE_PAYMENT_DUE_DATE = "dde_payment_due_date"
DDE_CUST_ID = "dde_cust_id"
DDE_NEXT_DUE_DATE = "dde_next_due_date"
NONE = "NA"
INNER = "inner"
LEFT = "left"
AREARS_REASN_TYPE_CD = "arears_reasn_type_cd"
AUTO_LTR_DAY_CNT = "auto_ltr_day_cnt"
CASE_SK = "case_sk"
CLECT_DT = "clect_dt"
CLECT_EPSD_HOST_ID = "clect_epsd_host_id"
CLECT_OUTSRCG_TYPE_CD = "clect_outsrcg_type_cd"
CREAT_DT = "creat_dt"
DEFER_WRITE_OFF_IND = "defer_write_off_ind"
FIN_SOLTN_TYPE_CD = "fin_soltn_type_cd"
JURIS_CNTRY_CD = "juris_cntry_cd"
LEND_ARMT_SK = "lend_armt_sk"
LITIGN_IND = "litign_ind"
LTR_SUPRSN_IND = "ltr_suprsn_ind"
NO_PH_CONTCT_IND = "no_ph_contct_ind"
NXT_LTR_MIN_DAY_CNT = "nxt_ltr_min_day_cnt"
OMIT_FROM_SUPVSRY_QU_DAY_CNT = "omit_from_supvsry_qu_day_cnt"
OWNG_LEND_ARMT_APPLN_KEY = "owng_lend_armt_appln_key"
RECOV_TYPE_CD = "recov_type_cd"
SOFT_CHRG_OFF_PNDG_IND = "soft_chrg_off_pndg_ind"
SRC_SYS_APPLN_ID = "src_sys_appln_id"
CLECT_ENTRY_REASN_TYPE_CD = "clect_entry_reasn_type_cd"
CLECT_STAT_TYPE_CD = "clect_stat_type_cd"
CREDIT_LISTING_AMOUNT_CURRENCY_CODE = "cr_listing_amt_crncy_cd"
RECOVERY_AMOUNT_CURRENCY_CODE = "recovery_amount_currency_code"
RECOV_OUTCM_TYPE_CD = "recov_outcm_type_cd"
CRAA_LODG_DT = "craa_lodg_dt"
RECOV_AMT = "recov_amt"
CR_LISTNG_AMT = "cr_listng_amt"
UNKNOWN = "UNKNOWN"
AUS = "AUS"
A0010E = "A0010E"
A0018E = "A0018E"
A001FC = "A001FC"
A0019B = "A0019B"
A00004 = "A00004"
AUD = "AUD"
A002F4 = "A002F4"
A002FB = "A002FB"
A001FC = "A001FC"
LIS = "LIS"
DDA = "DDA"
CRDS = "CRDS"
UNK = "UNK"
APPLN_KEY = "appln_key"
A001F5 = "A001F5"
A0030A = "A0030A"

PRV_SYS_APPLN_ID = "prv_sys_appln_id"
JURIS_CNTRY_CD = "juris_cntry_cd"
SRC_SYS_APPLN_ID = "src_sys_appln_id"
PRMS_AMT = "prms_amt"
PRMS_DT = "prms_dt"
PRMS_REVW_DT = "prms_revw_dt"
TOT_PRMS_AMT = "tot_prms_amt"
PRMS_STAT_TYPE_CD = "prms_stat_type_cd"
PAYMT_PLAN_PERIOD_TYPE_CD = "paymt_plan_period_type_cd"
PROMISE_AMOUNT_CURRENCY_CODE = "prms_amt_crncy_cd"
CASE_SK = "case_sk"
JURIS_CNTRY_CD = "juris_cntry_cd"
AUS = "AUS"
A001FC = "A001FC"
A002FB = "A002FB"
A002F4 = "A002F4"
ICB = "ICB"
INF = "INF"
MSS = "MSS"
TBK = "TBK"
A0010E = "A0010E"
A0018E = "A0018E"
A00004 = "A00004"
A0019B = "A0019B"
LEFT = "left"
AUD = "AUD"

CLECT_EPSD_HOST_ID = "clect_epsd_host_id"
FIN_RLF_ARMT_HOST_ID = "fin_rlf_armt_host_id"
DDE_ACCT_ID = "dde_acct_id"
DDE_21D3_LIST_AMNT = "dde_21d3_list_amnt"
DDE_AMT_TOWARDS_PROM = "dde_amt_towards_prom"
DDE_ARREARS_REASON = "dde_arrears_reason"
DDE_AUTO_LTR_DAYS = "dde_auto_ltr_days"
DDE_COLL_ENTRY_REAS = "dde_coll_entry_reas"
DDE_CURR_DT_ADD = "dde_curr_dt_add"
DDE_OL_WRITEOFF_IND = "dde_ol_writeoff_ind"
DDE_DTE_COLCTED = "dde_dte_colcted"
DDE_DTE_LAST_REVW_DT = "dde_dte_last_revw_dt"
DDE_HARDSHIP_INFO_EXP_DT = "dde_hardship_info_exp_dt"
DDE_HARDSHIP_LVR = "dde_hardship_lvr"
DDE_OL_LRD_IND = "dde_ol_lrd_ind"
DDE_PROM_AMT = "dde_prom_amt"
DDE_PROM_DTE = "dde_prom_dte"
DDE_PROM_SCHED_WK_IND = "dde_prom_sched_wk_ind"
DDE_PROM_STAT = "dde_prom_stat"
DDE_REC_STAT = "dde_rec_stat"
DDE_RECOVERY_STAT_CODE = "dde_recovery_stat_code"
DDE_SCO_PENDING = "dde_sco_pending"
DDE_SUPPRESS_LTR = "dde_suppress_ltr"
DDE_SUPV_SUPR_DAYS = "dde_supv_supr_days"
DDE_VP_ASSIST_IND = "dde_vp_assist_ind"
ACTY_CD = "acty_cd"
DDE_ACTY_CDS = "dde_acty_cds"
DDE_BANK_ERR_PRESENT = "dde_bank_err_present"
DDE_ALT_STRATEGY_IND = "dde_alt_strategy_ind"
DDE_APRA = "dde_apra"
DDE_COMMERCIALLY_AVAILABLE_PRESENT = "dde_commercially_available_present"
DDE_FINANCIAL_SOLUTION = "dde_financial_solution"
DDE_IL_RECOVERY_AMOUNT = "dde_il_recovery_amount"
DDE_IL_RECOVERY_OUTCOME = "dde_il_recovery_outcome"
DDE_HARDSHIP_EXP_DATE = "dde_hardship_exp_date"
DDE_HARDSHIP_IND = "dde_hardship_ind"
DDE_MI_ARRS_REASON = "dde_mi_arrs_reason"
DDE_LETR_WARNING_DATE = "dde_letr_warning_date"
DDE_NO_PHONE_CONTACT = "dde_no_phone_contact"
DDE_ORIG_PROD_SYS = "dde_orig_prod_sys"
DDE_OUTSOURCING_CODE = "dde_outsourcing_code"
DDE_PROM_SCHEDULE = "dde_prom_schedule"
DDE_RECOVERY_CODE = "dde_recovery_code"
DDE_SOFT_CHARGEOFF_IND = "dde_soft_chargeoff_ind"
DDE_SUPRES_AUTO_LTR_IND = "dde_supres_auto_ltr_ind"
DDE_DEFER_WRITE_OFF_IND = "dde_defer_write_off_ind"

DDE_FIN_RLF_END_DT = "fin_rlf_end_dt"
DDE_FIN_RLF_ST_DT = "dde_fin_rlf_st_dt"
FIN_RLF_ST_DT = "fin_rlf_st_dt"
ARMT_SK = "armt_sk"
RLTD_ARMT_SK = "rltd_armt_sk"
FDP_CLECT_EPSD_CASE_SK = "clect_epsd_case_sk"
CLECT_EPSD_CASE_SK = "clect_epsd_case_sk"
FDP_FIN_RLF_ST_DT = "fin_rlf_st_dt"
FDP_FIN_RLF_END = "fin_rlf_end_dt"
CASE_SK = "case_sk"
EPISODE_START = "episode_start"
EPISODE_END = "episode_end"
CLECT_EPSD_HOST_ID = "clect_epsd_host_id"
FIN_RLF_ARMT_HOST_ID = "fin_rlf_armt_host_id"
LEND_ARMT_SK = "lend_armt_sk"
FIN_RLF_END_DT = "fin_rlf_end_dt"
LEND_ARMT_HOST_ID = "lend_armt_host_id"
FIN_REF_ENTITY_SUBTYPE_NM = "FINANCIAL RELIEF ARRANGEMENT SUBTYPE"
FIN_REF_ENTITY_STAT_NM = "FINANCIAL RELIEF ARMT STATUS"
HARDSP_LOAN_VALN_RATIO = "hardsp_loan_valn_ratio"
HARDSP_EXPIR_DT = "hardsp_expir_dt"
THRESHOLD_ODATE = "2021-10-01"
INFLS_THRESHOLD_ODATE = "2021-10-01"
RDE_THRESHOLD_ODATE =  "2021-10-01"
PCC_THRESHOLD_ODATE = "2021-10-01"
OWNG_LEND_ARMT_APPLN_KEY_CRD = "A0077D:A001FC"
OWNG_LEND_ARMT_APPLN_KEY_DDA = "A0077D:A002FB"
OWNG_LEND_ARMT_APPLN_KEY_LIS = "A0077D:A002F4"
OWNG_LEND_ARMT_APPLN_KEY_INFLIS = "A0077D:A0018E"
SRC_BDP_PARTITION = "src_bdp_partition"
LIS_KMR_HOST = "^(\\w{1})"
TCS_CUSTID_FORMAT = "(\\d+)(?!.*\\d)"

IT_APPLN_KEY = "IT_APPLN_RSRC_KEY"
REF_TYPE_SRC_CD = "REF_TYPE_SRC_CD"
REF_TYPE_SRC_DESC = "REF_TYPE_SRC_DESC"
REF_ENTITY_NM = "REF_ENTITY_NM"
REF_TYPE_CD = "REF_TYPE_CD"
Collection_Entry_Reason = "Collections Entry Reason"
Harmonized_Code = "harmonized_code"
Harmonized_Desc = "harmonized_desc"
CLECT_ENTRY_REASN_TYPE_CD = "clect_entry_reasn_type_cd"
CLECT_ENTRY_REASN_TYPE_DESC = "clect_entry_reasn_type_desc"
AREARS_REASN_TYPE_CD = "arears_reasn_type_cd"
AREARS_REASN_TYPE_DESC = "arears_reasn_type_desc"
CLECT_EPSD_PARTY_ROLE_TYPE_CD = "clect_epsd_party_role_type_cd"
CLECT_EPSD_PARTY_ROLE_TYPE_DESC = "clect_epsd_party_role_type_desc"
CUSTOMER_PARTY_ROLE = "CUST"
PARTY_SK = "party_sk"
FIN_RLF_ARMT_STATUS_CODE = "FINANCIAL RELIEF ARMT STATUS"
FIN_RLF_ARMT_STAT_CD = "fin_rlf_armt_stat_cd"
FIN_RLF_ARMT_STAT_TYPE_CD = "fin_rlf_armt_stat_type_cd"
FIN_RLF_ARMT_STAT_DESC = "fin_rlf_armt_stat_desc"
MIP_IND = "secrty_in_possn_ind"
ACCT_BRND = "acct_brnd"
ACCT_NUM = "acct_num"
STAT_DESC = "stat_desc"
PROPTY_ACQUISITION_DT = "propty_acquisition_dt"
DATE_OF_SALE = "date_of_sale"
Arears_Reason_Type = "Arrear Reason Type"
CLECT_OUTSRCG_TYPE_CD = "clect_outsrcg_type_cd"
CLECT_OUTSRCG_TYPE_DESC = "clect_outsrcg_type_desc"
Collection_outsrcg_type = "Collections Outsourcing Type"
CLECT_STAT_TYPE_CD = "armt_clect_stat_type_cd"
CLECT_STAT_TYPE_DESC = "clect_stat_type_desc"
Collections_Stat_Type = "Collections Status Type"
FIN_RLF_ARMT_SUBTYPE_CD = "fin_rlf_armt_subtype_cd"
FIN_RLF_ARMT_STAT_CD = "fin_rlf_armt_stat_cd"
COMRC_AVAIL_IND = "comrc_avail_ind"
FIN_RLF_ARMT_SUBTYPE_DESC = "fin_rlf_armt_subtype_desc"
Financial_Relief_Armt_Relationship_Type = "Financial Relief Armt Relationship Type"
FIN_RLF_ARMT_RLSHP_TYPE_CD = "fin_rlf_armt_rlshp_type_cd"
FIN_RLF_ARMT_RLSHP_TYPE_DESC = "fin_rlf_armt_rlshp_type_desc"
FIN_RLF_ARMT_SUBTYPE = "Financial Relief Arrangement Subtype"
FIN_SOLTN_TYPE_CD = "fin_soltn_type_cd"
FIN_SOLTN_TYPE_DESC = "fin_soltn_type_desc"
FIN_SOLTN_TYPE = "Financial Solution Type"
PRMS_STAT_TYPE_CD = "prms_stat_type_cd"
PRMS_STAT_TYPE_DESC = "prms_stat_type_desc"
PRMS_STAT_TYPE = "Promise Status Type"
RECOV_OUTCM_TYPE_CD = "recov_outcm_type_cd"
RECOV_OUTCM_TYPE_DESC = "recov_outcm_type_desc"
Recovery_Outcome_Type = "Recovery Outcome Type"
RECOV_TYPE_CD = "recov_type_cd"
RECOV_TYPE_DESC = "recov_type_desc"
Recovery_Type = "Recovery Type"
FDP048 = "FDP048"
entity_name = "entity_name"
A0077D = "A0077D"
REF_CSV_FILE = "collections_ref_data.csv"
B2K_APPLN_KEY = "A0077D:A001F5"
TCS_APPLN_KEY = "A0077D:A0030A"
RDE_APPLN_KEY = "A0077D:A0075C"
RDE_PRV_SYS_APPLN_ID = "A0077D:A00845"
PCC_APPLN_KEY = "A0077D:A001FC"
PCC_PRV_SYS_APPLN_ID = "A0077D:A0103F"
MIP_SRC_APPLN_KEY = "A0077D:A01519"
B2K_APPLN_KEY_ERD = "A001F5"
TCS_APPLN_KEY_ERD = "A0030A"
RDE_APPLN_KEY_ERD = "A00845"
MIP_APPLN_KEY_ERD = "A01519"
CMT_APPN_KEY = "A0077D:A00D41"
INFLIS_APPN_KEY = "A0077D:A001F5"


PROMISE_SCHEDULE_TYPE = "PROMISE SCHEDULE TYPE"
LIS_CODE = "A0077D:A002F4"
CRD_CODE = "A0077D:A001FC"
DDA_CODE = "A0077D:A002FB"
RAMS_CODE = "A0077D:A00845"
RAMS_APPLN_KEY = "A0077D:A00845"
BANK_ERROR_CODE = "BANK ERROR INVESTIGATION CODE"
RECOVERY_TYPE_CODE = "RECOVERY TYPE CODE"
RECOVERY_STATUS_CODE = "RECOVERY STATUS CODE"
CLECT_RECOV_TYPE_CD = "recov_type_cd"
CLECT_RECOV_TYPE_DESC = "recov_type_desc"
CLECT_RECOV_STATUS_CD = "recov_stat_cd"
CLECT_RECOV_STATUS_DESC = "recov_stat_desc"
CLECT_BANK_ERROR_CD = "bank_error_invstgn_cd"
CLECT_BANK_ERROR_DESC = "bank_error_invstgn_desc"
PARTY_ROLE_TYPE_CODE = "PARTY ROLE TYPE CODE"
DDE_LITIGATION = "dde_litigation"
DDE_LITIGATION_IND = "dde_litigation_ind"
TITLE_REFERENCE = "title_reference"
LIT_START_DT = "litign_st_dt"
WORKLIST_WORKFLOW_SK = "worklist_workflow_sk"
ALTERNATE_STRATEGY_CODE = "ALTERNATE STRATEGY CODE"
ALTN_STRAT_CD = "altn_strat_cd"
DDE_ALT_STRATEGY_IND = "dde_alt_strategy_ind"
DDE_APRA = "dde_apra"

SALE_SETLMT_DT = "sale_setlmt_dt"
SALE_AMT = "sale_amt"
SALE_AMT_CRNCY_CD = "sale_amt_crncy_cd"
LST_VALN_DT = "lst_valn_dt"
CURNT_LOW_VALN_AMT = "curnt_low_valn_amt"
SECRTY_VALN_CRNCY_CD = "secrty_valn_crncy_cd"

MOD_DEBT_ACCOUNT_NUMBER = "mod_debt_account_number"
REL_SECURITY_NUMBER = "security_number"
FIN_RLF_ST_DT = "fin_rlf_st_dt"

TRN = "trn"
VTRN = "vtrn"
TBLNM = "tblnm"
SECURITY_NUMBER = "security_number"

ACCT_NUM = "acct_num"
TITLE_REFERENCE = "title_reference"
PROPTY_ACQUISITION_DT = "propty_acquisition_dt"
DDE_ACCT_ID = "dde_acct_id"
DDE_DTE_COLCTED = "dde_dte_colcted"
FMT_SECURITY_NUM = "fmt_security_num"
STATE_CD = "state_cd"
DESCRIPTION = "descriptions"
SECURITY_KEY = "security_key"
STREET_ADDR = "street_addr"

COMPANY_NUM = "company_num"
SEQUENCE_NUM = "sequence_num"
THRESHOLD_ODATE_RDE = "2021-10-01"

COMPANY_NUM = "company_num"
SEQUENCE_NUM = "sequence_num"
SECRTYNUM = "secrtynum"
SECURITYSKEY = "securityskey"
TITLDETAILS = "titldetails"
LINKCHECKDIGIT = "linkcheckdigit"
SECRTY_RSRC_SK = "secrty_rsrc_sk"
SECRTY_SALE_STAT_CD = "secrty_sale_stat_cd"
SECRTY_SALE_DT = "secrty_sale_dt"
SECRTY_SETL_DT = "secrty_setl_dt"
SECRTY_ACQ_DT = "secrty_acq_dt"
SALE_AMT = "sale_amt"
SALE_AMT_CRNCY_CD = "sale_amt_crncy_cd"
SECRTY_VALN_DT = "secrty_valn_dt"
SECRTY_VALN_MIN_AMT = "secrty_valn_min_amt"
SECRTY_VALN_CRNCY_CD = "secrty_valn_crncy_cd"



class CommonAttributes(Enum):
    DDE_DTE_COLCTED = "dde_dte_colcted"
    DDE_ORIG_PROD_SYS = "dde_orig_prod_sys"
    SRC_BDP_PARTITION = "src_bdp_partition"
    BDP_YEAR = "bdp_year"
    BDP_MONTH = "bdp_month"
    BDP_DAY = "bdp_day"


class ClectoutsrcgType(Enum):
    D = "D"
    OUTSOURCE_DECLINED = "OUTSOURCE- DECLINED"
    A = "A"
    OUTSOURCE_READY = "OUTSOURCE- READY"
    O = "O"
    OUTSOURCE_CAUSE = "OUTSOURCE-NO CAUSE"
    X = "X"
    OUTSOURCE_EXCLUDE = "If OUTSOURCING CODE NOT = 'X' , then it is 'OUTSOURCE- EXCLUDE'"
    E = "E"
    OUTSOURCE_EARLY = "OUTSOURCE-EARLY WRITE OFF"


class PrmsStatType(Enum):
    B = "B"
    PROMISE_BROKEN = "Broken Promise"
    K = "K"
    PROMISE_KEPT = "Kept Promise"
    O = "O"
    PROMISE_OPEN = "Open Promise"
    N = ""
    PROMISE_NO = "No Promise"


class RlshpType(Enum):
    FRACE = "FRACE"
    RLSHP_DESC = "Financial Relief Arrangement to Collections Episode"


class CollectEpi(Enum):
    ALIAS = "colepi"
    DDE_MI_ARRS_REASON = ALIAS + ".dde_mi_arrs_reason"
    RDE_MI_ARRS_REASON = ALIAS + ".arears_reasn_type_cd"
    DDE_AUTO_LTR_DAYS = ALIAS + ".dde_auto_ltr_days"
    DDE_ACCT_ID = ALIAS + ".dde_acct_id"
    DDE_CURR_DT_ADD = ALIAS + ".dde_curr_dt_add"
    DDE_DTE_COLCTED = ALIAS + "." + CommonAttributes.DDE_DTE_COLCTED.value
    DDE_OUTSOURCING_CODE = ALIAS + ".dde_outsourcing_code"
    DDE_DEFER_WRITE_OFF_IND = ALIAS + ".dde_defer_write_off_ind"
    DDE_FINANCIAL_SOLUTION = ALIAS + ".dde_financial_solution"
    RDE_FINANCIAL_SOLUTION = ALIAS + ".fin_soltn_type_cd"
    DDE_ORIG_PROD_SYS = ALIAS + "." + CommonAttributes.DDE_ORIG_PROD_SYS.value
    DDE_OL_LRD_IND = ALIAS + ".dde_ol_lrd_ind"
    DDE_SUPRES_AUTO_LTR_IND = ALIAS + ".dde_supres_auto_ltr_ind"
    DDE_NO_PHONE_CONTACT = ALIAS + ".dde_no_phone_contact"
    DDE_SUPV_SUPR_DAYS = ALIAS + ".dde_supv_supr_days"
    DDE_RECOVERY_CODE = ALIAS + ".dde_recovery_code"
    DDE_SOFT_CHARGEOFF_IND = ALIAS + ".dde_soft_chargeoff_ind"
    DDE_COLL_ENTRY_REAS = ALIAS + ".dde_coll_entry_reas"
    DDE_REC_STAT = ALIAS + ".dde_rec_stat"
    DDE_IL_RECOVERY_OUTCOME = ALIAS + ".dde_il_recovery_outcome"
    DDE_LETR_WARNING_DATE = ALIAS + ".dde_letr_warning_date"
    DDE_IL_RECOVERY_AMOUNT = ALIAS + ".dde_il_recovery_amount"
    CLECT_EPSD_CASE_KMR_KEY = ALIAS + ".kmr_case_key"
    CLECT_EPSD_LEND_KMR_KEY = ALIAS + ".kmr_lend_key"
    DDE_ARREARS_REASON = ALIAS + ".dde_arrears_reason"
    DDE_OL_WRITEOFF_IND = ALIAS + ".dde_ol_writeoff_ind"
    DDE_SUPPRESS_LTR = ALIAS + ".dde_suppress_ltr"
    APPLN_KEY = ALIAS + ".appln_key"
    DDE_SCO_PENDING = ALIAS + ".dde_sco_pending"
    DDE_RECOVERY_STAT_CODE = ALIAS + ".dde_recovery_stat_code"
    DDE_21D3_LIST_AMNT = ALIAS + ".dde_21d3_list_amnt"
    CLECT_EPSD_HOST_ID_CE = ALIAS + "." + CLECT_EPSD_HOST_ID_ALL
    DDE_ASSISTANCE_LEVEL = ALIAS + ".dde_assistance_level"
    SRC_BDP_PARTITION = ALIAS + ".src_bdp_partition"
    CREATED = ALIAS + ".created"
    SOFT_ENTRY_FLAG = ALIAS + ".soft_entry_flag"
    EPISODE_END_DELAY_DATE = ALIAS + ".episode_end_delay_date"
    WRITEOFFID = ALIAS + ".writeoffid"
    MOBILE_PHONE_ID = ALIAS + ".mobile_phone_id"
    HS_REASONCODE = ALIAS + ".hs_reasoncode"
    CYCLE_CODE = ALIAS + ".cycle_code"
    ARREARSTOCAP_CODE = ALIAS + ".arrearstocap_code"
    DDE_BANK_ERR_PRESENT = ALIAS + ".dde_bank_err_present"
    RDE_BANK_ERR_PRESENT = ALIAS + ".bank_error_invstgn_cd"
    BANK_ERR_CD = "bank_error_cd"
    DDE_COMPLAINTS_CUST_RELTNS = ALIAS + ".dde_complaints_cust_reltns"
    DDE_SGB_FOS = ALIAS + ".dde_sgb_fos"
    DDE_HARDSHIP_IND = ALIAS + ".dde_hardship_ind"
    DDE_ARREARS_CODE = ALIAS + ".dde_arrears_code"
    DDE_VP_ASSIST_IND = ALIAS + ".dde_vp_assist_ind"
    DDE_LITIGATION_IND = ALIAS + ".dde_litigation_ind"
    DDE_LITIGATION = ALIAS + ".dde_litigation"
    RDE_LITIGATION = ALIAS + ".litign_ind_rams"
    ACCT_BRND = "acct_brnd"
    STATE_CD = "state_cd"
    RDE_LIT_START_DT = ALIAS + ".lit_st_dt"
    RDE_EPISODE_START = ALIAS + ".episode_start"
    RDE_EPISODE_END = ALIAS + ".episode_end"
    RDE_FIN_RLF_ARMT_SUBTYPE_CD = ALIAS + ".fin_rlf_armt_subtype_cd"
    RDE_FIN_RLF_ARMT_STAT_CD = ALIAS + ".fin_rlf_armt_stat_cd"
    RDE_STAT_CD = ALIAS + ".recov_stat_cd"
    STAT_DESC = ALIAS + ".stat_desc"
    SECRTY_IN_POSSN_STAT_TYPE_SRC_CD = ALIAS + ".secrty_in_possn_stat_type_src_cd"
    TCS_LIT_START_DT = ALIAS + ".litign_st_dt"
    B2K_LIT_START_DT = ALIAS + ".litign_st_dt"
    FIN_RLF_ARMT_STAT_TYPE_CD = ALIAS + ".fin_rlf_armt_stat_type_cd"
    WORKLIST_WORKFLOW_SK = ALIAS + ".wrkflw_sk"
    DDE_ALT_STRATEGY_IND = ALIAS + ".dde_alt_strategy_ind"
    DDE_APRA = ALIAS + ".dde_apra"



class HarmRef(Enum):
    ALIAS = "haref"
    ATTRIBUTE_NAME = ALIAS + ".attribute_name"
    SOURCE_APPID = ALIAS + ".source_appid"
    HARMONIZED_CODE = ALIAS + ".harmonized_code"
    SOURCE_CODE = ALIAS + ".source_code"
    REF_TYPE_CD = ALIAS + ".REF_TYPE_CD"
    REF_TYPE_SRC_CD = ALIAS + ".REF_TYPE_SRC_CD"
    IT_APPLN_KEY = ALIAS + ".IT_APPLN_RSRC_KEY"
    REF_ENTITY_NM = ALIAS + ".REF_ENTITY_NM"
    REF_TYPE_SRC_DESC = ALIAS + ".REF_TYPE_SRC_DESC"
    ACTIVE_IND = ALIAS + ".ACTIVE_IND"


class RecovOutcmType(Enum):
    ALIAS = "recout"
    RECOV_OUTCM_TYPE_CD = ALIAS + ".recov_outcm_type_cd"
    WOFF = "WOFF"
    LTG_SHORTFALL = "LTG SHORTFALL"
    WOF2 = "WOF2"
    LIT_SHORTFALL = "LIT-SHORTFALL-STAGE2"


class RecovType(Enum):
    ALIAS = "rectype"
    RECOV_TYPE_CD = ALIAS + ".recov_type_cd"


class TempListclectsepsd(Enum):
    DDE_ACCT_ID = "dde_acct_id"
    DDE_ARREARS_REASON = "dde_arrears_reason"
    DDE_AUTO_LTR_DAYS = "dde_auto_ltr_days"
    DDE_DTE_COLCTED = "dde_dte_colcted"
    DDE_CURR_DT_ADD = "dde_curr_dt_add"
    DDE_OL_WRITEOFF_IND = "dde_ol_writeoff_ind"
    DDE_OL_LRD_IND = "dde_ol_lrd_ind"
    DDE_SUPPRESS_LTR = "dde_suppress_ltr"
    DDE_SUPV_SUPR_DAYS = "dde_supv_supr_days"
    DDE_RECOVERY_STAT_CODE = "dde_recovery_stat_code"
    DDE_SCO_PENDING = "dde_sco_pending"
    DDE_COLL_ENTRY_REAS = "dde_coll_entry_reas"
    DDE_REC_STAT = "dde_rec_stat"
    DDE_21D3_LIST_AMNT = "dde_21d3_list_amnt"
    DDE_HARDSHIP_INFO_EXP_DT = "dde_hardship_info_exp_dt"
    DDE_HARDSHIP_LVR = "dde_hardship_lvr"
    DDE_PROM_SCHED_WK_IND = "dde_prom_sched_wk_ind"
    DDE_PROM_STAT = "dde_prom_stat"
    DDE_PROM_AMT = "dde_prom_amt"
    DDE_PROM_DTE = "dde_prom_dte"
    DDE_DTE_LAST_REVW_DT = "dde_dte_last_revw_dt"
    DDE_AMT_TOWARDS_PROM = "dde_amt_towards_prom"
    DDE_ARREARS_CODE = "dde_arrears_code"
    DDE_PROM_SCHEDULE = "dde_prom_schedule"
    DDE_ASSISTANCE_LEVEL = "dde_assistance_level"
    DDE_COMPLAINTS_CUST_RELTNS = "dde_complaints_cust_reltns"
    DDE_SGB_FOS = "dde_sgb_fos"
    DDE_COMMERCIAL = "dde_commercial"
    DDE_CUST_ID = "dde_cust_id"
    DDE_LITIGATION = "dde_litigation"
    DDE_APRA = "dde_apra"


class TempCredclectsepsd(Enum):
    DDE_ACCT_ID_CRED = "dde_acct_id"
    DDE_ARREARS_REASON_CRED = "dde_arrears_reason"
    DDE_AUTO_LTR_DAYS_CRED = "dde_auto_ltr_days"
    DDE_DTE_COLCTED_CRED = "dde_dte_colcted"
    DDE_CURR_DT_ADD_CRED = "dde_curr_dt_add"
    DDE_OL_WRITEOFF_IND_CRED = "dde_ol_writeoff_ind"
    DDE_OL_LRD_IND_CRED = "dde_ol_lrd_ind"
    DDE_SUPPRESS_LTR_CRED = "dde_suppress_ltr"
    DDE_SUPV_SUPR_DAYS_CRED = "dde_supv_supr_days"
    DDE_RECOVERY_STAT_CODE_CRED = "dde_recovery_stat_code"
    DDE_SCO_PENDING_CRED = "dde_sco_pending"
    DDE_COLL_ENTRY_REAS_CRED = "dde_coll_entry_reas"
    DDE_REC_STAT_CRED = "dde_rec_stat"
    DDE_21D3_LIST_AMNT_CRED = "dde_21d3_list_amnt"
    DDE_HARDSHIP_INFO_EXP_DT_CRED = "dde_hardship_info_exp_dt"
    DDE_HARDSHIP_LVR_CRED = "dde_hardship_lvr"
    DDE_PROM_SCHED_WK_IND_CRED = "dde_prom_sched_wk_ind"
    DDE_PROM_STAT_CRED = "dde_prom_stat"
    DDE_PROM_AMT_CRED = "dde_prom_amt"
    DDE_PROM_DTE_CRED = "dde_prom_dte"
    DDE_DTE_LAST_REVW_DT_CRED = "dde_dte_last_revw_dt"
    DDE_AMT_TOWARDS_PROM_CRED = "dde_amt_towards_prom"
    DDE_VP_ASSIST_IND_CRED = "dde_vp_assist_ind"
    DDE_PROM_SCHEDULE_CRED = "dde_prom_schedule"
    DDE_ASSISTANCE_LEVEL_CRED = "dde_assistance_level"
    DDE_COMPLAINTS_CUST_RELTNS_CRED = "dde_complaints_cust_reltns"
    DDE_SGB_FOS_CRED = "dde_sgb_fos"
    DDE_COMMERCIAL_CRED = "dde_commercial"
    DDE_DUE_DATE_CRED = "dde_due_date"
    DDE_CUST_ID_CRED = "dde_cust_id"
    DDE_LITIGATION_CRED = "dde_litigation"
    DDE_APRA_CRED = "dde_apra"


class TempDdaclectsepsd(Enum):
    DDE_ACCT_ID_DDA = "dde_acct_id"
    DDE_ARREARS_REASON_DDA = "dde_arrears_reason"
    DDE_AUTO_LTR_DAYS_DDA = "dde_auto_ltr_days"
    DDE_DTE_COLCTED_DDA = "dde_dte_colcted"
    DDE_CURR_DT_ADD_DDA = "dde_curr_dt_add"
    DDE_OL_WRITEOFF_IND_DDA = "dde_ol_writeoff_ind"
    DDE_OL_LRD_IND_DDA = "dde_ol_lrd_ind"
    DDE_SUPPRESS_LTR_DDA = "dde_suppress_ltr"
    DDE_SUPV_SUPR_DAYS_DDA = "dde_supv_supr_days"
    DDE_SCO_PENDING_DDA = "dde_sco_pending"
    DDE_COLL_ENTRY_REAS_DDA = "dde_coll_entry_reas"
    DDE_REC_STAT_DDA = "dde_rec_stat"
    DDE_21D3_LIST_AMNT_DDA = "dde_21d3_list_amnt"
    DDE_HARDSHIP_INFO_EXP_DT_DDA = "dde_hardship_info_exp_dt"
    DDE_HARDSHIP_LVR_DDA = "dde_hardship_lvr"
    DDE_PROM_SCHED_WK_IND_DDA = "dde_prom_sched_wk_ind"
    DDE_PROM_STAT_DDA = "dde_prom_stat"
    DDE_PROM_AMT_DDA = "dde_prom_amt"
    DDE_PROM_DTE_DDA = "dde_prom_dte"
    DDE_DTE_LAST_REVW_DT_DDA = "dde_dte_last_revw_dt"
    DDE_AMT_TOWARDS_PROM_DDA = "dde_amt_towards_prom"
    DDE_ARREARS_CODE_DDA = "dde_arrears_code"
    DDE_PROM_SCHEDULE_DDA = "dde_prom_schedule"
    DDE_ASSISTANCE_LEVEL_DDA = "dde_assistance_level"
    DDE_COMPLAINTS_CUST_RELTNS_DDA = "dde_complaints_cust_reltns"
    DDE_SGB_FOS_DDA = "dde_sgb_fos"
    DDE_COMMERCIAL_DDA = "dde_commercial"
    DDE_DUE_DATE_DDA = "dde_due_date"
    DDE_CUST_ID_DDA = "dde_cust_id"
    DDE_RECOVERY_STAT_CODE_DDA = "dde_recovery_stat_code"
    DDE_LITIGATION_DDA = "dde_litigation"
    DDE_APRA_DDA = "dde_apra"


class Tempb2kclectsepsd(Enum):
    DDE_ACCT_ID_B2K = "dde_acct_id"
    DDE_AMT_TOWARDS_PROM_B2K = "dde_amt_towards_prom"
    DDE_AUTO_LTR_DAYS_B2K = "dde_auto_ltr_days"
    DDE_COLL_ENTRY_REAS_B2K = "dde_coll_entry_reas"
    DDE_CURR_DT_ADD_B2K = "dde_curr_dt_add"
    DDE_DTE_COLCTED_B2K = "dde_dte_colcted"
    DDE_DTE_LAST_REVW_DT_B2K = "dde_dte_last_revw_dt"
    DDE_HARDSHIP_LVR_B2K = "dde_hardship_lvr"
    DDE_OL_LRD_IND_B2K = "dde_ol_lrd_ind"
    DDE_PROM_AMT_B2K = "dde_prom_amt"
    DDE_PROM_DTE_B2K = "dde_prom_dte"
    DDE_PROM_STAT_B2K = "dde_prom_stat"
    DDE_REC_STAT_B2K = "dde_rec_stat"
    DDE_SUPV_SUPR_DAYS_B2K = "dde_supv_supr_days"
    DDE_FINANCIAL_SOLUTION_B2K = "dde_financial_solution"
    DDE_IL_RECOVERY_AMOUNT_B2K = "dde_il_recovery_amount"
    DDE_IL_RECOVERY_OUTCOME_B2K = "dde_il_recovery_outcome"
    DDE_HARDSHIP_EXP_DATE_B2K = "dde_hardship_exp_date"
    DDE_HARDSHIP_IND_B2K = "dde_hardship_ind"
    DDE_MI_ARRS_REASON_B2K = "dde_mi_arrs_reason"
    DDE_LETR_WARNING_DATE_B2K = "dde_letr_warning_date"
    DDE_NO_PHONE_CONTACT_B2K = "dde_no_phone_contact"
    DDE_ORIG_PROD_SYS_B2K = "dde_orig_prod_sys"
    DDE_OUTSOURCING_CODE_B2K = "dde_outsourcing_code"
    DDE_PROM_SCHEDULE_B2K = "dde_prom_schedule"
    DDE_RECOVERY_CODE_B2K = "dde_recovery_code"
    DDE_SOFT_CHARGEOFF_IND_B2K = "dde_soft_chargeoff_ind"
    DDE_SUPRES_AUTO_LTR_IND_B2K = "dde_supres_auto_ltr_ind"
    DDE_DEFER_WRITE_OFF_IND_B2K = "dde_defer_write_off_ind"
    DDE_BANK_ERR_PRESENT_B2K = "dde_bank_err_present"
    DDE_COMMERCIALLY_AVAILABLE_B2K = "dde_commercially_available"
    DDE_NEXT_DUE_DATE_B2K = "dde_next_due_date"
    DDE_CUST_ID_B2K = "dde_cust_id"
    DDE_LITIGATION_IND_B2K = "dde_litigation_ind"
    DDE_ALT_STRATEGY_IND_B2K = "dde_alt_strategy_ind"
    


class FinSolType(Enum):
    Y = "Y"
    C = "C"
    ASSIST_APPLICATION = "ASSIST APPLICATION"
    M = "M"
    ASSIST_MONITOR = "ASSIST MONITOR"
    A = "A"
    FS_APPROVED = "FS APPROVED"
    BLANK = ""
    N = "N"
    NO_ASSIST = "NO ASSIST"
    X = "X"
    ASSIST_WRITEOFF = "ASSIST WRITEOFF"
    W = "W"
    ASSIST_SOFT_CHARGE_OFF = "ASSIST SOFT CHARGE-OFF"
    A0 = "0"
    ASSISTANCE_LEVEL = "ASSISTANCE LEVEL"
    DC = "DC"
    FS_DECEASED = "FS DECEASED"
    DS = "DS"
    DISASTER_SHORTFALL = "DISASTER SHORTFALL"


class ClectEntry:
    ALIAS = "ce"
    harmonised_code = "harmonized_code"
    harmonised_desc = "harmonized_desc"
    entity_name = "entity_name"


class RefDataAppln:
    ALIAS = "rda"
    APP_ID = "app_id"
    ENTITY_NAME = "entity_name"
    SRC_CODE = "src_code"
    SRC_DESC = "src_desc"
    HARMONIZED_CODE = "harmonized_code"
    HARMONIZED_DESC = "harmonized_desc"


class RefDataApplnMap:
    ALIAS = "ce"
    HARMONIZED_CODE = "harmonized_code"
    HARMONIZED_DESC = "harmonized_desc"
    entity_name = "entity_name"


class FinRlfArmtSubtype(Enum):
    FIN_RLF_ARMT_SUBTYPE_CD_1 = "Online Session"
    FIN_RLF_ARMT_SUBTYPE_DESC_1 = "Online Session"
    FIN_RLF_ARMT_SUBTYPE_CD_2 = "Clickstream Interaction"
    FIN_RLF_ARMT_SUBTYPE_DESC_2 = "Clickstream Interaction"
    FIN_RLF_ARMT_SUBTYPE_CD_3 = "Digital Notification"
    FIN_RLF_ARMT_SUBTYPE_DESC_3 = "Digital Notification"
    FIN_RLF_ARMT_SUBTYPE_CD_4 = "Customer Action"
    FIN_RLF_ARMT_SUBTYPE_DESC_4 = "Customer Action"


class FinRlfArmt(Enum):
    FDP_CLECT_EPSD_CASE_SK = "clect_epsd_case_sk"
    FDP_FIN_RLF_ST_DT = "fin_rlf_st_dt"
    FDP_FIN_RLF_END = "fin_rlf_end_dt"


class TemphostidKmrkey(Enum):
    CASE_SK = "case_sk"
    CLECT_EPSD_HOST_ID = "clect_epsd_host_id"
    EPISODE_START = "episode_start"
    EPISODE_END = "episode_end"
    ARMT_SK = "armt_sk"
    WRKFLW_SK = "wrkflw_sk"
    FIN_RLF_ARMT_HOST_ID = "fin_rlf_armt_host_id"
    LEND_ARMT_SK = "lend_sk_a002f4"
    LEND_ARMT_HOST_ID = "lend_armt_host_id"
    DDE_FIN_RLF_ST_DT = "dde_fin_rlf_st_dt"
    FIN_RLF_END_DT = "fin_rlf_end_dt"
    LEND_ARMT_SK_LIS = "lend_sk_a002fb"
    LEND_ARMT_SK_CRD = "lend_sk_a001fc"
    LEND_ARMT_SK_B2K_ICB = "lend_armt_sk_a0010e"
    LEND_ARMT_SK_B2K_INF = "lend_armt_sk_a0018e"
    LEND_ARMT_SK_B2K_MSS = "lend_armt_sk_a0019b"
    LEND_ARMT_SK_B2K_TBK = "lend_armt_sk_a00004"
    PARTY_SK = "party_sk"
    DDE_ORIG_PROD_SYS = "dde_orig_prod_sys"
    LEND_ARMT_SK_RDE = "lend_armt_sk_a00845"
    CLECT_EPSD_LEND_HOST_ID = "clect_epsd_lend_host_id"
    FIN_RLF_ST_DT = "fin_rlf_st_dt"
    SRC_BDP_PARTITION = "src_bdp_partition"

class TempwrkflwidKmrkey(Enum):
    WRKFLW_SK = "wrkflw_sk"
    ID = "id"
    NAME = "name"
    DESCRIPTION = "description"

class Worklist(Enum):
    ALIAS = "worklistrde"
    WRKFLW_SK = ALIAS + "." + "wrkflw_sk"
    WRKLIST_NM = ALIAS + "." + "wrklist_nm"
    WRKLIST_DESC = ALIAS + "." + "wrklist_desc"

class WorklistPcc(Enum):
    ALIAS = "worklistpcc"
    WRKFLW_SK_PCC = ALIAS + "." + "wrkflw_sk_pcc"
    WRKLIST_NM = ALIAS + "." + "wrklist_nm"
    WRKLIST_DESC = ALIAS + "." + "wrklist_desc"

class PrmsToPay(Enum):
    ALIAS = "ptp"
    DDE_PROM_AMT = ALIAS + ".dde_prom_amt"
    DDE_PROM_DTE = ALIAS + ".dde_prom_dte"
    DDE_DTE_LAST_REVW_DT = ALIAS + ".dde_dte_last_revw_dt"
    DDE_AMT_TOWARDS_PROM = ALIAS + ".dde_amt_towards_prom"
    DDE_PROM_STAT = ALIAS + ".dde_prom_stat"
    DDE_PROM_SCHEDULE = ALIAS + ".dde_prom_schedule"
    DDE_ORIG_PROD_SYS = ALIAS + "." + CommonAttributes.DDE_ORIG_PROD_SYS.value
    PROMISE_AMOUNT_CURRENCY_CODE = ALIAS + ".AUD"
    DDE_ACCT_ID = ALIAS + ".dde_acct_id"
    DDE_CURR_DT_ADD = ALIAS + ".dde_curr_dt_add"
    JURIS_CNTRY_CD = ALIAS + ".AUS"
    DDE_PROM_SCHED_WK_IND = ALIAS + ".dde_prom_sched_wk_ind"
    CASE_KMR_KEY = ALIAS + ".case_kmr_key"
    CLECT_EPSD_HOST_ID_PTP = ALIAS + "." + CLECT_EPSD_HOST_ID_ALL
    BDP_YEAR = ALIAS + "." + CommonAttributes.BDP_YEAR.value
    BDP_MONTH = ALIAS + "." + CommonAttributes.BDP_MONTH.value
    BDP_DAY = ALIAS + "." + CommonAttributes.BDP_DAY.value
    DDE_DTE_COLCTED = ALIAS + "." + CommonAttributes.DDE_DTE_COLCTED.value
    SRC_BDP_PARTITION = ALIAS + "." + CommonAttributes.SRC_BDP_PARTITION.value


class TmpPreprocessCrdclctepsd(Enum):
    CLECT_EPSD_HOST_ID_CRD = "clect_epsd_host_id"
    FIN_RLF_ARMT_HOST_ID_CRD = "fin_rlf_armt_host_id"
    DDE_FIN_RLF_ST_DT_CRD = "dde_fin_rlf_st_dt"
    FIN_RLF_ARMT_STAT_CD = "fin_rlf_armt_stat_cd"
    FIN_RLF_END_DT_CRD = "fin_rlf_end_dt"
    DDE_ARREARS_REASON_CRED = "dde_arrears_reason"
    DDE_HARDSHIP_LVR_CRED = "dde_hardship_lvr"
    DDE_HARDSHIP_INFO_EXP_DT_CRED = "dde_hardship_info_exp_dt"
    DDE_VP_ASSIST_IND_CRED = "dde_vp_assist_ind"
    DDE_COMMERCIAL_CRED = "dde_commercial"
    DDE_DUE_DATE_CRED = "dde_due_date"
    SRC_BDP_PARTITION_CRD = "src_bdp_partition"


class TmpPreprocessB2kclctepsd(Enum):
    CLECT_EPSD_HOST_ID_B2K = "clect_epsd_host_id"
    FIN_RLF_ARMT_HOST_ID_B2K = "fin_rlf_armt_host_id"
    DDE_FIN_RLF_ST_DT_B2K = "dde_fin_rlf_st_dt"
    FIN_RLF_ARMT_STAT_CD = "fin_rlf_armt_stat_cd"
    FIN_RLF_END_DT_B2K = "fin_rlf_end_dt"
    DDE_HARDSHIP_LVR_B2K = "dde_hardship_lvr"
    DDE_HARDSHIP_INFO_EXP_DT_B2K = "dde_hardship_info_exp_dt"
    DDE_ORIG_PROD_SYS_B2K = "dde_orig_prod_sys"
    DDE_MI_ARRS_REASON_B2K = "dde_mi_arrs_reason"
    DDE_HARDSHIP_EXP_DATE_B2K = "dde_hardship_exp_date"
    DDE_HARDSHIP_IND_B2K = "dde_hardship_ind"
    DDE_OL_LRD_IND_B2K = "dde_ol_lrd_ind"
    DDE_COMMERCIALLY_AVAILABLE_PRESENT_B2K = "dde_commercially_available_present"
    DDE_NEXT_DUE_DATE_B2K = "dde_next_due_date"
    SRC_BDP_PARTITION = "src_bdp_partition"


class TmpPreprocessDdaclctepsd(Enum):
    CLECT_EPSD_HOST_ID_DDA = "clect_epsd_host_id"
    FIN_RLF_ARMT_HOST_ID_DDA = "fin_rlf_armt_host_id"
    DDE_FIN_RLF_ST_DT_DDA = "dde_fin_rlf_st_dt"
    FIN_RLF_END_DT_DDA = "fin_rlf_end_dt"
    FIN_RLF_ARMT_STAT_CD = "fin_rlf_armt_stat_cd"
    DDE_ARREARS_REASON_DDA = "dde_arrears_reason"
    DDE_HARDSHIP_LVR_DDA = "dde_hardship_lvr"
    DDE_HARDSHIP_INFO_EXP_DT_DDA = "dde_hardship_info_exp_dt"
    DDE_ARREARS_CODE_DDA = "dde_arrears_code"
    DDE_COMMERCIAL_DDA = "dde_commercial"
    DDE_DUE_DATE_DDA = "dde_due_date"
    SRC_BDP_PARTITION_DDA = "src_bdp_partition"


class TmpPreprocessLisclctepsd(Enum):
    CLECT_EPSD_HOST_ID_LIS = "clect_epsd_host_id"
    FIN_RLF_ARMT_HOST_ID_LIS = "fin_rlf_armt_host_id"
    FIN_RLF_ARMT_STAT_CD = "fin_rlf_armt_stat_cd"
    DDE_FIN_RLF_ST_DT_LIS = "dde_fin_rlf_st_dt"
    FIN_RLF_END_DT_LIS = "fin_rlf_end_dt"
    DDE_ARREARS_REASON_LIS = "dde_arrears_reason"
    DDE_HARDSHIP_LVR_LIS = "dde_hardship_lvr"
    DDE_HARDSHIP_INFO_EXP_DT_LIS = "dde_hardship_info_exp_dt"
    DDE_ARREARS_CODE_LIS = "dde_arrears_code"
    DDE_COMMERCIAL_LIS = "dde_commercial"
    DDE_PAYMENT_DUE_DATE_LIS = "dde_payment_due_date"
    SRC_BDP_PARTITION_LIS = "src_bdp_partition"


class KmrHostidKeys(Enum):
    CASE_SK = "case_sk"
    ARMT_SK = "armt_sk"
    LEND_ARMT_SK = "lend_armt_sk"
    CLECT_EPSD_HOST_ID = "clect_epsd_host_id"


class TempHostId(Enum):
    ALIAS = "thi"
    CLECT_EPSD_HOST_ID_TEMP = ALIAS + "." + CLECT_EPSD_HOST_ID_ALL
    LEND_ARMT_SK = ALIAS + ".lend_armt_sk"
    ARMT_SK = ALIAS + ".armt_sk"
    CASE_SK = ALIAS + ".case_sk"
    PARTY_SK = ALIAS + ".party_sk"
    FIN_RLF_ST_DT = ALIAS + ".fin_rlf_st_dt"
    SRC_BDP_PARTITION = ALIAS + ".src_bdp_partition"
    wrkflw_sk = ALIAS + ".wrkflw_sk"


class DdepSCDColumns(Enum):
    JURIS_CNTRY_CD = "juris_cntry_cd"
    SRC_SYS_APPLN_ID = "src_sys_appln_id"
    ACTIVE_IND = "active_ind"
    ST_DT = "st_dt"
    END_DT = "end_dt"
    DDEP_RCRD_DELET_IND = "ddep_rcrd_delet_ind"
    DDEP_HASH = "ddep_hash"
    DDEP_ST_TS = "ddep_st_ts"
    DDEP_END_TS = "ddep_end_ts"


class ActivityAcraamaa(Enum):
    DDE_ACCT_ID = "dde_acct_id"
    DDE_CMP_DATE = "dde_cmp_date"
    DDE_CMP_TIME = "dde_cmp_time"
    DDE_ACTY_CD = "dde_acty_cd"


class MIPB2K(Enum):
    ACCT_BRND = "acct_brnd"
    ACCT_NUM = "acct_num"
    STAT_DESC = "stat_desc"
    PROPTY_ACQUISITION_DT = "propty_acquisition_dt"
    DATE_OF_SALE = "date_of_sale"
    ACC_ID = "acc_id"
    SECURITY_KEY = "security_key"


class MIPTCS(Enum):
    ACCT_BRND_TCS = MIPB2K.ACCT_BRND.value
    ACCT_NUM_TCS = MIPB2K.ACCT_NUM.value
    STAT_DESC_TCS = MIPB2K.STAT_DESC.value
    PROPTY_ACQUISITION_DT_TCS = MIPB2K.PROPTY_ACQUISITION_DT.value
    DATE_OF_SALE_TCS = MIPB2K.DATE_OF_SALE.value
    ACC_ID_TCS = MIPB2K.ACC_ID.value


class TempMipRecov(Enum):
    ALIAS = "miprecov"
    ACCT_BRND = ALIAS + ".acct_brnd"
    ACCT_NUM = ALIAS + ".acct_num"
    STAT_DESC = ALIAS + ".stat_desc"
    PROPTY_ACQUISITION_DT = ALIAS + ".propty_acquisition_dt"
    DATE_OF_SALE = ALIAS + ".date_of_sale"
    TITLE_REFERENCE = ALIAS + ".title_reference"
    SALE_SETLMT_DT = ALIAS + ".sale_setlmt_dt"
    SALE_AMT = ALIAS + ".sale_amt"
    SALE_AMT_CRNCY_CD = ALIAS + ".sale_amt_crncy_cd"
    LST_VALN_DT = ALIAS + ".lst_valn_dt"
    CURNT_LOW_VALN_AMT = ALIAS + ".curnt_low_valn_amt"
    SECRTY_VALN_CRNCY_CD = ALIAS + ".secrty_valn_crncy_cd"
    STATE_CD = ALIAS + ".state_cd"
    SECURITY_KEY = ALIAS + ".security_key"
    STREET_ADDR = ALIAS + ".street_addr"


class HardShipDate(Enum):
    ALIAS = "hardship"
    ACCT_NUM_A002F4 = ALIAS + ".acct_num"
    NEXT_REPAY_DUE_DATE_A002F4 = ALIAS + ".next_repay_due_date"
    BDP_YEAR_A002F4 = ALIAS + "." + CommonAttributes.BDP_YEAR.value
    BDP_MONTH_A002F4 = ALIAS + "." + CommonAttributes.BDP_MONTH.value
    BDP_DAY_A002F4 = ALIAS + "." + CommonAttributes.BDP_DAY.value


class ClectEpsdCustRole(Enum):
    ALIAS = "ptp"
    CASE_KMR_KEY = ALIAS + ".case_kmr_key"
    CLECT_EPSD_HOST_ID_PTP = ALIAS + "." + CLECT_EPSD_HOST_ID_ALL
    DDE_ORIG_PROD_SYS = ALIAS + "." + CommonAttributes.DDE_ORIG_PROD_SYS.value
    BDP_YEAR = ALIAS + "." + CommonAttributes.BDP_YEAR.value
    BDP_MONTH = ALIAS + "." + CommonAttributes.BDP_MONTH.value
    BDP_DAY = ALIAS + "." + CommonAttributes.BDP_DAY.value
    DDE_DTE_COLCTED = ALIAS + "." + CommonAttributes.DDE_DTE_COLCTED.value
    SRC_BDP_PARTITION = ALIAS + "." + CommonAttributes.SRC_BDP_PARTITION.value


class PublishRamsHardship(Enum):
    HIST_START_DATE = "2022-01-01"
    COLS_TO_DROP = [
        "bdp_sor_extracttimestamp",
        "bdp_actiontype",
        "bdp_processcode",
        "bdp_processmessage",
        "bdp_createtimestamp",
        "bdp_rowstatus",
        "bdp_updatetimestamp",
        "bdp_partition",
    ]


class MssSecurityLoanRltsp(Enum):
    DEBT_ACCOUNT_NUMBER = "debt_account_number"
    SECURITY_NUMBER = "security_number"


class MssTitle(Enum):
    SECURITY_NUMBER = "security_number"
    TITLE_VOLUME = "title_volume"
    TITLE_FOLIO = "title_folio"
    TITLE_REFERENCE = "title_reference"
    TITLE_FOLIO_IDENTIFICATION = "title_folio_identification"


class TmpSecuritySale(Enum):
    TRN = "trn"
    VTRN = "vtrn"
    MOD_DEBT_ACCOUNT_NUMBER = "mod_debt_account_number"
    SECURITY_NUMBER = "security_number"
    PROPTY_ACQUISITION_DT = "propty_acquisition_dt"
    TITLE_REFERENCE = "title_reference"
    DDE_ACCT_ID_B2K = "dde_acct_id"
    DDE_DTE_COLCTED_B2K = "dde_dte_colcted"
    ACCT_NUM = "acct_num"


class A002f4SecMast(Enum):
    LOAN_ACCT_NUM = "loan_acct_num"
    VOLUME_NUM = "volume_num"
    FOLIO_NUM = "folio_num"
    FOLIO_IDENTIFIER = "folio_identifier"
    COMPANY_NUM = "company_num"
    SEQUENCE_NUM = "sequence_num"


class A002f4AcctLoanSecur(Enum):
    ACCT_NUM = "acct_num"
    SECURITY_VOLUME_NUM = "security_volume_num"
    SECURITY_FOLIO_NUM = "security_folio_num"
    SECURITY_FOLIO_ID = "security_folio_id"


class TempMipDda(Enum):
    ACCT_BRND = TempMipRecov.ACCT_BRND.value
    ACCT_NUM = TempMipRecov.ACCT_NUM.value
    STAT_DESC = TempMipRecov.STAT_DESC.value
    PROPTY_ACQUISITION_DT = TempMipRecov.PROPTY_ACQUISITION_DT.value
    DATE_OF_SALE = TempMipRecov.DATE_OF_SALE.value
    TITLE_REFERENCE = TempMipRecov.TITLE_REFERENCE.value
    SALE_SETLMT_DT = TempMipRecov.SALE_SETLMT_DT.value
    SALE_AMT = TempMipRecov.SALE_AMT.value
    SALE_AMT_CRNCY_CD = TempMipRecov.SALE_AMT_CRNCY_CD.value
    LST_VALN_DT = TempMipRecov.LST_VALN_DT.value
    CURNT_LOW_VALN_AMT = TempMipRecov.CURNT_LOW_VALN_AMT.value
    SECRTY_VALN_CRNCY_CD = TempMipRecov.SECRTY_VALN_CRNCY_CD.value
    STATE_CD = TempMipRecov.STATE_CD.value
    SECURITY_KEY = TempMipRecov.SECURITY_KEY.value


class TempMipLis(Enum):
    ACCT_BRND = TempMipRecov.ACCT_BRND.value
    ACCT_NUM = TempMipRecov.ACCT_NUM.value
    STAT_DESC = TempMipRecov.STAT_DESC.value
    PROPTY_ACQUISITION_DT = TempMipRecov.PROPTY_ACQUISITION_DT.value
    DATE_OF_SALE = TempMipRecov.DATE_OF_SALE.value
    TITLE_REFERENCE = TempMipRecov.TITLE_REFERENCE.value
    SALE_SETLMT_DT = TempMipRecov.SALE_SETLMT_DT.value
    SALE_AMT = TempMipRecov.SALE_AMT.value
    SALE_AMT_CRNCY_CD = TempMipRecov.SALE_AMT_CRNCY_CD.value
    LST_VALN_DT = TempMipRecov.LST_VALN_DT.value
    CURNT_LOW_VALN_AMT = TempMipRecov.CURNT_LOW_VALN_AMT.value
    SECRTY_VALN_CRNCY_CD = TempMipRecov.SECRTY_VALN_CRNCY_CD.value
    STATE_CD = TempMipRecov.STATE_CD.value
    SECURITY_KEY = TempMipRecov.SECURITY_KEY.value


class RdeCsAccount(Enum):
    ACCOUNTS1 = "accounts1"
    REFERENCE = "reference"
    TREATMENT_CD = "treatment_code"


class RdeAcctActivities(Enum):
    ACCOUNTID = "accountid"
    DATETIME = "datetime"
    CHILDID = "childid"


class RdeAccounts(Enum):
    ID = "id"
    WORKLISTID = "worklistid"


class RdeHardship(Enum):
    ACCOUNTS1 = "accounts1"
    HARDSHIP_REASON = "hardship_reason"
    TREATMENT_STATUS = "treatment_status"
    DISASTER_FLAG = "disaster_flag"
    STATUS = "status"
    APPROVE_DATE = "approve_date"
    SERVICEABILITYENDDT = "serviceabilityenddt"
    ASSISTANCE_END_DATE = "assistance_end_date"
    STATUS_UPDATE_DATE = "status_update_date"
    ARREARSTOCAP_CODE = "arrearstocap_code"
    NONCOMMERCIAL = "noncommercial"


class RdeSecurity(Enum):
    ALIAS = "rdesecurity"
    SECRTYNUM = ALIAS + ".secrtynum"
    SECURITYSKEY = ALIAS + "." + "securityskey"


class RdeTitleDeedDetail(Enum):
    ALIAS = "rdetitledeeddetail"
    TITLDETAILS = ALIAS + ".titldetails"
    SECURITYSKEY = ALIAS + "." + "securityskey"


class RdeLoanSecurity(Enum):
    ALIAS = "rdeloansecurity"
    LOANID = ALIAS + ".loanid"
    SECURITYSKEY = ALIAS + "." + "securityskey"


class RdeLoanVariation(Enum):
    ALIAS = "rdeloanvariation"
    ARMTIDENT = ALIAS + ".armtident"
    SOLACELOANNUM = ALIAS + ".solaceloannum"


class RdeAccountBalance(Enum):
    ALIAS = "rdeaccountbalance"
    CURRENT_LOAN = ALIAS + ".current_loan"
    ACCOUNTSKEY = ALIAS + "." + "accountskey"
    ACTIVEFLAG = ALIAS + ".activeflag"


class RdeAccount(Enum):
    ALIAS = "rdeaccount"
    ACCOUNTSKEY = ALIAS + "." + "accountskey"
    ACCOUNTNUM = ALIAS + ".accountnum"


class RdeCustomerAccount(Enum):
    ALIAS = "rdecustomeraccount"
    ACCOUNTSKEY = ALIAS + "." + "accountskey"
    LINKCHECKDIGIT = ALIAS + ".linkcheckdigit"
    ACTIVEFLAG = ALIAS + ".activeflag"


class RdeCsAccounts(Enum):
    ALIAS = "rdecsaccounts"
    ACCOUNTS1 = ALIAS + ".accounts1"
    REFERENCE = ALIAS + ".reference"
    CLECT_EPSD_HOST_ID = ALIAS + ".clect_epsd_host_id"


class RdeTmpIntScrty(Enum):
    ALIAS = "rdetmpintscrty"
    TITLDETAILS = ALIAS + "." + "titldetails"
    LINKCHECKDIGIT = ALIAS + "." + "linkcheckdigit"
    SECRTYNUM = ALIAS + "." + "secrtynum"


class TmpPreprocessRdeclctepsd(Enum):
    ALIAS = "tmppreprocessrdeclctepsd"
    CLECT_EPSD_HOST_ID_RDE = "clect_epsd_host_id"
    FIN_RLF_ARMT_HOST_ID_RDE = "fin_rlf_armt_host_id"
    FIN_RLF_ST_DT_RDE = "fin_rlf_st_dt"
    FIN_RLF_END_DT_RDE = "fin_rlf_end_dt"
    DDE_MI_ARRS_REASON_RDE = "dde_mi_arrs_reason"
    DDE_HARDSHIP_IND_RDE = "dde_hardship_ind"
    NONCOMMERCIAL_RDE = "noncommercial"
    SRC_BDP_PARTITION_RDE = "src_bdp_partition"
    FIN_RLF_ARMT_SUBTYPE_CD = "fin_rlf_armt_subtype_cd"
    FIN_RLF_ARMT_STAT_CD = "fin_rlf_armt_stat_cd"
    FIN_RLF_ARMT_STAT_TYPE_CD = "fin_rlf_armt_stat_type_cd"


class TmpPreprocessPccclectepsd(Enum):
    ALIAS = "tmppreprocesspccclectepsd"
    CLECT_EPSD_HOST_ID_PCC = "clect_epsd_host_id"
    FIN_RLF_ARMT_HOST_ID_PCC = "fin_rlf_armt_host_id"
    FIN_RLF_ST_DT_PCC = "fin_rlf_st_dt"
    FIN_RLF_END_DT_PCC = "fin_rlf_end_dt"
    DDE_MI_ARRS_REASON_PCC = "dde_mi_arrs_reason"
    DDE_HARDSHIP_IND_PCC = "dde_hardship_ind"
    NONCOMMERCIAL_PCC = "noncommercial"
    COMMERCIALFLAG = "commercialflag"
    ASSISTSOLUTIONENDDATE = "assistsolutionenddate"
    SRC_BDP_PARTITION_PCC = "src_bdp_partition"
    FIN_RLF_ARMT_SUBTYPE_CD = "fin_rlf_armt_subtype_cd"
    FIN_RLF_ARMT_STAT_CD = "fin_rlf_armt_stat_cd"
    FIN_RLF_ARMT_STAT_TYPE_CD = "fin_rlf_armt_stat_type_cd"


class RecovSecrtySale(Enum):
    ALIAS = "recovsecrtysale"
    CASE_SK = ALIAS + "." + "case_sk"
    RSRC_SK_DDA = ALIAS + "." + "rsrc_sk_dda"
    RSRC_SK_LIS = ALIAS + "." + "rsrc_sk_lis"
    RSRC_SK_MSS = ALIAS + "." + "rsrc_sk_mss"
    RSRC_SK_RDE = ALIAS + "." + "rsrc_sk_rde"

class SecrtySale(Enum):
    ALIAS = "secrtysale"
    SKRTY_SK_DDA = ALIAS + "." + "rsrc_sk_dda"
    SKRTY_SK_LIS = ALIAS + "." + "rsrc_sk_lis"
    RSRC_SK_MSS = ALIAS + "." + "rsrc_sk_mss"
    RSRC_SK_RDE = ALIAS + "." + "rsrc_sk_rde"
    DATE_OF_SALE = ALIAS + "." + "date_of_sale"
    SALE_SETLMT_DT = ALIAS + "." + "sale_setlmt_dt"
    PROPTY_ACQUISITION_DT = ALIAS + "." + "propty_acquisition_dt"
    SALE_AMT = ALIAS + "." + "sale_amt"
    SALE_AMT_CRNCY_CD = ALIAS + "." + "sale_amt_crncy_cd"
    LST_VALN_DT = ALIAS + "." + "lst_valn_dt"
    CURNT_LOW_VALN_AMT = ALIAS + "." + "curnt_low_valn_amt"
    STAT_DESC = ALIAS + "." + "stat_desc"
    SECRTY_VALN_CRNCY_CD = ALIAS + ".AUD"
    STAT_DESC = ALIAS + "." + "stat_desc"
    SECRTY_POSSN = ALIAS + ".secrty_in_possn_stat_type_src_cd"
    ACCT_BRND = ALIAS + ".acct_brnd"
    SECURITY_KEY = ALIAS + ".security_key"


class ErdData(Enum):
    ALIAS = "erddata"
    IT_APPLN_RSRC_KEY = ALIAS + "." + "it_appln_rsrc_key"
    SECRTY_IN_POSSN_STAT_TYPE_CD = ALIAS + "." + "secrty_in_possn_stat_type_cd"
    SECRTY_IN_POSSN_STAT_TYPE_SRC_CD = ALIAS + "." + "secrty_in_possn_stat_type_src_cd"
    FIN_RLF_ARMT_STAT_TYPE_CD = ALIAS + "." + "fin_rlf_armt_stat_type_cd"
    FIN_RLF_ARMT_STAT_TYPE_SRC_CD = ALIAS + "." + "fin_rlf_armt_stat_type_src_cd"


class CMTAccts(Enum):
    ALIAS = "cmtaccts"
    CASE_ID = ALIAS + "." + "case_id"
    ACCOUNT_ID = ALIAS + "." + "account_id"
    BRAND = ALIAS + "." + "brand"
    PRODUCT_TYPE = ALIAS + "." + "product_type"
    ASSIST_HARDSHIP_START_DT_1 = ALIAS + "." + "assist_hardship_start_dt_1"
    WORK_LIST_NAME = ALIAS + "." + "work_list_name"
    DATE_LAST_MOVED_WORKLIST = ALIAS + "." + "date_last_moved_worklist"
    ASSIST_REASON = ALIAS + "." + "assist_reason"
    BDP_YEAR = ALIAS + "." + "bdp_year"
    BDP_MONTH = ALIAS + "." + "bdp_month"
    BDP_DAY = ALIAS + "." + "bdp_day"

class TempCMTKmrKey(Enum):
    ARMT_SK = "armt_sk"
    ACCOUNT_ID = "account_id"
    LEND_ARMT_SK = "lend_armt_sk"
    BRAND = "brand"
    PRODUCT_TYPE = "product_type"
    ASSIST_HARDSHIP_START_DT_1 = "assist_hardship_start_dt_1"
    WORK_LIST_NAME = "work_list_name"
    DATE_LAST_MOVED_WORKLIST = "date_last_moved_worklist"
    ASSIST_REASON = "assist_reason"

class FinRlfArmtCMT(Enum):
    ARMT_SK = "armt_sk"
    LEND_ARMT_SK = "lend_armt_sk"
    CLECT_EPSD_CASE_SK = "clect_epsd_case_sk"
    FIN_RLF_ARMT_SUBTYPE_CD = "fin_rlf_armt_subtype_cd"
    OWNG_LEND_ARMT_APPLN_KEY = "owng_lend_armt_appln_key"
    FIN_RLF_ST_DT = "fin_rlf_st_dt"
    FIN_RLF_END_DT = "fin_rlf_end_dt"
    HARDSP_LOAN_VALN_RATIO = "hardsp_loan_valn_ratio"
    HARDSP_EXPIR_DT = "hardsp_expir_dt"
    FIN_RLF_ARMT_STAT_TYPE_CD = "fin_rlf_armt_stat_type_cd"
    COMRC_AVAIL_IND = "comrc_avail_ind"
    PRV_SYS_APPLN_ID = "prv_sys_appln_id"


class TempInflsPreKMR(Enum):
    INFLS_ACCT_ID = "infls_acct_id"
    ARMT_SK = "armt_sk"
    CLECT_EPSD_CASE_SK = "clect_epsd_case_sk"
    LEND_ARMT_SK = "lend_armt_sk"
    DER_ARMT_SK = "der_armt_sk"
    DER_CASE_SK = "der_case_sk"
    DER_LEND_ARMT_SK = "der_lend_armt_sk"
    FIN_RLF_ST_DT = "fin_rlf_st_dt"
    FIN_RLF_END_DT = "fin_rlf_end_dt"
    HARDSP_LOAN_VALN_RATIO = "hardsp_loan_valn_ratio"
    HARDSP_EXPIR_DT = "hardsp_expir_dt"
    COMRC_AVAIL_IND = "comrc_avail_ind"
    DDE_MI_ARRS_REASON = "dde_mi_arrs_reason"
    DDE_FINANCIAL_SOLUTION = "dde_financial_solution"
    DDE_REVIEW_OUTCOME_IND = "dde_review_outcome_ind"
	
class PccAccount(Enum):
    PRODUCT_TYPE = "product_type"
    ACCOUNTS1 = "accounts1"
