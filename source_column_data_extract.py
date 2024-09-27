import pandas as pd
import sqlparse
import re
from collections import OrderedDict

KEYWORDS = {
    "SUM", "MIN", "MAX", "AVG", "CASE", "WHEN", "AND",
    "THEN", "END", "INTEGER", "FLOOR", "CURRENT", "DATE"
}

query = """
SELECT  A.EMP_ID,          A.JOB_TYP_DSCR,         A.EMP_STS_TYP_CDE,         A.CURR_ROW_IND, A.TRMN_DT,         A.ADJ_SVC_DT,        /* TENURE */         (DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT) ) / 365.25 AS TENURE,         /* TENURE_GROUP */         (CASE WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 1 THEN 'NFR1'                WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 1 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 2 THEN 'NFR2'              WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 2 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 3 THEN 'NFR3'               WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 3 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 4 THEN 'NFR4'               WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 4 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 5 THEN 'VET5'               WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 5 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 6 THEN 'VET6'              WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 6 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 7 THEN 'VET7'                ELSE 'VET8+'          END) AS TENURE_GROUP              FROM HUMAN_RESOURCES.CNF_EMP_DIM_DTL_CURR_CFDL A    WHERE A.EMP_STS_TYP_CDE IN ('A','I','L','D','T','R')     AND A.CURR_ROW_IND = 'Y'               AND (A.JOB_FMLY_CDE LIKE 'FLD%' OR          A.JOB_FMLY_CDE ='SALES' OR          A.TF_JOB_CLAS_CDE ='FAC')     AND A.ORZN_DEPT_CDE NOT IN ('2410','3005','7151','7152',                                 '9150','9134','NA', '4100',                                 '6053','6100','8900')     AND (A.JOB_TYP_CDE IN ('002003','002010','002011','002012','002016','002017','002018','002019',                            '002000','002020','002021','002022','002024','002026','002027',                            '002030','002031','002032','003100','003500','003602',                            '003604','003605','003606','003607','003608') OR           A.ORZN_DEPT_CDE IN ('5405','5407','5408','5409','6100','1701','1702','1703'))    AND A.ADJ_SVC_DT IS NOT NULL     ORDER BY A.EMP_ID
""",

"""SELECT DISTINCT 
	T1.ORZN_DEPT_CDE AS RFO_CDE, 
	T1.ORZN_ZONE_CDE AS RFO_ZONE_NM, 
	(SUBSTRING(T1.ORZN_ZONE_CDE,6,5)) AS MARKET_ID, 
	T3.ORZN_DEPT_DSCR, 
	T3.EMP_ID, 
	T3.EMP_STS_TYP_CDE, 
	T3.TRMN_DT, 
	T3.JOB_TYP_CDE, 
	T3.JOB_TYP_DSCR, 
	T3.EMP_NM, 
	CASE WHEN T3.EMP_NM IS NULL THEN 'VACANT' ELSE T3.EMP_NM END AS MARKET_LEADERS 
FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN ( SELECT DISTINCT T1.SALE_HIER_ID, T1.ORZN_ZONE_CDE, T1.ORZN_DEPT_DSCR, T2.ORZN_DEPT_CDE, T2.EMP_ID, T2.EMP_STS_TYP_CDE, T2.TRMN_DT, T2.JOB_TYP_CDE, T2.JOB_TYP_DSCR, T2.EMP_NM FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN HUMAN_RESOURCES.CNF_EMP_DIM_DTL_CURR_CFDL T2 ON T1.SALE_HIER_ID = T2.EMP_ID WHERE T1.CURR_ROW_IND = 'Y' AND T2.EMP_STS_TYP_CDE IN ('A') AND T1.EFF_END_TMSP IS NULL AND T2.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL AND T2.JOB_TYP_CDE IN ('001002', '001003',/* '001004', '001005',*/ '001007') AND T2.EMP_ID NOT IN('TS62904','TS67022') ) T3 ON T1.ORZN_ZONE_CDE = T3.ORZN_ZONE_CDE WHERE T1.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL --AND T1.RFO_ZONE_STS_CDE IN ('A') AND SUBSTRING(T1.ORZN_ZONE_CDE,6,2) <> '00' AND T1.ORZN_DEPT_CDE <> 'UKWN' ORDER BY T1.ORZN_ZONE_CDE; ']),    #'FILTERED ROWS' = TABLE.SELECTROWS(SOURCE, EACH ([EMP_ID] <> 'TS18670    ' AND [EMP_ID] <> 'TS73067    ' AND [EMP_ID] <> 'TS73124    ' AND [EMP_ID] <> 'TS74827    ' AND [EMP_ID] <> 'TS77319    '))IN    #'FILTERED ROWS
 """,

""" SELECT DT_SK,         CAL_DAY_DT,         DTRB_PERF_RPT_WK_END_DT,         DTRB_PERF_RPT_WK_NBR,        DTRB_PERF_RPT_YR_NBR,        DTRB_PERF_RPT_YR_WK_NBR,        SRC_SYS_ID,         CRET_TMSP,         LST_UPDT_TMSP,         CRET_USER_ID,         LST_UPDT_USER_ID,         DTRB_PERF_RPT_DAY_TXT,         DTRB_PERF_RPT_MTH_TXT,         DTRB_PERF_RPT_QTR_TXT,         DTRB_PERF_RPT_WK_TXT,        DTRB_PERF_RPT_YR_TXT       FROM COMMON.DTRB_PERF_DATES       WHERE DTRB_PERF_RPT_YR_NBR IN (2020,2021, 2022, 2023, 2024)
""",

""" SELECT COMP_PYEE_ID,        CONF_YR,        NEW_MONY_CONF_CR_AMT,        NEW_MONY_AFA_CONF_CR_AMT,        NEW_MONY_TOT_CONF_CR_AMT,        ON_TRGT_CONF_IDVL_CDE,        CONF_QUAL_IDVL_TYP_CDE,        EXTR_DAY_IDVL_IND    FROM COMP.CONF_QUAL_DTL  WHERE PROC_DT = (SELECT MAX(PROC_DT)                     FROM COMP.CONF_QUAL_DTL)']),    #'DUPLICATED COLUMN' = TABLE.DUPLICATECOLUMN(SOURCE, 'ON_TRGT_CONF_IDVL_CDE', 'ON_TRGT_CONF_IDVL_CDE - COPY'),    #'RENAMED COLUMNS' = TABLE.RENAMECOLUMNS(#'DUPLICATED COLUMN',{{'ON_TRGT_CONF_IDVL_CDE - COPY', 'TARGETCONFERENCE'}}),    #'REPLACED VALUE' = TABLE.REPLACEVALUE(#'RENAMED COLUMNS','NOT QUALIFIED','NOT ON TARGET',REPLACER.REPLACETEXT,{'TARGETCONFERENCE'}),    #'REPLACED VALUE1' = TABLE.REPLACEVALUE(#'REPLACED VALUE','PINNACLE A','PINNACLE A',REPLACER.REPLACETEXT,{'TARGETCONFERENCE'}),    #'REPLACED VALUE2' = TABLE.REPLACEVALUE(#'REPLACED VALUE1','PINNACLE B','PINNACLE B',REPLACER.REPLACETEXT,{'TARGETCONFERENCE'}),    #'REPLACED VALUE3' = TABLE.REPLACEVALUE(#'REPLACED VALUE2','PINNACLE C','PINNACLE C',REPLACER.REPLACETEXT,{'TARGETCONFERENCE'}),    #'REPLACED VALUE4' = TABLE.REPLACEVALUE(#'REPLACED VALUE3','SIERRA A','SIERRA A',REPLACER.REPLACETEXT,{'TARGETCONFERENCE'}),    #'REPLACED VALUE5' = TABLE.REPLACEVALUE(#'REPLACED VALUE4','SIERRA B','SIERRA B',REPLACER.REPLACETEXT,{'TARGETCONFERENCE'}),    #'REPLACED VALUE6' = TABLE.REPLACEVALUE(#'REPLACED VALUE5','SUMMIT A','SUMMIT A',REPLACER.REPLACETEXT,{'TARGETCONFERENCE'}),    #'REPLACED VALUE7' = TABLE.REPLACEVALUE(#'REPLACED VALUE6','SUMMIT B','SUMMIT B',REPLACER.REPLACETEXT,{'TARGETCONFERENCE'}),    #'REPLACED VALUE8' = TABLE.REPLACEVALUE(#'REPLACED VALUE7','SUMMIT C','SUMMIT C',REPLACER.REPLACETEXT,{'TARGETCONFERENCE'}),    #'FILTERED ROWS' = TABLE.SELECTROWS(#'REPLACED VALUE8', EACH TRUE)IN    #'FILTERED ROWS
""",

"""SELECT 
	B.SRC_SYS_KEY_TXT, 
	B.FRST_NM+' '+B.LST_NM AS EMP_NM, 
	C.ORZN_ZONE_CDE, 
	C.ORZN_DEPT_CDE, 
	A.JOB_TYP_CDE, 
	CAST(A.EFF_BEG_DT AS DATE) AS EFF_BEG_DT, 
	ADJ_SVC_DT 
FROM [DM_01].[WORKER_STATUS_FCT] A LEFT JOIN [DM_01].[ORGANIZATION_DIM] B ON A.ORZN_DIM_SK=B.ORZN_DIM_SK LEFT JOIN [DM_01].[SALE_HIER_DIM] C ON (B.SRC_SYS_KEY_TXT=C.SALE_HIER_ID AND C.EFF_END_DT='9999-12-31 00:00:00' AND C.CURR_ROW_IND='Y') WHERE ( --A.JOB_TYP_CDE='001004' OR A.JOB_TYP_CDE='001005' OR A.JOB_TYP_CDE='001011' OR A.JOB_TYP_CDE='001007' ) AND  B.CURR_ROW_IND='Y' AND A.EFF_END_DT='9999-12-31 00:00:00' AND EMP_STS_TYP_CDE='A'']),    #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(SOURCE,{{'EFF_BEG_DT', TYPE DATE}, {'ADJ_SVC_DT', TYPE DATE}}),    #'FILTERED ROWS' = TABLE.SELECTROWS(#'CHANGED TYPE', EACH [JOB_TYP_CDE] <> '001004'),    #'REMOVED COLUMNS' = TABLE.REMOVECOLUMNS(#'FILTERED ROWS',{'ADJ_SVC_DT'}),    #'MERGED QUERIES' = TABLE.NESTEDJOIN(#'REMOVED COLUMNS', {'SRC_SYS_KEY_TXT'}, #'WORKDAY LEADER DIRECTORY', {'EMPLOYEE ID'}, 'WORKDAY LEADER DIRECTORY', JOINKIND.LEFTOUTER),    #'EXPANDED WORKDAY LEADER DIRECTORY' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES', 'WORKDAY LEADER DIRECTORY', {'COST CENTER', 'START DATE IN CURRENT JOB OR HIRE DATE', 'WORKER'}, {'WORKDAY LEADER DIRECTORY.COST CENTER', 'WORKDAY LEADER DIRECTORY.START DATE IN CURRENT JOB OR HIRE DATE', 'WORKDAY LEADER DIRECTORY.WORKER'}),    #'ADDED CUSTOM1' = TABLE.ADDCOLUMN(#'EXPANDED WORKDAY LEADER DIRECTORY', 'FINAL NAME', EACH IF [WORKDAY LEADER DIRECTORY.WORKER] IS NULL THEN [EMP_NM] ELSE [WORKDAY LEADER DIRECTORY.WORKER]),    #'REMOVED COLUMNS2' = TABLE.REMOVECOLUMNS(#'ADDED CUSTOM1',{'EMP_NM', 'WORKDAY LEADER DIRECTORY.WORKER'}),    #'REORDERED COLUMNS' = TABLE.REORDERCOLUMNS(#'REMOVED COLUMNS2',{'SRC_SYS_KEY_TXT', 'FINAL NAME', 'ORZN_ZONE_CDE', 'ORZN_DEPT_CDE', 'JOB_TYP_CDE', 'EFF_BEG_DT', 'WORKDAY LEADER DIRECTORY.COST CENTER', 'WORKDAY LEADER DIRECTORY.START DATE IN CURRENT JOB OR HIRE DATE'}),    #'RENAMED COLUMNS2' = TABLE.RENAMECOLUMNS(#'REORDERED COLUMNS',{{'FINAL NAME', 'EMP_NM'}}),    #'MERGED QUERIES1' = TABLE.NESTEDJOIN(#'RENAMED COLUMNS2', {'SRC_SYS_KEY_TXT'}, #'MARKET MAPPING', {'ZONE_LEADER_TSID'}, 'MARKET MAPPING', JOINKIND.LEFTOUTER),    #'EXPANDED MARKET MAPPING' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES1', 'MARKET MAPPING', {'HIER_ID'}, {'MARKET MAPPING.HIER_ID'}),    #'RENAMED COLUMNS' = TABLE.RENAMECOLUMNS(#'EXPANDED MARKET MAPPING',{{'ORZN_ZONE_CDE', 'WORKER_STS_FCT_MKT'}, {'MARKET MAPPING.HIER_ID', 'ORZN_ZONE_CDE'}, {'EFF_BEG_DT', 'WORKER_STS_FCT_DT'}, {'WORKDAY LEADER DIRECTORY.START DATE IN CURRENT JOB OR HIRE DATE', 'EFF_BEG_DT'}}),    #'MERGED QUERIES2' = TABLE.NESTEDJOIN(#'RENAMED COLUMNS', {'SRC_SYS_KEY_TXT'}, #'0630 LEADER BACKUP', {'EMPLOYEE ID'}, '0630 LEADER BACKUP', JOINKIND.LEFTOUTER),    #'EXPANDED 0630 LEADER BACKUP' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES2', '0630 LEADER BACKUP', {'EMPLOYEE ID', 'START DATE IN CURRENT JOB OR HIRE DATE'}, {'0630 LEADER BACKUP.EMPLOYEE ID', '0630 LEADER BACKUP.START DATE IN CURRENT JOB OR HIRE DATE'}),    #'FILTERED ROWS1' = TABLE.SELECTROWS(#'EXPANDED 0630 LEADER BACKUP', EACH ([0630 LEADER BACKUP.EMPLOYEE ID] <> NULL))IN    #'FILTERED ROWS1
""",

""" SELECT A.BEN_CTRC_NBR,           A.EMP_ID,           A.SVC_AGMT_PLAN_YR,     A.SPLIT_CR_AGT_INDIC,    A.AGT_SPLT_PCT,    A.SVAG_ADVR_RLTN_BEG_DT,    A.SVAG_ADVR_RLTN_END_DT,    A.RCD_STS_CDE,    A.END_TMSP,    A.CRET_TMSP,    A.LST_UPDT_TMSP     FROM CONTRACT.SVAG_ADVR_RLTN A    ']),    #'REMOVED COLUMNS' = TABLE.REMOVECOLUMNS(SOURCE,{'CRET_TMSP', 'LST_UPDT_TMSP'}),    #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(#'REMOVED COLUMNS',{{'SVAG_ADVR_RLTN_BEG_DT', TYPE DATE}, {'SVAG_ADVR_RLTN_END_DT', TYPE DATE}, {'END_TMSP', TYPE DATE}})IN    #'CHANGED TYPE""",

""" SELECT 
	A.PREV_JOB_TYP_CDE, 
	A.JOB_TYP_CDE, 
	A.EFF_BEG_DT, 
	A.EFF_END_DT, 
	B.SRC_SYS_KEY_TXT, 
	CONCAT(B.FRST_NM,' ',B.LST_NM) AS NAME, 
	C.ORZN_ZONE_CDE, 
	C.ORZN_DEPT_CDE 
FROM [ENTERPRISEDATAMART].[DM_01].[WORKER_STATUS_FCT] A LEFT JOIN [ENTERPRISEDATAMART].[DM_01].[ORGANIZATION_DIM] B ON A.ORZN_DIM_SK=B.ORZN_DIM_SK LEFT JOIN [ENTERPRISEDATAMART].[DM_01].[SALE_HIER_DIM] C ON (B.SRC_SYS_KEY_TXT=C.SALE_HIER_ID AND C.CURR_ROW_IND='Y')  WHERE ( A.PREV_JOB_TYP_CDE = '002003' OR A.PREV_JOB_TYP_CDE = '002011' OR A.PREV_JOB_TYP_CDE = '002012' OR A.PREV_JOB_TYP_CDE = '002030' OR A.PREV_JOB_TYP_CDE = '002031' OR A.PREV_JOB_TYP_CDE = '002032' ) AND A.JOB_TYP_CDE = '002022' GROUP BY A.PREV_JOB_TYP_CDE, A.JOB_TYP_CDE, A.EFF_BEG_DT, A.EFF_END_DT, B.SRC_SYS_KEY_TXT, C.ORZN_ZONE_CDE, C.ORZN_DEPT_CDE, CONCAT(B.FRST_NM,' ',B.LST_NM)']),    #'RENAMED COLUMNS' = TABLE.RENAMECOLUMNS(SOURCE,{{'EFF_BEG_DT', 'TENURE DATE'}}),    #'FILTERED ROWS' = TABLE.SELECTROWS(#'RENAMED COLUMNS', EACH [TENURE DATE] > #DATETIME(2023, 12, 31, 0, 0, 0)),    #'FILTERED ROWS1' = TABLE.SELECTROWS(#'FILTERED ROWS', EACH ([EFF_END_DT] = #DATETIME(9999, 12, 31, 0, 0, 0))),    #'RENAMED COLUMNS1' = TABLE.RENAMECOLUMNS(#'FILTERED ROWS1',{{'SRC_SYS_KEY_TXT', 'TSID'}}),    #'MERGED QUERIES' = TABLE.NESTEDJOIN(#'RENAMED COLUMNS1', {'TSID'}, RR_IN_AND_OUT, {'TSID'}, 'RR_IN_AND_OUT', JOINKIND.LEFTOUTER),    #'EXPANDED RR_IN_AND_OUT' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES', 'RR_IN_AND_OUT', {'TSID'}, {'RR_IN_AND_OUT.TSID'}),    #'FILTERED ROWS2' = TABLE.SELECTROWS(#'EXPANDED RR_IN_AND_OUT', EACH ([RR_IN_AND_OUT.TSID] = NULL))IN    #'FILTERED ROWS2
""",

""" SELECT 
	A.DT_SK,         
	A.CAL_DAY_DT,         
	A.DTRB_PERF_RPT_WK_END_DT,         
	A.DTRB_PERF_RPT_WK_NBR,        
	A.DTRB_PERF_RPT_YR_NBR,        
	A.DTRB_PERF_RPT_YR_WK_NBR,        
	A.SRC_SYS_ID,         
	A.CRET_TMSP,         
	A.LST_UPDT_TMSP,         
	A.CRET_USER_ID,         
	A.LST_UPDT_USER_ID,         
	A.DTRB_PERF_RPT_DAY_TXT,         
	A.DTRB_PERF_RPT_MTH_TXT,         
	A.DTRB_PERF_RPT_QTR_TXT,         
	A.DTRB_PERF_RPT_WK_TXT,        
	A.DTRB_PERF_RPT_YR_TXT,        
	B.CAL_WK_STRT_DT,        
	B.CAL_WK_END_DT,        
	B.CAL_MTH_STRT_DT,        
	B.CAL_MTH_END_DT,        
	B.HDAY_IND,        
	B.WKDY_IND   
FROM COMMON.DTRB_PERF_DATES  AS A INNER JOIN        COMMON.IA_DATES_ITRL AS B     ON A.DT_SK=B.DT_SK  WHERE A.DTRB_PERF_RPT_YR_NBR IN (2021, 2022, 2023, 2024)      ']),    #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(SOURCE,{{'CAL_DAY_DT', TYPE DATE}, {'DTRB_PERF_RPT_WK_END_DT', TYPE DATE}, {'CAL_WK_STRT_DT', TYPE DATE}, {'CAL_WK_END_DT', TYPE DATE}}),    #'ADDED CUSTOM' = TABLE.ADDCOLUMN(#'CHANGED TYPE', 'WEEK START DATE', EACH DATE.ADDDAYS([CAL_WK_STRT_DT],1)),    #'ADDED CUSTOM1' = TABLE.ADDCOLUMN(#'ADDED CUSTOM', 'WEEK END DATE', EACH DATE.ADDDAYS([CAL_WK_END_DT],1)),    #'REMOVED COLUMNS' = TABLE.REMOVECOLUMNS(#'ADDED CUSTOM1',{'CAL_WK_STRT_DT', 'CAL_WK_END_DT'}),    #'ADDED CUSTOM2' = TABLE.ADDCOLUMN(#'REMOVED COLUMNS', 'CURRENT YEAR', EACH IF DATE.YEAR(DATETIME.LOCALNOW()) = [DTRB_PERF_RPT_YR_NBR] THEN 'CURRENT YEAR' ELSE [DTRB_PERF_RPT_YR_NBR]),    #'REMOVED COLUMNS1' = TABLE.REMOVECOLUMNS(#'ADDED CUSTOM2',{'SRC_SYS_ID', 'CRET_TMSP', 'LST_UPDT_TMSP', 'CRET_USER_ID', 'LST_UPDT_USER_ID'}),    #'ADDED CONDITIONAL COLUMN' = TABLE.ADDCOLUMN(#'REMOVED COLUMNS1', 'BUSINESS DAY IND', EACH IF [HDAY_IND] = 'Y' THEN 0 ELSE IF [WKDY_IND] = 'Y' THEN 1 ELSE 0),    #'CHANGED TYPE1' = TABLE.TRANSFORMCOLUMNTYPES(#'ADDED CONDITIONAL COLUMN',{{'BUSINESS DAY IND', INT64.TYPE}})IN    #'CHANGED TYPE1""",

""" SELECT DISTINCT 
	T1.ORZN_DEPT_CDE AS RFO_CDE, 
	--T1.ORZN_ZONE_CDE AS RFO_ZONE_NM, 
	T1.ORZN_SUB_DEPT_CDE AS MVP_ID, 
	--(SUBSTRING(T1.ORZN_ZONE_CDE,6,5)) AS MARKET_ID, 
	T3.ORZN_DEPT_DSCR, T3.EMP_ID, 
	T3.EMP_STS_TYP_CDE, 
	T3.TRMN_DT, 
	T3.JOB_TYP_CDE, 
	T3.JOB_TYP_DSCR, 
	T3.EMP_NM, 
	CASE WHEN T3.EMP_NM IS NULL THEN 'VACANT' ELSE T3.EMP_NM END AS MARKET_LEADERS 
FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN ( SELECT DISTINCT T1.SALE_HIER_ID, T1.ORZN_ZONE_CDE, T1.ORZN_DEPT_DSCR, T1.ORZN_SUB_DEPT_CDE, T2.ORZN_DEPT_CDE, T2.EMP_ID, T2.EMP_STS_TYP_CDE, T2.TRMN_DT, T2.JOB_TYP_CDE, T2.JOB_TYP_DSCR, T2.EMP_NM FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN HUMAN_RESOURCES.CNF_EMP_DIM_DTL_CURR_CFDL T2 ON T1.SALE_HIER_ID = T2.EMP_ID WHERE T1.CURR_ROW_IND = 'Y' AND T2.EMP_STS_TYP_CDE IN ('A') AND T1.EFF_END_TMSP IS NULL AND T2.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL AND T2.JOB_TYP_CDE IN ('001001') ) T3 ON T1.ORZN_SUB_DEPT_CDE = T3.ORZN_SUB_DEPT_CDE WHERE T1.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL --AND T1.RFO_ZONE_STS_CDE IN ('A') --AND T3.EMP_STS_TYP_CDE = 'A' --AND SUBSTRING(T1.ORZN_ZONE_CDE,6,2) <> '00' AND T1.ORZN_ZONE_CDE <> 'UKWN' ORDER BY T1.ORZN_SUB_DEPT_CDE;']),    #'FILTERED ROWS' = TABLE.SELECTROWS(SOURCE, EACH ([RFO_CDE] <> '0386      ' AND [RFO_CDE] <> '0710      ' AND [RFO_CDE] <> '0714      ' AND [RFO_CDE] <> '0715      ' AND [RFO_CDE] <> '0718      ' AND [RFO_CDE] <> '0997      ' AND [RFO_CDE] <> 'UKWN      ') AND ([MVP_ID] <> '          ' AND [MVP_ID] <> '0410-00   ' AND [MVP_ID] <> '0529-00   '))IN    #'FILTERED ROWS""",

"""SELECT DISTINCT 
	T1.ORZN_DEPT_CDE AS RFO_CDE, 
	--T1.ORZN_ZONE_CDE AS RFO_ZONE_NM, 
	T1.ORZN_SUB_DEPT_CDE AS MVP_ID, 
	--(SUBSTRING(T1.ORZN_ZONE_CDE,6,5)) AS MARKET_ID, 
	T3.ORZN_DEPT_DSCR, 
	T3.EMP_ID, 
	T3.EMP_STS_TYP_CDE, 
	T3.TRMN_DT, 
	T3.JOB_TYP_CDE, 
	T3.JOB_TYP_DSCR, 
	T3.EMP_NM, 
	T3.PEFR_FULL_NM, 
	CASE WHEN T3.PEFR_FULL_NM IS NULL THEN 'VACANT' ELSE T3.PEFR_FULL_NM END AS MARKET_LEADERS 
FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN ( SELECT DISTINCT T1.SALE_HIER_ID, T1.ORZN_ZONE_CDE, T1.ORZN_DEPT_DSCR, T1.ORZN_SUB_DEPT_CDE, T2.ORZN_DEPT_CDE, T2.EMP_ID, T2.EMP_STS_TYP_CDE, T2.TRMN_DT, T2.JOB_TYP_CDE, T2.JOB_TYP_DSCR, T2.EMP_NM, T2.PEFR_FULL_NM FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN HUMAN_RESOURCES.CNF_EMP_DIM_DTL_CURR_CFDL T2 ON T1.SALE_HIER_ID = T2.EMP_ID WHERE T1.CURR_ROW_IND = 'Y'  AND T2.EMP_STS_TYP_CDE IN ('A') AND T1.EFF_END_TMSP IS NULL AND T2.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL AND T1.ORZN_DEPT_CDE IN ('0001','0714', '0716','0361', '0435','0283', '0291', '0525', '0115', '0190', '0384','0383') AND T2.JOB_TYP_CDE IN ('001001','1658','3503')  ) T3 ON T1.ORZN_SUB_DEPT_CDE = T3.ORZN_SUB_DEPT_CDE WHERE T1.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL --AND T1.RFO_ZONE_STS_CDE IN ('A') --AND T3.EMP_STS_TYP_CDE = 'A' --AND SUBSTRING(T1.ORZN_ZONE_CDE,6,2) <> '00' AND T1.ORZN_ZONE_CDE <> 'UKWN' AND T1.ORZN_DEPT_CDE IN ('0001','0714', '0716','0361', '0435','0283', '0291', '0525', '0115', '0190', '0384','0383')  ORDER BY T1.ORZN_SUB_DEPT_CDE;']),    #'FILTERED ROWS' = TABLE.SELECTROWS(SOURCE, EACH ([MVP_ID] <> '          '))IN    #'FILTERED ROWS
""",

"""SELECT DISTINCT        
	[DEPARTMENT IDENTIFIER] RFO_CODE       ,
	[DEPARTMENT NAME] RFO_NM       ,
	CONCAT(TRIM([DEPARTMENT IDENTIFIER]),'-',[DEPARTMENT NAME]) AS NM       
FROM [ENTERPRISEDATAMART].[DM_01].[ORGANIZATION DIMENSION]    WHERE [DEPARTMENT IDENTIFIER] IN ('0283', '0435','0115', '0190',                  '0361', '0384','0291', '0525','0001','0383','0716','0708')         AND [CURRENT ROW INDICATOR] = 'Y'         AND [EFFECTIVE BEGIN DATE] > '1/1/2019'         AND [EFFECTIVE END DATE] = '12/31/9999'']),    #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(SOURCE,{{'RFO_CODE', INT64.TYPE}})IN    #'CHANGED TYPE
""",

"""SELECT 
	PERS_DIM.[SOURCE SYSTEM KEY TEXT] AS THRIVENTID,        
	PERS_DIM.[FIRST NAME] + ' ' + PERS_DIM.[LAST NAME] AS CLIENTNAME,        
	APPT_FCT.[WORKER APPOINTMENT ASSOCIATION],        
	APPT_FCT.[APPOINTMENT DIMENSION SURROGATE KEY],        
	APPT_FCT.[PERSON DIMENSION SURROGATE KEY],        
	SUM(APPT_FCT.[APPOINTMENT OCCURS]) [APPT_OCCURS],       
	MAX(APPT_FCT.[PERSON OCCURS]) [PERSON OCCURS],        
	MBR_DIM.[MEMBERSHIP TYPE DESCRIPTION],     
	CASE WHEN APPT_DIM.[FIELD USER COUNT] <> 0 AND APPT_DIM.[APPOINTMENT TYPE CODE] IN ('CONNECT','GATHER DATA','TAKE ACTION','STRATEGY CALL/MEETING','REVIEW','SERVICE','DISCOVER','DELIVER')    THEN 1    ELSE 0     END [JFW_IND],        
	APPT_DIM.[APPOINTMENT DATE],        
	CASE WHEN APPT_DIM.[APPOINTMENT TYPE CODE] IN ('GATHER DATA') THEN 'DISCOVER'             WHEN APPT_DIM.[APPOINTMENT TYPE CODE] IN ('TAKE ACTION') THEN 'DELIVER'             WHEN APPT_DIM.[APPOINTMENT TYPE CODE] NOT IN ('GATHER DATA','TAKE ACTION') THEN APPT_DIM.[APPOINTMENT TYPE DESCRIPTION]        END AS [APPOINTMENT TYPE CODE],        
	DATE_DIM.[CALENDAR YEAR NAME] AS [PERFORMANCE YEAR NAME],        
	ORG_DIM.[SOURCE SYSTEM KEY TEXT] [EMP_ID],        
	ORG_DIM.[FIRST NAME] + ' ' + ORG_DIM.[LAST NAME] [EMP_NM]    
FROM ENTERPRISEDATAMART.DM_01.[APPOINTMENT COMBINE FACT] APPT_FCT LEFT JOIN         ENTERPRISEDATAMART.DM_01.[PERSON DIMENSION] AS PERS_DIM     ON (APPT_FCT.[PERSON DIMENSION SURROGATE KEY]=PERS_DIM.[PERSON DIMENSION SURROGATE KEY] AND         APPT_FCT.[PERSON DIMENSION VERSION NUMBER] = PERS_DIM.[PERSON DIMENSION VERSION NUMBER]) LEFT JOIN        ENTERPRISEDATAMART.DM_01.[MEMBERSHIP TYPE DIMENSION] MBR_DIM     ON (APPT_FCT.[MEMBERSHIP TYPE DIMENSION SURROGATE KEY]=MBR_DIM.[MEMBERSHIP TYPE DIMENSION SURROGATE KEY]) LEFT JOIN         ENTERPRISEDATAMART.DM_01.[APPOINTMENT DIMENSION] APPT_DIM     ON (APPT_FCT.[APPOINTMENT DIMENSION SURROGATE KEY]=APPT_DIM.[APPOINTMENT DIMENSION SURROGATE KEY]) LEFT JOIN         ENTERPRISEDATAMART.DM_01.[DATE DIMENSION] DATE_DIM     ON (APPT_DIM.[APPOINTMENT DATE]=DATE_DIM.[CALENDAR DATE DATE]) LEFT JOIN         ENTERPRISEDATAMART.DM_01.[ORGANIZATION DIMENSION] ORG_DIM     ON ((APPT_FCT.[APPOINTMENT ORGANIZATION DIMENSION SURROGATE KEY]=ORG_DIM.[ORGANIZATION DIMENSION SURROGATE KEY]) AND         ((ORG_DIM.[EFFECTIVE BEGIN DATE] <= APPT_DIM.[APPOINTMENT DATE]) AND (APPT_DIM.[APPOINTMENT DATE] <= ORG_DIM.[EFFECTIVE END DATE]))) LEFT JOIN           ENTERPRISEDATAMART.DM_01.[SALE_HIER_DIM] SALE_HIER_DIM     ON ((ORG_DIM.[SOURCE SYSTEM KEY TEXT]=SALE_HIER_DIM.[SALE_HIER_ID] AND  (SALE_HIER_DIM.EFF_BEG_DT<=APPT_DIM.[APPOINTMENT DATE]) AND  (APPT_DIM.[APPOINTMENT DATE]<= SALE_HIER_DIM.EFF_END_DT) AND    SALE_HIER_DIM.CURR_ROW_IND = 'Y'))  WHERE APPT_DIM.[CURRENT ROW INDICATOR] = 'Y'   AND APPT_DIM.[APPOINTMENT CREATE DATE OVER 14 DAYS] = 'N'   AND APPT_FCT.[PERSON DIMENSION SURROGATE KEY] <> 0   AND APPT_FCT.[APPOINTMENT STATUS NAME] <> 'DECLINED'   AND APPT_DIM.[APPOINTMENT TYPE CODE] <> 'UNK'   AND SALE_HIER_DIM.ORZN_DEPT_CDE IN ('0115','0165','0190','0240','0283','0291','0361','0365', '0383','0384','0410','0435','0475','0496','0525','0529','0810')  AND (ORG_DIM.[JOB TYPE CODE] IN ('002000', '002003', '002008', '002010', '002011', '002012', '002016', '002022', '002024', '002025', '002026', '002027', '003100', '003500')   OR  ORG_DIM.[DEPARTMENT IDENTIFIER] IN ('5405','5407','5408','5409','1701','1702','1703','0383'))    AND APPT_DIM.[EVENT RESULT DESCRIPTION] NOT IN ('NO SHOW','CANCELLED')   AND DATE_DIM.[CALENDAR YEAR NAME] IN ('2024')   GROUP BY   APPT_FCT.[WORKER APPOINTMENT ASSOCIATION],   APPT_FCT.[APPOINTMENT DIMENSION SURROGATE KEY],   APPT_FCT.[PERSON DIMENSION SURROGATE KEY],   MBR_DIM.[MEMBERSHIP TYPE DESCRIPTION],   APPT_DIM.[FIELD USER COUNT],   APPT_DIM.[APPOINTMENT DATE],   APPT_DIM.[APPOINTMENT TYPE CODE],   APPT_DIM.[APPOINTMENT TYPE DESCRIPTION],   DATE_DIM.[CALENDAR YEAR NAME],   ORG_DIM.[SOURCE SYSTEM KEY TEXT],   ORG_DIM.[FIRST NAME] + ' ' + ORG_DIM.[LAST NAME],   PERS_DIM.[SOURCE SYSTEM KEY TEXT],   PERS_DIM.[FIRST NAME] + ' ' + PERS_DIM.[LAST NAME]   HAVING  SUM(APPT_FCT.[APPOINTMENT OCCURS]) > 0 ']),    #'FILTERED ROWS1' = TABLE.SELECTROWS(SOURCE, EACH ([WORKER APPOINTMENT ASSOCIATION] = 'OWNER')),    #'FILTERED ROWS' = TABLE.SELECTROWS(#'FILTERED ROWS1', EACH ([APPOINTMENT TYPE CODE] = 'CONNECT') AND ([MEMBERSHIP TYPE DESCRIPTION] <> 'BENEFIT')),    #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(#'FILTERED ROWS',{{'APPOINTMENT DATE', TYPE DATE}, {'THRIVENTID', INT64.TYPE}})IN    #'CHANGED TYPE
""",

"""SELECT 
	A.[APPOINTMENT COUNT],        
	A.[APPOINTMENT ORGANIZATION DIMENSION SURROGATE KEY],        
	A.[APPOINTMENT ORGANIZATION VERSION NUMBER],        
	A.[CONTRACT COUNT],        
	A.[GROSS DEALER CONCESSION AMOUNT],        
	A.[GROSS DEALER CONCESSION CONTRACT COUNT],        
	A.[NEW SALES CREDIT AMOUNT],        
	A.[NEW SALES CREDIT CONTRACT COUNT],     
	A.[PERSON APPOINTMENT COUNT],     
	A.[PERSON CONTRACT COUNT],     
	A.[PERSON CONTRACT GROSS DEALER CONCESSION COUNT],     
	A.[PERSON CONTRACT NEW SALES CREDIT COUNT],     
	A.[PERSON COUNT],     
	A.[REFERRAL COUNT],    
	A.[VITAL STATS CALCULATION DATE],     
	B.[SOURCE SYSTEM KEY TEXT] [EMP_ID]  
FROM ENTERPRISEDATAMART.DM_01.[VITAL STATS SUMMARY FACT] AS A LEFT JOIN       ENTERPRISEDATAMART.DM_01.[ORGANIZATION DIMENSION] B   ON A.[APPOINTMENT ORGANIZATION DIMENSION SURROGATE KEY]=B.[ORGANIZATION DIMENSION SURROGATE KEY] AND B.[CURRENT ROW INDICATOR] = 'Y'   ']),    #'ADDED CUSTOM' = TABLE.ADDCOLUMN(SOURCE, 'APPOINTMENTS_PER_CUSTOMER', EACH [PERSON COUNT]/[PERSON APPOINTMENT COUNT]),    #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(#'ADDED CUSTOM',{{'APPOINTMENTS_PER_CUSTOMER', TYPE NUMBER}}),    #'ADDED CUSTOM1' = TABLE.ADDCOLUMN(#'CHANGED TYPE', 'TOTAL CLOSE RATE', EACH [PERSON CONTRACT COUNT] / [PERSON COUNT]),    #'CHANGED TYPE1' = TABLE.TRANSFORMCOLUMNTYPES(#'ADDED CUSTOM1',{{'TOTAL CLOSE RATE', TYPE NUMBER}})IN    #'CHANGED TYPE1""",

"""SELECT 
	(DTRB_PERF_RPT_WK_END_DT+1) AS ''WEEK_START_DATE'', 
	SUM(1) AS ROW_CT 
FROM COMMON.DTRB_PERF_DATES WHERE DTRB_PERF_RPT_YR_TXT='CURRENT' AND DTRB_PERF_RPT_WK_END_DT+1<=CURRENT_DATE GROUP BY (DTRB_PERF_RPT_WK_END_DT+1)']),    #'REMOVED COLUMNS' = TABLE.REMOVECOLUMNS(SOURCE,{'ROW_CT'}),    #'ADDED CUSTOM' = TABLE.ADDCOLUMN(#'REMOVED COLUMNS', 'JOIN', EACH 1),    #'MERGED QUERIES' = TABLE.NESTEDJOIN(#'ADDED CUSTOM', {'JOIN'}, #'PROCESS DATES', {'JOIN'}, 'PROCESS DATES', JOINKIND.LEFTOUTER),    #'REMOVED COLUMNS1' = TABLE.REMOVECOLUMNS(#'MERGED QUERIES',{'JOIN'}),    #'ALL DATES' = TABLE.EXPANDTABLECOLUMN(#'REMOVED COLUMNS1', 'PROCESS DATES', {'PROCESS DATE'}, {'PROCESS DATES.PROCESS DATE'}),    #'ADDED CUSTOM1' = TABLE.ADDCOLUMN(#'ALL DATES', 'NEW_PROCESS_DT', EACH IF DURATION.DAYS([PROCESS DATES.PROCESS DATE]-[WEEK_START_DATE])>0 THEN NULL ELSE [PROCESS DATES.PROCESS DATE]),    #'GROUP BY PROC DATE' = TABLE.GROUP(#'ADDED CUSTOM1', {'WEEK_START_DATE'}, {{'COUNT', EACH _, TYPE TABLE [WEEK_START_DATE=DATETIME, PROCESS DATES.PROCESS DATE=NULLABLE DATETIME, NEW_PROCESS_DT=NULLABLE DATETIME]}}),    #'ADDED CUSTOM2' = TABLE.ADDCOLUMN(#'GROUP BY PROC DATE', 'WEEKLY PROC DT', EACH LETDATEMAX=LIST.MAX([COUNT] [NEW_PROCESS_DT])INDATEMAX),    #'FILTERED ROWS' = TABLE.SELECTROWS(#'ADDED CUSTOM2', EACH ([WEEKLY PROC DT] <> NULL))IN    #'FILTERED ROWS""",

"""SELECT 
	B.SRC_SYS_KEY_TXT, 
	B.FRST_NM+' '+B.LST_NM AS EMP_NM, 
	C.ORZN_ZONE_CDE, 
	C.ORZN_DEPT_CDE, 
	A.JOB_TYP_CDE, 
	CAST(A.EFF_BEG_DT AS DATE) AS EFF_BEG_DT, 
	ADJ_SVC_DT 
FROM [DM_01].[WORKER_STATUS_FCT] A LEFT JOIN [DM_01].[ORGANIZATION_DIM] B ON A.ORZN_DIM_SK=B.ORZN_DIM_SK LEFT JOIN [DM_01].[SALE_HIER_DIM] C ON (B.SRC_SYS_KEY_TXT=C.SALE_HIER_ID AND C.EFF_END_DT='9999-12-31 00:00:00' AND C.CURR_ROW_IND='Y') WHERE ( --A.JOB_TYP_CDE='001004' OR A.JOB_TYP_CDE='001005' OR A.JOB_TYP_CDE='001011' OR A.JOB_TYP_CDE='001007' ) AND  B.CURR_ROW_IND='Y' AND A.EFF_END_DT='9999-12-31 00:00:00' AND EMP_STS_TYP_CDE='A'']),    #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(SOURCE,{{'EFF_BEG_DT', TYPE DATE}, {'ADJ_SVC_DT', TYPE DATE}}),    #'FILTERED ROWS' = TABLE.SELECTROWS(#'CHANGED TYPE', EACH [JOB_TYP_CDE] <> '001004'),    #'REMOVED COLUMNS' = TABLE.REMOVECOLUMNS(#'FILTERED ROWS',{'ADJ_SVC_DT'}),    #'MERGED QUERIES' = TABLE.NESTEDJOIN(#'REMOVED COLUMNS', {'SRC_SYS_KEY_TXT'}, #'WORKDAY LEADER DIRECTORY', {'EMPLOYEE ID'}, 'WORKDAY LEADER DIRECTORY', JOINKIND.LEFTOUTER),    #'EXPANDED WORKDAY LEADER DIRECTORY' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES', 'WORKDAY LEADER DIRECTORY', {'COST CENTER', 'START DATE IN CURRENT JOB OR HIRE DATE', 'WORKER'}, {'WORKDAY LEADER DIRECTORY.COST CENTER', 'WORKDAY LEADER DIRECTORY.START DATE IN CURRENT JOB OR HIRE DATE', 'WORKDAY LEADER DIRECTORY.WORKER'}),    #'ADDED CUSTOM1' = TABLE.ADDCOLUMN(#'EXPANDED WORKDAY LEADER DIRECTORY', 'FINAL NAME', EACH IF [WORKDAY LEADER DIRECTORY.WORKER] IS NULL THEN [EMP_NM] ELSE [WORKDAY LEADER DIRECTORY.WORKER]),    #'REMOVED COLUMNS2' = TABLE.REMOVECOLUMNS(#'ADDED CUSTOM1',{'EMP_NM', 'WORKDAY LEADER DIRECTORY.WORKER'}),    #'REORDERED COLUMNS' = TABLE.REORDERCOLUMNS(#'REMOVED COLUMNS2',{'SRC_SYS_KEY_TXT', 'FINAL NAME', 'ORZN_ZONE_CDE', 'ORZN_DEPT_CDE', 'JOB_TYP_CDE', 'EFF_BEG_DT', 'WORKDAY LEADER DIRECTORY.COST CENTER', 'WORKDAY LEADER DIRECTORY.START DATE IN CURRENT JOB OR HIRE DATE'}),    #'RENAMED COLUMNS2' = TABLE.RENAMECOLUMNS(#'REORDERED COLUMNS',{{'FINAL NAME', 'EMP_NM'}}),    #'MERGED QUERIES1' = TABLE.NESTEDJOIN(#'RENAMED COLUMNS2', {'SRC_SYS_KEY_TXT'}, #'MARKET MAPPING', {'ZONE_LEADER_TSID'}, 'MARKET MAPPING', JOINKIND.LEFTOUTER),    #'EXPANDED MARKET MAPPING' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES1', 'MARKET MAPPING', {'HIER_ID'}, {'MARKET MAPPING.HIER_ID'}),    #'RENAMED COLUMNS' = TABLE.RENAMECOLUMNS(#'EXPANDED MARKET MAPPING',{{'ORZN_ZONE_CDE', 'WORKER_STS_FCT_MKT'}, {'MARKET MAPPING.HIER_ID', 'ORZN_ZONE_CDE'}, {'EFF_BEG_DT', 'WORKER_STS_FCT_DT'}, {'WORKDAY LEADER DIRECTORY.START DATE IN CURRENT JOB OR HIRE DATE', 'EFF_BEG_DT'}}),    #'MERGED QUERIES2' = TABLE.NESTEDJOIN(#'RENAMED COLUMNS', {'SRC_SYS_KEY_TXT'}, #'0630 LEADER BACKUP', {'EMPLOYEE ID'}, '0630 LEADER BACKUP', JOINKIND.LEFTOUTER),    #'EXPANDED 0630 LEADER BACKUP' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES2', '0630 LEADER BACKUP', {'EMPLOYEE ID', 'START DATE IN CURRENT JOB OR HIRE DATE'}, {'0630 LEADER BACKUP.EMPLOYEE ID', '0630 LEADER BACKUP.START DATE IN CURRENT JOB OR HIRE DATE'}),    #'FILTERED ROWS1' = TABLE.SELECTROWS(#'EXPANDED 0630 LEADER BACKUP', EACH ([0630 LEADER BACKUP.EMPLOYEE ID] <> NULL)),    #'DATE FILTER' = TABLE.SELECTROWS(#'FILTERED ROWS1', EACH [EFF_BEG_DT] >= #DATE(2023, 1, 1)),    #'JOB CODE FILTER' = TABLE.SELECTROWS(#'DATE FILTER', EACH ([JOB_TYP_CDE] = '001007')),    #'APPEND EXCEPTIONS' = TABLE.COMBINE({#'JOB CODE FILTER', #'DIR EXCEPTIONS'}),    CUSTOM1 = TABLE.ADDCOLUMN(#'APPEND EXCEPTIONS', 'PICTURE', EACH 'HTTPS://MYFIELD.THRIVENT.COM/CONTENT/FAIMAGES/' & [SRC_SYS_KEY_TXT] & '.JPG""",

"""SELECT DISTINCT        
	[DEPARTMENT IDENTIFIER] RFO_CODE       ,
	[DEPARTMENT NAME] RFO_NM       ,
	CASE        WHEN [DEPARTMENT IDENTIFIER]='0383' THEN '0383-VIRTUAL ADVICE'        ELSE CONCAT(TRIM([DEPARTMENT IDENTIFIER]),'-',LEFT([DEPARTMENT NAME],LEN([DEPARTMENT NAME])-13))         END AS NM       
FROM [ENTERPRISEDATAMART].[DM_01].[ORGANIZATION DIMENSION]    WHERE [DEPARTMENT IDENTIFIER] IN ('0283', '0435','0115', '0190',                  '0361', '0384','0291', '0525','0001','0383')         AND [CURRENT ROW INDICATOR] = 'Y'         AND [EFFECTIVE BEGIN DATE] > '1/1/2019'         AND [EFFECTIVE END DATE] = '12/31/9999'""",

""" SELECT DT_SK,         CAL_DAY_DT,         DTRB_PERF_RPT_WK_END_DT,         DTRB_PERF_RPT_WK_NBR,        DTRB_PERF_RPT_YR_NBR,        DTRB_PERF_RPT_YR_WK_NBR,        SRC_SYS_ID,         CRET_TMSP,         LST_UPDT_TMSP,         CRET_USER_ID,         LST_UPDT_USER_ID,         DTRB_PERF_RPT_DAY_TXT,         DTRB_PERF_RPT_MTH_TXT,         DTRB_PERF_RPT_QTR_TXT,         DTRB_PERF_RPT_WK_TXT,        DTRB_PERF_RPT_YR_TXT       FROM COMMON.DTRB_PERF_DATES       WHERE DTRB_PERF_RPT_YR_NBR IN (2023, 2024)']),    #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(SOURCE,{{'CAL_DAY_DT', TYPE DATE}, {'DTRB_PERF_RPT_WK_END_DT', TYPE DATE}})IN    #'CHANGED TYPE
""",

"""SELECT 
	EMP_ID AS TSID, 
	EMP_NM AS EMPLOYEE_NAME, 
	EMP_STS_TYP_DSCR AS EMP_STATUS 
FROM ''HUMAN_RESOURCES''.''CNF_EMP_DIM_DTL_CURR_CFDL'' 
WHERE CURR_ROW_IND = 'Y'']),    #'TRIMMED TEXT' = TABLE.TRANSFORMCOLUMNS(SOURCE,{{'TSID', TEXT.TRIM, TYPE TEXT}})IN    #'TRIMMED TEXT""",

"""SELECT DISTINCT 
	T1.ORZN_DEPT_CDE AS RFO_CDE, 
	--T1.ORZN_ZONE_CDE AS RFO_ZONE_NM, 
	T1.ORZN_SUB_DEPT_CDE AS MVP_ID, 
	--(SUBSTRING(T1.ORZN_ZONE_CDE,6,5)) AS MARKET_ID, 
	T3.ORZN_DEPT_DSCR, 
	T3.EMP_ID, 
	T3.EMP_STS_TYP_CDE, 
	T3.TRMN_DT, 
	T3.JOB_TYP_CDE, 
	T3.JOB_TYP_DSCR, 
	T3.EMP_NM, 
	CASE WHEN T3.EMP_NM IS NULL THEN 'VACANT' ELSE T3.EMP_NM END AS MARKET_LEADERS 
FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN ( SELECT DISTINCT T1.SALE_HIER_ID, T1.ORZN_ZONE_CDE, T1.ORZN_DEPT_DSCR, T1.ORZN_SUB_DEPT_CDE, T2.ORZN_DEPT_CDE, T2.EMP_ID, T2.EMP_STS_TYP_CDE, T2.TRMN_DT, T2.JOB_TYP_CDE, T2.JOB_TYP_DSCR, T2.EMP_NM FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN HUMAN_RESOURCES.CNF_EMP_DIM_DTL_CURR_CFDL T2 ON T1.SALE_HIER_ID = T2.EMP_ID WHERE T1.CURR_ROW_IND = 'Y' AND T2.EMP_STS_TYP_CDE IN ('A') AND T1.EFF_END_TMSP IS NULL AND T2.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL AND T2.JOB_TYP_CDE IN ('001001') ) T3 ON T1.ORZN_SUB_DEPT_CDE = T3.ORZN_SUB_DEPT_CDE WHERE T1.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL --AND T1.RFO_ZONE_STS_CDE IN ('A') --AND T3.EMP_STS_TYP_CDE = 'A' --AND SUBSTRING(T1.ORZN_ZONE_CDE,6,2) <> '00' AND T1.ORZN_ZONE_CDE <> 'UKWN' ORDER BY T1.ORZN_SUB_DEPT_CDE; ']),    #'FILTERED ROWS' = TABLE.SELECTROWS(SOURCE, EACH ([RFO_CDE] = '0001      ' OR [RFO_CDE] = '0115      ' OR [RFO_CDE] = '0190      ' OR [RFO_CDE] = '0283      ' OR [RFO_CDE] = '0291      ' OR [RFO_CDE] = '0361      ' OR [RFO_CDE] = '0383      ' OR [RFO_CDE] = '0384      ' OR [RFO_CDE] = '0435      ' OR [RFO_CDE] = '0525      ' OR [RFO_CDE] = '0716      ') AND ([MVP_ID] <> '          ' AND [MVP_ID] <> '0529-00   '))IN    #'FILTERED ROWS
""",

"""SELECT DISTINCT        [DEPARTMENT IDENTIFIER] RFO_CODE       ,
[DEPARTMENT NAME] RFO_NM       ,CONCAT(TRIM([DEPARTMENT IDENTIFIER]),'-',[DEPARTMENT NAME]) AS NM       
FROM [ENTERPRISEDATAMART].[DM_01].[ORGANIZATION DIMENSION]    
WHERE [DEPARTMENT IDENTIFIER] IN ('0283', '0365', '0435', '0496', '0810','0115', '0165', '0190', '0240',                  
'0361', '0384','0291', '0410', '0475', '0525', '0529','0001','0383')         
AND [CURRENT ROW INDICATOR] = 'Y'         
AND [EFFECTIVE BEGIN DATE] > '1/1/2019'']),    
#'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(SOURCE,{{'RFO_CODE', TYPE TEXT}, {'RFO_NM', TYPE TEXT}, 
# {'NM', TYPE TEXT}}),    #'RENAMED COLUMNS' = TABLE.RENAMECOLUMNS(#'CHANGED TYPE',{{'NM', 'TAG'}}),    
# #'FILTERED ROWS' = TABLE.SELECTROWS(#'RENAMED COLUMNS', EACH ([RFO_CODE] = '0115' OR [RFO_CODE] = '0190' 
# OR [RFO_CODE] = '0283' OR [RFO_CODE] = '0291' OR [RFO_CODE] = '0361' OR [RFO_CODE] = '0384'
#  OR [RFO_CODE] = '0435' OR [RFO_CODE] = '0525')),    #'FILTERED ROWS1' = 
# TABLE.SELECTROWS(#'FILTERED ROWS', EACH NOT TEXT.CONTAINS([RFO_NM], 'REGION')),  
#   #'RENAMED COLUMNS1' = TABLE.RENAMECOLUMNS(#'FILTERED ROWS1',{{'TAG', 'ADVISOR GROUP'}}),    
# #'SPLIT COLUMN BY POSITION' = TABLE.SPLITCOLUMN(#'RENAMED COLUMNS1', 'ADVISOR GROUP',
#  SPLITTER.SPLITTEXTBYPOSITIONS({0, 14}, TRUE), {'ADVISOR GROUP.1', 'ADVISOR GROUP.2'}),  
#   #'CHANGED TYPE1' = TABLE.TRANSFORMCOLUMNTYPES(#'SPLIT COLUMN BY POSITION',{{'ADVISOR GROUP.1', TYPE TEXT}, 
# {'ADVISOR GROUP.2', TYPE TEXT}}),    #'RENAMED COLUMNS2' = TABLE.RENAMECOLUMNS(#'CHANGED TYPE1',
# {{'ADVISOR GROUP.1', 'ADVISOR GROUP'}})IN    #'RENAMED COLUMNS2
"""

# Function to check if column is complete
def is_balanced(val):
    count_open_sqr = val.count("[")
    count_clse_sqr = val.count("]")
    count_open_flr = val.count("{")
    count_clse_flr = val.count("{")
    count_open_brc = val.count("(")
    count_clse_brc = val.count(")")
    count_quote = val.count("'")
    if ((count_open_sqr - count_clse_sqr) != 0 or (count_open_flr - count_clse_flr) != 0 or
        (count_open_brc - count_clse_brc) != 0 or (count_quote % 2 != 0)):
        return False
    else:
        # Check if Case statements are complete or not
        if val.strip().startswith("CASE ") and (" END " not in val or " AS " not in val):
            return False
        else:
            return True 

# Function to clean and simplify the SQL query
def clean_sql_query(query):
    query = re.sub(r"(\n|\t|\r)", " ", query)  # Remove new lines and tabs
    query = re.sub(r"\s+", " ", query)  # Replace multiple spaces with a single space
    return query.strip()

# Function to extract tables and columns from the SELECT part of the query
def extract_columns(query):
    # Set to hold extracted columns
    columns = set()

    # Patterns to identify source columns and tables
    column_pattern = re.compile(r"SELECT\s+(.*?)\s+FROM", re.IGNORECASE)

    # Clean and split the query into components
    cleaned_query = clean_sql_query(query)

    # Extract columns from SELECT clause
    select_match = re.search(column_pattern, cleaned_query)
    if select_match:
        columns_clause = select_match.group(1)
        if columns_clause.startswith("DISTINCT "):
            columns_clause = columns_clause[8:]

        columnArr = columns_clause.split(",")
        i = 0
        while i < len(columnArr):
            val = columnArr[i]
            while (not is_balanced(val) and (i < len(columnArr) - 1)):
                i += 1
                val += "," + columnArr[i]
            columns.add(val)
            i += 1

    return columns


# Function to split a column into output and alias
def split_column_and_alias(column):
    column = column.strip()
    contains_table_ref = "." in column
    columnArr = column.split(" ")
    n = len(columnArr)
    output_column = columnArr[n - 1]

    # Check if there is a table reference in column name
    if "." in output_column:
        output_columnArr = output_column.split(".")
        m = len(output_columnArr)
        output_column = output_columnArr[m - 1]
    
    # Check if there is a [] format to extract the columns
    input_columns = []
    res = re.findall(r'\[.*?\]', column)
    if not res:
        if ("AS" in columnArr) and (columnArr[n - 2] == "AS"):
            input_columnArr = columnArr[: n - 2]
        else:
            input_columnArr = columnArr[: n - 1]

        for col in input_columnArr:
            if "." in col:
                val = col.split(".")[1]
                res = re.split(r"[^_a-zA-Z0-9\s]", val)
                val = res[0]
                if val not in input_columns and not val.isnumeric():
                    input_columns.append(val)   
            else:
                if not contains_table_ref:
                    res = re.split(r"[^_a-zA-Z0-9\s]", col)
                    for val in res:
                        if (val not in KEYWORDS) and (len(val) > 1 and not val.isnumeric()):
                            if val not in input_columns:
                                input_columns.append(val)                                
    
    else:
        if column.endswith("]"):
            total = len(res)
            if total > 1:
                for i in range(total - 1):
                    val = str(res[i]).replace("[", "").replace("]", "")
                    if val not in input_columns and not val.isnumeric():
                        input_columns.append(val)
                val = str(res[total - 1]).replace("[", "").replace("]", "")
                output_column = val
            else:
                val = str(res[0]).replace("[", "").replace("]", "")
                output_column = val
        else:
            for col in res:
                val = str(col).replace("[", "").replace("]", "")
                if val not in input_columns and not val.isnumeric():
                    input_columns.append(val)

    return input_columns, output_column

# Clean the SQL query and extract columns
cleaned_query = clean_sql_query(query)
columns = extract_columns(query)

# Prepare data for DataFrame
data = []
for column in columns:
    input_columns, output_column = split_column_and_alias(column)
    for input_col in input_columns:
        data.append({'Query': cleaned_query, 'Output Column': output_column, 'Source Column': input_col})

# Create DataFrame and save to Excel
df = pd.DataFrame(data)

# Save the DataFrame to an Excel file
excel_file_path = 'output_columns.xlsx'
df.to_excel(excel_file_path, index=False)

print(f'Data saved to {excel_file_path}')
