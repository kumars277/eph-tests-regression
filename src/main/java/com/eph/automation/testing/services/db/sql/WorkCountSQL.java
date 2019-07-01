package com.eph.automation.testing.services.db.sql;

public class WorkCountSQL {

    public static String PMX_WORKS_COUNT = "SELECT count(*) as workCountPmx\n" +
    "FROM PMX.GD_PRODUCT_WORK W\n" +
    "LEFT JOIN PMX.GD_WORK_ALT_IDENTIFIER A ON W.PRODUCT_WORK_ID = A.F_PRODUCT_WORK AND A.F_ALTERNATIVE_IDENTIFIER_TY = 24\n" +
    "LEFT JOIN PMX.GD_OPEN_ACCESS_JOURNAL_TYPE O ON W.F_OPEN_ACCESS_JOURNAL_TYPE = O.OPEN_ACCESS_JOURNAL_TYPE_\n" +
    "LEFT JOIN PMX.GD_PRODUCT_SUBSTATUS SS ON W.F_WORK_SUBSTATUS = SS.PRODUCT_SUBSTATUS_ID\n" +
    "LEFT JOIN PMX.GD_PRODUCT_STATUS S ON W.F_WORK_STATUS = S.PRODUCT_STATUS_ID\n" +
    "LEFT JOIN PMX.GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
    "LEFT JOIN PMX.GD_IMPRINT I ON W.F_IMPRINT = I.IMPRINT_ID\n" +
    "LEFT JOIN GD_PRODUCT_OWNER PO ON W.PRODUCT_WORK_ID = PO.F_PRODUCT_WORK\n" +
    "LEFT JOIN GD_OWNERSHIP_SUB_TYPE SO ON PO.F_OWNERSHIP_SUB_TYPE = SO.OWNERSHIP_SUB_TYPE_ID\n" +
    "LEFT JOIN (SELECT P.F_PRODUCT_WORK, CASE WHEN C.LANGUAGE_COUNT > 1 THEN 'ZZ' ELSE UPPER(L.LANGUAGE_ISO_CODE) END AS LANGUAGE_CODE\n" +
    "             FROM\n" +
    "\t            (SELECT F_PRODUCT_WORK, COUNT(PRODUCT_LANGUAGE_ID) AS LANGUAGE_COUNT FROM GD_PRODUCT_LANGUAGE GROUP BY F_PRODUCT_WORK) C\n" +
    "             JOIN\n" +
    "\t            GD_PRODUCT_LANGUAGE P  ON C.F_PRODUCT_WORK = P.F_PRODUCT_WORK\n" +
    "             JOIN\n" +
    "\t            GD_LANGUAGES L ON P.F_LANGUAGES = L.LANGUAGES_ID) WL ON W.PRODUCT_WORK_ID = WL.F_PRODUCT_WORK\n" +
    "WHERE T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES')";

    public static String PMX_STG_WORKS_COUNT = "select count (*) as workCountPMXSTG from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork";

    public static String GET_REFRESH_DATE = "select refresh_value as refresh_timestamp from "+GetEPHDBUser.getDBUser()+".etl_run \n" +
            " where loadset_code='10_PMX_FULL' and refresh_value is not null\n"+
            "order by TO_DATE(refresh_value,'YYYYMMDDHH24MI') desc";

    public static String PMX_STG_WORKS_COUNT_DELTA = "select count(distinct \"PRODUCT_WORK_ID\") as workCountPMXSTG from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork\n" +
            "where TO_DATE(\"UPDATED\",'YYYYMMDDHH24MI') > TO_DATE('PARAM1','YYYYMMDDHH24MI')";


    public static String PMX_STG_WORKS_COUNT_Distinct = "  select count(distinct \"PRODUCT_WORK_ID\") as workCountPMXSTG from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork";

    public static String PMX_STG_DQ_WORKS_COUNT = "select count (*) as workCountDQSTG from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq";

    public static String PMX_STG_DQ_WORKS_COUNT_NoErr = "select count (*) as workCountDQSTGnoError from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq where dq_err='N'";

    public static String EPH_SA_WORKS_COUNT = "select count (distinct external_reference) as workCountEPH from semarchy_eph_mdm.sa_wwork "+
            " where f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.sa_event\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )";


    public static String EPH_GD_WORKS_COUNT = "select count (*) as workCountEPHGD from semarchy_eph_mdm.GD_wwork"+
            " where f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.gd_event\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )";

    public static String EPH_AE_WORKS_COUNT = "select count (*) as errorsCountEPH from semarchy_eph_mdm.ae_wwork ae\n"+
            " where ae.b_batchid =  (select max (b_batchid) from\n" +
            "semarchy_eph_mdm.gd_event e\n"+
            "where  e.f_event_type = 'PMX'\n"+
            "and e.workflow_id = 'talend'\n"+
            "AND e.f_event_type = 'PMX'\n"+
            "and e.f_workflow_source = 'PMX' )";
}
