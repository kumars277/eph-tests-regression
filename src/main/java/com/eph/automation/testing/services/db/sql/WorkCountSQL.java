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
    "WHERE T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES')";

    public static String PMX_STG_WORKS_COUNT = "select count (*) as workCountPMXSTG from ephsit.ephsit_talend_owner.stg_pmx_wwork";

    public static String PMX_STG_WORKS_COUNT_Distinct = "  select count(distinct \"PRODUCT_WORK_ID\") as workCountPMXSTG from ephsit_talend_owner.stg_pmx_wwork";

    public static String EPH_SA_WORKS_COUNT = "select count (*) as workCountEPH from ephsit.semarchy_eph_mdm.sa_wwork " +
            "join semarchy_eph_mdm.sa_event on f_event = event_id and f_event = (select max (f_event) from semarchy_eph_mdm.sa_wwork) " +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'";


    public static String EPH_GD_WORKS_COUNT = "select count (*) as workCountEPH from ephsit.semarchy_eph_mdm.GD_wwork";

    public static String EPH_AE_WORKS_COUNT = "select count (*) as errorsCountEPH from ephsit.semarchy_eph_mdm.ae_wwork";
}
