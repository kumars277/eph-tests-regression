package com.eph.automation.testing.services.db.sql;

public class ProductCountSQL {
    public static String PMX_PRODUCT_Count="SELECT count(*) as pmxCount FROM GD_PRODUCT_MANIFESTATION M\n" +
            "LEFT JOIN GD_PRODUCT_SUBSTATUS MSS ON M.F_MANIFESTATION_SUBSTATUS = MSS.PRODUCT_SUBSTATUS_ID";

    public static String EPH_STG_PRODUCT_Count="select count(*) as stgCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product";

    public static String EPH_STG_PRODUCT_Count_BOOKS="select count(*) as booksCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"ONE_OFF_ACCESS\" = 'Y'";

    public static String EPH_STG_PRODUCT_Count_Sub = "select count(*) as subCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"SUBSCRIPTION\" = 'Y'";

    public static String EPH_STG_PRODUCT_Count_Bulk="select count(*) as bulkCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"WORK_TYPE\" = 'JOURNAL' and \"BULK_SALES\" = 'Y'";

    public static String EPH_STG_PRODUCT_Count_Reprint="select count(*) as reprintCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"WORK_TYPE\" = 'JOURNAL' and \"REPRINTS\" = 'Y'";

    public static String EPH_STG_PRODUCT_Count_Back="select count(*) as backCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"WORK_TYPE\" = 'JOURNAL' and \"BACK_FILES\" = 'Y'";

    public static String EPH_STG_PRODUCT_Count_OA="select count(*) as oaCount from \n" +
            "(select \"F_PRODUCT_WORK\" from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where  \"WORK_TYPE\" = 'JOURNAL' and  \"OPEN_ACCESS\" = 'Y' group by \"F_PRODUCT_WORK\" having count(*) = 1) a";

    public static String EPH_STG_PRODUCT_Count_OA_More="select count(*) as oaMoreCount from \n" +
            "(select \"F_PRODUCT_WORK\" from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"WORK_TYPE\" = 'JOURNAL' and \"OPEN_ACCESS\" = 'Y'  group by \"F_PRODUCT_WORK\" having count(*) > 1) a";

    public static String EPH_STG_PRODUCT_Count_AC="select count(*) as acCount from \n" +
            "(select \"F_PRODUCT_WORK\" from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where  \"WORK_TYPE\" = 'JOURNAL' and  \"AUTHOR_CHARGES\" = 'Y' group by \"F_PRODUCT_WORK\" having count(*) = 1) a";

    public static String EPH_STG_PRODUCT_Count_AC_More="select count(*) as acMoreCount from \n" +
            "(select \"F_PRODUCT_WORK\" from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where  \"WORK_TYPE\" = 'JOURNAL' and  \"AUTHOR_CHARGES\" = 'Y' group by \"F_PRODUCT_WORK\" having count(*) > 1) a";

    public static String EPH_STG_PRODUCT_Packages="select count(*) as packagesCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"PACKAGES\" = 'Y'";

    public static String EPH_STG_PRODUCT_Count_Updated="select count(*) as stgCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product " +
//            "where TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')";
            "where TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI')";


    public static String EPH_STG_PRODUCT_Count_BOOKS_Updated="select count(*) as booksCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"ONE_OFF_ACCESS\" = 'Y' " +
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')";
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI')";

    public static String EPH_STG_PRODUCT_Count_Sub_Updated = "select count(*) as subCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"SUBSCRIPTION\" = 'Y' " +
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')";
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI')";

    public static String EPH_STG_PRODUCT_Count_Bulk_Updated="select count(*) as bulkCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"WORK_TYPE\" = 'JOURNAL' and \"BULK_SALES\" = 'Y' " +
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')";
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI')";


    public static String EPH_STG_PRODUCT_Count_Reprint_Updated="select count(*) as reprintCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"WORK_TYPE\" = 'JOURNAL' and \"REPRINTS\" = 'Y' " +
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')";
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI')";


    public static String EPH_STG_PRODUCT_Count_Back_Updated="select count(*) as backCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"WORK_TYPE\" = 'JOURNAL' and \"BACK_FILES\" = 'Y' " +
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')";
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI')";

    public static String EPH_STG_PRODUCT_Count_OA_Updated="select count(*) as oaCount from \n" +
            "(select \"F_PRODUCT_WORK\" from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where  \"WORK_TYPE\" = 'JOURNAL' and  \"OPEN_ACCESS\" = 'Y' " +
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')\n" +
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI')" +
            " group by \"F_PRODUCT_WORK\" having count(*) = 1) a ";

    public static String EPH_STG_PRODUCT_Count_OA_More_Updated="select count(*) as oaMoreCount from \n" +
            "(select \"F_PRODUCT_WORK\" from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"WORK_TYPE\" = 'JOURNAL' and \"OPEN_ACCESS\" = 'Y' " +
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')" +
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI')" +
            " group by \"F_PRODUCT_WORK\" having count(*) > 1) a ";

    public static String EPH_STG_PRODUCT_Count_AC_Updated="select count(*) as acCount from \n" +
            "(select \"F_PRODUCT_WORK\" from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where  \"WORK_TYPE\" = 'JOURNAL' and  \"AUTHOR_CHARGES\" = 'Y' " +
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')" +
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI')" +
            " group by \"F_PRODUCT_WORK\" having count(*) = 1) a ";

    public static String EPH_STG_PRODUCT_Count_AC_More_Updated="select count(*) as acMoreCount from \n" +
            "(select \"F_PRODUCT_WORK\" from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where  \"WORK_TYPE\" = 'JOURNAL' and  \"AUTHOR_CHARGES\" = 'Y' " +
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')" +
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI')" +
            " group by \"F_PRODUCT_WORK\" having count(*) > 1) a " ;

    public static String EPH_STG_PRODUCT_Packages_Updated="select count(*) as packagesCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product where \"PACKAGES\" = 'Y' " +
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')";
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI')";

    public static String EPH_STG_CAN_Count = "SELECT count(*) as ephCanCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product_dq";

    public static String EPH_STG_CAN_DQ_Count = "SELECT count(*) as ephCanDQCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product_dq pdq left join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq w  on pdq.f_work_source_ref::int = w.pmx_source_reference::int \n" +
            "left join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation_dq m on pdq.f_manifestation_source_ref::int = m.pmx_source_reference::int \n" +
            "where pdq.dq_err != 'Y'";

    public static String EPH_SA_PRODUCT_Count="select count(*) as ephSACount FROM semarchy_eph_mdm.sa_product sa\n" +
           " where f_event =  (select max (event_id) from\n" +
   "semarchy_eph_mdm.sa_event\n"+
    "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
    "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
    "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
    "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )";

    public static String EPH_GD_PRODUCT_Count="select count(*) as ephGDCount FROM semarchy_eph_mdm.gd_product\n" +
            "     where f_event =  (select max (event_id) from\n" +
            "   semarchy_eph_mdm.gd_event\n" +
            "    where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n" +
            "    and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n" +
            "    AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n" +
            "    and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )";

    public static String EPH_AE_PRODUCT_Count="select count(distinct product_id) as aeCount FROM semarchy_eph_mdm.ae_product ae\n" +
            "     where ae.b_batchid =  (select max (b_batchid) from\n" +
            "   semarchy_eph_mdm.gd_event e\n" +
            "    where  e.f_event_type = 'PMX'\n" +
            "    and e.workflow_id = 'talend'\n" +
            "    AND e.f_event_type = 'PMX'\n" +
            "    and e.f_workflow_source = 'PMX' )";
}
