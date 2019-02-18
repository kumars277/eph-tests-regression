package com.eph.automation.testing.services.db.sql;

public class ProductCountSQL {
    public static String PMX_PRODUCT_Count="SELECT count(*) as pmxCount FROM GD_PRODUCT_MANIFESTATION M\n" +
            "LEFT JOIN GD_PRODUCT_SUBSTATUS MSS ON M.F_MANIFESTATION_SUBSTATUS = MSS.PRODUCT_SUBSTATUS_ID";

    public static String EPH_STG_PRODUCT_Count="select count(*) as stgCount from ephsit_talend_owner.stg_pmx_product";

    public static String EPH_STG_PRODUCT_Count_BOOKS="select count(*) as booksCount from ephsit_talend_owner.stg_pmx_product where \"ONE_OFF_ACCESS\" = 'Y'";

    public static String EPH_STG_PRODUCT_Count_Sub = "select count(*) as subCount from ephsit_talend_owner.stg_pmx_product where \"SUBSCRIPTION\" = 'Y'";

    public static String EPH_STG_PRODUCT_Count_Bulk="select count(*) as bulkCount from ephsit_talend_owner.stg_pmx_product where \"SUBSCRIPTION\" = 'Y' and \"BULK_SALES\" = 'Y'";

    public static String EPH_STG_PRODUCT_Count_Reprint="select count(*) as reprintCount from ephsit_talend_owner.stg_pmx_product where \"SUBSCRIPTION\" = 'Y' and \"REPRINTS\" = 'Y'";

    public static String EPH_STG_PRODUCT_Count_Back="select count(*) as backCount from ephsit_talend_owner.stg_pmx_product where \"SUBSCRIPTION\" = 'Y' and \"BACK_FILES\" = 'Y'";

    public static String EPH_STG_PRODUCT_Count_OA="select count(*) as oaCount from \n" +
            "(select \"F_PRODUCT_WORK\" from ephsit_talend_owner.stg_pmx_product where  \"SUBSCRIPTION\" ='Y' and  \"OPEN_ACCESS\" = 'Y' group by \"F_PRODUCT_WORK\" having count(*) = 1) a";

    public static String EPH_STG_PRODUCT_Count_OA_More="select count(*) as oaMoreCount from \n" +
            "(select \"F_PRODUCT_WORK\" from ephsit_talend_owner.stg_pmx_product where \"SUBSCRIPTION\" ='Y' and \"OPEN_ACCESS\" = 'Y'  group by \"F_PRODUCT_WORK\" having count(*) > 1) a";

    public static String EPH_STG_PRODUCT_Count_AC="select count(*) as acCount from \n" +
            "(select \"F_PRODUCT_WORK\" from ephsit_talend_owner.stg_pmx_product where  \"SUBSCRIPTION\" ='Y' and  \"AUTHOR_CHARGES\" = 'Y' group by \"F_PRODUCT_WORK\" having count(*) = 1) a";

    public static String EPH_STG_PRODUCT_Count_AC_More="select count(*) as acMoreCount from \n" +
            "(select \"F_PRODUCT_WORK\" from ephsit_talend_owner.stg_pmx_product where  \"SUBSCRIPTION\" ='Y' and  \"AUTHOR_CHARGES\" = 'Y' group by \"F_PRODUCT_WORK\" having count(*) > 1) a";


    public static String EPH_SA_PRODUCT_Count="select count(*) as ephSACount FROM semarchy_eph_mdm.sa_product sa\n" +
            "join semarchy_eph_mdm.sa_event on f_event = event_id and f_event = (select max (f_event) from semarchy_eph_mdm.sa_product)\n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'";

    public static String EPH_GD_PRODUCT_Count="select count(*) as ephGDCount FROM semarchy_eph_mdm.gd_product";
}