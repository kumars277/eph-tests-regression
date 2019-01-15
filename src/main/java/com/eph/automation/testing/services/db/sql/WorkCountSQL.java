package com.eph.automation.testing.services.db.sql;

public class WorkCountSQL {

    public static String PMX_WORKS_COUNT = "select count (*) as workCountPmx from pmx.gd_product_work";

    public static String PMX_STG_WORKS_COUNT = "select count (*) as workCountPMXSTG from stg_product_work";

    public static String EPH_SA_WORKS_COUNT = "select count (*) as workCountEPH from ephsit.semarchy_eph_mdm.sa_wwork";

    public static String EPH_AE_WORKS_COUNT = "select count (*) as errorsCountEPH from ephsit.semarchy_eph_mdm.ae_wwork";
}
