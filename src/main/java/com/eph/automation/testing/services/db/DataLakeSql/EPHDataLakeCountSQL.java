package com.eph.automation.testing.services.db.DataLakeSql;

public  class EPHDataLakeCountSQL {

    public static String EPH_WORK_COUNT = "Select count(*) as EPH_Work_Count from semarchy_eph_mdm.gd_wwork";

    public static String DL_WORK_COUNT = "SELECT count(*) as DL_Work_Count FROM product_database_sit.gd_wwork";



}
