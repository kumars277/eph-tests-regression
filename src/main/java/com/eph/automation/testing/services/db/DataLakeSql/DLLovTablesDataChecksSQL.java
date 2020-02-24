package com.eph.automation.testing.services.db.DataLakeSql;

public class DLLovTablesDataChecksSQL {

    public static String GET_RANDOM_LOV_CODE = "select code as LOV_CODE from semarchy_eph_mdm.%s order by random() limit %s";

    public static String lovDataBuildSql(String db){

        String GET_DATA_LOV_EPH = "select \n" +
            "CODE as CODE\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",B_BATCHID as B_BATCHID\n" +
            ",B_CREDATE as B_CREDATE\n" +
            ",B_UPDDATE as B_UPDDATE\n" +
            ",B_CREATOR as B_CREATOR\n" +
            ",B_UPDATOR as B_UPDATOR\n" +
            ",L_DESCRIPTION as L_DESCRIPTION\n" +
            ",L_START_DATE as L_START_DATE\n" +
            ",L_END_DATE as L_END_DATE\n" +
            "from "+ db + ".%s \n" +
            "where CODE in ('%s')";

        return GET_DATA_LOV_EPH;
    }

    public static String GET_DATA_LOV_DL = "select \n" +
            "CODE as CODE\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",B_BATCHID as B_BATCHID\n" +
            ",B_CREDATE as B_CREDATE\n" +
            ",B_UPDDATE as B_UPDDATE\n" +
            ",B_CREATOR as B_CREATOR\n" +
            ",B_UPDATOR as B_UPDATOR\n" +
            ",L_DESCRIPTION as L_DESCRIPTION\n" +
            ",L_START_DATE as L_START_DATE\n" +
            ",L_END_DATE as L_END_DATE\n" +
            "from product_database_sit.%s \n" +
            "where CODE in ('%s')";


}
