package com.eph.automation.testing.services.db.DataLakeSql;

/**
 * Created by Thomas Kruck on 15/02/2020
 */

public class DLLovTablesDataChecksSQL {

    public static String getRandomLovCodes(String db){
           String GET_RANDOM_LOV_CODE = "select code as LOV_CODE from " +db+".%s order by random() limit %s";
    return GET_RANDOM_LOV_CODE;
    }

    public static String gd_x_Lov_event_type (String db) {
       String GET_GD_X_Lov_EVENT_TYPE = "select \n" +
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
                ",LEVEL_2_EVENT as LEVEL_2_EVENT \n" +
                ",LEVEL_3_EVENT as LEVEL_3_EVENT \n"+
            "from "+ db + ".%s \n" +
            "where CODE in ('%s')";

        return GET_GD_X_Lov_EVENT_TYPE;
    }

    public static String gd_x_lov_identifier_type (String db) {
        String GET_GD_X_Lov_Identifier_Type = "select \n" +
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
                ",VALID_AT_WORK as VALID_AT_WORK \n" +
                ",VALID_AT_MANIFESTATION as VALID_AT_MANIFESTATION \n" +
                ",VALID_AT_PRODUCT as VALID_AT_PRODUCT \n" +
                ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n" +
                "from "+ db + ".%s \n" +
                "where CODE in ('%s')";

        return GET_GD_X_Lov_Identifier_Type;
    }

    public static String gd_x_lov_manif_status (String db) {
        String GET_GD_X_Lov_Manif_Status = "select \n" +
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
                ",ROLL_UP_STATUS as ROLL_UP_STATUS \n" +
                ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n" +
                "from "+ db + ".%s \n" +
                "where CODE in ('%s')";

        return GET_GD_X_Lov_Manif_Status;
    }

    public static String gd_x_lov_manif_type (String db) {
        String GET_GD_X_Lov_Manif_Type = "select \n" +
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
                ",ROLL_UP_TYPE as ROLL_UP_TYPE \n" +
                "from "+ db + ".%s \n" +
                "where CODE in ('%s')";

        return GET_GD_X_Lov_Manif_Type;
    }

    public static String gd_x_lov_pmc (String db) {
        String GET_GD_X_Lov_PMC = "select \n" +
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
                ",F_PMG as F_PMG \n" +
                "from "+ db + ".%s \n" +
                "where CODE in ('%s')";

        return GET_GD_X_Lov_PMC ;
    }

    public static String gd_x_lov_product_status (String db) {
        String GET_GD_X_Lov_Product_Status = "select \n" +
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
                ",ROLL_UP_STATUS as ROLL_UP_STATUS \n" +
                ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n" +
                ",VALID_FOR_DIGITAL_PACKAGE as VALID_FOR_DIGITAL_PACKAGE \n" +
                "from "+ db + ".%s \n" +
                "where CODE in ('%s')";

        return GET_GD_X_Lov_Product_Status ;
    }

    public static String gd_x_lov_relationship_type (String db) {
        String GET_GD_X_Lov_Relationship_Type = "select \n" +
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
                ",PARENT_DESCRIPTION as PARENT_DESCRIPTION \n" +
                ",CHILD_DESCRIPTION as CHILD_DESCRIPTION \n" +
                "from "+ db + ".%s \n" +
                "where CODE in ('%s')";

        return GET_GD_X_Lov_Relationship_Type ;
    }

    public static String gd_x_lov_work_status (String db) {
        String GET_GD_X_Lov_Work_Status = "select \n" +
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
                ",ROLL_UP_STATUS as ROLL_UP_STATUS \n" +
                ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n" +
                "from "+ db + ".%s \n" +
                "where CODE in ('%s')";

        return GET_GD_X_Lov_Work_Status ;
    }

    public static String gd_x_lov_work_type (String db) {
        String GET_GD_X_Lov_Work_Type = "select \n" +
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
                ",ROLL_UP_TYPE as ROLL_UP_TYPE \n" +
                "from "+ db + ".%s \n" +
                "where CODE in ('%s')";

        return GET_GD_X_Lov_Work_Type ;
    }

    public static String gd_x_lov_society_ownership (String db) {
        String GET_GD_X_Lov_Society_Ownership = "select \n" +
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
                ",ROLL_UP_OWNERSHIP as ROLL_UP_OWNERSHIP \n" +
                "from "+ db + ".%s \n" +
                "where CODE in ('%s')";

        return GET_GD_X_Lov_Society_Ownership ;
    }

      public static String gd_x_lov_core_sql (String db){
        String GET_DATA_LOV_CORE = "select \n" +
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

        return GET_DATA_LOV_CORE;
    }


}
