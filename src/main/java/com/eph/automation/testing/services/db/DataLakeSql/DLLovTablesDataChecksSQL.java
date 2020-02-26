package com.eph.automation.testing.services.db.DataLakeSql;

public class DLLovTablesDataChecksSQL {

    public static String getRandomLovCodes(String db){
           String GET_RANDOM_LOV_CODE = "select code as LOV_CODE from " +db+".%s order by random() limit %s";
    return GET_RANDOM_LOV_CODE;
    }

    public static String EPHlovDataBuildSql(String db , String tableName) {
        String extendedColumns = "";

//      Inserts table specific columns into SQL statement
        if ("gd_x_lov_event_type".equals(tableName)) {
            extendedColumns = ",LEVEL_2_EVENT as LEVEL_2_EVENT \n" +
                    ",LEVEL_3_EVENT as LEVEL_3_EVENT \n";
        } else if ("gd_x_lov_identifier_type".equals(tableName)) {
            extendedColumns = ",VALID_AT_WORK as VALID_AT_WORK \n" +
                    ",VALID_AT_MANIFESTATION as VALID_AT_MANIFESTATION \n" +
                    ",VALID_AT_PRODUCT as VALID_AT_PRODUCT \n" +
                    ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                    ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n";
        }
        else if ("gd_x_lov_manif_status".equals(tableName)) {
            extendedColumns = ",ROLL_UP_STATUS as ROLL_UP_STATUS \n" +
                    ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                    ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n";
        }
        else if ("gd_x_lov_manif_type".equals(tableName)) {
            extendedColumns = ",ROLL_UP_TYPE as ROLL_UP_TYPE \n";
        }
        else if ("gd_x_lov_pmc".equals(tableName)) {
            extendedColumns = ",F_PMG as F_PMG \n";
        }
        else if ("gd_x_lov_product_status".equals(tableName)) {
            extendedColumns = ",ROLL_UP_STATUS as ROLL_UP_STATUS \n" +
                    ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                    ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n" +
                    ",VALID_FOR_DIGITAL_PACKAGE as VALID_FOR_DIGITAL_PACKAGE \n";
        }
        else if ("gd_x_lov_relationship_type".equals(tableName)) {
            extendedColumns = ",PARENT_DESCRIPTION as PARENT_DESCRIPTION \n" +
                    ",CHILD_DESCRIPTION as CHILD_DESCRIPTION \n";
        }
        else if ("gd_x_lov_work_status".equals(tableName)) {
            extendedColumns = ",ROLL_UP_STATUS as ROLL_UP_STATUS \n" +
                    ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                    ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n";
        }
        else if ("gd_x_lov_work_type".equals(tableName)) {
            extendedColumns = ",ROLL_UP_TYPE as ROLL_UP_TYPE \n";
        }
        else if ("gd_x_lov_society_ownership".equals(tableName)) {
            extendedColumns = ",ROLL_UP_OWNERSHIP as ROLL_UP_OWNERSHIP \n";
        }

//      Core columns SQL statement
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
                extendedColumns +
            "from "+ db + ".%s \n" +
            "where CODE in ('%s')";

        return GET_DATA_LOV_EPH;
    }

    public static String DLlovDataBuildSql(String db,  String tableName ) {
        String extendedColumns = "";

        //      Inserts table specific columns into SQL statement
        if ("gd_x_lov_event_type".equals(tableName)) {
            extendedColumns = ",LEVEL_2_EVENT as LEVEL_2_EVENT \n" +
                    ",LEVEL_3_EVENT as LEVEL_3_EVENT \n";
        }else if ("gd_x_lov_identifier_type".equals(tableName)) {
            extendedColumns = ",VALID_AT_WORK as VALID_AT_WORK \n" +
                    ",VALID_AT_MANIFESTATION as VALID_AT_MANIFESTATION \n" +
                    ",VALID_AT_PRODUCT as VALID_AT_PRODUCT \n" +
                    ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                    ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n";
        }
        else if ("gd_x_lov_manif_status".equals(tableName)) {
            extendedColumns = ",ROLL_UP_STATUS as ROLL_UP_STATUS \n" +
                    ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                    ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n";
        }
        else if ("gd_x_lov_manif_type".equals(tableName)) {
            extendedColumns = ",ROLL_UP_TYPE as ROLL_UP_TYPE \n";
        }
        else if ("gd_x_lov_pmc".equals(tableName)) {
            extendedColumns = ",F_PMG as F_PMG \n";
        }
        else if ("gd_x_lov_product_status".equals(tableName)) {
            extendedColumns = ",ROLL_UP_STATUS as ROLL_UP_STATUS \n" +
                    ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                    ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n" +
                    ",VALID_FOR_DIGITAL_PACKAGE as VALID_FOR_DIGITAL_PACKAGE \n";
        }
        else if ("gd_x_lov_relationship_type".equals(tableName)) {
            extendedColumns = ",PARENT_DESCRIPTION as PARENT_DESCRIPTION \n" +
                    ",CHILD_DESCRIPTION as CHILD_DESCRIPTION \n";
        }
        else if ("gd_x_lov_work_status".equals(tableName)) {
            extendedColumns = ",ROLL_UP_STATUS as ROLL_UP_STATUS \n" +
                    ",VALID_FOR_BOOKS as VALID_FOR_BOOKS \n" +
                    ",VALID_FOR_JOURNALS as VALID_FOR_JOURNALS \n";
        }
        else if ("gd_x_lov_work_type".equals(tableName)) {
            extendedColumns = ",ROLL_UP_TYPE as ROLL_UP_TYPE \n";
        }
        else if ("gd_x_lov_society_ownership".equals(tableName)) {
            extendedColumns = ",ROLL_UP_OWNERSHIP as ROLL_UP_OWNERSHIP \n";
        }

//        Core columns SQL
        String GET_DATA_LOV_DL = "select \n" +
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
                 extendedColumns +
                "from " +db+ ".%s \n" +
                "where CODE in ('%s')";

        return GET_DATA_LOV_DL;
    }


}
