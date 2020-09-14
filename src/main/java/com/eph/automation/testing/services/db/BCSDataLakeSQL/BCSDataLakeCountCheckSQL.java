/* Created by Nishant @ 04 Aug 2020 */
package com.eph.automation.testing.services.db.BCSDataLakeSQL;

public class BCSDataLakeCountCheckSQL {

    public static String GET_BCS_CLASSIFICATION_SOURCE_COUNT =
            "SELECT  count(*)as Source_Count FROM  (bcs_ingestion_database_sit.initial_ingest f\n" +
            "CROSS JOIN UNNEST(\"distributionclassification\") x (cl))";

    public static String GET_BCS_CLASSIFICATION_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_classification";

    public static String GET_BCS_CONTENT_SOURCE_COUNT = "SELECT count(*) as Source_Count FROM bcs_ingestion_database_sit.initial_ingest df";

    public static String GET_BCS_CONTENT_CURRENT_COUNT="select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_content";

}
