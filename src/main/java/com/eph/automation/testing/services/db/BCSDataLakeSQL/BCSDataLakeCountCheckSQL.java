/* Created by Nishant @ 04 Aug 2020 */
package com.eph.automation.testing.services.db.BCSDataLakeSQL;

public class BCSDataLakeCountCheckSQL {

    public static String GET_BCS_CLASSIFICATION_SOURCE_COUNT =
            "SELECT  count(*) FROM  (bcs_ingestion_database_sit.initial_ingest f\n" +
            "CROSS JOIN UNNEST(\"distributionclassification\") x (cl))";

    public static String GET_BCS_CURRENT_CLASSIFICATION_COUNT=
            "select count(*) from bcs_ingestion_database_sit.stg_current_classification";


}
