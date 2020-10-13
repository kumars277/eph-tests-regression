//Created by Nishant @ 13 Oct 2020 for EPHD-2145
package com.eph.automation.testing.services.db.BCSDataLakeSQL;

public class BCSCurrentVsHistoryCountCheckSQL {

    public static String GET_BCS_CLASSIFICATION_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
      "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_classification_part " +
      "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_classification_part)";


    public static String GET_BCS_CONTENT_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_content_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_content_part)";

    public static String GET_BCS_EXTOBJECT_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_extobject_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_extobject_part)";

    public static String GET_BCS_FULLVERSIONFAMILY_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_fullversionfamily_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_fullversionfamily_part)";

    public static String GET_BCS_ORIGINATORS_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_originators_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_originators_part)";

    public static String GET_BCS_PRICING_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_pricing_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_pricing_part)";

    public static String GET_BCS_PRODUCT_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_product_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_product_part)";

    public static String GET_BCS_PRODUCTION_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_production_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_production_part)";

    public static String GET_BCS_RELATIONS_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_relations_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_relations_part)";

    public static String GET_BCS_RESPONSIBILITIES_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_responsibilities_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_responsibilities_part)";

    public static String GET_BCS_SUBLOCATION_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_sublocation_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_sublocation_part)";

    public static String GET_BCS_TEXT_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_text_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_text_part)";

    public static String GET_BCS_VERSIONFAMILY_HISTORY_COUNT_FOR_CURRENT_COUNT_VERIFICATION=
       "SELECT count(*) as History_Count FROM bcs_ingestion_database_sit.stg_history_versionfamily_part " +
       "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_sit.stg_history_versionfamily_part)";

}
