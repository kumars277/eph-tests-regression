/* Created by Nishant @ 04 Aug 2020 */
package com.eph.automation.testing.services.db.BCSDataLakeSQL;

public class BCSDataLakeCountCheckSQL {

    public static String GET_BCS_CLASSIFICATION_SOURCE_COUNT =
            "SELECT  count(*)as Source_Count FROM  (bcs_ingestion_database_sit.initial_ingest f\n" +
            "CROSS JOIN UNNEST(\"distributionclassification\") x (cl))";

    public static String GET_BCS_CLASSIFICATION_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_classification";

    public static String GET_BCS_CONTENT_SOURCE_COUNT =
            "SELECT count(*) as Source_Count FROM bcs_ingestion_database_sit.initial_ingest df";

    public static String GET_BCS_CONTENT_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_content";

    public static String GET_BCS_EXTOBJECT_SOURCE_COUNT=
            "select count(*)  as Source_Count from (bcs_ingestion_database_sit.initial_ingest df\n" +
            "CROSS JOIN UNNEST(\"productexternalobjects\") x (cj))";

    public static String GET_BCS_EXTOBJECT_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_extobject";


    public static String GET_BCS_FULLVERSIONFAMILY_SOURCE_COUNT=
            "select count(*) as Source_Count from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(\"contentfullversionfamily\") x (fv))";

   public static String GET_BCS_FULLVERSIONFAMILY_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_fullversionfamily";

    public static String GET_BCS_ORIGINATORADDRESS_SOURCE_COUNT=
            "with originator as (\n" +
                    "  select\n" +
                    "    \"df\".\"metainfdeleted\" \"metadeleted\"\n" +
                    "  , \"df\".\"metainfmodifiedon\" \"metamodifiedon\"\n" +
                    "  --, \"df\".\"productprojectno\" \"sourceref\"\n" +
                    "  , \"co\".\"businesspartnerid\"         \"businesspartnerid\"\n" +
                    "  , \"co\".\"authoraddress\"\n" +
                    "  from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "  CROSS JOIN UNNEST(\"contactsoriginators\") x (\"co\"))),\n" +
                    "  address as (\n" +
                    "  select\n" +
                    "     \"uo\".\"metadeleted\"\n" +
                    "   , \"uo\".\"metamodifiedon\"\n" +
                    "--   , \"uo\".\"sourceref\"\n" +
                    "   , \"uo\".\"businesspartnerid\"\n" +
                    "   , \"ua\".\"addressline2\"\n" +
                    "   , \"ua\".\"country\"\n" +
                    "   , \"ua\".\"fax\"\n" +
                    "   , \"ua\".\"street\"\n" +
                    "   , \"ua\".\"additionaladdress\"\n" +
                    "   , \"ua\".\"city\"\n" +
                    "   , \"ua\".\"addressline1\"\n" +
                    "   , \"ua\".\"addressid\"\n" +
                    "   , \"ua\".\"telephonemain\"\n" +
                    "   , \"ua\".\"ismainaddress\"\n" +
                    "   , \"ua\".\"internet\"\n" +
                    "   , \"ua\".\"houseno\"\n" +
                    "   , \"ua\".\"email\"\n" +
                    "   , \"ua\".\"addressline3\"\n" +
                    "   , \"ua\".\"telephoneother\"\n" +
                    "   , \"ua\".\"postalcode\"\n" +
                    "   , \"ua\".\"mobile\"\n" +
                    "   , \"ua\".\"district\"\n" +
                    "   from (\"originator\" \"uo\"\n" +
                    "   CROSS JOIN UNNEST(\"authoraddress\") z (\"ua\")))\n" +
                    "select count(*) as Source_Count from (select distinct\n" +
                    "* from \"address\")";
    public static String GET_BCS_ORIGINATORADDRESS_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_originatoraddress";

    public static String GET_BCS_ORIGINATORS_SOURCE_COUNT=
            "select count(*) as Source_Count from (bcs_ingestion_database_sit.initial_ingest df\n" +
            "CROSS JOIN UNNEST(\"contactsoriginators\") x (\"co\"))";

    public static String GET_BCS_ORIGINATORS_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_originators";

    public static String GET_BCS_PRICING_SOURCE_COUNT=
            "select count(*) as Source_Count from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(\"productprice\") x (\"cp\"))";

    public static String GET_BCS_PRICING_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_pricing";

    public static String GET_BCS_PRODUCT_SOURCE_COUNT=
            "select count(*) as Source_Count from (bcs_ingestion_database_sit.initial_ingest df)";

    public static String GET_BCS_PRODUCT_CURRENT_COUNT=
            "select count(*) as Current_Count from \n" +
                    "bcs_ingestion_database_sit.stg_current_product";

    public static String GET_BCS_PRODUCTION_SOURCE_COUNT=
            "select count(*) as Source_Count from (bcs_ingestion_database_sit.initial_ingest df)";

    public static String GET_BCS_PRODUCTION_CURRENT_COUNT=
            "select  count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_production";

    public static String GET_BCS_RELATIONS_SOURCE_COUNT=
            "select  count(*) as Source_Count from (bcs_ingestion_database_sit.initial_ingest df \n" +
                    "CROSS JOIN UNNEST(\"productrelation\") x (\"cj\"))";

    public static String GET_BCS_RELATIONS_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_relations";

    public static String GET_BCS_RESPONSIBILITIES_SOURCE_COUNT=
            "select COUNT(*) as Source_Count from (bcs_ingestion_database_sit.initial_ingest df \n" +
                    "CROSS JOIN UNNEST(\"contactsresponsibilities\") x (\"cj\"))";

    public static String GET_BCS_RESPONSIBILITIES_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_responsibilities";

    public static String GET_BCS_SUBLOCATION_SOURCE_COUNT=
            "select COUNT(*) as Source_Count from (bcs_ingestion_database_sit.initial_ingest df \n" +
                    "CROSS JOIN UNNEST(\"productsublocation\") x (\"cj\"))";

    public static String GET_BCS_SUBLOCATION_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_sublocation";

    public static String GET_BCS_TEXT_SOURCE_COUNT=
            "select COUNT(*) as Source_Count from bcs_ingestion_database_sit.\"initial_ingest\" \"df\"\n" +
                    "CROSS JOIN UNNEST(\"distributiontext\") x (\"cj\")";

    public static String GET_BCS_TEXT_CURRENT_COUNT=
            "select COUNT(*) as Current_Count from bcs_ingestion_database_sit.stg_current_text";

    public static String GET_BCS_VERSIONFAMILY_SOURCE_COUNT=
            "select count(*) as Source_Count from bcs_ingestion_database_sit.\"initial_ingest\" \"df\"\n" +
                    "CROSS JOIN UNNEST(\"contentversionfamily\") x (\"cj\")";

    public static String GET_BCS_VERSIONFAMILY_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_sit.stg_current_versionfamily";







}
