/* Created by Nishant @ 04 Aug 2020 */
//resumed automation on 23 Sep 2020
//updation on 19-04-2021 Dinesh removed hardcoded DB.
package com.eph.automation.testing.services.db.BCSDataLakeSQL;

public class BCSIngestVsCurrentCountCheckSQL {

    public static String GET_BCS_CLASSIFICATION_SOURCE_COUNT =
            "SELECT  count(*)as Source_Count FROM  (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product f CROSS JOIN UNNEST(distributionclassification) x (cl))";

    public static String GET_BCS_CLASSIFICATION_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_classification";

    public static String GET_BCS_CLASSIFICATION_PREVIOUS_COUNT=
            "select count(*) as Previous_Count from  bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_classification";

    public static String GET_BCS_CLASSIFICATION_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_classification_part " +
            "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_classification_part " +
            "where inbound_ts <(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_classification_part))";

    public static String GET_BCS_CONTENT_SOURCE_COUNT =
            "SELECT count(*) as Source_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df";

    public static String GET_BCS_CONTENT_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_content";

    public static String GET_BCS_CONTENT_PREVIOUS_COUNT=
            "select count(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_content";

    public static String GET_BCS_CONTENT_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_content_part " +
             "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_content_part " +
             "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_content_part))";


    public static String GET_BCS_EXTOBJECT_SOURCE_COUNT=
            "select count(*)  as Source_Count from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
            "CROSS JOIN UNNEST(\"productexternalobjects\") x (cj))";

    public static String GET_BCS_EXTOBJECT_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_extobject";

    public static String GET_BCS_EXTOBJECT_PREVIOUS_COUNT=
            "select count(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_extobject";

    public static String GET_BCS_EXTOBJECT_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_extobject_part " +
              "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_extobject_part " +
              "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_extobject_part))";


    public static String GET_BCS_FULLVERSIONFAMILY_SOURCE_COUNT=
            "select count(*) as Source_Count from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(\"contentfullversionfamily\") x (fv))";

   public static String GET_BCS_FULLVERSIONFAMILY_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_fullversionfamily";

    public static String GET_BCS_FULLVERSIONFAMILY_PREVIOUS_COUNT=
            "select count(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_fullversionfamily ";

    public static String GET_BCS_FULLVERSIONFAMILY_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM \"bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_fullversionfamily_part " +
              "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_fullversionfamily_part " +
              "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_fullversionfamily_part))";

    public static String GET_BCS_ORIGINATORADDRESS_SOURCE_COUNT=
            "with originator as (\n" +
                    "  select\n" +
                    "    \"df\".\"metainfdeleted\" \"metadeleted\"\n" +
                    "  , \"df\".\"metainfmodifiedon\" \"metamodifiedon\"\n" +
                    "  --, \"df\".\"productprojectno\" \"sourceref\"\n" +
                    "  , \"co\".\"businesspartnerid\"         \"businesspartnerid\"\n" +
                    "  , \"co\".\"authoraddress\"\n" +
                    "  from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
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
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatoraddress";

    //public static String GET_BCS_ORIGINATORADDRESS_PREVIOUS_COUNT=
    //public static String GET_BCS_ORIGINATORADDRESS_HISTORY_COUNT=

    public static String GET_BCS_ORIGINATORS_SOURCE_COUNT=
            "select count(*) as Source_Count from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
            "CROSS JOIN UNNEST(\"contactsoriginators\") x (\"co\"))";

    public static String GET_BCS_ORIGINATORS_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originators";

    public static String GET_BCS_ORIGINATORS_PREVIOUS_COUNT=
            "select count(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_originators";

    public static String GET_BCS_ORIGINATORS_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originators_part " +
             "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originators_part " +
             "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originators_part))";

    public static String GET_BCS_PRICING_SOURCE_COUNT=
            "select count(*) as Source_Count from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df CROSS JOIN UNNEST(\"productprice\") x (\"cp\"))";

    public static String GET_BCS_PRICING_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_pricing";

    public static String GET_BCS_PRICING_PREVIOUS_COUNT=
            "select count(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_pricing";

    public static String GET_BCS_PRICING_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_pricing_part\" " +
             "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_pricing_part " +
             "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_pricing_part))";

    public static String GET_BCS_PRODUCT_SOURCE_COUNT=
            "select count(*) as Source_Count from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df)";

    public static String GET_BCS_PRODUCT_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_product";

    public static String GET_BCS_PRODUCT_PREVIOUS_COUNT=
            "select count(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_product";

    public static String GET_BCS_PRODUCT_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_product_part " +
             "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_product_part " +
             "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_product_part))";

    public static String GET_BCS_PRODUCTION_SOURCE_COUNT=
            "select count(*) as Source_Count from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df)";

    public static String GET_BCS_PRODUCTION_CURRENT_COUNT=
            "select  count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_production";

    public static String GET_BCS_PRODUCTION_PREVIOUS_COUNT=
            "select  count(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_production ";

    public static String GET_BCS_PRODUCTION_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_production_part " +
             "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_production_part " +
             "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_production_part))";

    public static String GET_BCS_RELATIONS_SOURCE_COUNT=
            "select  count(*) as Source_Count from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df CROSS JOIN UNNEST(\"productrelation\") x (\"cj\"))";

    public static String GET_BCS_RELATIONS_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_relations";

    public static String GET_BCS_RELATIONS_PREVIOUS_COUNT=
            "select count(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_relations";

    public static String GET_BCS_RELATIONS_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_relations_part " +
             "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_relations_part " +
             "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_relations_part ))";

    public static String GET_BCS_RESPONSIBILITIES_SOURCE_COUNT=
            "select COUNT(*) as Source_Count from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df CROSS JOIN UNNEST(\"contactsresponsibilities\") x (\"cj\"))";

    public static String GET_BCS_RESPONSIBILITIES_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_responsibilities";

    public static String GET_BCS_RESPONSIBILITIES_PREVIOUS_COUNT=
            "select count(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_responsibilities";

    public static String GET_BCS_RESPONSIBILITIES_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_responsibilities_part " +
             "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_responsibilities_part " +
             "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_responsibilities_part ))";

    public static String GET_BCS_SUBLOCATION_SOURCE_COUNT=
            "select COUNT(*) as Source_Count from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df CROSS JOIN UNNEST(\"productsublocation\") x (\"cj\"))";

    public static String GET_BCS_SUBLOCATION_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_sublocation";

    public static String GET_BCS_SUBLOCATION_PREVIOUS_COUNT=
            "select count(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_sublocation";

    public static String GET_BCS_SUBLOCATION_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_sublocation_part " +
             "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_sublocation_part " +
             "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_sublocation_part ))";

    public static String GET_BCS_TEXT_SOURCE_COUNT=
            "select COUNT(*) as Source_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product \"df\" CROSS JOIN UNNEST(\"distributiontext\") x (\"cj\")";

    public static String GET_BCS_TEXT_CURRENT_COUNT=
            "select COUNT(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_text";

    public static String GET_BCS_TEXT_PREVIOUS_COUNT=
            "select COUNT(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_text";

    public static String GET_BCS_TEXT_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_text_part " +
             "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_text_part " +
             "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_text_part ))";

    public static String GET_BCS_VERSIONFAMILY_SOURCE_COUNT=
            "select count(*) as Source_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".\"initial_ingest_product\" \"df\" CROSS JOIN UNNEST(\"contentversionfamily\") x (\"cj\")";

    public static String GET_BCS_ORIGINATORNOTES_SOURCE_COUNT=
           "  select count(*) as Source_Count from (\n" +
                   " select \n" +
                   "uo.metainfdeleted metadeleted\n" +
                   ", uo.metainfmodifiedon metamodifiedon\n" +
                   ", uo.productprojectno sourceref\n" +
                   ", uo.businesspartnerid businesspartnerid\n" +
                   ", ua.notestype notestype\n" +
                   ", ua.notes notes\n" +
                   ", ua.companygroup companygroup\n" +
                   "from ((select\n" +
                   "df.metainfdeleted\n" +
                   ", df.metainfmodifiedon\n" +
                   ",df.productprojectno\n" +
                   ", co.businesspartnerid\n" +
                   ", co.authornotes\n" +
                   "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                   "CROSS JOIN UNNEST(contactsoriginators) x (co)))uo\n" +
                   "CROSS JOIN UNNEST(authornotes) z (ua))\n" +
                   ")";


    public static String GET_BCS_VERSIONFAMILY_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_versionfamily";

    public static String GET_BCS_ORIGINATORNOTES_CURRENT_COUNT=
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatornotes";


    public static String GET_BCS_VERSIONFAMILY_PREVIOUS_COUNT=
            "select count(*) as Previous_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_previous_versionfamily";

    public static String GET_BCS_VERSIONFAMILY_HISTORY_COUNT=
            "SELECT count(*) as History_Count FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_versionfamily_part " +
             "where inbound_ts =(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_versionfamily_part " +
             "where inbound_ts < (select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_versionfamily_part ))";

    public static String GET_BCS_CLASSIFICATION_SERIES_SOURCE_COUNT =
            "select count(*) as Source_Count from(SELECT metainfdeleted metadeleted, metainfmodifiedon metamodifiedon,\n" +
                    "contentseriesid sourceref, cl.businessunit, cl.value, cl.classificationtype, cl.priority, cl.classificationcode\n" +
                    "FROM (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series f\n" +
                    "CROSS JOIN UNNEST(distributionclassification) x (cl)))";

    public static String GET_BCS_CONTENT_SERIES_SOURCE_COUNT =
           "select count(*) as Source_Count from(SELECT df.metainfdeleted metadeleted,df.metainfmodifiedon metamodifiedon,df.contentseriesid sourceref,df.contentsubgroup subgroup\n" +
                   ",df.contentseriescode seriescode,df.contentmedium medium,df.contentwmyn wmyn\n" +
                   ",df.contentsubtitle subtitle,df.contenttitle title,df.contentserialtype serialtype\n" +
                   ",df.contentdivision division,df.contentobjType objType,df.contentcompanygroup companygroup,df.contentseriesissn seriesissn,df.contentfirstbinding binding\n" +
                   ",df.contentvolumeno volumeno,df.contentlanguage language\n" +
                   ",df.contentpublisher publisher,df.contentseriesid seriesid,df.contentshortTitle shorttitle\n" +
                   ",df.contentpiidack piidack,df.contentownership ownership,df.contentdeltype deltype\n" +
                   ",df.contentnumbered numbered,df.contentbibliographicserial bibliographicserial\n" +
                   ",df.contentmainseries mainseries,df.contenteditionid editionid FROM\n" +
                   " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df)";

    public static String GET_BCS_ORIGINATORADDRESS_SERIES_SOURCE_COUNT =
           "select count(*) Source_Count from (select distinct uo.metainfdeleted metadeleted, uo.metainfmodifiedon metamodifiedon, uo.businesspartnerid\tbusinesspartnerid\n" +
                   ", ua.country\tcountry, ua.postalcode\tpostalcode, ua.additionaladdress\tadditionaladdress, ua.houseno\thouseno\n" +
                   ", ua.internet\tinternet, ua.city\tcity, ua.street\tstreet, ua.email\temail, ua.district\tdistrict\n" +
                   ", ua.mobile\tmobile, ua.fax\tfax, ua.telephoneother\ttelephoneother, ua.telephonemain\ttelephonemain\n" +
                   "from ((select df.metainfdeleted , df.metainfmodifiedon , co.businesspartnerid , co.authoraddress\n" +
                   "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df CROSS JOIN UNNEST(contactsoriginators) x (co)))uo\n" +
                   "CROSS JOIN UNNEST(authoraddress) z (ua)))";

    public static String GET_BCS_ORIGINATORNOTES_SERIES_SOURCE_COUNT =
            "select count(*) as Source_Count" +
                    " from " +
                    "(select distinct  uo.metainfdeleted metadeleted, uo.metainfmodifiedon metamodifiedon,\n" +
                    "uo.contentseriesid sourceref, uo.businesspartnerid businesspartnerid, ua.notes notes,\n" +
                    "ua.companygroup companygroup from ((select df.metainfdeleted ,df.metainfmodifiedon ,df.contentseriesid, co.businesspartnerid ,\n" +
                    "co.authornotes from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df  CROSS JOIN UNNEST(contactsoriginators) x (co)))uo " +
                    "CROSS JOIN UNNEST(authornotes) z (ua)))";

    public static String GET_BCS_ORIGINATOR_SERIES_SOURCE_COUNT =
            "select count(*) as Source_Count" +
                    " from" +
                    "(select\n" +
                    "\"df\".\"metainfdeleted\" \"metadeleted\", \"df\".\"metainfmodifiedon\" \"metamodifiedon\"\n" +
                    ", \"df\".\"contentseriesid\" \"sourceref\", \"co\".\"firstname\"\t\"firstname\", \"co\".\"businesspartnerid\"\t\"businesspartnerid\", \"co\".\"lastname\"\t\"lastname\", \"co\".\"sequence\"\t\"sequence\"\n" +
                    ", \"co\".\"prefix\"\t\"prefix\", \"co\".\"copyrightholdertype\"\t\"copyrightholdertype\"\n" +
                    ", \"co\".\"searchterm\"\t\"searchterm\"  from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df\n" +
                    "CROSS JOIN UNNEST(\"contactsoriginators\") x (\"co\")))";

    public static String GET_BCS_PRODUCT_SERIES_SOURCE_COUNT =
            "select count(*) as Source_Count from(select df.metainfdeleted metadeleted, \n" +
                    "df.metainfmodifiedon metamodifiedon, df.contentseriesid sourceref, df.productorderno orderno\n" +
                    ", df.productversiontype versiontype from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df))";

    public static String GET_BCS_TEXT_SERIES_SOURCE_COUNT =
            "select count(*) as Source_Count from(select df.metainfdeleted metadeleted, df.metainfmodifiedon metamodifiedon, df.contentseriesid sourceref, cj.tab, cj.texttype, cj.name\n" +
                    ", cj.text, cj.status from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df\n" +
                    "CROSS JOIN UNNEST(distributiontext) x (cj))";

    public static String GET_BCS_CLASSIFICATION_SERIES_CURRENT_COUNT =
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_classification_series";

    public static String GET_BCS_CONTENT_SERIES_CURRENT_COUNT =
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_content_series";

    public static String GET_BCS_ORIGINATORADDRESS_SERIES_CURRENT_COUNT =
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatoraddress_series";

    public static String GET_BCS_ORIGINATORNOTES_SERIES_CURRENT_COUNT =
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatornotes_series";

    public static String GET_BCS_ORIGINATOR_SERIES_CURRENT_COUNT =
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originators_series";

    public static String GET_BCS_PRODUCT_SERIES_CURRENT_COUNT =
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_product_series";

    public static String GET_BCS_TEXT_SERIES_CURRENT_COUNT =
            "select count(*) as Current_Count from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_text_series";



}
