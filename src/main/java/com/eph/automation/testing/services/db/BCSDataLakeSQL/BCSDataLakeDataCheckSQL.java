//created by Nishant @ 29 Sep 2020

package com.eph.automation.testing.services.db.BCSDataLakeSQL;

public class BCSDataLakeDataCheckSQL {

    public static String randomId_ingestTableFor_stg_current_classification=
            "select sourceref from(SELECT cl.value, productprojectno sourceref\n" +
              "FROM (bcs_ingestion_database_sit.initial_ingest f CROSS JOIN UNNEST(distributionclassification) x (cl)))\n" +
              "where value is not null AND value!='' AND value!='0' order by rand() limit %s";

    public static String getInitialIngestDataFor_stg_current_classification=
        "select * from(SELECT\n" +
                "  metainfdeleted metadeleted\n" +
                ", metainfmodifiedon metamodifiedon\n" +
                ", productprojectno sourceref\n" +
                ", cl.classificationcode\n" +
                ", cl.value value\n" +
                ", cl.classificationtype\n" +
                ", cl.priority\n" +
                ", cl.businessunit\n" +
                "FROM  (bcs_ingestion_database_sit.initial_ingest f\n" +
                "CROSS JOIN UNNEST(distributionclassification) x (cl)))\n" +
                "where sourceref in ('%s')\n" +
                "order by sourceref,value,classificationcode desc";

    public static String getCurrentTableDataFor_stg_current_classification=
        "select metadeleted, metamodifiedon,sourceref,classificationcode,\n" +
                "value,classificationtype,priority,businessunit\n" +
                "from bcs_ingestion_database_sit.stg_current_classification \n" +
                "where sourceref in ('%s')\n" +
                "order by sourceref,value,classificationcode desc";


    //created by Nishant @ 21 Oct 2020
    public static String randomId_ingestTableFor_stg_current_content=
            "select sourceref from(SELECT df.productprojectno sourceref FROM\n" +
            "bcs_ingestion_database_sit.initial_ingest df)  order by rand() limit %s";


    public static String getInitialIngestDataFor_stg_current_content=
            "select* from(SELECT\n" +
                    "  df.metainfdeleted\t\"metadeleted\"\n" +
                    ", df.metainfmodifiedon\t\"metamodifiedon\"\n" +
                    ", df.productprojectno\t\"sourceref\"\n" +
                    ", df.contentoriginimpid\t\"originimpid\"\n" +
                    ", df.contentsubgroup\t\"subgroup\"\n" +
                    ", df.contentseries\t\"series\"\n" +
                    ", df.contentcopyrightyear\t\"copyrightyear\"\n" +
                    ", df.contenttitle\t\"title\"\n" +
                    ", df.contentworktitle\t\"worktitle\"\n" +
                    ", df.contentisset\t\"isset\"\n" +
                    "--, df.contentimpressionid\t\"impressionid\"\n" +
                    ", df.contentdoistatus\t\"doistatus\"\n" +
                    ", df.contentimprint\t\"imprint\"\n" +
                    ", df.contentdivision\t\"division\"\n" +
                    ", df.contentlanguage3\t\"language3\"\n" +
                    ", df.contentregstatus\t\"regstatus\"\n" +
                    ", df.contentapprovedondate\t\"approvedondate\"\n" +
                    ", df.contentlanguage2\t\"language2\"\n" +
                    ", df.contentdoi\t\"doi\"\n" +
                    ", df.contentvolumeno\t\"volumeno\"\n" +
                    ", df.contenteditionid\t\"editionid\"\n" +
                    ", df.contentsynctemplate\t\"synctemplate\"\n" +
                    ", df.contentvolumename\t\"volumename\"\n" +
                    ", df.contentseriesissn\t\"seriesissn\"\n" +
                    ", df.contentseriesid\t\"seriesid\"\n" +
                    ", df.contentownership\t\"ownership\"\n" +
                    ", df.contentfirstapproval\t\"firstapproval\"\n" +
                    ", df.contentcompanygroup\t\"companygroup\"\n" +
                    ", df.contentshorttitle\t\"shorttitle\"\n" +
                    ", df.contenteditionno\t\"editionno\"\n" +
                    ", df.contentwmyn\t\"work_master_flag\"\n" +
                    ", df.contentlanguage\t\"language\"\n" +
                    ", df.contentpiidack\t\"piidack\"\n" +
                    ", df.contentpublisher\t\"publisher\"\n" +
                    ", df.contenttitleid\t\"titleid\"\n" +
                    ", df.contentsubtitle\t\"subtitle\"\n" +
                    ", df.contentseriescode\t\"seriescode\"\n" +
                    ", df.contentobjtype\t\"objecttype\"\n" +
                    "FROM bcs_ingestion_database_sit.initial_ingest df) \n" +
                    "  where sourceref in ('%s') order by sourceref desc";

    public static String getCurrentTableDataFor_stg_current_content=
            "select metadeleted,metamodifiedon,sourceref,originimpid,subgroup,series,copyrightyear,title,worktitle," +
                    "isset,doistatus,imprint,division,language3,regstatus,approvedondate,language2,doi," +
                    "volumeno,editionid,synctemplate,volumename,seriesissn,seriesid,ownership,firstapproval,companygroup," +
                    "shorttitle,editionno,work_master_flag,language,piidack,publisher,titleid,subtitle,seriescode,objecttype " +
                    "from bcs_ingestion_database_sit.stg_current_content \n" +
                    "where sourceref in ('%s') order by sourceref desc";

    //added by Nishant @ 22 Oct 2020
    public static String randomId_ingestTableFor_stg_current_extobject=
            "select sourceref from(select productprojectno sourceref, cj.object, cj.name\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(\"productexternalobjects\") x (cj)))\n" +
                    "where name is not null and name !='' and name!='0'\n" +
                    "order by rand() limit %s";

    public static String getInitialIngestDataFor_stg_current_extobject=
            "select * from(select" +
                    "  metainfdeleted metadeleted\n" +
                    ", metainfmodifiedon metamodifiedon\n" +
                    ", productprojectno sourceref\n" +
                    ", cj.object\n" +
                    ", cj.type\n" +
                    ", cj.name\n" +
                    ", cj.comments\n" +
                    ", cj.source\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(productexternalobjects) x (cj)))\n" +
                    "where sourceref in ('%s') \n" +
                    "order by sourceref,name,object desc";

    public static String getCurrentTableDataFor_stg_current_extobject=
            "select metadeleted,metamodifiedon,sourceref,object,type,name,comments,source \n" +
                    "from bcs_ingestion_database_sit.stg_current_extobject \n" +
                    "where sourceref in ('%s') order by sourceref,name,object desc";


    public static String randomId_ingestTableFor_stg_current_fullversionfamily=
            "select sourceref from (select productprojectno sourceref, fv.projectno\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(contentfullversionfamily) x (fv)))\n" +
                    "where projectno is not null and projectno!=''\n" +
                    "order by rand() limit %s";

    public static String getInitialIngestDataFor_stg_current_fullversionfamily=
            "select* from(select \n" +
                    "  metainfdeleted metadeleted\n" +
                    ", metainfmodifiedon metamodifiedon\n" +
                    ", productprojectno sourceref\n" +
                    ", fv.versiontype\n" +
                    ", fv.editionno\n" +
                    ", fv.isbn\n" +
                    ", fv.projectno\n" +
                    ", fv.workmaster\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(contentfullversionfamily) x (fv)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,projectno desc";

    public static String getCurrentTableDataFor_stg_current_fullversionfamily=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "versiontype,editionno,isbn,projectno,workmaster\n" +
                    "from bcs_ingestion_database_sit.stg_current_fullversionfamily \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,projectno desc";


    public static String randomId_ingestTableFor_stg_current_originatoraddress=
            "with originator as (select\n" +
                    "    df.metainfdeleted metadeleted\n" +
                    "  , df.metainfmodifiedon metamodifiedon\n" +
                    "  --, df.productprojectno sourceref\n" +
                    "  , co.businesspartnerid businesspartnerid\n" +
                    "  , co.authoraddress\n" +
                    "  from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "  CROSS JOIN UNNEST(contactsoriginators) x (co))),\n" +
                    "  address as (\n" +
                    "  select\n" +
                    "     uo.metadeleted\n" +
                    "   , uo.metamodifiedon\n" +
                    "--   , uo.sourceref\n" +
                    "   , uo.businesspartnerid\n" +
                    "   , ua.addressline2\n" +
                    "   , ua.country\n" +
                    "   , ua.fax\n" +
                    "   , ua.street\n" +
                    "   , ua.additionaladdress\n" +
                    "   , ua.city\n" +
                    "   , ua.addressline1\n" +
                    "   , ua.addressid\n" +
                    "   , ua.telephonemain\n" +
                    "   , ua.ismainaddress\n" +
                    "   , ua.internet\n" +
                    "   , ua.houseno\n" +
                    "   , ua.email\n" +
                    "   , ua.addressline3\n" +
                    "   , ua.telephoneother\n" +
                    "   , ua.postalcode\n" +
                    "   , ua.mobile\n" +
                    "   , ua.district\n" +
                    "   from (originator uo\n" +
                    "   CROSS JOIN UNNEST(authoraddress) z (ua)))\n" +
                    "select businesspartnerid from (select distinct\n" +
                    "* from address) where businesspartnerid is not null order by rand() limit %s";

    public static String getInitialIngestDataFor_stg_current_originatoraddress=
            "with originator as (\n" +
                    "  select\n" +
                    "    df.metainfdeleted metadeleted\n" +
                    "  , df.metainfmodifiedon metamodifiedon\n" +
                    "  --, df.productprojectno sourceref\n" +
                    "  , co.businesspartnerid businesspartnerid\n" +
                    "  , co.authoraddress\n" +
                    "  from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "  CROSS JOIN UNNEST(contactsoriginators) x (co))),\n" +
                    "  address as (\n" +
                    "  select\n" +
                    "     uo.metadeleted\n" +
                    "   , uo.metamodifiedon\n" +
                    "--   , uo.sourceref\n" +
                    "   , uo.businesspartnerid\n" +
                    "   , ua.addressline2\n" +
                    "   , ua.country\n" +
                    "   , ua.fax\n" +
                    "   , ua.street\n" +
                    "   , ua.additionaladdress\n" +
                    "   , ua.city\n" +
                    "   , ua.addressline1\n" +
                    "   , ua.addressid\n" +
                    "   , ua.telephonemain\n" +
                    "   , ua.ismainaddress\n" +
                    "   , ua.internet\n" +
                    "   , ua.houseno\n" +
                    "   , ua.email\n" +
                    "   , ua.addressline3\n" +
                    "   , ua.telephoneother\n" +
                    "   , ua.postalcode\n" +
                    "   , ua.mobile\n" +
                    "   , ua.district\n" +
                    "   from (originator uo\n" +
                    "   CROSS JOIN UNNEST(authoraddress) z (ua)))\n" +
                    "select * from (select distinct\n" +
                    "* from address) \n" +
                    "where businesspartnerid in (%s)\n" +
                    "order by businesspartnerid,country,metamodifiedon desc";

    public static String getCurrentTableDataFor_stg_current_originatoraddress=
            "select metadeleted,metamodifiedon,businesspartnerid,addressline2,\n" +
                    "country,fax,street,additionaladdress,city,addressline1,addressid,telephonemain,\n" +
                    "ismainaddress,internet,houseno,email,addressline3,telephoneother,postalcode,\n" +
                    "mobile,district from bcs_ingestion_database_sit.stg_current_originatoraddress \n" +
                    "where businesspartnerid in (%s)\n" +
                    "order by businesspartnerid,country,metamodifiedon desc";



    public static String randomId_ingestTableFor_stg_current_originators=
            "select sourceref from " +
                    "(select df.productprojectno sourceref, co.businesspartnerid businesspartnerid\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df CROSS JOIN UNNEST(contactsoriginators) x (co)))\n" +
                    "where businesspartnerid is not null order by rand() limit %s";

    public static String getInitialIngestDataFor_stg_current_originators=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", co.prefix\tprefix\n" +
                    ", co.sequence\tsequence\n" +
                    ", co.businesspartnerid\tbusinesspartnerid\n" +
                    ", co.originatortitle\toriginatorid\n" +
                    ", co.isperson\tisperson\n" +
                    ", co.locationid\tlocationid\n" +
                    ", co.copyrightholdertype\tcopyrightholdertype\n" +
                    ", co.institution\tinstitution\n" +
                    ", co.firstname\tfirstname\n" +
                    ", co.department\tdepartment\n" +
                    ", co.lastname\tlastname\n" +
                    ", co.searchterm\tsearchterm\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(contactsoriginators) x (co)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,copyrightholdertype,businesspartnerid desc";

    public static String getCurrentTableDataFor_stg_current_originators=
            "select metadeleted,metamodifiedon,sourceref, prefix,sequence,businesspartnerid,originatorid,\n" +
                    "isperson,locationid,copyrightholdertype,institution,\n" +
                    "firstname,department,lastname,searchterm\n" +
                    "from bcs_ingestion_database_sit.stg_current_originators \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,copyrightholdertype,businesspartnerid desc";

    public static String randomId_ingestTableFor_stg_current_pricing=
            "select sourceref from(select \n" +
                    " df.productprojectno sourceref\n" +
                    ", cp.validupto validto\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(productprice) x (cp)))\n" +
                    "where validto is not null and validto!=''\n" +
                    "order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_pricing=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", cp.validfrom validfrom\n" +
                    ", cp.type type\n" +
                    ", cp.currency currency\n" +
                    ", cp.priceapprox priceapprox\n" +
                    ", cp.price price\n" +
                    ", cp.validupto validto\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(productprice) x (cp)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,type,currency,validto desc;";
    public static String getCurrentTableDataFor_stg_current_pricing=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "validfrom,type,currency,priceapprox,price,validto\n" +
                    "from bcs_ingestion_database_sit.stg_current_pricing\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,type,currency,validto desc;";

    public static String randomId_ingestTableFor_stg_current_tableName=
            "";
    public static String getInitialIngestDataFor_stg_current_tableName=
            "";
    public static String getCurrentTableDataFor_stg_current_tableName=
            "";
}
