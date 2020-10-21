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
            "bcs_ingestion_database_sit.initial_ingest df)  order by rand() limit %s;";


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
                    ", df.contentimpressionid\t\"impressionid\"\n" +
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
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "originimpid,subgroup,series,copyrightyear,\n" +
                    "title,worktitle,isset,impressionid,doistatus,\n" +
                    "imprint,division,language3,regstatus,approvedondate,\n" +
                    "language2,doi,volumeno,editionid,synctemplate,\n" +
                    "volumename,seriesissn,seriesid,ownership,\n" +
                    "firstapproval,companygroup,shorttitle,editionno,\n" +
                    "work_master_flag,language,piidack,publisher,\n" +
                    "titleid,subtitle,seriescode,objecttype\n" +
                    "from bcs_ingestion_database_sit.stg_current_content \n" +
                    "where sourceref in ('%s') order by sourceref desc;";

    public static String randomId_ingestTableFor_stg_current_extobject=
            "";
    public static String getInitialIngestDataFor_stg_current_extobject=
            "";
    public static String getCurrentTableDataFor_stg_current_extobject=
            "";


}
