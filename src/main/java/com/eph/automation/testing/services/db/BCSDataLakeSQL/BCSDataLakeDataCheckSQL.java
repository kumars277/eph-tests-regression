//created by Nishant @ 29 Sep 2020

package com.eph.automation.testing.services.db.BCSDataLakeSQL;

public class BCSDataLakeDataCheckSQL {

    public static String randomId_ingestTable=
            "select sourceref from(SELECT cl.value, productprojectno sourceref\n" +
              "FROM (bcs_ingestion_database_sit.initial_ingest f CROSS JOIN UNNEST(distributionclassification) x (cl)))\n" +
              "where value is not null AND value!='' AND value!='0' order by rand() limit %s";

public static String getInitialIngestData=
        "select * from(SELECT\n" +
                "  metainfdeleted metadeleted\n" +
                ", metainfmodifiedon metamodifiedon\n" +
                ", productprojectno sourceref\n" +
                ", cl.classificationcode\n" +
                ", cl.value clvalue\n" +
                ", cl.classificationtype\n" +
                ", cl.priority\n" +
                ", cl.businessunit\n" +
                "FROM  (bcs_ingestion_database_sit.initial_ingest f\n" +
                "CROSS JOIN UNNEST(distributionclassification) x (cl)))\n" +
                "where sourceref in ('%s')\n" +
                "order by sourceref,clvalue,classificationcode desc";

}
