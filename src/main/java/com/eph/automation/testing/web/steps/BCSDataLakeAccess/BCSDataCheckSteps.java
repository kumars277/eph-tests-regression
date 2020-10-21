//created by Nishant @ 29 Sep 2020
package com.eph.automation.testing.web.steps.BCSDataLakeAccess;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSDataQualityContext;
import com.eph.automation.testing.services.db.BCSDataLakeSQL.BCSDataLakeDataCheckSQL;
import com.eph.automation.testing.models.dao.BCSDataLake.*;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BCSDataCheckSteps {

    private static String sql;
    private static List<String> Ids;
    private static BCSDataQualityContext bcsDataQualityContext= new BCSDataQualityContext();

    @Given("^We get the (.*) random ids from initial ingest (.*)$")
    public void getRandomIdsFromInitialIngest(String countOfRandomIds, String targetTable)
    {Log.info("getting random reference ids from initial ingest...");
    switch(targetTable)
    {
        case "stg_current_classification"       :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_classification, countOfRandomIds);break;
        case "stg_current_content"              :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_content, countOfRandomIds);break;
        case "stg_current_extobject"            :  break;
        case "stg_current_fullversionfamily"    :  break;
        case "stg_current_originatoraddress"    :  break;
        case "stg_current_originators"          :  break;
        case "stg_current_pricing"              :  break;
        case "stg_current_product"              :  break;
        case "stg_current_production"           :  break;
        case "stg_current_relations"            :  break;
        case "stg_current_responsibilities"     :  break;
        case "stg_current_sublocation"          :  break;
        case "stg_current_text"                 :  break;
        case "stg_current_versionfamily"        :  break;
    }

        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomEPRIds.stream().map(m -> (String) m.get("sourceref")).collect(Collectors.toList());
        Log.info("Randomly picked ids..."+ Ids);
    }

    @When("Get the data records from initial ingest for (.*)")
    public void getInitialIngestData(String targetTable){
        Log.info("We get initial ingest records...");

        switch(targetTable)
        {
            case "stg_current_classification"       :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_classification,
                        Joiner.on("','").join(Ids));break;

            case "stg_current_content"              :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_content,
                        Joiner.on("','").join(Ids));break;

            case "stg_current_extobject"            :  break;
            case "stg_current_fullversionfamily"    :  break;
            case "stg_current_originatoraddress"    :  break;
            case "stg_current_originators"          :  break;
            case "stg_current_pricing"              :  break;
            case "stg_current_product"              :  break;
            case "stg_current_production"           :  break;
            case "stg_current_relations"            :  break;
            case "stg_current_responsibilities"     :  break;
            case "stg_current_sublocation"          :  break;
            case "stg_current_text"                 :  break;
            case "stg_current_versionfamily"        :  break;
        }
        bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
   }

    @Then ("^Get the records from current tables (.*)$")
    public void getCurrentTableData(String targetTable)    {//created by Nishant @ 21 Oct 2020
        Log.info("We get current table records...");
        switch(targetTable)
        {
            case "stg_current_classification"       :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_classification,
                Joiner.on("','").join(Ids));break;
            case "stg_current_content"              :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_content,
                        Joiner.on("','").join(Ids));break;

            case "stg_current_extobject"            :  break;
            case "stg_current_fullversionfamily"    :  break;
            case "stg_current_originatoraddress"    :  break;
            case "stg_current_originators"          :  break;
            case "stg_current_pricing"              :  break;
            case "stg_current_product"              :  break;
            case "stg_current_production"           :  break;
            case "stg_current_relations"            :  break;
            case "stg_current_responsibilities"     :  break;
            case "stg_current_sublocation"          :  break;
            case "stg_current_text"                 :  break;
            case "stg_current_versionfamily"        :  break;
        }
        bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
    }

    @And("Compare the records of initial ingest and current table (.*)")
    public void compareInitialIngestWithCurrentTable(String targetTable) {//created by Nishant @ 21 Oct 2020
        if (!bcsDataQualityContext.bcsInitialIngestDataObjectList.isEmpty() |
                !bcsDataQualityContext.bcsCurrentTableDataObjectList.isEmpty()) {
            Log.info("comparing initial ingest data with current table.....");

            Assert.assertEquals("initial ingest data count match with " + targetTable + "\n"
                            + "Initial ingest count = " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size()
                            + "current table count= " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size()
                    , bcsDataQualityContext.bcsInitialIngestDataObjectList.size(),
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.size());

            switch(targetTable)
            {
                case "stg_current_classification"       :  compareIngestVsCurrentClassification();break;
                case "stg_current_content"              :  compareIngestVsCurrentContent();break;
                case "stg_current_extobject"            :  break;
                case "stg_current_fullversionfamily"    :  break;
                case "stg_current_originatoraddress"    :  break;
                case "stg_current_originators"          :  break;
                case "stg_current_pricing"              :  break;
                case "stg_current_product"              :  break;
                case "stg_current_production"           :  break;
                case "stg_current_relations"            :  break;
                case "stg_current_responsibilities"     :  break;
                case "stg_current_sublocation"          :  break;
                case "stg_current_text"                 :  break;
                case "stg_current_versionfamily"        :  break;
            }
        }
    }

    public void compareIngestVsCurrentClassification(){//created by Nishant @ 21 Oct 2020
            for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++)
            {
                boolean classificationcodeFound=false;
                for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                    if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode()
                            .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getClassificationcode()))
                    {
                        classificationcodeFound=true;

                        Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));

                        Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));

                        Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));

                        Assert.assertEquals("value mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValue(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getValue()));

                        Assert.assertEquals("classificationtype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationtype(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getClassificationtype()));

                        Assert.assertEquals("priority mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPriority(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPriority()));

                        Assert.assertEquals("businessunit mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinessunit(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinessunit()));

                        Log.info("verified data at position " +i);
                        break;
                    }
                }
                   Assert.assertTrue("classificationCode value missing in current table"
                        +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode(),classificationcodeFound);
            }
        }

    public void compareIngestVsCurrentContent(){
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++)
        {
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getClassificationcode()))
                {
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));

                    Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));

                    Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));

                    Assert.assertEquals("value mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValue(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getValue()));

                    Assert.assertEquals("classificationtype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationtype(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getClassificationtype()));

                    Assert.assertEquals("priority mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPriority(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPriority()));

                    Assert.assertEquals("businessunit mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinessunit(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinessunit()));

                    Log.info("verified data at position " +i);
                    break;
                }
            }
            Assert.assertTrue("classificationCode value missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode(),Found);
        }
    }


    public void compareIngestVsCurrentExtobject(){
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++)
        {
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getClassificationcode()))
                {
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    Log.info("verified data at position " +i);
                    break;
                }
            }
            Assert.assertTrue("classificationCode value missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode(),Found);
        }
    }

}
