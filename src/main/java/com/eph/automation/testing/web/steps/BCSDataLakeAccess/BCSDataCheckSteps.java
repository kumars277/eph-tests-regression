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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BCSDataCheckSteps {

    private static String sql;
    private static List<String> Ids;
    private static BCSDataQualityContext bcsDataQualityContext= new BCSDataQualityContext();

    @Given("^We get the (.*) random ids from initial ingest (.*)$")
    public void getRandomIdsFromInitialIngest(String countOfRandomIds, String targetTable){
        Log.info("getting random reference ids from initial ingest...");
        switch(targetTable)
        {
            case "stg_current_classification"       :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_classification, countOfRandomIds);break;
            case "stg_current_content"              :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_content, countOfRandomIds);break;
            case "stg_current_extobject"            :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_extobject, countOfRandomIds);break;
            case "stg_current_fullversionfamily"    :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_fullversionfamily, countOfRandomIds);break;
            case "stg_current_originatoraddress"    :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_originatoraddress, countOfRandomIds);break;
            case "stg_current_originators"          :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_originators, countOfRandomIds);break;
            case "stg_current_pricing"              :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_pricing, countOfRandomIds);break;
            case "stg_current_product"              :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_product, countOfRandomIds);break;
            case "stg_current_production"           :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_production, countOfRandomIds);break;
            case "stg_current_relations"            :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_relations, countOfRandomIds);break;
            case "stg_current_responsibilities"     :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_responsibilities, countOfRandomIds);break;
            case "stg_current_sublocation"          :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_sublocation, countOfRandomIds);break;
            case "stg_current_text"                 :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_text, countOfRandomIds);break;
            case "stg_current_versionfamily"        :
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_versionfamily, countOfRandomIds);break;

        }

        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        if(targetTable.equalsIgnoreCase("stg_current_originatoraddress"))
            Ids = randomEPRIds.stream().map(m -> (Integer) m.get("businesspartnerid")).map(String::valueOf).collect(Collectors.toList());
        else
            Ids = randomEPRIds.stream().map(m -> (String) m.get("sourceref")).collect(Collectors.toList());
        Log.info("Randomly picked ids..."+ Ids);
        //added by Nishant to debug failures
       // Ids.clear();Ids.add("559522");
    }

    @When("Get the data records from initial ingest for (.*)")
    public void getInitialIngestData(String targetTable){
        Log.info("We get initial ingest records...");

        switch(targetTable)
        {
            case "stg_current_classification"       :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_classification,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));break;

            case "stg_current_content"              :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_content,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));break;

            case "stg_current_extobject"            :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_extobject,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getComments));break;

            case "stg_current_fullversionfamily"    :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_fullversionfamily,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getProjectno));break;

            case "stg_current_originatoraddress"    :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_originatoraddress,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getMetamodifiedon));break;

            case "stg_current_originators"          :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_originators,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
                break;

            case "stg_current_pricing"              :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_pricing,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_product"              :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_product,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_production"           :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_production,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_relations"            :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_relations,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_responsibilities"     :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_responsibilities,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_sublocation"          :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_sublocation,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_text"                 :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_text,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_versionfamily"        :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_versionfamily,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

        }

    }

    @Then ("^Get the records from current tables (.*)$")
    public void getCurrentTableData(String targetTable)    {//created by Nishant @ 21 Oct 2020
        Log.info("We get current table records...");
        switch(targetTable)
        {
            case "stg_current_classification"       :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_classification,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));break;

            case "stg_current_content"              :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_content,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));break;

            case "stg_current_extobject"            :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_extobject,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getComments));break;

            case "stg_current_fullversionfamily"    :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_fullversionfamily,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getProjectno));break;

            case "stg_current_originatoraddress"    :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originatoraddress,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));break;

            case "stg_current_originators"          :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originators,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                //   bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                break;

            case "stg_current_pricing"              :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_pricing,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                break;

            case "stg_current_product"              :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_product,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                break;

            case "stg_current_production"           :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_production,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                break;

            case "stg_current_relations"            :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_relations,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                break;

            case "stg_current_responsibilities"     :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_responsibilities,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                break;

            case "stg_current_sublocation"          :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_sublocation,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                break;

            case "stg_current_text"                 :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_text,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                break;

            case "stg_current_versionfamily"        :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_versionfamily,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                break;
        }
    }

    @And("Compare the records of initial ingest and current table (.*)")
    public void compareInitialIngestWithCurrentTable(String targetTable) {//created by Nishant @ 21 Oct 2020
        if (!bcsDataQualityContext.bcsInitialIngestDataObjectList.isEmpty() |
                !bcsDataQualityContext.bcsCurrentTableDataObjectList.isEmpty()) {

            Assert.assertEquals("initial ingest data count match with " + targetTable + "\n"
                            + "Initial ingest count = " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size()
                            + "current table count= " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size()
                    , bcsDataQualityContext.bcsInitialIngestDataObjectList.size(),
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.size());

            switch(targetTable)
            {
                case "stg_current_classification"       :  compareIngestVsCurrentClassification();break;
                case "stg_current_content"              :  compareIngestVsCurrentContent();break;
                case "stg_current_extobject"            :  compareIngestVsCurrentExtobject();break;
                case "stg_current_fullversionfamily"    :  compareIngestVsCurrentFullVersionFamily();break;
                case "stg_current_originatoraddress"    :  compareIngestVsCurrentOriginatoraddress();break;
                case "stg_current_originators"          :  compareIngestVsCurrentOriginators();break;
                case "stg_current_pricing"              :  compareIngestVsCurrentPricing();break;
                case "stg_current_product"              :  compareIngestVsCurrentProduct();break;
                case "stg_current_production"           :  compareIngestVsCurrentProduction();break;
                case "stg_current_relations"            :  compareIngestVsCurrentRelations();break;
                case "stg_current_responsibilities"     :  compareIngestVsCurrentresponsibilities();break;
                case "stg_current_sublocation"          :  compareIngestVsCurrentsublocation();break;
                case "stg_current_text"                 :  compareIngestVsCurrentText();break;
                case "stg_current_versionfamily"        :  compareIngestVsCurrentversionfamily();break;
            }
        }
    }

    public void compareIngestVsCurrentClassification(){//created by Nishant @ 21 Oct 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
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

    public void compareIngestVsCurrentContent(){//created by Nishant @ 22 Oct 2020
        Log.info("verifying initial ingest Vs Current content...\n");
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++)
        {
            Log.info((i+1)+". verification for sourceref "+bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref());
            Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
            printLog("Metadeleted");

            Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
            printLog("metamodifiedon");

            Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
            printLog("sourceref");

            Assert.assertEquals("approvedondate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getApprovedondate(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getApprovedondate()));
            printLog("approvedondate");


            Assert.assertEquals("companygroup mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCompanygroup(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCompanygroup()));
            printLog("companygroup");


            Assert.assertEquals("copyrightyear mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCopyrightyear(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCopyrightyear()));
            printLog("copyrightyear");


            Assert.assertEquals("division mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDivision(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDivision()));
            printLog("division");

            Assert.assertEquals("doi mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDoi(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDoi()));
            printLog("doi");

            Assert.assertEquals("doistatus mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDoistatus(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDoistatus()));
            printLog("doistatus");


            Assert.assertEquals("editionid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEditionid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEditionid()));
            printLog("editionid");


            Assert.assertEquals("editionno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEditionno(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEditionno()));
            printLog("editionno");

            Assert.assertEquals("firstapproval mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFirstapproval(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFirstapproval()));
            printLog("firstapproval");

            Assert.assertEquals("impressionid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getImpressionid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getImpressionid()));
            printLog("impressionid");

            Assert.assertEquals("imprint mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getImprint(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getImprint()));
            printLog("imprint");

            Assert.assertEquals("isset mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsset(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsset()));
            printLog("isset");

            Assert.assertEquals("language mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLanguage(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage()));
            printLog("language");

            Assert.assertEquals("language2 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLanguage2(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage2()));
            printLog("language2");

            Assert.assertEquals("language3 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLanguage3(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage3()));
            printLog("language3");

            Assert.assertEquals("objecttype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getObjecttype(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getObjecttype()));
            printLog("objecttype");

            Assert.assertEquals("originimpid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOriginimpid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOriginimpid()));
            printLog("originimpid");

            Assert.assertEquals("ownership mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOwnership(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOwnership()));
            printLog("ownership");

            Assert.assertEquals("piidack mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPiidack(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPiidack()));
            printLog("piidack");

            Assert.assertEquals("publisher mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPublisher(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPublisher()));
            printLog("publisher");

            Assert.assertEquals("regstatus mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getRegstatus(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRegstatus()));
            printLog("regstatus");

            Assert.assertEquals("series mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeries(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeries()));
            printLog("series");

            Assert.assertEquals("seriescode mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeriescode(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriescode()));
            printLog("seriescode");

            Assert.assertEquals("seriesid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeriesid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriesid()));
            printLog("seriesid");

            Assert.assertEquals("seriesissn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeriesissn(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriesissn()));
            printLog("seriesissn");

            Assert.assertEquals("shorttitle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getShorttitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getShorttitle()));
            printLog("shorttitle");

            Assert.assertEquals("subgroup mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSubgroup(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSubgroup()));
            printLog("subgroup");

            Assert.assertEquals("subtitle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSubtitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSubtitle()));
            printLog("subtitle");

            Assert.assertEquals("synctemplate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSynctemplate(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSynctemplate()));
            printLog("synctemplate");

            Assert.assertEquals("title mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTitle()));
            printLog("title");

            Assert.assertEquals("titleid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTitleid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTitleid()));
            printLog("titleid");

            Assert.assertEquals("volumename mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getVolumename(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVolumename()));
            printLog("volumename");

            Assert.assertEquals("volumeno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getVolumeno(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVolumeno()));
            printLog("volumeno");

            Assert.assertEquals("work_master_flag mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWork_master_flag(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWork_master_flag()));
            printLog("work_master_flag");

            Assert.assertEquals("worktitle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorktitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorktitle()));
            printLog("worktitle");
            Log.info("------------------------------------------");
        }
    }

    public void compareIngestVsCurrentExtobject(){//created by Nishant @ 22 Oct 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSource()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSource()))
                {
                    Log.info("verification for - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getComments());
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));
                    printLog("metamodifiedon");

                    Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));
                    printLog("sourceref");

                    Assert.assertEquals("object mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getObject(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getObject()));
                    printLog("object");

                    Assert.assertEquals("type mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getType(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getType()));
                    printLog("type");

                    Assert.assertEquals("name mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getName(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getName()));
                    printLog("name");

                    Assert.assertEquals("comments mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getComments(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getComments()));
                    printLog("comments");

                    Assert.assertEquals("source mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSource(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSource()));
                    printLog("source");

                    Log.info("------------------------------------------");
                    break;
                }

            }
            Assert.assertTrue("source missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode(),Found);
        }

    }

    public void compareIngestVsCurrentFullVersionFamily(){//created by Nishant @ 05 Nov 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getProjectno()))
                {
                    Log.info("verification for projectno - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno());
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));
                    printLog("metamodifiedon");

                    Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));
                    printLog("sourceref");

                    Assert.assertEquals("versiontype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getVersiontype(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getVersiontype()));
                    printLog("versiontype");

                    Assert.assertEquals("editionno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEditionno(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getEditionno()));
                    printLog("editionno");

                    Assert.assertEquals("isbn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getIsbn()));
                    printLog("isbn");

                    Assert.assertEquals("projectno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getProjectno()));
                    printLog("projectno");

                    Assert.assertEquals("workmaster mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorkmaster(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getWorkmaster()));
                    printLog("workmaster");

                    Log.info("------------------------------------------");
                    break;
                }
            }
            Assert.assertTrue("projectno missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno(),Found);
        }

        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ "projectno(s) verified for sourceref"
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentOriginatoraddress(){//created by Nishant @ 6 Nov 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()))
                {
                    Log.info("verification for metamodifiedon - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon());
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals("additionaladdress mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAdditionaladdress(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAdditionaladdress()));
                    printLog("additionaladdress");

                    Assert.assertEquals("addressid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressid()));
                    printLog("addressid");

                    Assert.assertEquals("addressline1 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressline1(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressline1()));
                    printLog("addressline1");

                    Assert.assertEquals("addressline2 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressline2(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressline2()));
                    printLog("addressline2");

                    Assert.assertEquals("addressline3 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressline3(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressline3()));
                    printLog("addressline3");

                    Assert.assertEquals("businesspartnerid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinesspartnerid()));
                    printLog("businesspartnerid");

                    Assert.assertEquals("city mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCity(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getCity()));
                    printLog("city");

                    Assert.assertEquals("country mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCountry(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getCountry()));
                    printLog("country");

                    Assert.assertEquals("district mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDistrict(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getDistrict()));
                    printLog("district");

                    Assert.assertEquals("email mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEmail(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getEmail()));
                    printLog("email");

                    Assert.assertEquals("fax mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFax(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getFax()));
                    printLog("fax");

                    Assert.assertEquals("houseno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getHouseno(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getHouseno()));
                    printLog("houseno");

                    Assert.assertEquals("internet mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInternet(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getInternet()));
                    printLog("internet");

                    Assert.assertEquals("ismainaddress mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsmainaddress(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getIsmainaddress()));
                    printLog("ismainaddress");

                    Assert.assertEquals("mobile mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMobile(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMobile()));
                    printLog("mobile");

                    Assert.assertEquals("postalcode mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPostalcode(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPostalcode()));
                    printLog("postalcode");

                    Assert.assertEquals("street mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getStreet(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getStreet()));
                    printLog("street");

                    Assert.assertEquals("telephonemain mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTelephonemain(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getTelephonemain()));
                    printLog("telephonemain");

                    Assert.assertEquals("telephoneother mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTelephoneother(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getTelephoneother()));
                    printLog("telephoneother");

                    Log.info("------------------------------------------");
                    break;
                }
            }
            Assert.assertTrue("modifiedon value missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries verified for businesspartnerid "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getBusinesspartnerid());

    }

    public void compareIngestVsCurrentOriginators(){//created by Nishant @ 11 Nov 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinesspartnerid()))
                {
                    Log.info("verification for businessPartnerId - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid());
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals("prefix mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPrefix(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPrefix()));
                    printLog("prefix");

                    Assert.assertEquals("sequence mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSequence(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSequence()));
                    printLog("sequence");

                    Assert.assertEquals("businesspartnerid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinesspartnerid()));
                    printLog("businesspartnerid");

                    Assert.assertEquals("originatorid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOriginatorid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getOriginatorid()));
                    printLog("originatorid");

                    Assert.assertEquals("isperson mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsperson(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getIsperson()));
                    printLog("isperson");

                    Assert.assertEquals("locationid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLocationid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getLocationid()));
                    printLog("locationid");

                    Assert.assertEquals("copyrightholdertype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCopyrightholdertype(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getCopyrightholdertype()));
                    printLog("copyrightholdertype");

                    Assert.assertEquals("institution mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInstitution(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getInstitution()));
                    printLog("institution");

                    Assert.assertEquals("firstname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFirstname(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getFirstname()));
                    printLog("firstname");

                    Assert.assertEquals("department mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDepartment(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getDepartment()));
                    printLog("department");

                    Assert.assertEquals("lastname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLastname(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getLastname()));
                    printLog("lastname");

                    Assert.assertEquals("searchterm mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSearchterm(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSearchterm()));
                    printLog("searchterm");

                    Log.info("------------------------------------------");
                    break;
                }
            }
            Assert.assertTrue("businessparternedId missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries verified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentPricing(){//created by Nishant @ 13 Nov 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            Log.info(i+"...\n --initial ingest pricing...");
            Log.info(bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon());
            Log.info(bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted());
            Log.info(bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref());
            Log.info(bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValidfrom());
            Log.info(bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getType());
            Log.info(bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCurrency());
            Log.info(bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPriceapprox());
            Log.info(bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPrice());
            Log.info(bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValidto());

            Log.info(" --current pricing...");
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getValidfrom());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getType());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCurrency());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPriceapprox());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPrice());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getValidto());

            Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
            printLog("Metadeleted");

            Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
            printLog("metamodifiedon");
            Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
            printLog("sourceref");

            Assert.assertEquals("validfrom mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValidfrom(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getValidfrom()));
            printLog("validfrom");

            Assert.assertEquals("type mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getType(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getType()));
            printLog("type");

            Assert.assertEquals("currency mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCurrency(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCurrency()));
            printLog("currency");

            Assert.assertEquals("priceapprox mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPriceapprox(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPriceapprox()));
            printLog("priceapprox");

            Assert.assertEquals("price mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPrice(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPrice()));
            printLog("price");

            Assert.assertEquals("validto mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValidto(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getValidto()));
            printLog("validto");

            Log.info("------------------------------------------");


        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries verified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentProduct(){//created by Nishant @ 16 Nov 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;

            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn()))
            {
                Log.info("verification for isbn - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn());
                Found=true;

                Assert.assertEquals("ausavailablestock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAusavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAusavailablestock()));
                printLog("ausavailablestock");

                Assert.assertEquals("binding mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBinding(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBinding()));
                printLog("binding");

                Assert.assertEquals("budgetpubdate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBudgetpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBudgetpubdate()));
                printLog("budgetpubdate");

                Assert.assertEquals("contractpubdate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getContractpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getContractpubdate()));
                printLog("contractpubdate");

                Assert.assertEquals("createdon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCreatedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCreatedon()));
                printLog("createdon");

                Assert.assertEquals("deliverystatus mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDeliverystatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDeliverystatus()));
                printLog("deliverystatus");

                Assert.assertEquals("externaleditionid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExternaleditionid(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExternaleditionid()));
                printLog("externaleditionid");

                Assert.assertEquals("externalimpressionid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExternalimpressionid(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExternalimpressionid()));
                printLog("externalimpressionid");

                Assert.assertEquals("firstprinting mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFirstprinting(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFirstprinting()));
                printLog("firstprinting");

                Assert.assertEquals("firstrelease mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFirstrelease(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFirstrelease()));
                printLog("firstrelease");

                Assert.assertEquals("fraavailablestock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFraavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFraavailablestock()));
                printLog("fraavailablestock");

                Assert.assertEquals("fratotalstock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFratotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFratotalstock()));
                printLog("fratotalstock");

                Assert.assertEquals("geravailablestock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getGeravailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGeravailablestock()));
                printLog("geravailablestock");

                Assert.assertEquals("gertotalstock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getGertotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGertotalstock()));
                printLog("gertotalstock");

                Assert.assertEquals("isbn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn()));
                printLog("isbn");

                Assert.assertEquals("isbn13 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn13(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn13()));
                printLog("isbn13");

                Assert.assertEquals("latestpubdate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLatestpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLatestpubdate()));
                printLog("latestpubdate");

                Assert.assertEquals("medium mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMedium(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMedium()));
                printLog("medium");

                Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals("modifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getModifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getModifiedon()));
                printLog("modifiedon");

                Assert.assertEquals("noofvolumes mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getNoofvolumes(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getNoofvolumes()));
                printLog("noofvolumes");

                Assert.assertEquals("orderno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOrderno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOrderno()));
                printLog("orderno");

                Assert.assertEquals("origtitle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOrigtitle(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOrigtitle()));
                printLog("origtitle");

                Assert.assertEquals("planned mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPlanned(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlanned()));
                printLog("planned");

                Assert.assertEquals("plannededitionsize mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPlannededitionsize(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlannededitionsize()));
                printLog("plannededitionsize");

                Assert.assertEquals("plannedfirstprint mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPlannedfirstprint(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlannedfirstprint()));
                printLog("plannedfirstprint");

                Assert.assertEquals("podsuitable mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPodsuitable(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPodsuitable()));
                printLog("podsuitable");

                Assert.assertEquals("projectno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProjectno()));
                printLog("projectno");

                Assert.assertEquals("pubdateplanned mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPubdateplanned(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPubdateplanned()));
                printLog("pubdateplanned");

                Assert.assertEquals("publishedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPublishedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPublishedon()));
                printLog("publishedon");

                Assert.assertEquals("reason mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getReason(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getReason()));
                printLog("reason");

                Assert.assertEquals("refkey mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getRefkey(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRefkey()));
                printLog("refkey");

                Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals("ukavailablestock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getUkavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUkavailablestock()));
                printLog("ukavailablestock");

                Assert.assertEquals("uktotalstock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getUktotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUktotalstock()));
                printLog("uktotalstock");

                Assert.assertEquals("unitcost mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getUnitcost(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUnitcost()));
                printLog("unitcost");

                Assert.assertEquals("usavailablestock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getUsavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUsavailablestock()));
                printLog("usavailablestock");

                Assert.assertEquals("ustotalstock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getUstotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUstotalstock()));
                printLog("ustotalstock");

                Assert.assertEquals("versiontype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getVersiontype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVersiontype()));
                printLog("versiontype");
                Log.info("------------------------------------------");
                break;
            }

            Assert.assertTrue("isbn missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries verified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentProduction(){
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;

            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()))
            {
                Log.info("verification for sourceref - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref());
                Found=true;

                Assert.assertEquals("addillustration mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddillustration(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAddillustration()));
                printLog("addillustration");

                Assert.assertEquals("approxpages mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getApproxpages(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getApproxpages()));
                printLog("approxpages");

                Assert.assertEquals("authoringsystem mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAuthoringsystem(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAuthoringsystem()));
                printLog("authoringsystem");

                Assert.assertEquals("backcoverpms mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBackcoverpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBackcoverpms()));
                printLog("backcoverpms");

                Assert.assertEquals("backpapercolour mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBackpapercolour(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBackpapercolour()));
                printLog("backpapercolour");

                Assert.assertEquals("biblioreference mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBiblioreference(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBiblioreference()));
                printLog("biblioreference");

                Assert.assertEquals("bindmeth mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBindmeth(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBindmeth()));
                printLog("bindmeth");

                Assert.assertEquals("boardthickness mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBoardthickness(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBoardthickness()));
                printLog("boardthickness");

                Assert.assertEquals("classification mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassification(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getClassification()));
                printLog("classification");

                Assert.assertEquals("copyedlevel mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCopyedlevel(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCopyedlevel()));
                printLog("copyedlevel");

                Assert.assertEquals("duotone mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDuotone(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDuotone()));
                printLog("duotone");

                Assert.assertEquals("eonlypages mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEonlypages(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEonlypages()));
                printLog("eonlypages");

                Assert.assertEquals("extentprod mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExtentprod(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExtentprod()));
                printLog("extentprod");

                Assert.assertEquals("extentstatus mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExtentstatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExtentstatus()));
                printLog("extentstatus");

                Assert.assertEquals("exteriorpms mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExteriorpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExteriorpms()));
                printLog("exteriorpms");

                Assert.assertEquals("externalads mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExternalads(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExternalads()));
                printLog("externalads");

                Assert.assertEquals("finishing mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFinishing(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFinishing()));
                printLog("finishing");

                Assert.assertEquals("format mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFormat(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFormat()));
                printLog("format");

                Assert.assertEquals("frontcoverpms mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFrontcoverpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFrontcoverpms()));
                printLog("frontcoverpms");

                Assert.assertEquals("frontpapercolour mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFrontpapercolour(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFrontpapercolour()));
                printLog("frontpapercolour");

                Assert.assertEquals("grammage mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getGrammage(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGrammage()));
                printLog("grammage");

                Assert.assertEquals("graphicsbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getGraphicsbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGraphicsbw()));
                printLog("graphicsbw");

                Assert.assertEquals("graphicscolors mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getGraphicscolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGraphicscolors()));
                printLog("graphicscolors");

                Assert.assertEquals("halftonesbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getHalftonesbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getHalftonesbw()));
                printLog("halftonesbw");

                Assert.assertEquals("halftonescolors mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getHalftonescolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getHalftonescolors()));
                printLog("halftonescolors");

                Assert.assertEquals("illustrationsbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIllustrationsbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIllustrationsbw()));
                printLog("illustrationsbw");

                Assert.assertEquals("illustrationscolors mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIllustrationscolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIllustrationscolors()));
                printLog("illustrationscolors");

                Assert.assertEquals("interiorcolour mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInteriorcolour(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInteriorcolour()));
                printLog("interiorcolour");

                Assert.assertEquals("interiorpms mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInteriorpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInteriorpms()));
                printLog("interiorpms");

                Assert.assertEquals("internalads mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInternalads(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInternalads()));
                printLog("internalads");

                Assert.assertEquals("lineartbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLineartbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLineartbw()));
                printLog("lineartbw");

                Assert.assertEquals("lineartcolors mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLineartcolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLineartcolors()));
                printLog("lineartcolors");

                Assert.assertEquals("manuscriptpages mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getManuscriptpages(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getManuscriptpages()));
                printLog("manuscriptpages");

                Assert.assertEquals("mapsbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMapsbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMapsbw()));
                printLog("mapsbw");

                Assert.assertEquals("mapscolor mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMapscolor(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMapscolor()));
                printLog("mapscolor");

                Assert.assertEquals("material mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMaterial(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMaterial()));
                printLog("material");

                Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals("mstype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMstype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMstype()));
                printLog("mstype");

                Assert.assertEquals("pagesarabic mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPagesarabic(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPagesarabic()));
                printLog("pagesarabic");

                Assert.assertEquals("pagesroman mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPagesroman(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPagesroman()));
                printLog("pagesroman");

                Assert.assertEquals("paperquality mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPaperquality(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPaperquality()));
                printLog("paperquality");

                Assert.assertEquals("printform mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPrintform(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPrintform()));
                printLog("printform");

                Assert.assertEquals("productiondetails mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProductiondetails(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProductiondetails()));
                printLog("productiondetails");

                Assert.assertEquals("productionmethod mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProductionmethod(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProductionmethod()));
                printLog("productionmethod");

                Assert.assertEquals("sectioncolours mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSectioncolours(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSectioncolours()));
                printLog("sectioncolours");

                Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals("spec mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSpec(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSpec()));
                printLog("spec");

                Assert.assertEquals("spinestyle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSpinestyle(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSpinestyle()));
                printLog("spinestyle");

                Assert.assertEquals("suppliera mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSuppliera(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSuppliera()));
                printLog("suppliera");

                Assert.assertEquals("supplierafullname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSupplierafullname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierafullname()));
                printLog("supplierafullname");

                Assert.assertEquals("supplierashortname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSupplierashortname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierashortname()));
                printLog("supplierashortname");

                Assert.assertEquals("supplierb mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSupplierb(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierb()));
                printLog("supplierb");

                Assert.assertEquals("supplierbfullname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSupplierbfullname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierbfullname()));
                printLog("supplierbfullname");

                Assert.assertEquals("supplierbshortname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSupplierbshortname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierbshortname()));
                printLog("supplierbshortname");

                Assert.assertEquals("tablesbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTablesbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTablesbw()));
                printLog("tablesbw");

                Assert.assertEquals("tablescolors mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTablescolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTablescolors()));
                printLog("tablescolors");

                Assert.assertEquals("tagging mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTagging(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTagging()));
                printLog("tagging");

                Assert.assertEquals("textdesigntype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTextdesigntype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTextdesigntype()));
                printLog("textdesigntype");

                Assert.assertEquals("trimother mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTrimother(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTrimother()));
                printLog("trimother");

                Assert.assertEquals("trimsize mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTrimsize(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTrimsize()));
                printLog("trimsize");

                Assert.assertEquals("weight mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWeight(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWeight()));
                printLog("weight");


            }

            Assert.assertTrue("sourceref value missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),Found);
        }

        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries verified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentRelations(){//created by Nishant @ 19 Nov 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;

            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn()))
            {
                Log.info("verification for isbn - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn());
                Found=true;

                Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals("orderno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOrderno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOrderno()));
                printLog("orderno");

                Assert.assertEquals("relationtype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getRelationtype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRelationtype()));
                printLog("relationtype");

                Assert.assertEquals("projectno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProjectno()));
                printLog("projectno");

                Assert.assertEquals("isbn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn()));
                printLog("isbn");

                Log.info("------------------------------------------");

            }

            Assert.assertTrue("isbn value missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " entries verified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentresponsibilities(){ //created by Nishant @ 20 Nov 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getResponsibility()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getResponsibility()))
            {
                Log.info("verification for responsibility - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getResponsibility());
                Found=true;

                Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals("responsibility mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getResponsibility(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getResponsibility()));
                printLog("responsibility");

                Assert.assertEquals("responsibleperson mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getResponsibleperson(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getResponsibleperson()));
                printLog("responsibleperson");

                Assert.assertEquals("personid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPersonid(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPersonid()));
                printLog("personid");


                Log.info("------------------------------------------");
            }

            Assert.assertTrue("responsibility value missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getResponsibility(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries verified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentsublocation(){//created by Nishant @ 20 Nov 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWarehouse()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWarehouse()))
            {
                Log.info("verification for warehouse - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWarehouse());
                Found=true;

                Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals("warehouse mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWarehouse(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWarehouse()));
                printLog("warehouse");

                Assert.assertEquals("pubdateactual mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPubdateactual(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPubdateactual()));
                printLog("pubdateactual");

                Assert.assertEquals("stocksplit mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getStocksplit(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getStocksplit()));
                printLog("stocksplit");

                Assert.assertEquals("refkey mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getRefkey(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRefkey()));
                printLog("refkey");

                Assert.assertEquals("status mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getStatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getStatus()));
                printLog("status");

                Assert.assertEquals("plannedpubdate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPlannedpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlannedpubdate()));
                printLog("plannedpubdate");



                Log.info("------------------------------------------");

            }

            Assert.assertTrue("warehouse value missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWarehouse(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries verified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentText(){
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getName()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getName()))
            {
                Log.info("verification for name - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getName());
                Found=true;

                Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals("tab mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTab(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTab()));
                printLog("tab");

                Assert.assertEquals("texttype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTexttype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTexttype()));
                printLog("texttype");

                Assert.assertEquals("name mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getName(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getName()));
                printLog("name");

                Assert.assertEquals("text mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getText(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getText()));
                printLog("text");

                Assert.assertEquals("status mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getStatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getStatus()));
                printLog("status");

                Log.info("------------------------------------------");

            }

            Assert.assertTrue("name missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getName(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries verified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentversionfamily(){
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()))
            {
                Log.info("verification for workmasterisbn - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorkmasterisbn());
                Found=true;

                Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals("workmasterisbn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorkmasterisbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorkmasterisbn()));
                printLog("workmasterisbn");

                Assert.assertEquals("workmasterprojectno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorkmasterprojectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorkmasterprojectno()));
                printLog("workmasterprojectno");

                Assert.assertEquals("childisbn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getChildisbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getChildisbn()));
                printLog("childisbn");

                Assert.assertEquals("childprojectno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getChildprojectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getChildprojectno()));
                printLog("childprojectno");

                Log.info("------------------------------------------");

            }

            Assert.assertTrue("classificationCode value missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries verified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    private void printLog(String verified){Log.info("verified..."+verified);}

}
