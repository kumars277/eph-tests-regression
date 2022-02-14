package com.eph.automation.testing.steps.bcs.BCSDataLakeAccess;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSDataQualityContext;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.bcs.BCSCurrentTableDataObject;
import com.eph.automation.testing.models.dao.bcs.BCSHistoryTableDataObject;
import com.eph.automation.testing.models.dao.bcs.BCSInitialIngestDataObject;
import com.eph.automation.testing.services.db.BCSDataLakeSQL.BCSDataLakeDataCheckSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.jsoup.Jsoup;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.lang.reflect.InvocationTargetException;

import static com.eph.automation.testing.models.contexts.DataQualityContext.*;

public class BCSDataCheckSteps {

    private static String sql;
    private static List<String> Ids;
    private static BCSDataQualityContext bcsDataQualityContext = new BCSDataQualityContext();

    @Given("^We get the (.*) random ids from initial ingest (.*)$")
    public void getRandomIdsFromInitialIngest(String countOfRandomIds, String targetTable) {
      //countOfRandomIds = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + countOfRandomIds);
        Log.info("getting random reference ids from initial ingest...");
        switch (targetTable) {
            case "stg_current_classification":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_classification, countOfRandomIds);
                break;
            case "stg_current_content":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_content, countOfRandomIds);
                break;
            case "stg_current_extobject":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_extobject, countOfRandomIds);
                break;
            case "stg_current_fullversionfamily":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_fullversionfamily, countOfRandomIds);
                break;
            case "stg_current_originatoraddress":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_originatoraddress, countOfRandomIds);
                break;
            case "stg_current_originators":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_originators, countOfRandomIds);
                break;
            case "stg_current_pricing":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_pricing, countOfRandomIds);
                break;
            case "stg_current_product":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_product, countOfRandomIds);
                break;
            case "stg_current_production":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_production, countOfRandomIds);
                break;
            case "stg_current_relations":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_relations, countOfRandomIds);
                break;
            case "stg_current_responsibilities":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_responsibilities, countOfRandomIds);
                break;
            case "stg_current_sublocation":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_sublocation, countOfRandomIds);
                break;
            case "stg_current_text":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_text, countOfRandomIds);
                break;
            case "stg_current_versionfamily":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_versionfamily, countOfRandomIds);
                break;
            case "stg_current_originatornotes":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_originatornotes, countOfRandomIds);
                break;

        }

        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        if (targetTable.equalsIgnoreCase("stg_current_originatoraddress")||targetTable.equalsIgnoreCase("stg_current_originatornotes"))
            Ids = randomEPRIds.stream().map(m -> (Integer) m.get("businesspartnerid")).map(String::valueOf).collect(Collectors.toList());
        else
            Ids = randomEPRIds.stream().map(m -> (String) m.get("sourceref")).collect(Collectors.toList());
        Log.info("Randomly picked ids..." + Ids);
       //  Ids.clear();Ids.add("506567");  Log.info("hard coded Id to debug is..." + Ids);       //added by Nishant to debug failures
        setBreadcrumbMessage(ids.toString());
    }

    @When("Get the data records from initial ingest for (.*)")
    public void getInitialIngestData(String targetTable) {
        Log.info("We get initial ingest records...");

        switch (targetTable) {
            case "stg_current_classification":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_classification,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));
                break;

            case "stg_current_content":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_content,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));
                break;

            case "stg_current_extobject":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_extobject,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getComments));
                break;

            case "stg_current_fullversionfamily":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_fullversionfamily,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getProjectno));
                break;

            case "stg_current_originatoraddress":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_originatoraddress,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getMetamodifiedon));
                break;

            case "stg_current_originators":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_originators,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_pricing":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_pricing,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_product":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_product,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_production":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_production,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_relations":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_relations,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_responsibilities":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_responsibilities,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_sublocation":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_sublocation,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_text":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_text,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_versionfamily":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_versionfamily,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_originatornotes":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_originatornotes,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getMetamodifiedon));
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getNotestype));
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));
                break;
        }
    }

    @Then("^Get the records from current tables (.*)$")
    public void getCurrentTableData(String targetTable) {//created by Nishant @ 21 Oct 2020
        Log.info("We get current table records...");
        switch (targetTable) {
            case "stg_current_classification":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_classification,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                break;

            case "stg_current_content":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_content,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                break;

            case "stg_current_extobject":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_extobject,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getComments));
                break;

            case "stg_current_fullversionfamily":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_fullversionfamily,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getProjectno));
                break;

            case "stg_current_originatoraddress":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originatoraddress,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                break;

            case "stg_current_originators":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originators,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                //   bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                break;

            case "stg_current_pricing":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_pricing,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_product":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_product,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_production":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_production,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_relations":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_relations,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_responsibilities":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_responsibilities,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_sublocation":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_sublocation,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_text":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_text,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_versionfamily":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_versionfamily,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_originatornotes":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originatornotes,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getNotestype));
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                break;
        }
    }

    @And("Compare the records of initial ingest and current table (.*)")
    public void compareInitialIngestWithCurrentTable(String targetTable) {//created by Nishant @ 21 Oct 2020
        if (!bcsDataQualityContext.bcsInitialIngestDataObjectList.isEmpty() |
                !bcsDataQualityContext.bcsCurrentTableDataObjectList.isEmpty()) {

            Assert.assertEquals(getBreadcrumbMessage()+" initial ingest data count match with " + targetTable + "\n"
                            + "Initial ingest count = " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size()
                            + "current table count= " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size()
                    , bcsDataQualityContext.bcsInitialIngestDataObjectList.size(),
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.size());

            switch (targetTable) {
                case "stg_current_classification":
                    compareIngestVsCurrentClassification();
                    break;
                case "stg_current_content":
                    compareIngestVsCurrentContent();
                    break;
                case "stg_current_extobject":
                    compareIngestVsCurrentExtobject();
                    break;
                case "stg_current_fullversionfamily":
                    compareIngestVsCurrentFullVersionFamily();
                    break;
                case "stg_current_originatoraddress":
                    compareIngestVsCurrentOriginatoraddress();
                    break;
                case "stg_current_originators":
                    compareIngestVsCurrentOriginators();
                    break;
                case "stg_current_pricing":
                    compareIngestVsCurrentPricing();
                    break;
                case "stg_current_product":
                    compareIngestVsCurrentProduct();
                    break;
                case "stg_current_production":
                    compareIngestVsCurrentProduction();
                    break;
                case "stg_current_relations":
                    compareIngestVsCurrentRelations();
                    break;
                case "stg_current_responsibilities":
                    compareIngestVsCurrentresponsibilities();
                    break;
                case "stg_current_sublocation":
                    compareIngestVsCurrentsublocation();
                    break;
                case "stg_current_text":
                    compareIngestVsCurrentText();
                    break;
                case "stg_current_versionfamily":
                    compareIngestVsCurrentversionfamily();
                    break;
                case "stg_current_originatornotes":
                    compareIngestVsCurrentOriginatorsNotes();
                    break;
            }
        }
    }

    public void compareIngestVsCurrentClassification() {//created by Nishant @ 21 Oct 2020
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean classificationcodeFound = false;
            for (int c = 0; c < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getClassificationcode())) {
                    classificationcodeFound = true;

                    Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));

                    Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));

                    Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));

                    Assert.assertEquals(getBreadcrumbMessage()+" value mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValue(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getValue()));

                    Assert.assertEquals(getBreadcrumbMessage()+" classificationtype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationtype(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getClassificationtype()));

                    Assert.assertEquals(getBreadcrumbMessage()+" priority mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPriority(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPriority()));

                    Assert.assertEquals(getBreadcrumbMessage()+" businessunit mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinessunit(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinessunit()));

                    Log.info("verified data at position " + i);
                    break;
                }
            }
            Assert.assertTrue(getBreadcrumbMessage()+" classificationCode value missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode(), classificationcodeFound);
        }
    }

    public void compareIngestVsCurrentContent() {//created by Nishant @ 22 Oct 2020
        Log.info("verifying initial ingest Vs Current content...\n");
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            Log.info((i + 1) + ". verification for sourceref " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref());
            Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
            printLog("Metadeleted");

            Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
            printLog("metamodifiedon");

            Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
            printLog("sourceref");

            Assert.assertEquals(getBreadcrumbMessage()+" approvedondate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getApprovedondate(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getApprovedondate()));
            printLog("approvedondate");


            Assert.assertEquals(getBreadcrumbMessage()+" companygroup mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCompanygroup(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCompanygroup()));
            printLog("companygroup");


            Assert.assertEquals(getBreadcrumbMessage()+" copyrightyear mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCopyrightyear(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCopyrightyear()));
            printLog("copyrightyear");


            Assert.assertEquals(getBreadcrumbMessage()+" division mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDivision(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDivision()));
            printLog("division");

            Assert.assertEquals(getBreadcrumbMessage()+" doi mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDoi(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDoi()));
            printLog("doi");

            Assert.assertEquals(getBreadcrumbMessage()+" doistatus mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDoistatus(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDoistatus()));
            printLog("doistatus");


            Assert.assertEquals(getBreadcrumbMessage()+" editionid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEditionid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEditionid()));
            printLog("editionid");


            Assert.assertEquals(getBreadcrumbMessage()+" editionno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEditionno(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEditionno()));
            printLog("editionno");

            Assert.assertEquals(getBreadcrumbMessage()+" firstapproval mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFirstapproval(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFirstapproval()));
            printLog("firstapproval");

            Assert.assertEquals(getBreadcrumbMessage()+" impressionid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getImpressionid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getImpressionid()));
            printLog("impressionid");

            Assert.assertEquals(getBreadcrumbMessage()+" imprint mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getImprint(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getImprint()));
            printLog("imprint");

            Assert.assertEquals(getBreadcrumbMessage()+" isset mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsset(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsset()));
            printLog("isset");

            Assert.assertEquals(getBreadcrumbMessage()+" language mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLanguage(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage()));
            printLog("language");

            Assert.assertEquals(getBreadcrumbMessage()+" language2 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLanguage2(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage2()));
            printLog("language2");

            Assert.assertEquals(getBreadcrumbMessage()+" language3 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLanguage3(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage3()));
            printLog("language3");

            Assert.assertEquals(getBreadcrumbMessage()+" objecttype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getObjecttype(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getObjecttype()));
            printLog("objecttype");

            Assert.assertEquals(getBreadcrumbMessage()+" originimpid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOriginimpid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOriginimpid()));
            printLog("originimpid");

            Assert.assertEquals(getBreadcrumbMessage()+" ownership mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOwnership(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOwnership()));
            printLog("ownership");

            Assert.assertEquals(getBreadcrumbMessage()+"  piidack mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPiidack(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPiidack()));
            printLog("piidack");

            Assert.assertEquals(getBreadcrumbMessage()+" publisher mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPublisher(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPublisher()));
            printLog("publisher");

            Assert.assertEquals(getBreadcrumbMessage()+" regstatus mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getRegstatus(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRegstatus()));
            printLog("regstatus");

            Assert.assertEquals(getBreadcrumbMessage()+" series mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeries(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeries()));
            printLog("series");

            Assert.assertEquals(getBreadcrumbMessage()+" seriescode mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeriescode(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriescode()));
            printLog("seriescode");

            Assert.assertEquals(getBreadcrumbMessage()+" seriesid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeriesid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriesid()));
            printLog("seriesid");

            Assert.assertEquals(getBreadcrumbMessage()+" seriesissn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeriesissn(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriesissn()));
            printLog("seriesissn");

            Assert.assertEquals(getBreadcrumbMessage()+" shorttitle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getShorttitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getShorttitle()));
            printLog("shorttitle");

            Assert.assertEquals(getBreadcrumbMessage()+" subgroup mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSubgroup(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSubgroup()));
            printLog("subgroup");

            Assert.assertEquals(getBreadcrumbMessage()+" subtitle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSubtitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSubtitle()));
            printLog("subtitle");

            Assert.assertEquals(getBreadcrumbMessage()+" synctemplate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSynctemplate(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSynctemplate()));
            printLog("synctemplate");

            Assert.assertEquals(getBreadcrumbMessage()+" title mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTitle()));
            printLog("title");

            Assert.assertEquals(getBreadcrumbMessage()+" titleid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTitleid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTitleid()));
            printLog("titleid");

            Assert.assertEquals(getBreadcrumbMessage()+" volumename mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getVolumename(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVolumename()));
            printLog("volumename");

            Assert.assertEquals(getBreadcrumbMessage()+" volumeno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getVolumeno(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVolumeno()));
            printLog("volumeno");

            Assert.assertEquals(getBreadcrumbMessage()+" work_master_flag mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWork_master_flag(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWork_master_flag()));
            printLog("work_master_flag");

            Assert.assertEquals(getBreadcrumbMessage()+" worktitle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorktitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorktitle()));
            printLog("worktitle");
            Log.info("------------------------------------------");
        }
    }

    public void compareIngestVsCurrentExtobject() {//created by Nishant @ 22 Oct 2020
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;
            for (int c = 0; c < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSource()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSource())) {
                    Log.info("verification for - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getComments());
                    Found = true;

                    Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));
                    printLog("metamodifiedon");

                    Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));
                    printLog("sourceref");

                    Assert.assertEquals(getBreadcrumbMessage()+" object mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getObject(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getObject()));
                    printLog("object");

                    Assert.assertEquals(getBreadcrumbMessage()+" type mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getType(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getType()));
                    printLog("type");

                    Assert.assertEquals(getBreadcrumbMessage()+" name mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getName(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getName()));
                    printLog("name");

                    Assert.assertEquals(getBreadcrumbMessage()+" comments mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getComments(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getComments()));
                    printLog("comments");

                    Assert.assertEquals(getBreadcrumbMessage()+" source mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSource(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSource()));
                    printLog("source");

                    Log.info("------------------------------------------");
                    break;
                }

            }
            Assert.assertTrue(getBreadcrumbMessage()+" source missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode(), Found);
        }

    }

    public void compareIngestVsCurrentFullVersionFamily() {//created by Nishant @ 05 Nov 2020
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;
            for (int c = 0; c < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getProjectno())) {
                    Log.info("verification for projectno - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno());
                    Found = true;

                    Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));
                    printLog("metamodifiedon");

                    Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));
                    printLog("sourceref");

                    Assert.assertEquals(getBreadcrumbMessage()+" versiontype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getVersiontype(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getVersiontype()));
                    printLog("versiontype");

                    Assert.assertEquals(getBreadcrumbMessage()+" editionno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEditionno(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getEditionno()));
                    printLog("editionno");

                    Assert.assertEquals(getBreadcrumbMessage()+" isbn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getIsbn()));
                    printLog("isbn");

                    Assert.assertEquals(getBreadcrumbMessage()+" projectno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getProjectno()));
                    printLog("projectno");

                    Assert.assertEquals(getBreadcrumbMessage()+" workmaster mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorkmaster(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getWorkmaster()));
                    printLog("workmaster");

                    Log.info("------------------------------------------");
                    break;
                }
            }
            Assert.assertTrue(getBreadcrumbMessage()+" projectno missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno(), Found);
        }

        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + "projectno(s) verified for sourceref"
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentOriginatoraddress() {//created by Nishant @ 6 Nov 2020
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;
            for (int c = 0; c < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon())) {
                    Log.info("verification for metamodifiedon - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon());
                    Found = true;

                    Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals(getBreadcrumbMessage()+" additionaladdress mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAdditionaladdress(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAdditionaladdress()));
                    printLog("additionaladdress");

                    Assert.assertEquals(getBreadcrumbMessage()+" addressid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressid()));
                    printLog("addressid");

                    Assert.assertEquals(getBreadcrumbMessage()+" addressline1 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressline1(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressline1()));
                    printLog("addressline1");

                    Assert.assertEquals(getBreadcrumbMessage()+" addressline2 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressline2(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressline2()));
                    printLog("addressline2");

                    Assert.assertEquals(getBreadcrumbMessage()+" addressline3 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressline3(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressline3()));
                    printLog("addressline3");

                    Assert.assertEquals(getBreadcrumbMessage()+" businesspartnerid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinesspartnerid()));
                    printLog("businesspartnerid");

                    Assert.assertEquals(getBreadcrumbMessage()+" city mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCity(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getCity()));
                    printLog("city");

                    Assert.assertEquals(getBreadcrumbMessage()+" country mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCountry(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getCountry()));
                    printLog("country");

                    Assert.assertEquals(getBreadcrumbMessage()+" district mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDistrict(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getDistrict()));
                    printLog("district");

                    Assert.assertEquals(getBreadcrumbMessage()+" email mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEmail(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getEmail()));
                    printLog("email");

                    Assert.assertEquals(getBreadcrumbMessage()+" fax mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFax(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getFax()));
                    printLog("fax");

                    Assert.assertEquals(getBreadcrumbMessage()+" houseno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getHouseno(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getHouseno()));
                    printLog("houseno");

                    Assert.assertEquals(getBreadcrumbMessage()+" internet mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInternet(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getInternet()));
                    printLog("internet");

                    Assert.assertEquals(getBreadcrumbMessage()+" ismainaddress mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsmainaddress(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getIsmainaddress()));
                    printLog("ismainaddress");

                    Assert.assertEquals(getBreadcrumbMessage()+" mobile mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMobile(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMobile()));
                    printLog("mobile");

                    Assert.assertEquals(getBreadcrumbMessage()+" postalcode mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPostalcode(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPostalcode()));
                    printLog("postalcode");

                    Assert.assertEquals(getBreadcrumbMessage()+" street mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getStreet(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getStreet()));
                    printLog("street");

                    Assert.assertEquals(getBreadcrumbMessage()+" telephonemain mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTelephonemain(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getTelephonemain()));
                    printLog("telephonemain");

                    Assert.assertEquals(getBreadcrumbMessage()+" telephoneother mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTelephoneother(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getTelephoneother()));
                    printLog("telephoneother");

                    Log.info("------------------------------------------");
                    break;
                }
            }
            Assert.assertTrue(getBreadcrumbMessage()+" modifiedon value missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + " modified entries verified for businesspartnerid "
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getBusinesspartnerid());

    }

    public void compareIngestVsCurrentOriginators() {//created by Nishant @ 11 Nov 2020
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;
            for (int c = 0; c < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinesspartnerid())) {
                    Log.info("verification for businessPartnerId - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid());
                    Found = true;

                    Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals(getBreadcrumbMessage()+" prefix mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPrefix(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPrefix()));
                    printLog("prefix");

                    Assert.assertEquals(getBreadcrumbMessage()+" sequence mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSequence(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSequence()));
                    printLog("sequence");

                    Assert.assertEquals(getBreadcrumbMessage()+" businesspartnerid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinesspartnerid()));
                    printLog("businesspartnerid");

                    Assert.assertEquals(getBreadcrumbMessage()+" originatorid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOriginatorid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getOriginatorid()));
                    printLog("originatorid");

                    Assert.assertEquals(getBreadcrumbMessage()+" isperson mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsperson(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getIsperson()));
                    printLog("isperson");

                    Assert.assertEquals(getBreadcrumbMessage()+" locationid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLocationid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getLocationid()));
                    printLog("locationid");

                    Assert.assertEquals(getBreadcrumbMessage()+" copyrightholdertype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCopyrightholdertype(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getCopyrightholdertype()));
                    printLog("copyrightholdertype");

                    Assert.assertEquals(getBreadcrumbMessage()+" institution mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInstitution(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getInstitution()));
                    printLog("institution");

                    Assert.assertEquals(getBreadcrumbMessage()+" firstname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFirstname(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getFirstname()));
                    printLog("firstname");

                    Assert.assertEquals(getBreadcrumbMessage()+" department mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDepartment(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getDepartment()));
                    printLog("department");

                    Assert.assertEquals(getBreadcrumbMessage()+" lastname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLastname(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getLastname()));
                    printLog("lastname");

                    Assert.assertEquals(getBreadcrumbMessage()+" searchterm mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSearchterm(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSearchterm()));
                    printLog("searchterm");

                    Log.info("------------------------------------------");
                }
            }
            Assert.assertTrue(getBreadcrumbMessage()+" businessparternedId missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentOriginatorsNotes() {//created by Dinesh @ 21 sept 2021
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;
            for (int c = 0; c < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinesspartnerid())) {
                    Log.info("verification for businessPartnerId - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid());
                    Found = true;

                    Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");


                    Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));
                    printLog("Metamodified on");

                    Assert.assertEquals(getBreadcrumbMessage()+" businesspartnerid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinesspartnerid()));
                    printLog("businesspartnerid");

                    Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));
                    printLog("sourceref");

                    Assert.assertEquals(getBreadcrumbMessage()+" notestype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getNotestype(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getNotestype()));
                    printLog("notestype");

                    Assert.assertEquals(getBreadcrumbMessage()+" Notes mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getNotes(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getNotes()));
                    printLog("Notes");

                    Log.info("------------------------------------------");
                }
            }
            Assert.assertTrue(getBreadcrumbMessage()+" businessparternedId missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentPricing() {//created by Nishant @ 13 Nov 2020
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            Log.info(i + "...\n --initial ingest pricing...");
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

            Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
            printLog("Metadeleted");

            Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
            printLog("metamodifiedon");
            Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
            printLog("sourceref");

            Assert.assertEquals(getBreadcrumbMessage()+" validfrom mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValidfrom(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getValidfrom()));
            printLog("validfrom");

            Assert.assertEquals(getBreadcrumbMessage()+" type mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getType(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getType()));
            printLog("type");

            Assert.assertEquals(getBreadcrumbMessage()+" currency mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCurrency(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCurrency()));
            printLog("currency");

            Assert.assertEquals(getBreadcrumbMessage()+" priceapprox mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPriceapprox(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPriceapprox()));
            printLog("priceapprox");

            Assert.assertEquals(getBreadcrumbMessage()+" price mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPrice(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPrice()));
            printLog("price");

            Assert.assertEquals(getBreadcrumbMessage()+" validto mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValidto(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getValidto()));
            printLog("validto");

            Log.info("------------------------------------------");


        }
        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentProduct() {//created by Nishant @ 16 Nov 2020
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;

            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn())) {
                Log.info("verification for isbn - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" ausavailablestock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAusavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAusavailablestock()));
                printLog("ausavailablestock");

                Assert.assertEquals(getBreadcrumbMessage()+" binding mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBinding(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBinding()));
                printLog("binding");

                Assert.assertEquals(getBreadcrumbMessage()+" budgetpubdate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBudgetpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBudgetpubdate()));
                printLog("budgetpubdate");

                Assert.assertEquals(getBreadcrumbMessage()+" contractpubdate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getContractpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getContractpubdate()));
                printLog("contractpubdate");

                Assert.assertEquals(getBreadcrumbMessage()+" createdon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCreatedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCreatedon()));
                printLog("createdon");

                Assert.assertEquals(getBreadcrumbMessage()+" deliverystatus mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDeliverystatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDeliverystatus()));
                printLog("deliverystatus");

                Assert.assertEquals(getBreadcrumbMessage()+" externaleditionid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExternaleditionid(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExternaleditionid()));
                printLog("externaleditionid");

                Assert.assertEquals(getBreadcrumbMessage()+" externalimpressionid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExternalimpressionid(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExternalimpressionid()));
                printLog("externalimpressionid");

                Assert.assertEquals(getBreadcrumbMessage()+" firstprinting mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFirstprinting(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFirstprinting()));
                printLog("firstprinting");

                Assert.assertEquals(getBreadcrumbMessage()+" firstrelease mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFirstrelease(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFirstrelease()));
                printLog("firstrelease");

                Assert.assertEquals(getBreadcrumbMessage()+" fraavailablestock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFraavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFraavailablestock()));
                printLog("fraavailablestock");

                Assert.assertEquals(getBreadcrumbMessage()+" fratotalstock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFratotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFratotalstock()));
                printLog("fratotalstock");

                Assert.assertEquals(getBreadcrumbMessage()+" geravailablestock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getGeravailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGeravailablestock()));
                printLog("geravailablestock");

                Assert.assertEquals(getBreadcrumbMessage()+" gertotalstock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getGertotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGertotalstock()));
                printLog("gertotalstock");

                Assert.assertEquals(getBreadcrumbMessage()+" isbn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn()));
                printLog("isbn");

                Assert.assertEquals(getBreadcrumbMessage()+" isbn13 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn13(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn13()));
                printLog("isbn13");

                Assert.assertEquals(getBreadcrumbMessage()+" latestpubdate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLatestpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLatestpubdate()));
                printLog("latestpubdate");

                Assert.assertEquals(getBreadcrumbMessage()+" medium mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMedium(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMedium()));
                printLog("medium");

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" modifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getModifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getModifiedon()));
                printLog("modifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" noofvolumes mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getNoofvolumes(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getNoofvolumes()));
                printLog("noofvolumes");

                Assert.assertEquals(getBreadcrumbMessage()+" orderno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOrderno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOrderno()));
                printLog("orderno");

                Assert.assertEquals(getBreadcrumbMessage()+" origtitle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOrigtitle(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOrigtitle()));
                printLog("origtitle");

                Assert.assertEquals(getBreadcrumbMessage()+" planned mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPlanned(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlanned()));
                printLog("planned");

                Assert.assertEquals(getBreadcrumbMessage()+" plannededitionsize mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPlannededitionsize(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlannededitionsize()));
                printLog("plannededitionsize");

                Assert.assertEquals(getBreadcrumbMessage()+" plannedfirstprint mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPlannedfirstprint(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlannedfirstprint()));
                printLog("plannedfirstprint");

                Assert.assertEquals(getBreadcrumbMessage()+" podsuitable mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPodsuitable(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPodsuitable()));
                printLog("podsuitable");

                Assert.assertEquals(getBreadcrumbMessage()+" projectno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProjectno()));
                printLog("projectno");

                Assert.assertEquals(getBreadcrumbMessage()+" pubdateplanned mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPubdateplanned(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPubdateplanned()));
                printLog("pubdateplanned");

                Assert.assertEquals(getBreadcrumbMessage()+" publishedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPublishedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPublishedon()));
                printLog("publishedon");

                Assert.assertEquals(getBreadcrumbMessage()+" reason mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getReason(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getReason()));
                printLog("reason");

                Assert.assertEquals(getBreadcrumbMessage()+" refkey mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getRefkey(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRefkey()));
                printLog("refkey");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" ukavailablestock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getUkavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUkavailablestock()));
                printLog("ukavailablestock");

                Assert.assertEquals(getBreadcrumbMessage()+" uktotalstock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getUktotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUktotalstock()));
                printLog("uktotalstock");

                Assert.assertEquals(getBreadcrumbMessage()+" unitcost mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getUnitcost(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUnitcost()));
                printLog("unitcost");

                Assert.assertEquals(getBreadcrumbMessage()+" usavailablestock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getUsavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUsavailablestock()));
                printLog("usavailablestock");

                Assert.assertEquals(getBreadcrumbMessage()+" ustotalstock mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getUstotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUstotalstock()));
                printLog("ustotalstock");

                Assert.assertEquals(getBreadcrumbMessage()+" versiontype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getVersiontype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVersiontype()));
                printLog("versiontype");
                Log.info("------------------------------------------");
                break;
            }

            Assert.assertTrue(getBreadcrumbMessage()+" isbn missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentProduction() {
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;

            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref())) {
                Log.info("verification for sourceref - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" addillustration mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddillustration(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAddillustration()));
                printLog("addillustration");

                Assert.assertEquals(getBreadcrumbMessage()+" approxpages mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getApproxpages(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getApproxpages()));
                printLog("approxpages");

                Assert.assertEquals(getBreadcrumbMessage()+" authoringsystem mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAuthoringsystem(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAuthoringsystem()));
                printLog("authoringsystem");

                Assert.assertEquals(getBreadcrumbMessage()+" backcoverpms mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBackcoverpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBackcoverpms()));
                printLog("backcoverpms");

                Assert.assertEquals(getBreadcrumbMessage()+" backpapercolour mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBackpapercolour(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBackpapercolour()));
                printLog("backpapercolour");

                Assert.assertEquals(getBreadcrumbMessage()+" biblioreference mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBiblioreference(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBiblioreference()));
                printLog("biblioreference");

                Assert.assertEquals(getBreadcrumbMessage()+" bindmeth mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBindmeth(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBindmeth()));
                printLog("bindmeth");

                Assert.assertEquals(getBreadcrumbMessage()+" boardthickness mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBoardthickness(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBoardthickness()));
                printLog("boardthickness");

                Assert.assertEquals(getBreadcrumbMessage()+" classification mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassification(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getClassification()));
                printLog("classification");

                Assert.assertEquals(getBreadcrumbMessage()+" copyedlevel mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCopyedlevel(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCopyedlevel()));
                printLog("copyedlevel");

                Assert.assertEquals(getBreadcrumbMessage()+" duotone mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDuotone(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDuotone()));
                printLog("duotone");

                Assert.assertEquals(getBreadcrumbMessage()+" eonlypages mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEonlypages(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEonlypages()));
                printLog("eonlypages");

                Assert.assertEquals(getBreadcrumbMessage()+" extentprod mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExtentprod(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExtentprod()));
                printLog("extentprod");

                Assert.assertEquals(getBreadcrumbMessage()+" extentstatus mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExtentstatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExtentstatus()));
                printLog("extentstatus");

                Assert.assertEquals(getBreadcrumbMessage()+" exteriorpms mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExteriorpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExteriorpms()));
                printLog("exteriorpms");

                Assert.assertEquals(getBreadcrumbMessage()+" externalads mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getExternalads(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExternalads()));
                printLog("externalads");

                Assert.assertEquals(getBreadcrumbMessage()+" finishing mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFinishing(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFinishing()));
                printLog("finishing");

                Assert.assertEquals(getBreadcrumbMessage()+" format mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFormat(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFormat()));
                printLog("format");

                Assert.assertEquals(getBreadcrumbMessage()+" frontcoverpms mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFrontcoverpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFrontcoverpms()));
                printLog("frontcoverpms");

                Assert.assertEquals(getBreadcrumbMessage()+" frontpapercolour mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFrontpapercolour(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFrontpapercolour()));
                printLog("frontpapercolour");

                Assert.assertEquals(getBreadcrumbMessage()+" grammage mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getGrammage(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGrammage()));
                printLog("grammage");

                Assert.assertEquals(getBreadcrumbMessage()+" graphicsbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getGraphicsbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGraphicsbw()));
                printLog("graphicsbw");

                Assert.assertEquals(getBreadcrumbMessage()+" graphicscolors mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getGraphicscolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGraphicscolors()));
                printLog("graphicscolors");

                Assert.assertEquals(getBreadcrumbMessage()+" halftonesbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getHalftonesbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getHalftonesbw()));
                printLog("halftonesbw");

                Assert.assertEquals(getBreadcrumbMessage()+" halftonescolors mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getHalftonescolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getHalftonescolors()));
                printLog("halftonescolors");

                Assert.assertEquals(getBreadcrumbMessage()+" illustrationsbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIllustrationsbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIllustrationsbw()));
                printLog("illustrationsbw");

                Assert.assertEquals(getBreadcrumbMessage()+" illustrationscolors mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIllustrationscolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIllustrationscolors()));
                printLog("illustrationscolors");

                Assert.assertEquals(getBreadcrumbMessage()+" interiorcolour mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInteriorcolour(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInteriorcolour()));
                printLog("interiorcolour");

                Assert.assertEquals(getBreadcrumbMessage()+" interiorpms mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInteriorpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInteriorpms()));
                printLog("interiorpms");

                Assert.assertEquals(getBreadcrumbMessage()+" internalads mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInternalads(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInternalads()));
                printLog("internalads");

                Assert.assertEquals(getBreadcrumbMessage()+" lineartbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLineartbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLineartbw()));
                printLog("lineartbw");

                Assert.assertEquals(getBreadcrumbMessage()+" lineartcolors mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLineartcolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLineartcolors()));
                printLog("lineartcolors");

                Assert.assertEquals(getBreadcrumbMessage()+" manuscriptpages mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getManuscriptpages(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getManuscriptpages()));
                printLog("manuscriptpages");

                Assert.assertEquals(getBreadcrumbMessage()+" mapsbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMapsbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMapsbw()));
                printLog("mapsbw");

                Assert.assertEquals(getBreadcrumbMessage()+" mapscolor mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMapscolor(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMapscolor()));
                printLog("mapscolor");

                Assert.assertEquals(getBreadcrumbMessage()+" material mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMaterial(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMaterial()));
                printLog("material");

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" mstype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMstype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMstype()));
                printLog("mstype");

                Assert.assertEquals(getBreadcrumbMessage()+" pagesarabic mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPagesarabic(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPagesarabic()));
                printLog("pagesarabic");

                Assert.assertEquals(getBreadcrumbMessage()+" pagesroman mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPagesroman(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPagesroman()));
                printLog("pagesroman");

                Assert.assertEquals(getBreadcrumbMessage()+" paperquality mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPaperquality(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPaperquality()));
                printLog("paperquality");

                Assert.assertEquals(getBreadcrumbMessage()+" printform mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPrintform(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPrintform()));
                printLog("printform");

                Assert.assertEquals(getBreadcrumbMessage()+" productiondetails mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProductiondetails(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProductiondetails()));
                printLog("productiondetails");

                Assert.assertEquals(getBreadcrumbMessage()+" productionmethod mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProductionmethod(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProductionmethod()));
                printLog("productionmethod");

                Assert.assertEquals(getBreadcrumbMessage()+" sectioncolours mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSectioncolours(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSectioncolours()));
                printLog("sectioncolours");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" spec mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSpec(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSpec()));
                printLog("spec");

                Assert.assertEquals(getBreadcrumbMessage()+" spinestyle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSpinestyle(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSpinestyle()));
                printLog("spinestyle");

                Assert.assertEquals(getBreadcrumbMessage()+" suppliera mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSuppliera(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSuppliera()));
                printLog("suppliera");

                Assert.assertEquals(getBreadcrumbMessage()+" supplierafullname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSupplierafullname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierafullname()));
                printLog("supplierafullname");

                Assert.assertEquals(getBreadcrumbMessage()+" supplierashortname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSupplierashortname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierashortname()));
                printLog("supplierashortname");

                Assert.assertEquals(getBreadcrumbMessage()+" supplierb mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSupplierb(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierb()));
                printLog("supplierb");

                Assert.assertEquals(getBreadcrumbMessage()+" supplierbfullname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSupplierbfullname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierbfullname()));
                printLog("supplierbfullname");

                Assert.assertEquals(getBreadcrumbMessage()+" supplierbshortname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSupplierbshortname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierbshortname()));
                printLog("supplierbshortname");

                Assert.assertEquals(getBreadcrumbMessage()+" tablesbw mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTablesbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTablesbw()));
                printLog("tablesbw");

                Assert.assertEquals(getBreadcrumbMessage()+" tablescolors mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTablescolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTablescolors()));
                printLog("tablescolors");

                Assert.assertEquals(getBreadcrumbMessage()+" tagging mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTagging(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTagging()));
                printLog("tagging");

                Assert.assertEquals(getBreadcrumbMessage()+" textdesigntype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTextdesigntype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTextdesigntype()));
                printLog("textdesigntype");

                Assert.assertEquals(getBreadcrumbMessage()+" trimother mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTrimother(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTrimother()));
                printLog("trimother");

                Assert.assertEquals(getBreadcrumbMessage()+" trimsize mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTrimsize(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTrimsize()));
                printLog("trimsize");

                Assert.assertEquals(getBreadcrumbMessage()+" weight mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWeight(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWeight()));
                printLog("weight");


            }

            Assert.assertTrue(getBreadcrumbMessage()+" sourceref value missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(), Found);
        }

        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentRelations() {//created by Nishant @ 19 Nov 2020
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;

            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn())) {
                Log.info("verification for isbn - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" orderno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOrderno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOrderno()));
                printLog("orderno");

                Assert.assertEquals(getBreadcrumbMessage()+" relationtype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getRelationtype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRelationtype()));
                printLog("relationtype");

                Assert.assertEquals(getBreadcrumbMessage()+" projectno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProjectno()));
                printLog("projectno");

                Assert.assertEquals(getBreadcrumbMessage()+" isbn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn()));
                printLog("isbn");

                Log.info("------------------------------------------");

            }

            Assert.assertTrue(getBreadcrumbMessage()+" isbn value missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + " entries verified for sourceref "
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentresponsibilities() { //created by Nishant @ 20 Nov 2020
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;
            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getResponsibility()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getResponsibility())) {
                Log.info("verification for responsibility - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getResponsibility());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" responsibility mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getResponsibility(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getResponsibility()));
                printLog("responsibility");

                Assert.assertEquals(getBreadcrumbMessage()+" responsibleperson mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getResponsibleperson(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getResponsibleperson()));
                printLog("responsibleperson");

                Assert.assertEquals(getBreadcrumbMessage()+" personid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPersonid(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPersonid()));
                printLog("personid");


                Log.info("------------------------------------------");
            }

            Assert.assertTrue(getBreadcrumbMessage()+" responsibility value missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getResponsibility(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentsublocation() {//created by Nishant @ 20 Nov 2020
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;
            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWarehouse()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWarehouse())) {
                Log.info("verification for warehouse - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWarehouse());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" warehouse mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWarehouse(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWarehouse()));
                printLog("warehouse");

                Assert.assertEquals(getBreadcrumbMessage()+" pubdateactual mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPubdateactual(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPubdateactual()));
                printLog("pubdateactual");

                Assert.assertEquals(getBreadcrumbMessage()+" stocksplit mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getStocksplit(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getStocksplit()));
                printLog("stocksplit");

                Assert.assertEquals(getBreadcrumbMessage()+" refkey mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getRefkey(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRefkey()));
                printLog("refkey");

                Assert.assertEquals(getBreadcrumbMessage()+" status mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getStatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getStatus()));
                printLog("status");

                Assert.assertEquals(getBreadcrumbMessage()+" plannedpubdate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPlannedpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlannedpubdate()));
                printLog("plannedpubdate");


                Log.info("------------------------------------------");

            }

            Assert.assertTrue(getBreadcrumbMessage()+" warehouse value missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWarehouse(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentText() {
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;
            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getName()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getName()))
            {
                Log.info("verification for name - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getName());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" tab mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTab(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTab()));
                printLog("tab");

                Assert.assertEquals(getBreadcrumbMessage()+" texttype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTexttype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTexttype()));
                printLog("texttype");

                Assert.assertEquals(getBreadcrumbMessage()+" name mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getName(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getName()));
                printLog("name");

                Assert.assertEquals(getBreadcrumbMessage()+" text mismatch ", Jsoup.parse(bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getText()).text(),
                        Jsoup.parse(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getText()).text());
                printLog("text");

                Assert.assertEquals(getBreadcrumbMessage()+" status mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getStatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getStatus()));
                printLog("status");

                Log.info("------------------------------------------");

            }

            Assert.assertTrue(getBreadcrumbMessage()+" name missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getName(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentversionfamily() {
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            boolean Found = false;
            if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref())) {
                Log.info("verification for workmasterisbn - " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorkmasterisbn());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" workmasterisbn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorkmasterisbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorkmasterisbn()));
                printLog("workmasterisbn");

                Assert.assertEquals(getBreadcrumbMessage()+" workmasterprojectno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorkmasterprojectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorkmasterprojectno()));
                printLog("workmasterprojectno");

                Assert.assertEquals(getBreadcrumbMessage()+" childisbn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getChildisbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getChildisbn()));
                printLog("childisbn");

                Assert.assertEquals(getBreadcrumbMessage()+" childprojectno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getChildprojectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getChildprojectno()));
                printLog("childprojectno");

                Log.info("------------------------------------------");

            }

            Assert.assertTrue(getBreadcrumbMessage()+" classificationCode value missing in current table"
                    + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    private void printLog(String verified) {
        Log.info("verified..." + verified);
    }

    @Given("We get (.*) randomIds for BCS Current table (.*)")
    public void getRandomIdsFromCurrentTable(String countOfRandomIds,String sourceTable) {//created by Nishant @ 26 Nov 2020
      //countOfRandomIds = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + countOfRandomIds);
        Log.info("getting random reference ids from bcs table " + sourceTable);
        switch (sourceTable) {
        case "stg_current_classification":
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_classification,countOfRandomIds);break;
        case "stg_current_content"              :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_content,countOfRandomIds);break;
        case "stg_current_extobject"            :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_extobject,countOfRandomIds);break;
        case "stg_current_fullversionfamily"    :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_fullversionfamily,countOfRandomIds);break;
        case "stg_current_originatoraddress"    :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_originatoraddress,countOfRandomIds);break;
        case "stg_current_originatornotes"    :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_originatornotes,countOfRandomIds);break;
        case "stg_current_originators"          :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_originators,countOfRandomIds);break;
        case "stg_current_pricing"              :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_pricing,countOfRandomIds);break;
        case "stg_current_product"              :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_product,countOfRandomIds);break;
        case "stg_current_production"           :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_production,countOfRandomIds);break;
        case "stg_current_relations"            :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_relations,countOfRandomIds);break;
        case "stg_current_responsibilities"     :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_responsibilities,countOfRandomIds);break;
        case "stg_current_sublocation"          :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_sublocation,countOfRandomIds);break;
        case "stg_current_text"                 :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_text,countOfRandomIds);break;
        case "stg_current_versionfamily"        :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_versionfamily,countOfRandomIds);break;

        }
        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        if (sourceTable.equalsIgnoreCase("stg_current_originatoraddress")||sourceTable.equalsIgnoreCase("stg_current_originatornotes"))
            Ids = randomEPRIds.stream().map(m -> (Integer) m.get("businesspartnerid")).map(String::valueOf).collect(Collectors.toList());
        else
            Ids = randomEPRIds.stream().map(m -> (String) m.get("sourceref")).collect(Collectors.toList());
        Log.info("Randomly picked ids..." + Ids);

       //  Ids.clear();Ids.add("550805"); //added by Nishant to debug failures
        setBreadcrumbMessage(Ids.toString());
    }

    @When("Get data from BCS stg_current (.*)")
    public void getDatafromBCSstgCurrentTables(String sourceTable) {//created by Nishant @ 26 Nov 2020

        Log.info("We get bcs current table records...");

        switch (sourceTable) {
            case "stg_current_classification":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_classification,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getClassificationcode));
                break;

        case "stg_current_content"              :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_content,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
            bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                break;

        case "stg_current_extobject"            :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_extobject,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject .class, Constants.AWS_URL);
            bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getComments));break;

        case "stg_current_fullversionfamily"    :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_fullversionfamily,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject .class, Constants.AWS_URL);
            bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getProjectno));break;

        case "stg_current_originatoraddress"    :
            sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originatoraddress,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject .class, Constants.AWS_URL);
            bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));break;

            case "stg_current_originatornotes"    :
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originatornotes,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject .class, Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getNotestype));
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                break;

            case "stg_current_originators"          :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_originators,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject .class, Constants.AWS_URL);
            break;

        case "stg_current_pricing"              :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_pricing,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
            break;

        case "stg_current_product"              :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_product,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
            break;

        case "stg_current_production"           :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_production,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
            break;

        case "stg_current_relations"            :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_relations,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
            break;

        case "stg_current_responsibilities"     :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_responsibilities,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
            break;

        case "stg_current_sublocation"          :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_sublocation,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
            break;

        case "stg_current_text"                 :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_text,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
            break;

        case "stg_current_versionfamily"        :
            sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_current_versionfamily,
                    Joiner.on("','").join(Ids));
            bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
            break;

        }
    }


    @When("Get data from BCS stg_history (.*)")
    public void getDatafromBCSstgHistoryTables(String targetTable){//created by Nishant @ 26 Nov 2020

        Log.info("We get bcs history table records...");

        switch(targetTable) {
            case "stg_history_classification_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_classification_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getClassificationcode));
                break;
            case "stg_history_content_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_content_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getSourceref));
                break;
            case "stg_history_extobject_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_extobject_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getComments));
                break;
            case "stg_history_fullversionfamily_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_fullversionfamily_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getProjectno));
                break;

            case "stg_history_originators_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_originators_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_history_originatornotes_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_originatornotes_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getMetamodifiedon));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getNotestype));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getSourceref));

                break;

            case "stg_history_originatoraddress_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_originatoraddress_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_history_pricing_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_pricing_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_history_product_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_product_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_history_production_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_production_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;
            case "stg_history_relations_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_relations_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;
            case "stg_history_responsibilities_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_responsibilities_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;
            case "stg_history_sublocation_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_sublocation_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;
            case "stg_history_text_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_text_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;
            case "stg_history_versionfamily_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getData_stg_history_versionfamily_part,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;
        }
    }


    @And("Data check of current table (.*) and history (.*) are identical")
    public void compareBCScurrentWithHistoryTable(String sourceTable,String targetTable) {//created by Nishant @ 26 Nov 2020
        if (!bcsDataQualityContext.bcsCurrentTableDataObjectList.isEmpty() |
                !bcsDataQualityContext.bcsHistoryTableDataObjectsList.isEmpty()) {

            Assert.assertEquals(getBreadcrumbMessage()+" history data match with " + sourceTable + "\n"
                            + "current table count = " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size()
                            + "history table count= " + bcsDataQualityContext.bcsHistoryTableDataObjectsList.size()
                    , bcsDataQualityContext.bcsCurrentTableDataObjectList.size(),
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.size());

            switch (sourceTable) {
                case "stg_current_classification":      compareCurrentVsHistoryClassification();    break;
                case "stg_current_content":             compareCurrentVsHistoryContent();           break;
                case "stg_current_extobject":           compareCurrentVsHistoryExtobject();         break;
                case "stg_current_fullversionfamily":   compareCurrentVsHistoryFullVersionFamily(); break;
                case "stg_current_originatoraddress":   compareCurrentVsHistoryOriginatoraddress();  break;
                case "stg_current_originators":         compareCurrentVsHistoryOriginators();       break;
                case "stg_current_pricing":             compareCurrentVsHistoryPricing();           break;
                case "stg_current_product":             compareCurrentVsHistoryProduct();           break;
                case "stg_current_production":          compareCurrentVsHistoryProduction();        break;
                case "stg_current_relations":           compareCurrentVsHistoryRelations();         break;
                case "stg_current_responsibilities":    compareCurrentVsHistoryResponsibilities();  break;
                case "stg_current_sublocation":         compareCurrentVsHistorySublocation();       break;
                case "stg_current_text":                compareCurrentVsHistoryText();              break;
                case "stg_current_versionfamily":       compareCurrentVsHistoryVersionfamily();      break;
                case "stg_current_originatornotes":       compareCurrentVsHistoryOriginatorNotes();      break;

            }
        }
    }



    public void compareCurrentVsHistoryClassification() {//created by Nishant @ 26 Nov 2020
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;
            if (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getClassificationcode()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getClassificationcode()))
            {
                Log.info((i + 1) + ". verification for classificationcode - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getClassificationcode());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" classificationcode mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getClassificationcode(),
                        (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getClassificationcode()));
                printLog("classificationcode");

                Assert.assertEquals(getBreadcrumbMessage()+" classificationtype mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getClassificationtype(),
                        (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getClassificationtype()));
                printLog("classificationtype");

                Assert.assertEquals(getBreadcrumbMessage()+" value mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getValue(),
                        (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getValue()));
                printLog("value");

                Assert.assertEquals(getBreadcrumbMessage()+" priority mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPriority(),
                        (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPriority()));
                printLog("priority");

                Assert.assertEquals(getBreadcrumbMessage()+" businessunit mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinessunit(),
                        (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBusinessunit()));
                printLog("businessunit");

                Log.info("------------------------------------------");

            }

            Assert.assertTrue(getBreadcrumbMessage()+" classificationcode missing in History table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getClassificationcode(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    public void compareCurrentVsHistoryContent() {//created by Nishant @ 03 Dec 2020
        Log.info("verifying current Vs History content...\n");
        for (int i = 0; i < bcsDataQualityContext.bcsHistoryTableDataObjectsList.size(); i++) {
            Log.info((i + 1) + ". verification for sourceref " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref());

            Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted(),
                    (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted()));
            printLog("Metadeleted");

            Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
            printLog("metamodifiedon");

            Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
            printLog("sourceref");

            Assert.assertEquals(getBreadcrumbMessage()+" approvedondate mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getApprovedondate(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getApprovedondate()));
            printLog("approvedondate");

            Assert.assertEquals(getBreadcrumbMessage()+" companygroup mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getCompanygroup(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCompanygroup()));
            printLog("companygroup");

            Assert.assertEquals(getBreadcrumbMessage()+" copyrightyear mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getCopyrightyear(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCopyrightyear()));
            printLog("copyrightyear");

            Assert.assertEquals(getBreadcrumbMessage()+" division mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getDivision(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDivision()));
            printLog("division");

            Assert.assertEquals(getBreadcrumbMessage()+" doi mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getDoi(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDoi()));
            printLog("doi");

            Assert.assertEquals(getBreadcrumbMessage()+" doistatus mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getDoistatus(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDoistatus()));
            printLog("doistatus");

            Assert.assertEquals(getBreadcrumbMessage()+" editionid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getEditionid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEditionid()));
            printLog("editionid");

            Assert.assertEquals(getBreadcrumbMessage()+" editionno mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getEditionno(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEditionno()));
            printLog("editionno");

            Assert.assertEquals(getBreadcrumbMessage()+" firstapproval mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFirstapproval(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFirstapproval()));
            printLog("firstapproval");

            Assert.assertEquals(getBreadcrumbMessage()+" impressionid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getImpressionid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getImpressionid()));
            printLog("impressionid");

            Assert.assertEquals(getBreadcrumbMessage()+" imprint mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getImprint(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getImprint()));
            printLog("imprint");

            Assert.assertEquals(getBreadcrumbMessage()+" isset mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIsset(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsset()));
            printLog("isset");

            Assert.assertEquals(getBreadcrumbMessage()+" language mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getLanguage(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage()));
            printLog("language");

            Assert.assertEquals(getBreadcrumbMessage()+" language2 mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getLanguage2(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage2()));
            printLog("language2");

            Assert.assertEquals(getBreadcrumbMessage()+" language3 mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getLanguage3(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage3()));
            printLog("language3");

            Assert.assertEquals(getBreadcrumbMessage()+" objecttype mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getObjecttype(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getObjecttype()));
            printLog("objecttype");

            Assert.assertEquals(getBreadcrumbMessage()+" originimpid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getOriginimpid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOriginimpid()));
            printLog("originimpid");

            Assert.assertEquals(getBreadcrumbMessage()+" ownership mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getOwnership(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOwnership()));
            printLog("ownership");

            Assert.assertEquals(getBreadcrumbMessage()+" piidack mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPiidack(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPiidack()));
            printLog("piidack");

            Assert.assertEquals(getBreadcrumbMessage()+" publisher mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPublisher(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPublisher()));
            printLog("publisher");

            Assert.assertEquals(getBreadcrumbMessage()+" regstatus mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getRegstatus(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRegstatus()));
            printLog("regstatus");

            Assert.assertEquals(getBreadcrumbMessage()+" series mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSeries(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeries()));
            printLog("series");

            Assert.assertEquals(getBreadcrumbMessage()+" seriescode mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSeriescode(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriescode()));
            printLog("seriescode");

            Assert.assertEquals(getBreadcrumbMessage()+" seriesid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSeriesid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriesid()));
            printLog("seriesid");

            Assert.assertEquals(getBreadcrumbMessage()+" seriesissn mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSeriesissn(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriesissn()));
            printLog("seriesissn");

            Assert.assertEquals(getBreadcrumbMessage()+" shorttitle mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getShorttitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getShorttitle()));
            printLog("shorttitle");

            Assert.assertEquals(getBreadcrumbMessage()+" subgroup mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSubgroup(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSubgroup()));
            printLog("subgroup");

            Assert.assertEquals(getBreadcrumbMessage()+" subtitle mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSubtitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSubtitle()));
            printLog("subtitle");

            Assert.assertEquals(getBreadcrumbMessage()+" synctemplate mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSynctemplate(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSynctemplate()));
            printLog("synctemplate");

            Assert.assertEquals(getBreadcrumbMessage()+" title mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTitle()));
            printLog("title");

            Assert.assertEquals(getBreadcrumbMessage()+" titleid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTitleid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTitleid()));
            printLog("titleid");

            Assert.assertEquals(getBreadcrumbMessage()+" volumename mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getVolumename(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVolumename()));
            printLog("volumename");

            Assert.assertEquals(getBreadcrumbMessage()+" volumeno mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getVolumeno(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVolumeno()));
            printLog("volumeno");

            Assert.assertEquals(getBreadcrumbMessage()+" work_master_flag mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getWork_master_flag(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWork_master_flag()));
            printLog("work_master_flag");

            Assert.assertEquals(getBreadcrumbMessage()+" worktitle mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getWorktitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorktitle()));
            printLog("worktitle");
            Log.info("------------------------------------------");
        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    public void compareCurrentVsHistoryExtobject() {//created by Nishant @ 04 Dec 2020
        boolean Found = false;
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {

                if (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSource()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSource())) {
                    Log.info((i + 1) + ". verification for - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getComments());
                    Found = true;

                    Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                    printLog("metamodifiedon");

                    Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                    printLog("sourceref");

                    Assert.assertEquals(getBreadcrumbMessage()+" object mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getObject(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getObject()));
                    printLog("object");

                    Assert.assertEquals(getBreadcrumbMessage()+" type mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getType(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getType()));
                    printLog("type");

                    Assert.assertEquals(getBreadcrumbMessage()+" name mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getName(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getName()));
                    printLog("name");

                    Assert.assertEquals(getBreadcrumbMessage()+" comments mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getComments(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getComments()));
                    printLog("comments");

                    Assert.assertEquals(getBreadcrumbMessage()+" source mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSource(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSource()));
                    printLog("source");

                    Log.info("------------------------------------------");

                }

                Assert.assertTrue(getBreadcrumbMessage()+" source missing in current table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSource(), Found);
        }

        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    public void compareCurrentVsHistoryFullVersionFamily() {//created by Nishant @ 04 Dec 2020
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;

                if (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProjectno()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getProjectno())) {

                    Log.info((i + 1) + ". verification for projectno - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProjectno());
                    Found = true;

                    Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                    printLog("metamodifiedon");

                    Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                    printLog("sourceref");

                    Assert.assertEquals(getBreadcrumbMessage()+" versiontype mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getVersiontype(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVersiontype()));
                    printLog("versiontype");

                    Assert.assertEquals(getBreadcrumbMessage()+" editionno mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getEditionno(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEditionno()));
                    printLog("editionno");

                    Assert.assertEquals(getBreadcrumbMessage()+" isbn mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIsbn(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn()));
                    printLog("isbn");

                    Assert.assertEquals(getBreadcrumbMessage()+" projectno mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getProjectno(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProjectno()));
                    printLog("projectno");

                    Assert.assertEquals(getBreadcrumbMessage()+" workmaster mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getWorkmaster(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorkmaster()));
                    printLog("workmaster");

                    Log.info("------------------------------------------");

                }

            Assert.assertTrue(getBreadcrumbMessage()+" projectno missing in current table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProjectno(), Found);
        }

        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + "projectno(s) verified for sourceref"
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    public void compareCurrentVsHistoryOriginatoraddress() {//created by Dinesh @ 21 sept 2021
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;
            for (int c = 0; c < bcsDataQualityContext.bcsHistoryTableDataObjectsList.size(); c++) {
                if (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getMetamodifiedon())) {
                    Log.info("verification for metamodifiedon - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon());
                    Found = true;

                    Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals(getBreadcrumbMessage()+" additionaladdress mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAdditionaladdress(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getAdditionaladdress()));
                    printLog("additionaladdress");

                    Assert.assertEquals(getBreadcrumbMessage()+" addressid mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAddressid(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getAddressid()));
                    printLog("addressid");

                    Assert.assertEquals(getBreadcrumbMessage()+" addressline1 mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAddressline1(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getAddressline1()));
                    printLog("addressline1");

                    Assert.assertEquals(getBreadcrumbMessage()+" addressline2 mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAddressline2(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getAddressline2()));
                    printLog("addressline2");

                    Assert.assertEquals(getBreadcrumbMessage()+" addressline3 mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAddressline3(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getAddressline3()));
                    printLog("addressline3");

                    Assert.assertEquals(getBreadcrumbMessage()+" businesspartnerid mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getBusinesspartnerid()));
                    printLog("businesspartnerid");

                    Assert.assertEquals(getBreadcrumbMessage()+" city mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCity(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getCity()));
                    printLog("city");

                    Assert.assertEquals(getBreadcrumbMessage()+" country mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCountry(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getCountry()));
                    printLog("country");

                    Assert.assertEquals(getBreadcrumbMessage()+" district mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDistrict(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getDistrict()));
                    printLog("district");

                    Assert.assertEquals(getBreadcrumbMessage()+" email mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEmail(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getEmail()));
                    printLog("email");

                    Assert.assertEquals(getBreadcrumbMessage()+" fax mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFax(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getFax()));
                    printLog("fax");

                    Assert.assertEquals(getBreadcrumbMessage()+" houseno mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getHouseno(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getHouseno()));
                    printLog("houseno");

                    Assert.assertEquals(getBreadcrumbMessage()+" internet mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInternet(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getInternet()));
                    printLog("internet");

                    Assert.assertEquals(getBreadcrumbMessage()+" ismainaddress mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsmainaddress(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getIsmainaddress()));
                    printLog("ismainaddress");

                    Assert.assertEquals(getBreadcrumbMessage()+" mobile mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMobile(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getMobile()));
                    printLog("mobile");

                    Assert.assertEquals(getBreadcrumbMessage()+" postalcode mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPostalcode(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getPostalcode()));
                    printLog("postalcode");

                    Assert.assertEquals(getBreadcrumbMessage()+" street mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getStreet(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getStreet()));
                    printLog("street");

                    Assert.assertEquals(getBreadcrumbMessage()+" telephonemain mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTelephonemain(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getTelephonemain()));
                    printLog("telephonemain");

                    Assert.assertEquals(getBreadcrumbMessage()+" telephoneother mismatch ", bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTelephoneother(),
                            (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(c).getTelephoneother()));
                    printLog("telephoneother");

                    Log.info("------------------------------------------");
                    break;
                }
            }
            Assert.assertTrue(getBreadcrumbMessage()+" modifiedon value missing in current table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for businesspartnerid "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getBusinesspartnerid());

    }


    public void compareCurrentVsHistoryOriginators() {//created by Nishant @ 07 Dec 2020
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;

                if (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBusinesspartnerid())) {
                    Log.info((i+1)+". verification for businessPartnerId - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid());
                    Found = true;

                    Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals(getBreadcrumbMessage()+" prefix mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPrefix(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPrefix()));
                    printLog("prefix");

                    Assert.assertEquals(getBreadcrumbMessage()+" sequence mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSequence(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSequence()));
                    printLog("sequence");

                    Assert.assertEquals(getBreadcrumbMessage()+" businesspartnerid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBusinesspartnerid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid()));
                    printLog("businesspartnerid");

                    Assert.assertEquals(getBreadcrumbMessage()+" originatorid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getOriginatorid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOriginatorid()));
                    printLog("originatorid");

                    Assert.assertEquals(getBreadcrumbMessage()+" isperson mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIsperson(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsperson()));
                    printLog("isperson");

                    Assert.assertEquals(getBreadcrumbMessage()+" locationid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getLocationid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLocationid()));
                    printLog("locationid");

                    Assert.assertEquals(getBreadcrumbMessage()+" copyrightholdertype mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getCopyrightholdertype(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCopyrightholdertype()));
                    printLog("copyrightholdertype");

                    Assert.assertEquals(getBreadcrumbMessage()+" institution mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getInstitution(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInstitution()));
                    printLog("institution");

                    Assert.assertEquals(getBreadcrumbMessage()+" firstname mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFirstname(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFirstname()));
                    printLog("firstname");

                    Assert.assertEquals(getBreadcrumbMessage()+" department mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getDepartment(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDepartment()));
                    printLog("department");

                    Assert.assertEquals(getBreadcrumbMessage()+" lastname mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getLastname(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLastname()));
                    printLog("lastname");

                    Assert.assertEquals(getBreadcrumbMessage()+" searchterm mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSearchterm(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSearchterm()));
                    printLog("searchterm");

                    Log.info("------------------------------------------");
                }

            Assert.assertTrue(getBreadcrumbMessage()+" businessparternedId missing in current table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    public void compareCurrentVsHistoryOriginatorNotes() {//created by Dinesh @ 07 sept 2021
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;

            if (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBusinesspartnerid())) {
                Log.info((i+1)+". verification for businessPartnerId - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" businesspartnerid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBusinesspartnerid(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid()));
                printLog("businesspartnerid");

                Assert.assertEquals(getBreadcrumbMessage()+" notestype mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getNotestype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getNotestype()));
                printLog("notestype");

                Assert.assertEquals(getBreadcrumbMessage()+" notes mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getNotes(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getNotes()));
                printLog("notes");

                Log.info("------------------------------------------");
            }

            Assert.assertTrue(getBreadcrumbMessage()+" businessparternedId missing in current table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    public void compareCurrentVsHistoryPricing() {//created by Nishant @ 07 Dec 2020
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            Log.info(i + "...\n --current pricing...");

            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getValidfrom());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getType());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCurrency());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPriceapprox());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPrice());
            Log.info(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getValidto());

            Log.info(" --History pricing...");
            Log.info(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon());
            Log.info(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted());
            Log.info(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref());
            Log.info(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getValidfrom());
            Log.info(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getType());
            Log.info(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getCurrency());
            Log.info(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPriceapprox());
            Log.info(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPrice());
            Log.info(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getValidto());


            Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
            printLog("Metadeleted");

            Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
            printLog("metamodifiedon");
            Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
            printLog("sourceref");

            Assert.assertEquals(getBreadcrumbMessage()+" validfrom mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getValidfrom(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getValidfrom()));
            printLog("validfrom");

            Assert.assertEquals(getBreadcrumbMessage()+" type mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getType(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getType()));
            printLog("type");

            Assert.assertEquals(getBreadcrumbMessage()+" currency mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getCurrency(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCurrency()));
            printLog("currency");

            Assert.assertEquals(getBreadcrumbMessage()+" priceapprox mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPriceapprox(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPriceapprox()));
            printLog("priceapprox");

            Assert.assertEquals(getBreadcrumbMessage()+" price mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPrice(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPrice()));
            printLog("price");

            Assert.assertEquals(getBreadcrumbMessage()+" validto mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getValidto(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getValidto()));
            printLog("validto");

            Log.info("------------------------------------------");


        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    public void compareCurrentVsHistoryProduct() {//created by Nishant @ 08 Dec 2020
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;

            if (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIsbn())) {
                Log.info("verification for isbn - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" ausavailablestock mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getAusavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAusavailablestock()));
                printLog("ausavailablestock");

                Assert.assertEquals(getBreadcrumbMessage()+" binding mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBinding(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBinding()));
                printLog("binding");

                Assert.assertEquals(getBreadcrumbMessage()+" budgetpubdate mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBudgetpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBudgetpubdate()));
                printLog("budgetpubdate");

                Assert.assertEquals(getBreadcrumbMessage()+" contractpubdate mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getContractpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getContractpubdate()));
                printLog("contractpubdate");

                Assert.assertEquals(getBreadcrumbMessage()+" createdon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getCreatedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCreatedon()));
                printLog("createdon");

                Assert.assertEquals(getBreadcrumbMessage()+" deliverystatus mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getDeliverystatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDeliverystatus()));
                printLog("deliverystatus");

                Assert.assertEquals(getBreadcrumbMessage()+" externaleditionid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getExternaleditionid(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExternaleditionid()));
                printLog("externaleditionid");

                Assert.assertEquals(getBreadcrumbMessage()+" externalimpressionid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getExternalimpressionid(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExternalimpressionid()));
                printLog("externalimpressionid");

                Assert.assertEquals(getBreadcrumbMessage()+" firstprinting mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFirstprinting(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFirstprinting()));
                printLog("firstprinting");

                Assert.assertEquals(getBreadcrumbMessage()+" firstrelease mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFirstrelease(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFirstrelease()));
                printLog("firstrelease");

                Assert.assertEquals(getBreadcrumbMessage()+" fraavailablestock mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFraavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFraavailablestock()));
                printLog("fraavailablestock");

                Assert.assertEquals(getBreadcrumbMessage()+" fratotalstock mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFratotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFratotalstock()));
                printLog("fratotalstock");

                Assert.assertEquals(getBreadcrumbMessage()+" geravailablestock mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getGeravailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGeravailablestock()));
                printLog("geravailablestock");

                Assert.assertEquals(getBreadcrumbMessage()+" gertotalstock mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getGertotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGertotalstock()));
                printLog("gertotalstock");

                Assert.assertEquals(getBreadcrumbMessage()+" isbn mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIsbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn()));
                printLog("isbn");

                Assert.assertEquals(getBreadcrumbMessage()+" isbn13 mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIsbn13(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn13()));
                printLog("isbn13");

                Assert.assertEquals(getBreadcrumbMessage()+" latestpubdate mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getLatestpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLatestpubdate()));
                printLog("latestpubdate");

                Assert.assertEquals(getBreadcrumbMessage()+" medium mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMedium(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMedium()));
                printLog("medium");

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" modifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getModifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getModifiedon()));
                printLog("modifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" noofvolumes mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getNoofvolumes(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getNoofvolumes()));
                printLog("noofvolumes");

                Assert.assertEquals(getBreadcrumbMessage()+" orderno mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getOrderno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOrderno()));
                printLog("orderno");

                Assert.assertEquals(getBreadcrumbMessage()+" origtitle mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getOrigtitle(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOrigtitle()));
                printLog("origtitle");

                Assert.assertEquals(getBreadcrumbMessage()+" planned mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPlanned(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlanned()));
                printLog("planned");

                Assert.assertEquals(getBreadcrumbMessage()+" plannededitionsize mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPlannededitionsize(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlannededitionsize()));
                printLog("plannededitionsize");

                Assert.assertEquals(getBreadcrumbMessage()+" plannedfirstprint mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPlannedfirstprint(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlannedfirstprint()));
                printLog("plannedfirstprint");

                Assert.assertEquals(getBreadcrumbMessage()+" podsuitable mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPodsuitable(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPodsuitable()));
                printLog("podsuitable");

                Assert.assertEquals(getBreadcrumbMessage()+" projectno mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getProjectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProjectno()));
                printLog("projectno");

                Assert.assertEquals(getBreadcrumbMessage()+" pubdateplanned mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPubdateplanned(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPubdateplanned()));
                printLog("pubdateplanned");

                Assert.assertEquals(getBreadcrumbMessage()+" publishedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPublishedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPublishedon()));
                printLog("publishedon");

                Assert.assertEquals(getBreadcrumbMessage()+" reason mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getReason(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getReason()));
                printLog("reason");

                Assert.assertEquals(getBreadcrumbMessage()+" refkey mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getRefkey(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRefkey()));
                printLog("refkey");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" ukavailablestock mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getUkavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUkavailablestock()));
                printLog("ukavailablestock");

                Assert.assertEquals(getBreadcrumbMessage()+" uktotalstock mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getUktotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUktotalstock()));
                printLog("uktotalstock");

                Assert.assertEquals(getBreadcrumbMessage()+" unitcost mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getUnitcost(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUnitcost()));
                printLog("unitcost");

                Assert.assertEquals(getBreadcrumbMessage()+" usavailablestock mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getUsavailablestock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUsavailablestock()));
                printLog("usavailablestock");

                Assert.assertEquals(getBreadcrumbMessage()+" ustotalstock mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getUstotalstock(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getUstotalstock()));
                printLog("ustotalstock");

                Assert.assertEquals(getBreadcrumbMessage()+" versiontype mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getVersiontype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVersiontype()));
                printLog("versiontype");
                Log.info("------------------------------------------");
                break;
            }

            Assert.assertTrue(getBreadcrumbMessage()+" isbn missing in current table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    public void compareCurrentVsHistoryProduction() {//created by Nishant @ 08 Dec 2020

        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;

            if (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref())) {
                Log.info("verification for sourceref - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref());
                Found = true;


                Log.info("addillustration History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getAddillustration()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAddillustration()));

                Log.info("approxpages History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getApproxpages()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getApproxpages()));

                Log.info("authoringsystem History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getAuthoringsystem()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAuthoringsystem()));

                Log.info("backcoverpms History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBackcoverpms()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBackcoverpms()));

                Log.info("backpapercolour History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBackpapercolour()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBackpapercolour()));

                Log.info("biblioreference History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBiblioreference()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBiblioreference()));

                Log.info("bindmeth History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBindmeth()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBindmeth()));

                Log.info("boardthickness History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBoardthickness()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBoardthickness()));

                Log.info("classification History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getClassification()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getClassification()));

                Log.info("copyedlevel History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getCopyedlevel()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCopyedlevel()));

                Log.info("duotone History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getDuotone()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDuotone()));

                Log.info("eonlypages History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getEonlypages()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEonlypages()));

                Log.info("extentprod History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getExtentprod()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExtentprod()));

                Log.info("extentstatus History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getExtentstatus()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExtentstatus()));

                Log.info("exteriorpms History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getExteriorpms()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExteriorpms()));

                Log.info("externalads History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getExternalads()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExternalads()));

                Log.info("finishing History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFinishing()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFinishing()));

                Log.info("format History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFormat()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFormat()));

                Log.info("frontcoverpms History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFrontcoverpms()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFrontcoverpms()));

                Log.info("frontpapercolour History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFrontpapercolour()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFrontpapercolour()));

                Log.info("grammage History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getGrammage()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGrammage()));

                Log.info("graphicsbw History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getGraphicsbw()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGraphicsbw()));

                Log.info("graphicscolors History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getGraphicscolors()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGraphicscolors()));

                Log.info("halftonesbw History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getHalftonesbw()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getHalftonesbw()));

                Log.info("halftonescolors History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getHalftonescolors()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getHalftonescolors()));

                Log.info("illustrationsbw History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIllustrationsbw()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIllustrationsbw()));

                Log.info("illustrationscolors History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIllustrationscolors()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIllustrationscolors()));

                Log.info("interiorcolour History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getInteriorcolour()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInteriorcolour()));

                Log.info("interiorpms History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getInteriorpms()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInteriorpms()));

                Log.info("internalads History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getInternalads()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInternalads()));

                Log.info("lineartbw History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getLineartbw()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLineartbw()));

                Log.info("lineartcolors History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getLineartcolors()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLineartcolors()));

                Log.info("manuscriptpages History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getManuscriptpages()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getManuscriptpages()));

                Log.info("mapsbw History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMapsbw()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMapsbw()));

                Log.info("mapscolor History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMapscolor()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMapscolor()));

                Log.info("material History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMaterial()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMaterial()));

                Log.info("Metadeleted History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));

                Log.info("metamodifiedon History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));

                Log.info("mstype History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMstype()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMstype()));

                Log.info("pagesarabic History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPagesarabic()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPagesarabic()));

                Log.info("pagesroman History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPagesroman()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPagesroman()));

                Log.info("paperquality History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPaperquality()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPaperquality()));

                Log.info("printform History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPrintform()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPrintform()));

                Log.info("productiondetails History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getProductiondetails()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProductiondetails()));

                Log.info("productionmethod History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getProductionmethod()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProductionmethod()));

                Log.info("sectioncolours History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSectioncolours()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSectioncolours()));

                Log.info("sourceref History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));

                Log.info("spec History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSpec()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSpec()));

                Log.info("spinestyle History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSpinestyle()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSpinestyle()));

                Log.info("suppliera History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSuppliera()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSuppliera()));

                Log.info("supplierafullname History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSupplierafullname()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierafullname()));

                Log.info("supplierashortname History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSupplierashortname()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierashortname()));

                Log.info("supplierb History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSupplierb()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierb()));

                Log.info("supplierbfullname History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSupplierbfullname()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierbfullname()));

                Log.info("supplierbshortname History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSupplierbshortname()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierbshortname()));

                Log.info("tablesbw History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTablesbw()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTablesbw()));

                Log.info("tablescolors History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTablescolors()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTablescolors()));

                Log.info("tagging History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTagging()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTagging()));

                Log.info("textdesigntype History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTextdesigntype()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTextdesigntype()));

                Log.info("trimother History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTrimother()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTrimother()));

                Log.info("trimsize History Vs Current =>"+ bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTrimsize()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTrimsize()));

                Log.info("weight History Vs Current =>"+  bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getWeight()+" Vs "+
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWeight()));


                Assert.assertEquals(getBreadcrumbMessage()+" addillustration mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getAddillustration(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAddillustration()));
                printLog("addillustration");

                Assert.assertEquals(getBreadcrumbMessage()+" approxpages mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getApproxpages(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getApproxpages()));
                printLog("approxpages");

                Assert.assertEquals(getBreadcrumbMessage()+" authoringsystem mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getAuthoringsystem(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getAuthoringsystem()));
                printLog("authoringsystem");

                Assert.assertEquals(getBreadcrumbMessage()+" backcoverpms mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBackcoverpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBackcoverpms()));
                printLog("backcoverpms");

                Assert.assertEquals(getBreadcrumbMessage()+" backpapercolour mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBackpapercolour(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBackpapercolour()));
                printLog("backpapercolour");

                Assert.assertEquals(getBreadcrumbMessage()+" biblioreference mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBiblioreference(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBiblioreference()));
                printLog("biblioreference");

                Assert.assertEquals(getBreadcrumbMessage()+" bindmeth mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBindmeth(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBindmeth()));
                printLog("bindmeth");

                Assert.assertEquals(getBreadcrumbMessage()+" boardthickness mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getBoardthickness(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBoardthickness()));
                printLog("boardthickness");

                Assert.assertEquals(getBreadcrumbMessage()+" classification mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getClassification(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getClassification()));
                printLog("classification");

                Assert.assertEquals(getBreadcrumbMessage()+" copyedlevel mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getCopyedlevel(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCopyedlevel()));
                printLog("copyedlevel");

                Assert.assertEquals(getBreadcrumbMessage()+" duotone mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getDuotone(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDuotone()));
                printLog("duotone");

                Assert.assertEquals(getBreadcrumbMessage()+" eonlypages mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getEonlypages(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEonlypages()));
                printLog("eonlypages");

                Assert.assertEquals(getBreadcrumbMessage()+" extentprod mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getExtentprod(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExtentprod()));
                printLog("extentprod");

                Assert.assertEquals(getBreadcrumbMessage()+" extentstatus mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getExtentstatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExtentstatus()));
                printLog("extentstatus");

                Assert.assertEquals(getBreadcrumbMessage()+" exteriorpms mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getExteriorpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExteriorpms()));
                printLog("exteriorpms");

                Assert.assertEquals(getBreadcrumbMessage()+" externalads mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getExternalads(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getExternalads()));
                printLog("externalads");

                Assert.assertEquals(getBreadcrumbMessage()+" finishing mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFinishing(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFinishing()));
                printLog("finishing");

                Assert.assertEquals(getBreadcrumbMessage()+" format mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFormat(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFormat()));
                printLog("format");

                Assert.assertEquals(getBreadcrumbMessage()+" frontcoverpms mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFrontcoverpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFrontcoverpms()));
                printLog("frontcoverpms");

                Assert.assertEquals(getBreadcrumbMessage()+" frontpapercolour mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getFrontpapercolour(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFrontpapercolour()));
                printLog("frontpapercolour");

                Assert.assertEquals(getBreadcrumbMessage()+" grammage mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getGrammage(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGrammage()));
                printLog("grammage");

                Assert.assertEquals(getBreadcrumbMessage()+" graphicsbw mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getGraphicsbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGraphicsbw()));
                printLog("graphicsbw");

                Assert.assertEquals(getBreadcrumbMessage()+" graphicscolors mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getGraphicscolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getGraphicscolors()));
                printLog("graphicscolors");

                Assert.assertEquals(getBreadcrumbMessage()+" halftonesbw mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getHalftonesbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getHalftonesbw()));
                printLog("halftonesbw");

                Assert.assertEquals(getBreadcrumbMessage()+" halftonescolors mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getHalftonescolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getHalftonescolors()));
                printLog("halftonescolors");

                Assert.assertEquals(getBreadcrumbMessage()+" illustrationsbw mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIllustrationsbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIllustrationsbw()));
                printLog("illustrationsbw");

                Assert.assertEquals(getBreadcrumbMessage()+" illustrationscolors mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIllustrationscolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIllustrationscolors()));
                printLog("illustrationscolors");

                Assert.assertEquals(getBreadcrumbMessage()+" interiorcolour mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getInteriorcolour(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInteriorcolour()));
                printLog("interiorcolour");

                Assert.assertEquals(getBreadcrumbMessage()+" interiorpms mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getInteriorpms(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInteriorpms()));
                printLog("interiorpms");

                Assert.assertEquals(getBreadcrumbMessage()+" internalads mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getInternalads(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getInternalads()));
                printLog("internalads");

                Assert.assertEquals(getBreadcrumbMessage()+" lineartbw mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getLineartbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLineartbw()));
                printLog("lineartbw");

                Assert.assertEquals(getBreadcrumbMessage()+" lineartcolors mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getLineartcolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLineartcolors()));
                printLog("lineartcolors");

                Assert.assertEquals(getBreadcrumbMessage()+" manuscriptpages mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getManuscriptpages(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getManuscriptpages()));
                printLog("manuscriptpages");

                Assert.assertEquals(getBreadcrumbMessage()+" mapsbw mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMapsbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMapsbw()));
                printLog("mapsbw");

                Assert.assertEquals(getBreadcrumbMessage()+" mapscolor mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMapscolor(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMapscolor()));
                printLog("mapscolor");

                Assert.assertEquals(getBreadcrumbMessage()+" material mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMaterial(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMaterial()));
                printLog("material");

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" mstype mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMstype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMstype()));
                printLog("mstype");

                Assert.assertEquals(getBreadcrumbMessage()+" pagesarabic mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPagesarabic(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPagesarabic()));
                printLog("pagesarabic");

                Assert.assertEquals(getBreadcrumbMessage()+" pagesroman mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPagesroman(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPagesroman()));
                printLog("pagesroman");

                Assert.assertEquals(getBreadcrumbMessage()+" paperquality mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPaperquality(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPaperquality()));
                printLog("paperquality");

                Assert.assertEquals(getBreadcrumbMessage()+" printform mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPrintform(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPrintform()));
                printLog("printform");

                Assert.assertEquals(getBreadcrumbMessage()+" productiondetails mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getProductiondetails(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProductiondetails()));
                printLog("productiondetails");

                Assert.assertEquals(getBreadcrumbMessage()+" productionmethod mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getProductionmethod(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProductionmethod()));
                printLog("productionmethod");

                Assert.assertEquals(getBreadcrumbMessage()+" sectioncolours mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSectioncolours(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSectioncolours()));
                printLog("sectioncolours");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" spec mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSpec(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSpec()));
                printLog("spec");

                Assert.assertEquals(getBreadcrumbMessage()+" spinestyle mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSpinestyle(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSpinestyle()));
                printLog("spinestyle");

                Assert.assertEquals(getBreadcrumbMessage()+" suppliera mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSuppliera(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSuppliera()));
                printLog("suppliera");

                Assert.assertEquals(getBreadcrumbMessage()+" supplierafullname mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSupplierafullname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierafullname()));
                printLog("supplierafullname");

                Assert.assertEquals(getBreadcrumbMessage()+" supplierashortname mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSupplierashortname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierashortname()));
                printLog("supplierashortname");

                Assert.assertEquals(getBreadcrumbMessage()+" supplierb mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSupplierb(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierb()));
                printLog("supplierb");

                Assert.assertEquals(getBreadcrumbMessage()+" supplierbfullname mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSupplierbfullname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierbfullname()));
                printLog("supplierbfullname");

                Assert.assertEquals(getBreadcrumbMessage()+" supplierbshortname mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSupplierbshortname(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSupplierbshortname()));
                printLog("supplierbshortname");

                Assert.assertEquals(getBreadcrumbMessage()+" tablesbw mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTablesbw(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTablesbw()));
                printLog("tablesbw");

                Assert.assertEquals(getBreadcrumbMessage()+" tablescolors mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTablescolors(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTablescolors()));
                printLog("tablescolors");

                Assert.assertEquals(getBreadcrumbMessage()+" tagging mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTagging(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTagging()));
                printLog("tagging");

                Assert.assertEquals(getBreadcrumbMessage()+" textdesigntype mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTextdesigntype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTextdesigntype()));
                printLog("textdesigntype");

                Assert.assertEquals(getBreadcrumbMessage()+" trimother mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTrimother(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTrimother()));
                printLog("trimother");

                Assert.assertEquals(getBreadcrumbMessage()+" trimsize mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTrimsize(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTrimsize()));
                printLog("trimsize");

                Assert.assertEquals(getBreadcrumbMessage()+" weight mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getWeight(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWeight()));
                printLog("weight");


            }

            Assert.assertTrue(getBreadcrumbMessage()+" sourceref value missing in History table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref(), Found);
        }

        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    public void compareCurrentVsHistoryRelations() {//created by Nishant @ 09 Dec 2020
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;

            if (bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIsbn()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn())) {
                Log.info("verification for isbn - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" orderno mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getOrderno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOrderno()));
                printLog("orderno");

                Assert.assertEquals(getBreadcrumbMessage()+" relationtype mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getRelationtype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRelationtype()));
                printLog("relationtype");

                Assert.assertEquals(getBreadcrumbMessage()+" projectno mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getProjectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getProjectno()));
                printLog("projectno");

                Assert.assertEquals(getBreadcrumbMessage()+" isbn mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getIsbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn()));
                printLog("isbn");

                Log.info("------------------------------------------");

            }

            Assert.assertTrue(getBreadcrumbMessage()+" isbn value missing in current table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsbn(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    public void compareCurrentVsHistoryResponsibilities() { //created by Nishant @ 10 Dec 2020
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;
            if (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getResponsibility()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getResponsibility())) {
                Log.info("verification for responsibility - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getResponsibility());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" responsibility mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getResponsibility(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getResponsibility()));
                printLog("responsibility");

                Assert.assertEquals(getBreadcrumbMessage()+" responsibleperson mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getResponsibleperson(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getResponsibleperson()));
                printLog("responsibleperson");

                Assert.assertEquals(getBreadcrumbMessage()+" personid mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPersonid(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPersonid()));
                printLog("personid");


                Log.info("------------------------------------------");
            }

            Assert.assertTrue(getBreadcrumbMessage()+" responsibility value missing in current table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getResponsibility(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    public void compareCurrentVsHistorySublocation() {//created by Nishant @ 10 Dec 2020
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;
            if (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWarehouse()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getWarehouse())) {

                Log.info("verification for warehouse - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWarehouse());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" warehouse mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getWarehouse(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWarehouse()));
                printLog("warehouse");

                Assert.assertEquals(getBreadcrumbMessage()+" pubdateactual mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPubdateactual(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPubdateactual()));
                printLog("pubdateactual");

                Assert.assertEquals(getBreadcrumbMessage()+" stocksplit mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getStocksplit(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getStocksplit()));
                printLog("stocksplit");

                Assert.assertEquals(getBreadcrumbMessage()+" refkey mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getRefkey(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRefkey()));
                printLog("refkey");

                Assert.assertEquals(getBreadcrumbMessage()+" status mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getStatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getStatus()));
                printLog("status");

                Assert.assertEquals(getBreadcrumbMessage()+" plannedpubdate mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getPlannedpubdate(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPlannedpubdate()));
                printLog("plannedpubdate");


                Log.info("------------------------------------------");

            }

            Assert.assertTrue(getBreadcrumbMessage()+" warehouse value missing in current table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWarehouse(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }


    public void compareCurrentVsHistoryText() {//created by Nishant @ 11 Dec 2020
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;
            if (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getName()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getName()))
            {
                Log.info("verification for name - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getName());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" tab mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTab(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTab()));
                printLog("tab");

                Assert.assertEquals(getBreadcrumbMessage()+" texttype mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getTexttype(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTexttype()));
                printLog("texttype");

                Assert.assertEquals(getBreadcrumbMessage()+" name mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getName(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getName()));
                printLog("name");

                Assert.assertEquals(getBreadcrumbMessage()+" text mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getText(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getText()));
                printLog("text");

                Assert.assertEquals(getBreadcrumbMessage()+" status mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getStatus(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getStatus()));
                printLog("status");

                Log.info("------------------------------------------");

            }

            Assert.assertTrue(getBreadcrumbMessage()+" name missing in current table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getName(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }


    public void compareCurrentVsHistoryVersionfamily() {//created by Nishant @ 11 Dec 2020
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            boolean Found = false;
            if (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()
                    .equalsIgnoreCase(bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref())) {
                Log.info("verification for workmasterisbn - " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorkmasterisbn());
                Found = true;

                Assert.assertEquals(getBreadcrumbMessage()+" Metadeleted mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetadeleted(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
                printLog("Metadeleted");

                Assert.assertEquals(getBreadcrumbMessage()+" metamodifiedon mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getMetamodifiedon(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
                printLog("metamodifiedon");

                Assert.assertEquals(getBreadcrumbMessage()+" sourceref mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getSourceref(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
                printLog("sourceref");

                Assert.assertEquals(getBreadcrumbMessage()+" workmasterisbn mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getWorkmasterisbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorkmasterisbn()));
                printLog("workmasterisbn");

                Assert.assertEquals(getBreadcrumbMessage()+" workmasterprojectno mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getWorkmasterprojectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorkmasterprojectno()));
                printLog("workmasterprojectno");

                Assert.assertEquals(getBreadcrumbMessage()+" childisbn mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getChildisbn(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getChildisbn()));
                printLog("childisbn");

                Assert.assertEquals(getBreadcrumbMessage()+" childprojectno mismatch ", bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i).getChildprojectno(),
                        (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getChildprojectno()));
                printLog("childprojectno");

                Log.info("------------------------------------------");

            }

            Assert.assertTrue(getBreadcrumbMessage()+" classificationCode value missing in current table"
                    + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getClassificationcode(), Found);
        }
        Log.info("total " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size() + " modified entries verified for sourceref "
                + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(0).getSourceref());
    }

    @Given("^We get the (.*) random ids for the initial ingest book series (.*)$")
    public void getRandomIdsFromInitialIngestSeries(String countOfRandomIds, String targetTable) {
        // countOfRandomIds = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + countOfRandomIds);
        Log.info("getting random reference ids from initial ingest...");
        switch (targetTable) {
            case "stg_current_classification_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_classification_series, countOfRandomIds);
                break;
            case "stg_current_content_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_content_series, countOfRandomIds);
                break;
            case "stg_current_originatoraddress_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_originatoraddress_series, countOfRandomIds);
                break;
            case "stg_current_originatornotes_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_originatornotes_series, countOfRandomIds);
                break;
            case "stg_current_originators_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_originator_series, countOfRandomIds);
                break;
            case "stg_current_product_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_product_series, countOfRandomIds);
                break;
            case "stg_current_text_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_text_series, countOfRandomIds);
                break;
        }

        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        if (targetTable.equalsIgnoreCase("stg_current_originatoraddress_series")||targetTable.equalsIgnoreCase("stg_current_originators_series")||
                targetTable.equalsIgnoreCase("stg_current_originatornotes_series"))
            Ids = randomEPRIds.stream().map(m -> (Integer) m.get("businesspartnerid")).map(String::valueOf).collect(Collectors.toList());
        else
            Ids = randomEPRIds.stream().map(m -> (String) m.get("sourceref")).collect(Collectors.toList());
        Log.info("Randomly picked ids..." + Ids);

        //  Ids.clear();Ids.add("506567");  Log.info("hard coded Id to debug is..." + Ids);       //added by Nishant to debug failures
        setBreadcrumbMessage(Ids.toString());
    }

    @When("Get the data records from initial ingest book series for (.*)")
    public void getInitialIngestDataForBookSeries(String targetTable) {
        Log.info("We get initial ingest records...");

        switch (targetTable) {
            case "stg_current_classification_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_classification_series_rec,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_content_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_content_series_rec,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_originatoraddress_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_originatoraddress_series_rec,
                        Joiner.on(",").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_originatornotes_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_originatornotes_series_rec,
                        Joiner.on(",").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_originators_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_originators_series_rec, Joiner.on(",").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_product_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_product_series_rec, Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_text_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_text_series_rec,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject.class, Constants.AWS_URL);
                break;
        }

    }


    @Then("^Get the records for current tables for the book series (.*)$")
    public void getCurrentTableDataSeries(String targetTable) {//created by Nishant @ 21 Oct 2020
        Log.info("We get current table records...");
        switch (targetTable) {
            case "stg_current_classification_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_classification_series,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_content_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_content_series,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_originatoraddress_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originatoraddress_series,
                        Joiner.on(",").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_originators_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originators_series,
                        Joiner.on(",").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_originatornotes_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originatornotes_series,
                        Joiner.on(",").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

             case "stg_current_product_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_product_series,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_current_text_series":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_text_series,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSCurrentTableDataObject.class, Constants.AWS_URL);
                break;
            }
    }

    @And("Compare the records for the initial ingest book series and current table book series (.*)")
    public void compareInitialIngestWithCurrentTableSeries(String targetTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (bcsDataQualityContext.bcsInitialIngestDataObjectList.isEmpty()) {
            Log.info("No Data Found in the ingest series Tables ....");
        } else {
            Log.info("Sorting the Ids to compare the records between ingest book series and current table series...");
        }
        for (int i = 0; i < bcsDataQualityContext.bcsInitialIngestDataObjectList.size(); i++) {
            switch (targetTable) {
                case "stg_current_classification_series":
                    Log.info("stg_current_classification_series Records:");
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getClassificationcode));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getClassificationcode));

                    String[] classification_series = {"getMetadeleted", "getMetamodifiedon", "getSourceref", "getBusinessunit", "getClassificationtype", "getPriority", "getClassificationcode", "getValue"};
                    for (String strTemp : classification_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSInitialIngestDataObject objectToCompare1 = bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i);
                        BCSCurrentTableDataObject objectToCompare2 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("SourceRef => " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                 " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for Sourceref:" + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "stg_current_content_series":
                    Log.info("stg_current_content_series Records:");
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                    String[] content_series = {"getMetadeleted", "getMetamodifiedon", "getSourceref", "getSubgroup", "getSeriescode", "getMedium", "getWmyn", "getSubtitle","getTitle","getSerialtype"
                    ,"getDivision","getObjtype","getCompanygroup","getSeriesissn","getBinding","getVolumeno","getLanguage","getPublisher","getSeriesid","getShorttitle",
                    "getPiidack","getOwnership","getDeltype","getNumbered","getBibliographicserial","getMainseries","getEditionid"};

                    for (String strTemp : content_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSInitialIngestDataObject objectToCompare1 = bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i);
                        BCSCurrentTableDataObject objectToCompare2 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("SourceRef => " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for Sourceref:" + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "stg_current_originatoraddress_series":
                    Log.info("stg_current_originatoraddress_series Records:");
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                    String[] originatoraddress_series = {"getMetadeleted", "getMetamodifiedon", "getBusinesspartnerid", "getCountry", "getPostalcode", "getAdditionaladdress", "getHouseno", "getInternet","getCity","getStreet","getEmail","getDistrict","getMobile","getFax","getTelephoneother","getTelephonemain"};
                    for (String strTemp : originatoraddress_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSInitialIngestDataObject objectToCompare1 = bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i);
                        BCSCurrentTableDataObject objectToCompare2 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("Businesspartnerid => " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for BusinessPartnerID:" + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "stg_current_originatornotes_series":
                    Log.info("stg_current_originatornotes_series Records:");
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getBusinesspartnerid));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getBusinesspartnerid));
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));

                    String[] originatornotes_series = {"getMetadeleted", "getMetamodifiedon", "getBusinesspartnerid", "getSourceref", "getNotes", "getCompanygroup"};
                    for (String strTemp : originatornotes_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSInitialIngestDataObject objectToCompare1 = bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i);
                        BCSCurrentTableDataObject objectToCompare2 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("Businesspartnerid => " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for BusinessPartnerID:" + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "stg_current_originators_series":
                    Log.info("stg_current_originators_series Records:");
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getBusinesspartnerid));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getBusinesspartnerid));
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getFirstname));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getFirstname));
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSequence));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSequence));

                    String[] originators_series = {"getMetadeleted", "getMetamodifiedon", "getSourceref", "getFirstname", "getBusinesspartnerid","getLastname","getSequence","getPrefix","getCopyrightholdertype","getSearchterm"};
                    for (String strTemp : originators_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSInitialIngestDataObject objectToCompare1 = bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i);
                        BCSCurrentTableDataObject objectToCompare2 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("Businesspartnerid => " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for BusinessPartnerID:" + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "stg_current_product_series":
                    Log.info("stg_current_product_series Records:");
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));

                    String[] product_series = {"getMetadeleted", "getMetamodifiedon", "getSourceref", "getOrderno", "getVersiontype"};
                    for (String strTemp : product_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSInitialIngestDataObject objectToCompare1 = bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i);
                        BCSCurrentTableDataObject objectToCompare2 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("Sourceref => " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for SourceRef:" + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "stg_current_text_series":
                    Log.info("stg_current_text_series Records:");
                    bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));

                    String[] text_series = {"getMetadeleted", "getMetamodifiedon", "getSourceref", "getTab", "getTexttype","getText","getStatus","getName"};
                    for (String strTemp : text_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSInitialIngestDataObject objectToCompare1 = bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i);
                        BCSCurrentTableDataObject objectToCompare2 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("Sourceref => " + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for SourceRef:" + bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;


            }
        }
    }


    @Given("^We get the (.*) random ids from the current book series (.*)$")
    public void getRandomIdsForCurrentSeries(String countOfRandomIds, String targetTable) {
        // countOfRandomIds = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + countOfRandomIds);
        Log.info("getting random reference ids from initial ingest...");
        switch (targetTable) {
            case "stg_current_classification_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_classification_series, countOfRandomIds);
                break;
            case "stg_current_content_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_content_series, countOfRandomIds);
                break;
            case "stg_current_originatoraddress_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_originatoraddress_series, countOfRandomIds);
                break;
            case "stg_current_originatornotes_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_originatornotes_series, countOfRandomIds);
                break;
            case "stg_current_originators_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_originator_series, countOfRandomIds);
                break;
            case "stg_current_product_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_product_series, countOfRandomIds);
                break;
            case "stg_current_text_series":
                sql = String.format(BCSDataLakeDataCheckSQL.randomId_stg_current_text_series, countOfRandomIds);
                break;
        }
        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        if (targetTable.equalsIgnoreCase("stg_current_originatoraddress_series")||targetTable.equalsIgnoreCase("stg_current_originators_series")||
                targetTable.equalsIgnoreCase("stg_current_originatornotes_series"))
            Ids = randomEPRIds.stream().map(m -> (Integer) m.get("businesspartnerid")).map(String::valueOf).collect(Collectors.toList());
        else
            Ids = randomEPRIds.stream().map(m -> (String) m.get("sourceref")).collect(Collectors.toList());
        Log.info("Randomly picked ids..." + Ids);

        setBreadcrumbMessage(Ids.toString());
    }



    @Then("^Get the records for current tables for the staging history book series (.*)$")
    public void getHistoryTableDataSeries(String targetTable) {//created by Nishant @ 21 Oct 2020
        Log.info("We get history table records...");
        switch (targetTable) {
            case "stg_history_classification_series_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getCurrentHistoryTableDataFor_stg_current_classification_series,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_history_content_series_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getHistoryTableDataFor_stg_current_content_series,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_history_originatoraddress_series_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getHistoryTableDataFor_stg_current_originatoraddress_series,
                        Joiner.on(",").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_history_originatornotes_series_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getHistoryTableDataFor_stg_current_originatornotes_series,
                        Joiner.on(",").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_history_originators_series_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getHistTableDataFor_stg_current_originators_series,
                        Joiner.on(",").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_history_product_series_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getHistoryTableDataFor_stg_current_product_series,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;

            case "stg_history_text_series_part":
                sql = String.format(BCSDataLakeDataCheckSQL.getHistoryTableDataFor_stg_current_text_series,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsHistoryTableDataObjectsList = DBManager.getDBResultAsBeanList(sql, BCSHistoryTableDataObject.class, Constants.AWS_URL);
                break;
        }

    }

    @And("Compare the records for the current book series and history table book series (.*)")
    public void compareCurrentTableSeriesWithHistory(String targetTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (bcsDataQualityContext.bcsCurrentTableDataObjectList.isEmpty()) {
            Log.info("No Data Found in the Current series Tables ....");
        } else {
            Log.info("Sorting the Ids to compare the records between current book series and history table series...");
        }
        for (int i = 0; i < bcsDataQualityContext.bcsCurrentTableDataObjectList.size(); i++) {
            switch (targetTable) {
                case "stg_history_classification_series_part":
                    Log.info("stg_current_classification_series Records:");
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getSourceref));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getClassificationcode));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getClassificationcode));

                    String[] classification_series = {"getMetadeleted", "getMetamodifiedon", "getSourceref", "getBusinessunit", "getClassificationtype", "getPriority", "getClassificationcode", "getValue"};
                    for (String strTemp : classification_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSCurrentTableDataObject objectToCompare1 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);
                        BCSHistoryTableDataObject objectToCompare2 = bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("SourceRef => " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for Sourceref:" + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "stg_history_content_series_part":
                    Log.info("stg_current_content_series Records:");
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getSourceref));
                    String[] content_series = {"getMetadeleted", "getMetamodifiedon", "getSourceref", "getSubgroup", "getSeriescode", "getMedium", "getWmyn", "getSubtitle","getTitle","getSerialtype"
                            ,"getDivision","getObjtype","getCompanygroup","getSeriesissn","getBinding","getVolumeno","getLanguage","getPublisher","getSeriesid","getShorttitle",
                            "getPiidack","getOwnership","getDeltype","getNumbered","getBibliographicserial","getMainseries","getEditionid"};

                    for (String strTemp : content_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSCurrentTableDataObject objectToCompare1 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);
                        BCSHistoryTableDataObject objectToCompare2 = bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("SourceRef => " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for Sourceref:" + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "stg_history_originatoraddress_series_part":
                    Log.info("stg_current_originatoraddress_series Records:");
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getMetamodifiedon));
                    String[] originatoraddress_series = {"getMetadeleted", "getMetamodifiedon", "getBusinesspartnerid", "getCountry", "getPostalcode", "getAdditionaladdress", "getHouseno", "getInternet","getCity","getStreet","getEmail","getDistrict","getMobile","getFax","getTelephoneother","getTelephonemain"};
                    for (String strTemp : originatoraddress_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSCurrentTableDataObject objectToCompare1 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);
                        BCSHistoryTableDataObject objectToCompare2 = bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("Businesspartnerid => " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for BusinessPartnerID:" + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "stg_history_originatornotes_series_part":
                    Log.info("stg_current_originatornotes_series Records:");
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getBusinesspartnerid));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getBusinesspartnerid));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getSourceref));

                    String[] originatornotes_series = {"getMetadeleted", "getMetamodifiedon", "getBusinesspartnerid", "getSourceref", "getNotes", "getCompanygroup"};
                    for (String strTemp : originatornotes_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSCurrentTableDataObject objectToCompare1 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);
                        BCSHistoryTableDataObject objectToCompare2 = bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("Businesspartnerid => " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for BusinessPartnerID:" + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "stg_history_originators_series_part":
                    Log.info("stg_current_originators_series Records:");
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getBusinesspartnerid));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getBusinesspartnerid));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getMetamodifiedon));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getSourceref));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSequence));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getSequence));
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getFirstname));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getFirstname));

                    String[] originators_series = {"getMetadeleted", "getMetamodifiedon", "getSourceref", "getFirstname", "getBusinesspartnerid","getLastname","getSequence","getPrefix","getCopyrightholdertype","getSearchterm"};
                    for (String strTemp : originators_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSCurrentTableDataObject objectToCompare1 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);
                        BCSHistoryTableDataObject objectToCompare2 = bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("Businesspartnerid => " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for BusinessPartnerID:" + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getBusinesspartnerid(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "stg_history_product_series_part":
                    Log.info("stg_current_product_series Records:");
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getSourceref));

                    String[] product_series = {"getMetadeleted", "getMetamodifiedon", "getSourceref", "getOrderno", "getVersiontype"};
                    for (String strTemp : product_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSCurrentTableDataObject objectToCompare1 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);
                        BCSHistoryTableDataObject objectToCompare2 = bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("Sourceref => " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for SourceRef:" + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "stg_history_text_series_part":
                    Log.info("stg_current_text_series Records:");
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));
                    bcsDataQualityContext.bcsHistoryTableDataObjectsList.sort(Comparator.comparing(BCSHistoryTableDataObject::getSourceref));

                    String[] text_series = {"getMetadeleted", "getMetamodifiedon", "getSourceref", "getTab", "getTexttype","getText","getStatus","getName"};
                    for (String strTemp : text_series) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCSCurrentTableDataObject objectToCompare1 = bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i);
                        BCSHistoryTableDataObject objectToCompare2 = bcsDataQualityContext.bcsHistoryTableDataObjectsList.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("Sourceref => " + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref() +
                                " " + strTemp + " => Ingest series = " + method.invoke(objectToCompare1) +
                                " "+targetTable+ " = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" table for SourceRef:" + bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
            }
        }
    }


}
