package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.services.db.sql.WorkExtractSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.eph.automation.testing.models.contexts.DataQualityContext.*;
import static org.junit.Assert.*;


@SuppressWarnings("unchecked")
public class ManifestationDataQualityCheckSteps {
    private String sql;
    private static int countManifestationsEPH;
    private static int countManifestationsPMX;
    private static int countManifestationsSTGPMX;
    private static int countManifestationsEPHGD;
    private List<Map<?, ?>> manifestationIds;
    private static List<String> ids;
    private static List<String> isbns;


//    @StaticInjection
//    public DataQualityContext dataQualityContext;

    @Given("We get the count of the manifestations records in PMX$")
    public void getCountManifestationsPmxGdProductManifestation() {
        Log.info("Given We get the count of the manifestations records in PMX ..");

        sql = WorkExtractSQL.COUNT_MANIFESTATIONS_IN_PMX_GD_PRODUCT_MANIFESTATION_TABLE;
        Log.info(sql);
        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.PMX_URL);
        countManifestationsPMX = ((BigDecimal) manifestationsNumber.get(0).get("count")).intValue();
        Log.info("Count of manifestations in PMX.GD_PRODUCT_MANIFESTATION table is: " + countManifestationsPMX);
    }

    @When("We get the count of the manifestations records in PMX STG$")
    public void getCountManifestationsPmxStg() {
        Log.info("When We get the count of the manifestations records in PMX STG ..");
        sql = WorkExtractSQL.COUNT_MANIFESTATIONS_IN_EPH_STG_PMX_MANIFESTATION_TABLE;
        Log.info(sql);
        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countManifestationsSTGPMX = ((Long) manifestationsNumber.get(0).get("count")).intValue();
        Log.info("Count of manifestations in STG_PMX_MANIFESTATION table is: " + countManifestationsSTGPMX);

    }

    @Then("^The number of the records in PMX and EPH staging table is equal$")
    public void verifyCountOfManifestationDataInPMXAndEPHIsEqual() {
        Assert.assertEquals("\nThe number of manifestations in PMX GD_PRODUCT_MANIFESTATION and STG_PMX_MANIFESTATION is not equal", countManifestationsPMX, countManifestationsSTGPMX);
    }

    @When("^The manifestations are transferred to EPH$")
    public void getCountSAManifestation() {
        com.eph.automation.testing.helper.Log.info("When The manifestations are transferred to EPH ..");
        sql = WorkExtractSQL.COUNT_MANIFESTATIONS_IN_SA_MANIFESTATION_TABLE;
        Log.info(sql);
        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countManifestationsEPH = ((Long) manifestationsNumber.get(0).get("count")).intValue();
        Log.info("Count of manifestations in EPH SA table is: " + countManifestationsEPH);

    }

    @Then("^The number of the records in EPH staging table and SA_MANIFESTATION is equal$")
    public void verifyCountOfManifestationDataInSTGEPHandSAManifestationIsEqual() {
        Assert.assertEquals("\nThe number of manifestations in PMX_STG and SA_MANIFESTATION is not equal", countManifestationsSTGPMX, countManifestationsEPH);
    }

    @Then("^The manifestations are transferred to the golden data table$")
    public void getCountGDManifestation() {
        Log.info("Then The manifestations are transferred to the golden data table ..");

        sql = WorkExtractSQL.COUNT_MANIFESTATIONS_IN_GD_MANIFESTATION_TABLE;
        Log.info(sql);

        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countManifestationsEPHGD = ((Long) manifestationsNumber.get(0).get("count")).intValue();
        Log.info("Count of manifestations in GD_MANIFESTATION table is: " + countManifestationsEPHGD);
    }

    @Then("^The number of the records in EPH SA and GD_MANIFESTATION is equal$")
    public void verifyCountOfManifestationsInSaAndGoldenDataTableIsEqual() {
        Assert.assertEquals("\nThe number of manifestations in EPH_STG and GD_MANIFESTATION is not equal", countManifestationsEPH, countManifestationsEPHGD);

    }

    @Given("^We get (.*) random records for (.*)$")
    public void getRandomData(String numberOfRecords, String journalType) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);


        switch (journalType) {
            case "JPR":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_MANIFESTATION_IDS_JPR, numberOfRecords);
                Log.info(sql);
                break;
            case "JEL":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_MANIFESTATION_IDS_JEL, numberOfRecords);
                Log.info(sql);
                break;
        }
        manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = manifestationIds.stream().map(m -> (BigDecimal) m.get("manifestation_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("ids : " + ids);
    }


    @Given("^We get (.*) random ISBNs for (.*)$")
    public void getRandomISBNs(String numberOfRecords, String bookType) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);


        switch (bookType) {
            case "PHB":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISBN_IDS_PHB, numberOfRecords);
                Log.info(sql);
                break;
            case "PSB":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISBN_IDS_PSB, numberOfRecords);
                Log.info(sql);
                break;
            case "EBK":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISBN_IDS_EBK, numberOfRecords);
                Log.info(sql);
                break;
            default:
                break;
        }

        List<Map<?, ?>> randomISBNIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        isbns = randomISBNIds.stream().map(m -> (String) m.get("isbn")).collect(Collectors.toList());
        Log.info(isbns.toString());
    }

    @When("^We get the manifestation ids for these books$")
    public void getManifestationsIds() {
        Log.info("Get manifestationApiObject ids ..");

        //  Get manifestations ids for isbns which are in PMX STG
        sql = String.format(WorkExtractSQL.SELECT_MANIFESTATIONS_IDS_FOR_SPECIFIC_ISBN, Joiner.on("','").join(isbns));
        Log.info(sql);

        manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = manifestationIds.stream().map(m -> (BigDecimal) m.get("manifestation_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("ids : " + ids);

    }

    @Then("^We get the manifestations records from PMX$")
    public void getPMXManifestationData() {
        Log.info("Then We get the manifestations records from PMX ..");
        sql = String.format(WorkExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX, Joiner.on("','").join(ids));
        Log.info(sql);

        manifestationDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.PMX_URL);
    }

    @Then("^We have the manifestations in PMX STG$")
    public void getEPHStagingManifestationData() {
        Log.info("Then We have the manifestations in PMX STG ..");
        sql = String.format(WorkExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX_STG, Joiner.on("','").join(ids));
        Log.info(sql);

        manifestationDataObjectsFromEPH = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);

    }


    @And("^The data for manifestations in PMX and PMX STG is equal$")
    public void compareManifestationDataBetweenPMXAndEPH() {
        Log.info("And The data for manifestations in PMX and PMX STG is equal ..");
//        assertThat("Data for manifestations in PMX and EPH staging is equal without order", dataQualityContext.manifestationDataObjectsFromPMX, containsInAnyOrder(dataQualityContext.manifestationDataObjectsFromEPH.toArray()));

        IntStream.range(0, manifestationDataObjectsFromPMX.size()).forEach(i -> {

            //verify manifestationApiObject ids are equal
            Log.info("ManifestationApiObject id in PMX: " + manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_ID());
            Log.info("ManifestationApiObject id in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Integer.parseInt(manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_ID()),
                    (Integer.parseInt(manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID())));

            //verify MANIFESTATION_KEY_TITLE
            Log.info("MANIFESTATION_KEY_TITLE in PMX: " + manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_KEY_TITLE());
            Log.info("MANIFESTATION_KEY_TITLE in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_KEY_TITLE(), manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE());

            //verify ISBN
            Log.info("ISBN in PMX: " + manifestationDataObjectsFromPMX.get(i).getISBN());
            Log.info("ISBN in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getISBN());

            Assert.assertEquals(manifestationDataObjectsFromPMX.get(i).getISBN(), manifestationDataObjectsFromEPH.get(i).getISBN());

            //verify COVER_HEIGHT
            Log.info("COVER_HEIGHT in PMX: " + manifestationDataObjectsFromPMX.get(i).getCOVER_HEIGHT());
            Log.info("COVER_HEIGHT in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getCOVER_HEIGHT());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", manifestationDataObjectsFromPMX.get(i).getCOVER_HEIGHT(), manifestationDataObjectsFromEPH.get(i).getCOVER_HEIGHT());

            //verify COVER_WIDTH
            Log.info("COVER_WIDTH in PMX: " + manifestationDataObjectsFromPMX.get(i).getCOVER_WIDTH());
            Log.info("COVER_WIDTH in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getCOVER_WIDTH());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", manifestationDataObjectsFromPMX.get(i).getCOVER_WIDTH(), (manifestationDataObjectsFromEPH.get(i).getCOVER_WIDTH()));

            //verify PAGE_HEIGHT
            Log.info("PAGE_HEIGHT in PMX: " + manifestationDataObjectsFromPMX.get(i).getPAGE_HEIGHT());
            Log.info("PAGE_HEIGHT in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getPAGE_HEIGHT());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", manifestationDataObjectsFromPMX.get(i).getPAGE_HEIGHT(), manifestationDataObjectsFromEPH.get(i).getPAGE_HEIGHT());

            //verify PAGE_WIDTH_AMOUNT
            Log.info("PAGE_WIDTH_AMOUNT in PMX: " + manifestationDataObjectsFromPMX.get(i).getPAGE_WIDTH());
            Log.info("PAGE_WIDTH_AMOUNT in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getPAGE_WIDTH());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", manifestationDataObjectsFromPMX.get(i).getPAGE_WIDTH(), manifestationDataObjectsFromEPH.get(i).getPAGE_WIDTH());

            //verify WEIGHT_AMOUNT
            Log.info("WEIGHT_AMOUNT in PMX: " + manifestationDataObjectsFromPMX.get(i).getWEIGHT());
            Log.info("WEIGHT_AMOUNT in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getWEIGHT());

            if (manifestationDataObjectsFromPMX.get(i).getWEIGHT() != null || manifestationDataObjectsFromEPH.get(i).getWEIGHT() != null)
                assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", Float.parseFloat(manifestationDataObjectsFromPMX.get(i).getWEIGHT().trim()), Float.parseFloat(manifestationDataObjectsFromEPH.get(i).getWEIGHT()), 0.0);


            //CARTON_QTY
            Log.info("CARTON_QTY in PMX: " + manifestationDataObjectsFromPMX.get(i).getCARTON_QTY());
            Log.info("CARTON_QTY in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getCARTON_QTY());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ",manifestationDataObjectsFromPMX.get(i).getCARTON_QTY(), manifestationDataObjectsFromEPH.get(i).getCARTON_QTY());

            //INTERNATIONAL_EDITION_IND
            Log.info("INTERNATIONAL_EDITION_IND in PMX: " + manifestationDataObjectsFromPMX.get(i).getINTERNATIONAL_EDITION_IND());
            Log.info("INTERNATIONAL_EDITION_IND in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getINTERNATIONAL_EDITION_IND());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", manifestationDataObjectsFromPMX.get(i).getINTERNATIONAL_EDITION_IND(), manifestationDataObjectsFromEPH.get(i).getINTERNATIONAL_EDITION_IND());


            //COPYRIGHT_DATE
            if (manifestationDataObjectsFromPMX.get(i).getCOPYRIGHT_DATE() != null || manifestationDataObjectsFromEPH.get(i).getCOPYRIGHT_DATE() != null) {
                try {
                    Date pmxCopyrightDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.s").parse(manifestationDataObjectsFromPMX.get(i).getCOPYRIGHT_DATE());
                    Log.info("COPYRIGHT_DATE in PMX: " + pmxCopyrightDate);

                    Date ephCopyrightDate = new SimpleDateFormat("yyyy-MM-dd").parse(manifestationDataObjectsFromEPH.get(i).getCOPYRIGHT_DATE());
                    Log.info("COPYRIGHT_DATE in EPH Staging: " + ephCopyrightDate);

                    assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", pmxCopyrightDate, ephCopyrightDate);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }


            //F_PRODUCT_MANIFESTATION_TYP
            Log.info("F_PRODUCT_MANIFESTATION_TYP in PMX: " + manifestationDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            Log.info("F_PRODUCT_MANIFESTATION_TYP in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", manifestationDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP(), manifestationDataObjectsFromEPH.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            //FORMAT_TXT
            Log.info("FORMAT_TXT in PMX: " + manifestationDataObjectsFromPMX.get(i).getFORMAT_TXT());
            Log.info("FORMAT_TXT in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getFORMAT_TXT());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", manifestationDataObjectsFromPMX.get(i).getFORMAT_TXT(), manifestationDataObjectsFromEPH.get(i).getFORMAT_TXT());

            //F_MANIFESTATION_STATUS
            Log.info("F_MANIFESTATION_STATUS in PMX: " + manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_STATUS());
            Log.info("F_MANIFESTATION_STATUS in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_STATUS());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_STATUS(), manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_STATUS());

            //F_PRODUCT_WORK
            Log.info("F_MANIFESTATION_STATUS in PMX: " + manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_STATUS());
            Log.info("F_MANIFESTATION_STATUS in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_STATUS());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", manifestationDataObjectsFromPMX.get(i).getF_PRODUCT_WORK(), manifestationDataObjectsFromEPH.get(i).getF_PRODUCT_WORK());

            //F_PRODUCT_TYPE
            Log.info("F_PRODUCT_TYPE in PMX: " + manifestationDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            Log.info("F_PRODUCT_TYPE in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ",manifestationDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP(), manifestationDataObjectsFromEPH.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            //MANIFESTATION_SUBTYPE
            Log.info("MANIFESTATION_SUBTYPE in PMX: " + manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_SUBTYPE());
            Log.info("MANIFESTATION_SUBTYPE in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_SUBTYPE());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ",manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_SUBTYPE(), manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_SUBTYPE());

            //COMMODITY
            Log.info("COMMODITY in PMX: " + manifestationDataObjectsFromPMX.get(i).getCOMMODITY());
            Log.info("COMMODITY in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getCOMMODITY());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ", manifestationDataObjectsFromPMX.get(i).getCOMMODITY(), manifestationDataObjectsFromEPH.get(i).getCOMMODITY());

            //MANIFESTATION_SUBSTATUS
            Log.info("ManifestationApiObject substatus in PMX: " + manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_SUBSTATUS());
            Log.info("ManifestationApiObject substatus in EPH Staging: " + manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_SUBSTATUS());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ",manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_SUBSTATUS(), manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_SUBSTATUS());
        });

    }


    @And("^We get the manifestations in EPH SA$")
    public void getManifestationsEPHSA() {
        Log.info("And We get the manifestations in EPH SA ..");
        // Get manifestations data from EPH SA_MANIFESTATION
        sql = String.format(WorkExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_SA, Joiner.on("','").join(ids));
        Log.info(sql);


       manifestationDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    }

    @And("^We check that mandatory fields are not null$")
    public void checkMandatoryFieldsInSAAreNotNull() {
        Log.info("And We check that mandatory fields are not null ..");
        int bound = manifestationDataObjectsFromEPHSA.size();
        IntStream.range(0, bound).forEach(i -> {
            //verify F_EVENT is not null
            assertNotNull(manifestationDataObjectsFromEPHSA.get(i).getF_EVENT());
            //verify manifestationApiObject id is not null
            assertNotNull(manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_ID());
            //verify manifestationApiObject key title is not null
            assertNotNull(manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_KEY_TITLE());
            //verify F_TYPE is not null
            assertNotNull(manifestationDataObjectsFromEPHSA.get(i).getF_TYPE());
            //verify F_STATUS is not null
            assertNotNull(manifestationDataObjectsFromEPHSA.get(i).getF_STATUS());
            //verify F_WWORK is not null
            assertNotNull(manifestationDataObjectsFromEPHSA.get(i).getF_WWORK());
        });
    }


    @And("^We compare the manifestations in PMX STG and EPH SA for (.*)$")
    public void compareDataBetweenStagingAndEPHSA(String type) {
        Log.info("And compare the manifestations in PMX STG and EPH  SA ..");
        //sort data in the lists
        manifestationDataObjectsFromEPH.sort(Comparator.comparing(ManifestationDataObject::getPRODUCT_MANIFESTATION_ID));
        manifestationDataObjectsFromEPHSA.sort(Comparator.comparing(ManifestationDataObject::getPMX_SOURCE_REFERENCE));

//        assertThat("Data for manifestations in EPH Staging and EPH SA is equal without order", dataQualityContext.manifestationDataObjectsFromEPH, containsInAnyOrder(dataQualityContext.manifestationDataObjectsFromEPHSA.toArray()));

        IntStream.range(0, manifestationDataObjectsFromEPH.size()).forEach(i -> {

            //B_CLASSNAME
            Log.info("B_CLASSNAME in sa_manifestation: " + manifestationDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
            assertEquals("ManifestationApiObject", manifestationDataObjectsFromEPHSA.get(i).getB_CLASSNAME());


            //PMX_SOURCE_REFERENCE
            Log.info("PMX_SOURCE_REFERENCE in stg_pmx_manifestation : " + manifestationDataObjectsFromEPH.get(i).getPRODUCT_MANIFESTATION_ID());
            Log.info("PMX_SOURCE_REFERENCE in sa_manifestation: " + manifestationDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());

            assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ",
                    manifestationDataObjectsFromEPH.get(i).getPRODUCT_MANIFESTATION_ID(),
                   manifestationDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());

            //MANIFESTATION_KEY_TITLE
            Log.info("ManifestationApiObject key title in sa_manifestation: " + manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_KEY_TITLE());
            Log.info("ManifestationApiObject key title in staging: " + manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE());


            if (Objects.equals(type, "JPR"))
                assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ", manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_KEY_TITLE(), manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE() + " " + "(Print)");
            else if (Objects.equals(type, "JEL"))
                assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ", manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_KEY_TITLE(), manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE() + " " + "(Online)");
            else
                assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ", manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_KEY_TITLE(), manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE());


            //INTER_EDITION_FLAG
            Log.info("INTER_EDITION_FLAG in stg : " + manifestationDataObjectsFromEPH.get(i).getINTERNATIONAL_EDITION_IND());
            Log.info("INTER_EDITION_FLAG in SA : " + manifestationDataObjectsFromEPHSA.get(i).getINTER_EDITION_FLAG());


            if (Objects.equals(manifestationDataObjectsFromEPH.get(i).getINTERNATIONAL_EDITION_IND(), "Y")) {
                assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ", "t", manifestationDataObjectsFromEPHSA.get(i).getINTER_EDITION_FLAG());
            } else
                assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ", "f",manifestationDataObjectsFromEPHSA.get(i).getINTER_EDITION_FLAG());

            //FIRST_PUB_DATE
            if (manifestationDataObjectsFromEPH.get(i).getCOPYRIGHT_DATE() != null ||manifestationDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE() != null) {
                try {
                    Date stgCopyrightDate = new SimpleDateFormat("yyyy-MM-dd").parse(manifestationDataObjectsFromEPH.get(i).getCOPYRIGHT_DATE());
                    Log.info("FIRST_PUB_DATE in stg :" + stgCopyrightDate);

                    Date saFirstPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(manifestationDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE());
                    Log.info("FIRST_PUB_DATE in sa :" + saFirstPubDate);

                    assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ", stgCopyrightDate, saFirstPubDate);

                } catch (ParseException e) {
                    e.printStackTrace();
                }

                //F_TYPE
                assertEquals("Expecting correct F_TYPE is populated in SA_MANIFESTATION ", manifestationDataObjectsFromEPHSA.get(i).getF_TYPE(), type);

                //F_STATUS
                String manifestationStatus = manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_STATUS();
                Log.info("ManifestationStatus in sa_manifestation : " + manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_STATUS());

                String manifestationSubStatus = manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_SUBSTATUS();
                Log.info("ManifestationSubStatus in sa_manifestation : " + manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_SUBSTATUS());


                if (Objects.equals(manifestationStatus, "81")) {
                    assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ", "MPU", manifestationDataObjectsFromEPHSA.get(i).getF_STATUS());
                } else if (Objects.equals(manifestationStatus, "83")) {
                    assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ", "MAP", manifestationDataObjectsFromEPHSA.get(i).getF_STATUS());
                } else if (Objects.equals(manifestationSubStatus, "Divested")) {
                    assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ", "MDI", manifestationDataObjectsFromEPHSA.get(i).getF_STATUS());
                } else if (manifestationStatus == null)
                    assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ", "UNK", manifestationDataObjectsFromEPHSA.get(i).getF_STATUS());
                else
                    assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ", "MST", manifestationDataObjectsFromEPHSA.get(i).getF_STATUS());
            }
        });
    }


    @And("^We get the manifestations in EPH golden data$")
    public void getManifestationsEPHGD() {
        Log.info("And get the manifestations in EPH GD ..");
        // Get manifestations data from EPH SA_MANIFESTATION
        sql = String.format(WorkExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD, Joiner.on("','").join(ids));
        Log.info(sql);

        manifestationDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    }

    @And("^We compare the manifestations in EPH SA and EPH golden data$")
    public void compareDataBetweenEPHSAndEPHGD() {
        Log.info("And compare the manifestations in EPH SA and EPH GD ..");
//        assertThat("Data for manifestations in EPH and EPH GD is equal without order", dataQualityContext.manifestationDataObjectsFromEPH, containsInAnyOrder(dataQualityContext.manifestationDataObjectsFromEPHGD.toArray()));
        //sort data in the lists
        manifestationDataObjectsFromEPHGD.sort(Comparator.comparing(ManifestationDataObject::getPMX_SOURCE_REFERENCE));

        IntStream.range(0, manifestationDataObjectsFromEPHSA.size()).forEach(i -> {

//            // F_EVENT
            Log.info("F_EVENT in sa_manifestation : " + manifestationDataObjectsFromEPH.get(i).getPRODUCT_MANIFESTATION_ID());
            Log.info("F_EVENT in gd_manifestation: " + manifestationDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());

            if (manifestationDataObjectsFromEPHSA.get(i).getF_EVENT() != null && manifestationDataObjectsFromEPHGD.get(i).getF_EVENT() != null)
                assertEquals(Integer.parseInt(manifestationDataObjectsFromEPHSA.get(i).getF_EVENT()), Integer.parseInt(manifestationDataObjectsFromEPHGD.get(i).getF_EVENT()));

            // B_CLASSNAME
            Log.info("B_CLASSNAME in sa_manifestation : " + manifestationDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
            Log.info("B_CLASSNAME in gd_manifestation : " + manifestationDataObjectsFromEPHGD.get(i).getB_CLASSNAME());

            assertEquals("ManifestationApiObject", manifestationDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
            assertEquals("ManifestationApiObject", manifestationDataObjectsFromEPHGD.get(i).getB_CLASSNAME());


            //MANIFESTATION_ID
            Log.info("MANIFESTATION_ID in sa_manifestation : " + manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_ID());
            Log.info("MANIFESTATION_ID in gd_manifestation : " + manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());

            assertEquals("Expecting the Product details from SA and GD are consistent ",
                  manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_ID(),
                    manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());

            //PMX_SOURCE_REFERENCE
            Log.info("PMX_SOURCE_REFERENCE in sa_manifestation : " + manifestationDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());
            Log.info("PMX_SOURCE_REFERENCE in gd_manifestation : " + manifestationDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE());


            assertEquals("Expecting the Product details from  SA and GD  are consistent ",
                  manifestationDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE(),
                  manifestationDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE());

            //MANIFESTATION_KEY_TITLE
            Log.info("\nMANIFESTATION_KEY_TITLE in sa_manifestation : " + manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_KEY_TITLE());
            Log.info("\nMANIFESTATION_KEY_TITLE in gd_manifestation : " +manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_KEY_TITLE());

            assertEquals("Expecting the Product details from  SA and GD are consistent ",manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_KEY_TITLE(), manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_KEY_TITLE());

            //INTER_EDITION_FLAG
            Log.info("INTER_EDITION_FLAG in sa_manifestation : " + manifestationDataObjectsFromEPHSA.get(i).getINTERNATIONAL_EDITION_IND());
            Log.info("INTER_EDITION_FLAG in gd_manifestation : " + manifestationDataObjectsFromEPHGD.get(i).getINTERNATIONAL_EDITION_IND());

            assertEquals("Expecting the Product details from  SA and GD are consistent ",manifestationDataObjectsFromEPHSA.get(i).getINTERNATIONAL_EDITION_IND(), manifestationDataObjectsFromEPHGD.get(i).getINTERNATIONAL_EDITION_IND());

            //FIRST_PUB_DATE
            if (manifestationDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE() != null)
                try {
                    Date saFirstPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(manifestationDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE());
                    Log.info("FIRST_PUB_DATE in sa_manifestation : " + saFirstPubDate);

                    Date gdFirstPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(manifestationDataObjectsFromEPHGD.get(i).getFIRST_PUB_DATE());
                    Log.info("FIRST_PUB_DATE in gd_manifestation : " + gdFirstPubDate);


                    assertEquals("Expecting the Product details from SA and GD  are consistent ", 0, saFirstPubDate.compareTo(gdFirstPubDate));

                } catch (ParseException e) {
                    e.printStackTrace();
                }

            //LAST_PUB_DATE
            Date saLastPubDate;
            Date gdLastPubDate;
            if (manifestationDataObjectsFromEPHSA.get(i).getLAST_PUB_DATE() != null)
                try {
                    if (DataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getLAST_PUB_DATE() != null && manifestationDataObjectsFromEPHGD.get(i).getLAST_PUB_DATE() != null) {
                        saLastPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(manifestationDataObjectsFromEPHSA.get(i).getLAST_PUB_DATE());
                        Log.info("LAST_PUB_DATE in sa_manifestation : " + manifestationDataObjectsFromEPHSA.get(i).getLAST_PUB_DATE());

                        gdLastPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(manifestationDataObjectsFromEPHGD.get(i).getLAST_PUB_DATE());
                        Log.info("LAST_PUB_DATE in gd_manifestation : " + gdLastPubDate);

                        assertEquals("Expecting the Product details from SA and GD  are consistent ", 0, saLastPubDate.compareTo(gdLastPubDate));
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            //F_TYPE
            Log.info("F_TYPE in sa_manifestation : " + manifestationDataObjectsFromEPHSA.get(i).getF_TYPE());
            Log.info("F_TYPE in gd_manifestation : " + manifestationDataObjectsFromEPHGD.get(i).getF_TYPE());

            assertEquals(manifestationDataObjectsFromEPHSA.get(i).getF_TYPE(), manifestationDataObjectsFromEPHGD.get(i).getF_TYPE());

            //F_STATUS
            Log.info("F_TYPE in sa_manifestation : " + manifestationDataObjectsFromEPHSA.get(i).getF_TYPE());
            Log.info("F_TYPE in gd_manifestation : " + manifestationDataObjectsFromEPHGD.get(i).getF_TYPE());

            assertEquals(manifestationDataObjectsFromEPHSA.get(i).getF_STATUS(), manifestationDataObjectsFromEPHGD.get(i).getF_STATUS());

            //F_FORMAT_TYPE
            Log.info("F_FORMAT_TYPE in sa_manifestation : " + manifestationDataObjectsFromEPHSA.get(i).getF_FORMAT_TYPE());
            Log.info("F_FORMAT_TYPE in gd_manifestation : " + manifestationDataObjectsFromEPHGD.get(i).getF_FORMAT_TYPE());

            assertEquals(manifestationDataObjectsFromEPHSA.get(i).getF_FORMAT_TYPE(), manifestationDataObjectsFromEPHGD.get(i).getF_FORMAT_TYPE());

            //F_WWORK
            Log.info("F_WWORK in sa_manifestation : " + manifestationDataObjectsFromEPHSA.get(i).getF_WWORK());
            Log.info("F_WWORK in gd_manifestation : " + manifestationDataObjectsFromEPHGD.get(i).getF_WWORK());

            assertEquals(manifestationDataObjectsFromEPHSA.get(i).getF_WWORK(), manifestationDataObjectsFromEPHGD.get(i).getF_WWORK());

        });
    }
}
