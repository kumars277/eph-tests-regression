package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.services.db.sql.ProductExtractSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static database.DataAccessUnitChecks.dataQualityContext;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.*;


public class ManifestationDataQualityCheckSteps {
    private String sql;
    private static int countManifestationsEPH;
    private static int countManifestationsPMX;
    private static int countManifestationsSTGPMX;
    private static int countManifestationsEPHGD;
    private List<Map<?, ?>> randomISBNIds;
    private List<Map<?, ?>> manifestationIds;
    private static List<String> ids;
    private static List<String> isbns;


    @Given("We get the count of the manifestations records in PMX$")
    public void getCountManifestationsPmxGdProductManifestation() {
        sql = ProductExtractSQL.COUNT_MANIFESTATIONS_IN_PMX_GD_PRODUCT_MANIFESTATION_TABLE;
        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.PMX_SIT_URL);
        countManifestationsPMX = ((BigDecimal) manifestationsNumber.get(0).get("count")).intValue();
        System.out.println("\nCount of manifestations in PMX.GD_PRODUCT_MANIFESTATION table is: " + countManifestationsPMX);
    }

    @When("We get the count of the manifestations records in PMX STG$")
    public void getCountManifestationsPmxStg() {
        sql = ProductExtractSQL.COUNT_MANIFESTATIONS_IN_EPH_STG_PMX_MANIFESTATION_TABLE;
        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);
        countManifestationsSTGPMX = ((Long) manifestationsNumber.get(0).get("count")).intValue();
        System.out.println("\nCount of manifestations in STG_PMX_MANIFESTATION table is: " + countManifestationsSTGPMX);

    }

    @Then("^The number of the records in PMX and EPH staging table is equal$")
    public void verifyCountOfManifestationDataInPMXAndEPHIsEqual() {
        Assert.assertEquals("\nThe number of manifestations in PMX GD_PRODUCT_MANIFESTATION and STG_PMX_MANIFESTATION is equal", countManifestationsPMX, countManifestationsSTGPMX);
    }

    @When("^The manifestations are transferred to EPH$")
    public void getCountSAManifestation() {
        sql = ProductExtractSQL.COUNT_MANIFESTATIONS_IN_SA_MANIFESTATION_TABLE;
        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);
        countManifestationsEPH = ((Long) manifestationsNumber.get(0).get("count")).intValue();
        System.out.println("\nCount of manifestations in EPH SA table is: " + countManifestationsEPH);

    }

    @Then("^The number of the records in EPH staging table and SA_MANIFESTATION is equal$")
    public void verifyCountOfManifestationDataInSTGEPHandSAManifestationIsEqual() {
        Assert.assertEquals("\nThe number of manifestations in PMX_STG and SA_MANIFESTATION is equal", countManifestationsSTGPMX, countManifestationsEPH);
    }

    @Then("^The manifestations are transferred to the golden data table$")
    public void getCountGDManifestation() {
        sql = ProductExtractSQL.COUNT_MANIFESTATIONS_IN_GD_MANIFESTATION_TABLE;
        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);
        countManifestationsEPHGD = ((Long) manifestationsNumber.get(0).get("count")).intValue();
        System.out.println("\nCount of manifestations in GD_MANIFESTATION table is: " + countManifestationsEPHGD);
    }

    @Then("^The number of the records in EPH staging table and GD_MANIFESTATION is equal$")
    public void verifyCountOfManifestationsInStagingAndGoldenDataTableIsEqual() {
        Assert.assertEquals("\nThe number of manifestations in EPH_STG and GD_MANIFESTATION is equal", countManifestationsEPH, countManifestationsEPHGD);

    }

    @Given("^We get (.*) random records for (.*)$")
    public void getRandomData(String numberOfRecords, String journalType) {
        switch (journalType) {
            case "JPR":
                sql = String.format(ProductExtractSQL.SELECT_RANDOM_MANIFESTATION_IDS_JPR, numberOfRecords);
                break;
            case "JEL":
                sql = String.format(ProductExtractSQL.SELECT_RANDOM_MANIFESTATION_IDS_JEL, numberOfRecords);
                break;
        }
        manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);
        ids = manifestationIds.stream().map(m -> (BigDecimal) m.get("manifestation_id")).map(String::valueOf).collect(Collectors.toList());
    }


    @Given("^We get (.*) random ISBNs for (.*)$")
    public void getRandomISBNs(String numberOfRecords, String bookType) {
        switch (bookType) {
            case "PHB":
                sql = String.format(ProductExtractSQL.SELECT_RANDOM_ISBN_IDS_PHB, numberOfRecords);
                break;
            case "PSB":
                sql = String.format(ProductExtractSQL.SELECT_RANDOM_ISBN_IDS_PSB, numberOfRecords);
                break;
            case "EBK":
                sql = String.format(ProductExtractSQL.SELECT_RANDOM_ISBN_IDS_EBK, numberOfRecords);
                break;
            default:
                break;
        }

        randomISBNIds = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);

        isbns = randomISBNIds.stream().map(m -> (String) m.get("isbn")).collect(Collectors.toList());
        System.out.println(isbns);
    }

    @When("^We get the manifestation ids for these books$")
    public void getManifestationsIds() {
        //  Get manifestations ids for isbns which are in PMX STG
        String sqlSelectManifestationIDS = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_IDS_FOR_SPECIFIC_ISBN, Joiner.on("','").join(isbns));

        manifestationIds = DBManager.getDBResultMap(sqlSelectManifestationIDS, Constants.EPH_SIT_URL);
        ids = manifestationIds.stream().map(m -> (BigDecimal) m.get("manifestation_id")).map(String::valueOf).collect(Collectors.toList());

    }

    @Then("^We get the manifestations records from PMX$")
    public void getPMXManifestationData() {
        sql = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX, Joiner.on("','").join(ids));

        dataQualityContext.manifestationDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.PMX_SIT_URL);
    }

    @Then("^We have the manifestations in PMX STG$")
    public void getEPHStagingManifestationData() {
        sql = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX_STG, Joiner.on("','").join(ids));

        dataQualityContext.manifestationDataObjectsFromEPH = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_SIT_URL);

    }


    @And("^The data for manifestations in PMX and PMX STG is equal$")
    public void compareManifestationDataBetweenPMXAndEPH() {
//        assertThat("Data for manifestations in PMX and EPH staging is equal without order", dataQualityContext.manifestationDataObjectsFromPMX, containsInAnyOrder(dataQualityContext.manifestationDataObjectsFromEPH.toArray()));

        IntStream.range(0, dataQualityContext.manifestationDataObjectsFromPMX.size()).forEach(i -> {

            //verify manifestation ids are equal
            System.out.println("\nManifestation id in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(0).getMANIFESTATION_ID());
            System.out.println("\nManifestation id in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(0).getMANIFESTATION_ID());

            assertEquals("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Integer.parseInt(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_ID()),
                    (Integer.parseInt(dataQualityContext.manifestationDataObjectsFromEPH.get(0).getMANIFESTATION_ID())));

            //verify MANIFESTATION_KEY_TITLE
            System.out.println("\nMANIFESTATION_KEY_TITLE in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(0).getMANIFESTATION_KEY_TITLE());
            System.out.println("\nMANIFESTATION_KEY_TITLE in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(0).getMANIFESTATION_KEY_TITLE());


            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    dataQualityContext.manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_KEY_TITLE()
                            .equals(dataQualityContext.manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE()));

            //verify ISBN
            System.out.println("\nISBN in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getISBN());
            System.out.println("\nISBN in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getISBN());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getISBN().trim(), dataQualityContext.manifestationDataObjectsFromEPH.get(0).getISBN().trim()));

            //verify COVER_HEIGHT
            System.out.println("\nCOVER_HEIGHT in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getCOVER_HEIGHT());
            System.out.println("\nCOVER_HEIGHT in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getCOVER_HEIGHT());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getCOVER_HEIGHT(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getCOVER_HEIGHT()));

            //verify COVER_WIDTH
            System.out.println("\nCOVER_WIDTH in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getCOVER_WIDTH());
            System.out.println("\nCOVER_WIDTH in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getCOVER_WIDTH());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getCOVER_WIDTH(),
                            (dataQualityContext.manifestationDataObjectsFromEPH.get(i).getCOVER_WIDTH())));

            //verify PAGE_HEIGHT
            System.out.println("\nPAGE_HEIGHT in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getPAGE_HEIGHT());
            System.out.println("\nPAGE_HEIGHT in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getPAGE_HEIGHT());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getPAGE_HEIGHT(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getPAGE_HEIGHT()));

            //verify PAGE_WIDTH_AMOUNT
            System.out.println("\nPAGE_WIDTH_AMOUNT in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getPAGE_WIDTH());
            System.out.println("\nPAGE_WIDTH_AMOUNT in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getPAGE_WIDTH());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getPAGE_WIDTH(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getPAGE_WIDTH()));

            //verify WEIGHT_AMOUNT
            System.out.println("\nWEIGHT_AMOUNT in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getWEIGHT());
            System.out.println("\nWEIGHT_AMOUNT in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getWEIGHT());

            if (dataQualityContext.manifestationDataObjectsFromPMX.get(i).getWEIGHT() != null || dataQualityContext.manifestationDataObjectsFromEPH.get(i).getWEIGHT() != null)
                assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                        Objects.equals(Float.parseFloat(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getWEIGHT().trim()),
                                Float.parseFloat(dataQualityContext.manifestationDataObjectsFromEPH.get(i).getWEIGHT())));


            //CARTON_QTY
            System.out.println("\nCARTON_QTY in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getCARTON_QTY());
            System.out.println("\nCARTON_QTY in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getCARTON_QTY());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getCARTON_QTY(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getCARTON_QTY()));

            //INTERNATIONAL_EDITION_IND
            System.out.println("\nINTERNATIONAL_EDITION_IND in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getINTERNATIONAL_EDITION_IND());
            System.out.println("\nINTERNATIONAL_EDITION_IND in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getINTERNATIONAL_EDITION_IND());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getINTERNATIONAL_EDITION_IND(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getINTERNATIONAL_EDITION_IND()));


            //COPYRIGHT_DATE
            if (dataQualityContext.manifestationDataObjectsFromPMX.get(i).getCOPYRIGHT_DATE() != null || dataQualityContext.manifestationDataObjectsFromEPH.get(i).getCOPYRIGHT_DATE() != null) {
                try {
                    Date pmxCopyrightDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.s").parse(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getCOPYRIGHT_DATE());
                    System.out.println("\nCOPYRIGHT_DATE in PMX: " + pmxCopyrightDate);

                    Date ephCopyrightDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.manifestationDataObjectsFromEPH.get(i).getCOPYRIGHT_DATE());
                    System.out.println("\nCOPYRIGHT_DATE in EPH Staging: " + ephCopyrightDate);

                    assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                            Objects.equals(pmxCopyrightDate, ephCopyrightDate));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }


            //F_PRODUCT_MANIFESTATION_TYP
            System.out.println("\nF_PRODUCT_MANIFESTATION_TYP in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            System.out.println("\nF_PRODUCT_MANIFESTATION_TYP in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getF_PRODUCT_MANIFESTATION_TYP()));

            //FORMAT_TXT
            System.out.println("\nFORMAT_TXT in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getFORMAT_TXT());
            System.out.println("\nFORMAT_TXT in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getFORMAT_TXT());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getFORMAT_TXT(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getFORMAT_TXT()));

            //F_MANIFESTATION_STATUS
            System.out.println("\nF_MANIFESTATION_STATUS in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_STATUS());
            System.out.println("\nF_MANIFESTATION_STATUS in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_STATUS());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_STATUS(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_STATUS()));

            //F_PRODUCT_WORK
            System.out.println("\nF_MANIFESTATION_STATUS in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_STATUS());
            System.out.println("\nF_MANIFESTATION_STATUS in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_STATUS());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getF_PRODUCT_WORK(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getF_PRODUCT_WORK()));

            //F_PRODUCT_TYPE
            System.out.println("\nF_PRODUCT_TYPE in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            System.out.println("\nF_PRODUCT_TYPE in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getF_PRODUCT_MANIFESTATION_TYP()));

            //MANIFESTATION_SUBTYPE
            System.out.println("\nMANIFESTATION_SUBTYPE in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getMANIFESTATTION_SUBTYPE());
            System.out.println("\nMANIFESTATION_SUBTYPE in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getMANIFESTATTION_SUBTYPE());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getMANIFESTATTION_SUBTYPE(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getMANIFESTATTION_SUBTYPE()));

            //COMMODITY
            System.out.println("\nCOMMODITY in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getCOMMODITY());
            System.out.println("\nCOMMODITY in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getCOMMODITY());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getCOMMODITY(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getCOMMODITY()));

            //MANIFESTATION_SUBSTATUS
            System.out.println("\nManifestation substatus in PMX: " + dataQualityContext.manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_SUBSTATUS());
            System.out.println("\nManifestation substatus in EPH Staging: " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_SUBSTATUS());

            assertTrue("Expecting the Product details from PMX and EPH Staging are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromPMX.get(i).getMANIFESTATION_SUBSTATUS(),
                            dataQualityContext.manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_SUBSTATUS()));
        });

    }


    @And("^We get the manifestations in EPH$")
    public void getManifestationsEPHSA() {
        // Get manifestations data from EPH SA_MANIFESTATION
        sql = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX_SA, Joiner.on("','").join(ids));

        dataQualityContext.manifestationDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_SIT_URL);
    }

    @And("^We check that mandatory fields are not null$")
    public void checkMandatoryFieldsInSAAreNotNull() {
        int bound = dataQualityContext.manifestationDataObjectsFromEPHSA.size();
        IntStream.range(0, bound).forEach(i -> {
            //verify F_EVENT is not null
            assertTrue(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_EVENT() != null);
            //verify manifestation id is not null
            assertTrue(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_ID() != null);
            //verify F_TYPE is not null
            assertTrue(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_TYPE() != null);
            //verify F_STATUS is not null
            assertTrue(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_STATUS() != null);
            //verify F_WWORK is not null
            assertTrue(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_WWORK() != null);
        });
    }


    @And("^We compare the manifestations in PMX STG and EPH for (.*)$")
    public void compareDataBetweenStagingAndEPHSA(String type) {
        //sort data from PMX STG by COPYRIGHT_DATE
        DataQualityContext.manifestationDataObjectsFromEPH.sort(Comparator.comparing(ManifestationDataObject::getCOPYRIGHT_DATE));

//        assertThat("Data for manifestations in EPH Staging and EPH SA is equal without order", dataQualityContext.manifestationDataObjectsFromEPH, containsInAnyOrder(dataQualityContext.manifestationDataObjectsFromEPHSA.toArray()));

        IntStream.range(0, dataQualityContext.manifestationDataObjectsFromEPH.size()).forEach(i -> {

            //B_CLASSNAME
//            assertTrue(Objects.equals("Manifestation", DataQualityContext.manifestationDataObjectsFromSA.get(i).getB_classname()));


            //PMX_SOURCE_REFERENCE
            System.out.println("\nPMX_SOURCE_REFERENCE in stg_pmx_manifestation : " + dataQualityContext.manifestationDataObjectsFromEPH.get(i).getPRODUCT_MANIFESTATION_ID());
            System.out.println("\nPMX_SOURCE_REFERENCE in sa_manifestation: " + dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());

            assertEquals("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ",
                    Integer.parseInt(dataQualityContext.manifestationDataObjectsFromEPH.get(i).getPRODUCT_MANIFESTATION_ID()),
                    Integer.parseInt(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE()));

            //MANIFESTATION_KEY_TITLE
            if (type == "JPR")
                assertTrue("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ",
                        Objects.equals(dataQualityContext.manifestationDataObjectsFromEPH.get(i).getPRODUCT_MANIFESTATION_ID(),
                                dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE() + "(Print)"));
            if (type == "JEL")
                assertTrue("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ",
                        Objects.equals(dataQualityContext.manifestationDataObjectsFromEPH.get(i).getPRODUCT_MANIFESTATION_ID(),
                                dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE() + "(Online)"));

            //INTER_EDITION_FLAG
            if (dataQualityContext.manifestationDataObjectsFromEPH.get(i).getINTERNATIONAL_EDITION_IND() == "Y") {
                assertTrue("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ",
                        Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getINTER_EDITION_FLAG(), "t"));
            } else
                assertTrue("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ",
                        Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getINTER_EDITION_FLAG(), "f"));

            //FIRST_PUB_DATE
            try {
                Date stgCopyrightDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.manifestationDataObjectsFromEPH.get(i).getCOPYRIGHT_DATE());

                Date saFirstPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE());

                assertTrue("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ",
                        Objects.equals(stgCopyrightDate, saFirstPubDate));

            } catch (ParseException e) {
                e.printStackTrace();
            }

            //F_TYPE
            assertTrue("Expecting correct F_TYPE is populated in SA_MANIFESTATION ",
                    dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_TYPE().equals(type));

            //F_STATUS
            int manifestationStatus = Integer.parseInt(dataQualityContext.manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_STATUS());
            String manifestationSubStatus = dataQualityContext.manifestationDataObjectsFromEPH.get(i).getMANIFESTATION_SUBSTATUS();

            if (manifestationStatus == 81) {
                assertTrue("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ",
                        Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_STATUS(), "MPU"));
            } else if (manifestationStatus == 83) {
                assertTrue("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ",
                        Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_STATUS(), "MAP"));
            } else if (manifestationSubStatus == "Divested") {
                assertTrue("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ",
                        Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_STATUS(), "MDI"));
            } else
                assertTrue("Expecting the Product details from EPH Staging and SA_MANIFESTATION are consistent ",
                        Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_STATUS(), "MST"));


        });
    }


    @And("^We get the manifestations in EPH golden data$")
    public void getManifestationsEPHGD() {
        // Get manifestations data from EPH SA_MANIFESTATION
        sql = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX_GD, Joiner.on("','").join(ids));

        dataQualityContext.manifestationDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_SIT_URL);
    }

    @And("^We compare the manifestations in EPH and EPH golden data$")
    public void compareDataBetweenEPHSAndEPHGD() {
//        assertThat("Data for manifestations in EPH and EPH GD is equal without order", dataQualityContext.manifestationDataObjectsFromEPH, containsInAnyOrder(dataQualityContext.manifestationDataObjectsFromEPHGD.toArray()));

        IntStream.range(0, dataQualityContext.manifestationDataObjectsFromEPHSA.size()).forEach(i -> {

//            // F_EVENT
            assertEquals(new BigInteger(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_EVENT()),
                    new BigInteger(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getF_EVENT()));

            // B_CLASSNAME
            assertEquals("Manifestation", DataQualityContext.manifestationDataObjectsFromSA.get(i).getB_classname());
            assertEquals("Manifestation", DataQualityContext.manifestationDataObjectsFromGD.get(i).getB_classname());


            //MANIFESTATION_ID
            assertEquals("Expecting the Product details from SA and GD are consistent ",
                    Integer.parseInt(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_ID()),
                    Integer.parseInt(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID()));

            //PMX_SOURCE_REFERENCE
            assertEquals("Expecting the Product details from  SA and GD  are consistent ",
                    Integer.parseInt(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE()),
                    Integer.parseInt(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE()));

            //MANIFESTATION_KEY_TITLE
            assertTrue("Expecting the Product details from  SA and GD are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getMANIFESTATION_KEY_TITLE(),
                            dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_KEY_TITLE()));

            //INTER_EDITION_FLAG
            assertTrue("Expecting the Product details from  SA and GD are consistent ",
                    Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getINTERNATIONAL_EDITION_IND(),
                            dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getINTERNATIONAL_EDITION_IND()));

            //FIRST_PUB_DATE
            try {
                Date saFirstPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE());

                Date gdFirstPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getFIRST_PUB_DATE());

                assertTrue("Expecting the Product details from SA and GD  are consistent ",
                        Objects.equals(saFirstPubDate, gdFirstPubDate));

            } catch (ParseException e) {
                e.printStackTrace();
            }

            //LAST_PUB_DATE
            try {
                Date saLastPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getLAST_PUB_DATE());

                Date gdLastPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getLAST_PUB_DATE());

                assertTrue("Expecting the Product details from SA and GD  are consistent ",
                        Objects.equals(saLastPubDate, gdLastPubDate));

            } catch (ParseException e) {
                e.printStackTrace();
            }

            //F_TYPE
            Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_TYPE(),
                    dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getF_TYPE());

            //F_STATUS
            Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_STATUS(),
                    dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getF_STATUS());

            //F_FORMAT_TYPE
            Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_FORMAT_TYPE(),
                    dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getF_FORMAT_TYPE());

            //F_WWORK
            Objects.equals(dataQualityContext.manifestationDataObjectsFromEPHSA.get(i).getF_WWORK(),
                    dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getF_WWORK());

        });
    }
}
