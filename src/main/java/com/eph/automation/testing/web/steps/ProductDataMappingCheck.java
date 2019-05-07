package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.ProductDataSQL;
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
import java.util.stream.Stream;

import static org.junit.Assert.*;


/**
 * Created by Bistra Drazheva on 11/02/2019
 */
public class ProductDataMappingCheck {

    @StaticInjection
    public DataQualityContext dataQualityContext;

    private String sql;
    private static List<String> ids;
    private static List<String> idsSA;
    private static List<String> idsDQ;
    private static List<String> pmxSourceReferenceIds;


    @Given("^We get (.*) random ids for (.*)$")
    public void getRandomProductManifestationIds(String numberOfRecords, String type) {
        Log.info("Get random ids ..");
        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);

        switch (type) {
            case "journal":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_JOURNALS, numberOfRecords);
                Log.info(sql);
                break;
            case "book":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_BOOKS, numberOfRecords);
                Log.info(sql);
                break;
            case "package":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_PACKAGES, numberOfRecords);
                Log.info(sql);
                break;
            default:
                break;
        }
        List<Map<?, ?>> randomProductManifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomProductManifestationIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product manifestation ids  : " + ids);
    }


    @Given("^We get (.*) ids of journals for (.*) with (.*) and (.*)$")
    public void getRandomProductManifestationIdsForJournals(String numberOfRecords, String type, String open_access, String author_charges) {
        Log.info("In Given method get random product manifestation ids for journals");
        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("Number of random records = " + numberOfRecords);


        switch (type) {
            case "print_journal":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_PRINT_JOURNALS, open_access, author_charges, numberOfRecords);
                Log.info(sql);
                break;
            case "electronic_journal":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_ELECTRONIC_JOURNALS, open_access, author_charges, numberOfRecords);
                Log.info(sql);
                break;
//            case "non_print_or_electronic_journal":
//                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_NON_PRINT_OR_ELECTRONIC_JOURNALS, numberOfRecords);
//                Log.info(sql);
//                break;
            default:
                break;
        }

        List<Map<?, ?>> randomProductManifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomProductManifestationIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected product manifestation ids : " + ids);


    }

    @When("^We get the corresponding records from PMX$")
    public void getProductsDataFromPMX() {
        Log.info("When We get the corresponding records from PMX : ");
        sql = String.format(ProductDataSQL.PMX_PRODUCT_EXTRACT, Joiner.on("','").join(ids));
        Log.info(sql);
        dataQualityContext.productDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.PMX_URL);

    }

    @Then("^We get the data from EPH STG$")
    public void getProductsDataFromEPHSTG() {
        Log.info("In Then method");
        sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    }

    @Then("^We get the data from EPH STG DQ for (.*)$")
    public void getProductsDataFromEPHSTGDQ(String type) {
        Log.info("In Then method");


        if (type.equals("book")) {
            idsDQ = new ArrayList<>(ids);

            IntStream.range(0, idsDQ.size()).forEach(i -> idsDQ.set(i, idsDQ.get(i) + "-OOA"));
            sql = String.format(ProductDataSQL.EPH_STG_DQ_PRODUCT_EXTRACT_BOOKS, Joiner.on("','").join(idsDQ));
            Log.info(sql);
        } else {
            List<String> workIds = new ArrayList<>();
            for (int i = 0; i < dataQualityContext.productDataObjectsFromEPHSTG.size(); i++) {
                if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getOPEN_ACCESS().equals("Y") )
                    workIds.add(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK());
                if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAUTHOR_CHARGES().equals("Y"))
                    workIds.add(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK());
                if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPACKAGES().equals("Y"))
                    workIds.add(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK());
            }


            //concatenate the ids used for pmx_source_reference in SA
            idsDQ = Stream.concat(ids.stream(), workIds.stream()).collect(Collectors.toList());
            IntStream.range(0, idsDQ.size()).forEach(i -> idsDQ.set(i, idsDQ.get(i) + "%"));
            Log.info(idsDQ.toString());

            sql = String.format(ProductDataSQL.EPH_STG_DQ_PRODUCT_EXTRACT_JOURNALS_OR_PACKAGES, Joiner.on("|").join(idsDQ));
            Log.info(sql);

        }

        dataQualityContext.productDataObjectsFromEPHSTGDQ = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
        sql.length();
    }

    @And("^Compare the records in PMX and EPH STG for (.*)$")
    public void compareProductsDataBetweenPMXAndEPHSTG(String type) {
        Log.info("Compare the records in PMX and EPH STG for " + type);

        IntStream.range(0, dataQualityContext.productDataObjectsFromPMX.size()).forEach(i -> {

            //verify PRODUCT_ID
            Log.info("PRODUCT_ID in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_ID());
            Log.info("PRODUCT_ID in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_ID());

            Log.info(("Expecting Product IDs in PMX and EPH Staging are consistent for " + type));

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_ID(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_ID());


            //PRODUCT_NAME
            Log.info("PRODUCT_ID in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_ID());
            Log.info("PRODUCT_ID in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_ID());

            Log.info("Expecting PRODUCT_NAME in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_NAME(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_NAME());


            //PRODUCT_SHORT_NAME
            Log.info("PRODUCT_SHORT_NAME in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_SHORT_NAME());
            Log.info("PRODUCT_SHORT_NAME in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME());

            Log.info("Expecting PRODUCT_SHORT_NAME in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_SHORT_NAME(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME());

            //TRIAL_ALLOWED_IND
            Log.info("TRIAL_ALLOWED_IND in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getTRIAL_ALLOWED_IND());
            Log.info("TRIAL_ALLOWED_IND in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND());

            Log.info("Expecting TRIAL_ALLOWED_IND in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getTRIAL_ALLOWED_IND(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND());

            //FIRST_PUB_DATE
            Log.info("Expecting FIRST_PUB_DATE in PMX and EPH Staging are consistent for " + type);

            if (dataQualityContext.productDataObjectsFromPMX.get(i).getFIRST_PUB_DATE() != null)
                try {
                    Date pmxFirstPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.productDataObjectsFromPMX.get(i).getFIRST_PUB_DATE());
                    Log.info("FIRST_PUB_DATE in PMX : " + pmxFirstPubDate);

                    Date ephStgFirstPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.productDataObjectsFromPMX.get(i).getFIRST_PUB_DATE());
                    Log.info("FIRST_PUB_DATE in EPH STG : " + ephStgFirstPubDate);


                    assertEquals(0, pmxFirstPubDate.compareTo(ephStgFirstPubDate));

                } catch (ParseException e) {
                    e.printStackTrace();
                }

            //ELSEVIER_TAX_CODE
            Log.info("ELSEVIER_TAX_CODE in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getELSEVIER_TAX_CODE());
            Log.info("ELSEVIER_TAX_CODE in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getELSEVIER_TAX_CODE());

            Log.info("Expecting ELSEVIER_TAX_CODE in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getELSEVIER_TAX_CODE(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getELSEVIER_TAX_CODE());

            // PRODUCT_MANIFESTATION_I
            Log.info("PRODUCT_MANIFESTATION_ID in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getELSEVIER_TAX_CODE());
            Log.info("PRODUCT_MANIFESTATION_ID in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getELSEVIER_TAX_CODE());

            Log.info("Expecting PRODUCT_MANIFESTATION_ID in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_MANIFESTATION_ID(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_MANIFESTATION_ID());

            //F_PRODUCT_WORK
            Log.info("F_PRODUCT_WORK in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_WORK());
            Log.info("F_PRODUCT_WORK in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK());

            Log.info("Expecting F_PRODUCT_WORK in PMX and EPH Staging are consistent for " + type);
            assertEquals(Integer.parseInt(dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_WORK()), Integer.parseInt(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK()));

            //F_PRODUCT_MANIFESTATION_TYP
            Log.info("F_PRODUCT_MANIFESTATION_TYP in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            Log.info("F_PRODUCT_MANIFESTATION_TYP in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            Log.info("Expecting F_PRODUCT_MANIFESTATION_TYP in PMX and EPH Staging are consistent for " + type);

            assertEquals(Integer.parseInt(dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP()), Integer.parseInt(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_MANIFESTATION_TYP()));

            //SUBSCRIPTION
            Log.info("SUBSCRIPTION in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getSUBSCRIPTION());
            Log.info("SUBSCRIPTION in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSUBSCRIPTION());

            Log.info("Expecting SUBSCRIPTION in PMX and EPH Staging are consistent for " + type);


            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getSUBSCRIPTION(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSUBSCRIPTION());

            //BULK_SALES
            Log.info("BULK_SALES in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getBULK_SALES());
            Log.info("BULK_SALES in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBULK_SALES());

            Log.info("Expecting BULK_SALES in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getBULK_SALES(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBULK_SALES());

            //BACK_FILES
            Log.info("BACK_FILES in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getBACK_FILES());
            Log.info("BACK_FILES in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBACK_FILES());

            Log.info("Expecting BACK_FILES in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getBACK_FILES(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBACK_FILES());

            //OPEN_ACCESS
            Log.info("OPEN_ACCESS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getOPEN_ACCESS());
            Log.info("OPEN_ACCESS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getOPEN_ACCESS());

            Log.info("Expecting OPEN_ACCESS in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getOPEN_ACCESS(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getOPEN_ACCESS());

            //REPRINTS
            Log.info("REPRINTS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getREPRINTS());
            Log.info("REPRINTS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getREPRINTS());

            Log.info("Expecting REPRINTS in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getREPRINTS(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getREPRINTS());

            //AUTHOR_CHARGES
            Log.info("AUTHOR_CHARGES in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getAUTHOR_CHARGES());
            Log.info("AUTHOR_CHARGES in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAUTHOR_CHARGES());

            Log.info("Expecting AUTHOR_CHARGES in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getAUTHOR_CHARGES(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAUTHOR_CHARGES());

            //ONE_OFF_ACCESS
            Log.info("ONE_OFF_ACCESS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getONE_OFF_ACCESS());
            Log.info("ONE_OFF_ACCESS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getONE_OFF_ACCESS());

            Log.info("Expecting ONE_OFF_ACCESS in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getONE_OFF_ACCESS(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getONE_OFF_ACCESS());

            //PACKAGES
            Log.info("PACKAGES in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getPACKAGES());
            Log.info("PACKAGES in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPACKAGES());

            Log.info("Expecting PACKAGES in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getPACKAGES(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPACKAGES());

            //AVAILABILITY_STATUS
            Log.info("AVAILABILITY_STATUS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getAVAILABILITY_STATUS());
            Log.info("AVAILABILITY_STATUS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAVAILABILITY_STATUS());

            Log.info("Expecting AVAILABILITY_STATUS in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getAVAILABILITY_STATUS(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAVAILABILITY_STATUS());

            //WORK_TITLE
            Log.info("WORK_TITLE in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getWORK_TITLE());
            Log.info("WORK_TITLE in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getWORK_TITLE());

            Log.info("Expecting WORK_TITLE in PMX and EPH Staging are consistent for " + type);

            assertEquals(dataQualityContext.productDataObjectsFromPMX.get(i).getWORK_TITLE(), dataQualityContext.productDataObjectsFromEPHSTG.get(i).getWORK_TITLE());


        });
    }


    @And("^We get the data from EPH GD$")
    public void getProductsDataFromEPHGD() {
        Log.info("And We get the data from EPH GD ...");
        sql = String.format(ProductDataSQL.EPH_GD_PRODUCT_EXTRACT, Joiner.on("','").join(idsSA));
        Log.info(sql);

        dataQualityContext.productDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    }

    @And("^We get the data from EPH SA for (.*)$")
    public void getProductsDataFromEPHSAForJournals(String type) {
        Log.info("And We get the data from EPH SA for " + type);

        idsSA = new ArrayList<>(ids);

        if (type.equals("book")) {
            IntStream.range(0, idsSA.size()).forEach(i -> idsSA.set(i, idsSA.get(i) + "-OOA"));
        } else {
            for (int i = 0; i < dataQualityContext.productDataObjectsFromEPHSTGDQ.size(); i++) {
                idsSA.add(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPMX_SOURCE_REFERENCE());
            }
        }

        sql = String.format(ProductDataSQL.EPH_SA_PRODUCT_EXTRACT, Joiner.on("','").join(idsSA));
        Log.info(sql);

        dataQualityContext.productDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
        sql.length();

    }

    @And("^We get the data from EPH GD for (.*)$")
    public void getProductsDataFromEPHGDForJournals(String type) {
        Log.info("And We get the data from EPH GD for journals ...");

        sql = String.format(ProductDataSQL.EPH_GD_PRODUCT_EXTRACT, Joiner.on("','").join(idsSA));

        Log.info(sql);

        dataQualityContext.productDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);


    }

    @And("^We check that mandatory columns are populated$")
    public void checkMandatoryColumnsForProductsInSAArePopulated() {
        Log.info("We check that mandatory columns are populated ...");

        IntStream.range(0, dataQualityContext.productDataObjectsFromEPHSA.size()).forEach(i -> {
            //verify F_EVENT is not null
            assertNotNull(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_EVENT());
            //verify product id is not null
            assertNotNull(dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_ID());
            //verify product name is not null
            assertNotNull(dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME());
            //verify product name is not null
            assertNotNull(dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME());
            //verify f type is not null
            assertNotNull(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE());
            //verify f status is not null
            assertNotNull(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS());

        });
    }

    @And("^Depends on the flags of every record from Staging check if we have the expected number of records in EPH STG DQ")
    public void checkCountOfRecordsInDQIsApplicableToDataInSTG() {
        Log.info("And Depends on the flags of every record from Staging check if we have the expected number of records in SA ...");

        int expectedNumberOfRecordsInDQ = 0;


        for (int i = 0; i < dataQualityContext.productDataObjectsFromEPHSTG.size(); i++) {


            // subscription flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSUBSCRIPTION().equals("Y"))
                expectedNumberOfRecordsInDQ++;

            // bulk_sale flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBULK_SALES().equals("Y"))
                expectedNumberOfRecordsInDQ++;

            // back_files flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBACK_FILES().equals("Y"))
                expectedNumberOfRecordsInDQ++;

            //open_access flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getOPEN_ACCESS().equals("Y"))
                expectedNumberOfRecordsInDQ++;

            //reprints flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getREPRINTS().equals("Y"))
                expectedNumberOfRecordsInDQ++;

            //author_charges flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAUTHOR_CHARGES().equals("Y"))
                expectedNumberOfRecordsInDQ++;

            //packages flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPACKAGES().equals("Y"))
                expectedNumberOfRecordsInDQ++;

        }

        Log.info("Expected number of records in EPH STG DQ is : " + expectedNumberOfRecordsInDQ);
        Log.info("Number of records in EPH STG DQ is : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.size());

        Log.info("Assert the number of records in EPH STG DQ is as expected ..");
        Assert.assertEquals(expectedNumberOfRecordsInDQ, dataQualityContext.productDataObjectsFromEPHSTGDQ.size());
    }


    @And("^Compare the records in EPH STG and EPH STG DQ for (.*)$")
    public void compareProductsDataBetweenSTGAndDQ(String type) {
        Log.info("Compare the records in EPH STG and EPH STG DQ for " + type + " ..");

        dataQualityContext.productDataObjectsFromEPHSTG.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
        dataQualityContext.productDataObjectsFromEPHSTGDQ.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));

        IntStream.range(0, dataQualityContext.productDataObjectsFromEPHSTGDQ.size()).forEach(i -> {

            //verify PMX_SOURCE_REFERENCE and get the manifestation or work id
            String id;
            String pmxSourceReference = dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPMX_SOURCE_REFERENCE();
            Log.info("PMX_SOURCE_REFERENCE in EPH STG DQ is " + pmxSourceReference);


            if (pmxSourceReference.contains("SUB")) {
                //get the id
                id = pmxSourceReference.replace("-SUB", "");

                if (type.equals("print_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 1);
                    Log.info(sql);
                } else if (type.equals("electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 2);
                    Log.info(sql);
                } else if (type.equals("non_print_or_electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL_NOT_PRINT_OR_ELECTRONIC, id);
                    Log.info(sql);
                }


                dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                        .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
                Log.info("PMX_SOURCE_REFERENCE in EPH STG for journal is " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID());

                Log.info("Assert pmxSourceReference value is correct ...");

                //assert pmxSourceReference value is correct
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID() + "-SUB", pmxSourceReference);

            } else if (pmxSourceReference.contains("RPR")) {
                //get the id
                id = pmxSourceReference.replace("-RPR", "");

                if (type.equals("print_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 1);
                    Log.info(sql);
                } else if (type.equals("electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 2);
                    Log.info(sql);
                } else if (type.equals("non_print_or_electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL_NOT_PRINT_OR_ELECTRONIC, id);
                    Log.info(sql);
                }

                dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                        .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
                Log.info("PMX_SOURCE_REFERENCE in EPH STG for journal is " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID());

                Log.info("Assert pmxSourceReference value is correct");

                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID() + "-RPR", pmxSourceReference);

            } else if (pmxSourceReference.contains("JBS")) {

                //get the id
                id = pmxSourceReference.replace("-JBS", "");

                if (type.equals("print_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 1);
                    Log.info(sql);
                } else if (type.equals("electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 2);
                    Log.info(sql);
                } else if (type.equals("non_print_or_electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL_NOT_PRINT_OR_ELECTRONIC, id);
                    Log.info(sql);
                }

                dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                        .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
                Log.info("PMX_SOURCE_REFERENCE in EPH STG for journal is " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID());

                Log.info("Assert pmxSourceReference value is correct ...");
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID() + "-JBS", pmxSourceReference);

            } else if (pmxSourceReference.contains("BKF")) {

                //get the id
                id = pmxSourceReference.replace("-BKF", "");

                if (type.equals("print_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 1);
                    Log.info(sql);
                } else if (type.equals("electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 2);
                    Log.info(sql);
                } else if (type.equals("non_print_or_electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL_NOT_PRINT_OR_ELECTRONIC, id);
                    Log.info(sql);
                }

                dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                        .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
                Log.info("PMX_SOURCE_REFERENCE in EPH STG for journal is " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID());

                Log.info("Assert pmxSourceReference value is correct ...");
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID() + "-BKF", pmxSourceReference);

            } else if (pmxSourceReference.contains("JAS")) {
                //get the id
                id = pmxSourceReference.replace("-JAS", "");

                if (type.equals("print_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_JAS, id, 1);
                    Log.info(sql);
                } else if (type.equals("electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_JAS, id, 2);
                    Log.info(sql);
                } else if (type.equals("non_print_or_electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_NOT_PRINT_OR_ELECTRONIC_JAS, id);
                    Log.info(sql);
                }


                dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                        .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
                Log.info("PMX_SOURCE_REFERENCE in EPH STG for journal is " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getF_PRODUCT_WORK());

                Log.info("Assert pmxSourceReference value is correct ...");
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getF_PRODUCT_WORK() + "-JAS", pmxSourceReference);

            } else if (pmxSourceReference.contains("OAA")) {

                //get the id
                id = pmxSourceReference.replace("-OAA", "");

                if (type.equals("print_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_OAA, id, 1);
                    Log.info(sql);
                } else if (type.equals("electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_OAA, id, 2);
                    Log.info(sql);
                } else if (type.equals("non_print_or_electronic_journal")) {
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_NOT_PRINT_OR_ELECTRONIC_JAS, id);
                    Log.info(sql);
                }


                dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                        .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
                Log.info("PMX_SOURCE_REFERENCE in EPH STG for journal is " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getF_PRODUCT_WORK());

                Log.info("Assert pmxSourceReference value is correct ..");
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getF_PRODUCT_WORK() + "-OAA", pmxSourceReference);
            } else if (pmxSourceReference.contains("OOA")) {
                Log.info("PRODUCT_MANIFESTATION_ID in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_MANIFESTATION_ID());
                Log.info("Expecting PMX_SOURCE_REFERENCE in EPH Staging and EPH STG DQ are consistent for ");

                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_MANIFESTATION_ID() + "-OOA", pmxSourceReference);
            } else if (pmxSourceReference.contains("PKG")) {
                Log.info("F_PRODUCT_WORK in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK());
                Log.info("Expecting PMX_SOURCE_REFERENCE in EPH Staging and EPH STG DQ are consistent for ");

                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK() + "-PKG", pmxSourceReference);

            }


            //PRODUCT_NAME
            Log.info("PRODUCT_NAME in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME());
            Log.info("PRODUCT_NAME in EPH STG DQ: " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());

            Log.info("Expecting PRODUCT_NAME in EPH STH and EPH STG DQ is consistent");

            String suffix;
            if (pmxSourceReference.contains("SUB")) {
                suffix = "Subscription";
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME() + " " + suffix, dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());
            } else if (pmxSourceReference.contains("JBS")) {
                suffix = " Bulk Sales";
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME() + " " + suffix, dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());
            } else if (pmxSourceReference.contains("BKF")) {
                suffix = " Back Files";
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME() + " " + suffix, dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());
            } else if (pmxSourceReference.contains("RPR")) {
                suffix = " Reprints";
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME() + " " + suffix, dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());
            } else if (pmxSourceReference.contains("OOA")) {
                suffix = " Purchase";
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_NAME() + " " + suffix, dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());

            } else if (pmxSourceReference.contains("OAA")) {
                suffix = " Open Access";
                String name = dataQualityContext.productDataObjectsFromEPHSTG.get(0).getWORK_TITLE();
                if (name.contains("(Print)")) {
                    assertEquals(name + suffix, dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());
                } else if (name.contains("(Online)")) {

                    assertEquals(name + suffix, dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());
                }
            } else if (pmxSourceReference.contains("JAS")) {
                suffix = " Author Charges";
                String name = dataQualityContext.productDataObjectsFromEPHSTG.get(0).getWORK_TITLE();

                if (name.contains("(Print)")) {
                    assertEquals(name + suffix, dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());
                } else if (name.contains("(Online)")) {
                    assertEquals(name + suffix, dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());
                }
            } else if (pmxSourceReference.contains("PKG")) {
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getWORK_TITLE(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());

            }


            //PRODUCT_SHORT_NAME
            Log.info("PRODUCT_SHORT_NAME in EPH STG DQ: " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_SHORT_NAME());

            if (type.equals("book") || type.equals("package")) {
                Log.info("PRODUCT_SHORT_NAME in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME());

                Log.info("Expecting PRODUCT_SHORT_NAME in EPH STH and EPH STG DQ is consistent");

                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_SHORT_NAME());
            } else {
                Log.info("PRODUCT_SHORT_NAME in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_SHORT_NAME());

                Log.info("Expecting PRODUCT_SHORT_NAME in EPH STH and EPH STG DQ is consistent");

                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_SHORT_NAME(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_SHORT_NAME());
            }

            //SEPARATELY_SALE_IND
            String availability_status;
            String index;
            if (type.equals("book") || type.equals("package")) {
                availability_status = dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAVAILABILITY_STATUS();
                index = dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSEPARATELY_SALEABLE_IND();

            } else {
                availability_status = dataQualityContext.productDataObjectsFromEPHSTG.get(0).getAVAILABILITY_STATUS();
                index = dataQualityContext.productDataObjectsFromEPHSTG.get(0).getSEPARATELY_SALEABLE_IND();


            }
            Log.info("Availability status: " + availability_status);
            Log.info("SEPARATELY_SALE_IND : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getSEPARATELY_SALEABLE_IND());

            Log.info("Expecting SEPARATELY_SALE_IND in EPH STG DQ is correct");


            if (availability_status.equals("PNS") || index.equals("N"))
                assertEquals("f", dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getSEPARATELY_SALEABLE_IND());
            else
                assertEquals("t", dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getSEPARATELY_SALEABLE_IND());

            //verify TRIAL_ALLOWED_IND
            if (type.equals("book") || type.equals("package")) {
                Log.info("TRIAL_ALLOWED_IND in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND());
                Log.info("TRIAL_ALLOWED_IND in EPH STG DQ: " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTRIAL_ALLOWED_IND());

                Log.info("Expecting TRIAL_ALLOWED_IND in EPH Staging and EPH STG DQ are consistent for ");

                if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND() == null)
                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTRIAL_ALLOWED_IND());
            } else {
                Log.info("TRIAL_ALLOWED_IND in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getTRIAL_ALLOWED_IND());
                Log.info("TRIAL_ALLOWED_IND in EPH STG DQ: " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTRIAL_ALLOWED_IND());

                Log.info("Expecting TRIAL_ALLOWED_IND in EPH Staging and EPH STG DQ are consistent for ");

                if (dataQualityContext.productDataObjectsFromEPHSTG.get(0).getTRIAL_ALLOWED_IND() == null)
                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTRIAL_ALLOWED_IND());

            }

            //verify FIRST_PUB_DATE
            if (type.equals("book") || type.equals("package")) {
                Log.info("FIRST_PUB_DATE in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getFIRST_PUB_DATE());
                Log.info("FIRST_PUB_DATE in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getFIRST_PUB_DATE());

                Log.info("Expecting FIRST_PUB_DATE in EPH Staging And EPH STG DQ are consistent for ");

                if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getFIRST_PUB_DATE() != null)
                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getFIRST_PUB_DATE(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getFIRST_PUB_DATE());
            } else {
                Log.info("FIRST_PUB_DATE in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getFIRST_PUB_DATE());
                Log.info("FIRST_PUB_DATE in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getFIRST_PUB_DATE());

                Log.info("Expecting FIRST_PUB_DATE in EPH Staging And EPH STG DQ are consistent for ");

                if (dataQualityContext.productDataObjectsFromEPHSTG.get(0).getFIRST_PUB_DATE() != null)
                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getFIRST_PUB_DATE(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getFIRST_PUB_DATE());

            }

            //verify F_TYPE
            String pmx_source_reference = dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPMX_SOURCE_REFERENCE();
            Log.info("pmx_source_reference : " + pmx_source_reference);

            Log.info("F_TYPE in EPH STG DQ: " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_TYPE());

            Log.info("Expecting F_TYPE in EPH STG DQ is correct");

            assertEquals(pmx_source_reference.substring(pmx_source_reference.length() - 3), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_TYPE());


            //F_STATUS
            Log.info("F_STATUS in EPH STG DQ: " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_STATUS());

            Log.info("Expecting F_STATUS in EPH STG DQ is correct");
            if (pmxSourceReference.contains("SUB") || pmxSourceReference.contains("JBS") || pmxSourceReference.contains("OAA") || pmxSourceReference.contains("JAS") || pmxSourceReference.contains("OOA") || pmxSourceReference.contains("PKG")) {
                if (availability_status.equals("PSTB"))
                    assertEquals("PST", dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_STATUS());
                else
                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_STATUS(), availability_status);
            } else if (pmxSourceReference.contains("BKF") || pmxSourceReference.contains("RPR")) {
                if (availability_status.equals("PSTB"))
                    assertEquals("PAS", dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_STATUS());
                else
                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_STATUS(), availability_status);
            }

            //F_REVENUE_MODEL
            Log.info("F_TYPE : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_TYPE());
            String fType = dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_TYPE();

            Log.info("Expecting F_REVENUE_MODEL in EPH STG DQ is correct");

            if (fType.equals("OOA") || fType.equals("JAS") || fType.equals("JBS") || fType.equals("JBF") || fType.equals("RPR"))
                assertEquals("ONE", dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_REVENUE_MODEL());
            else if (fType.equals("OAA"))
                assertEquals("EVE", dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_REVENUE_MODEL());
            else if (fType.equals("SUB") && type.equals("print_journal"))
                assertEquals("EVE", dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_REVENUE_MODEL());
            else
                assertEquals("SUB", dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_REVENUE_MODEL());


            //verify F_WWORK (F_PRODUCT_WORK)
            Log.info("F_PRODUCT_WORK in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_PRODUCT_WORK());

            if (pmxSourceReference.contains("OAA") || pmxSourceReference.contains("JAS"))
                if (type.equals("book") || type.equals("package")) {
                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_PRODUCT_WORK());
                } else
                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getF_PRODUCT_WORK(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_PRODUCT_WORK());
            else
                assertNull(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_PRODUCT_WORK());


            //verify F_MANIFESTATION (F_PRODUCT_MANIFESTATION_TYP)

            Log.info("F_PRODUCT_MANIFESTATION_TYP in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_PRODUCT_MANIFESTATION_TYP());


            if (pmxSourceReference.contains("SUB") || pmxSourceReference.contains("JBS") || pmxSourceReference.contains("BKF") || pmxSourceReference.contains("RPR") || pmxSourceReference.contains("OOA")) {
                if (type.equals("book") || type.equals("package")) {
                    Log.info("F_PRODUCT_MANIFESTATION_TYP in EPH STG  : " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_MANIFESTATION_ID());

                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_MANIFESTATION_ID(), Integer.parseInt(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_PRODUCT_MANIFESTATION_TYP()));
                } else {
                    Log.info("F_PRODUCT_MANIFESTATION_TYP in EPH STG  : " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID());
                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID(), Integer.parseInt(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_PRODUCT_MANIFESTATION_TYP()));
                }
            }else {
                    assertNull(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                }



            //WORK_TYPE
            Log.info("WORK_TYPE in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getWORK_TYPE());

            if (type.equals("book") || type.equals("package")) {
                Log.info("WORK_TYPE in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getWORK_TYPE());
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getWORK_TYPE(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getWORK_TYPE());
            } else {
                Log.info("WORK_TYPE in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getWORK_TYPE());
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getWORK_TYPE(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getWORK_TYPE());

            }




            //ULT_WORK_REF
            Log.info("ULT_WORK_REF in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getULT_WORK_REF());

            if (type.equals("book") || type.equals("package")) {
                Log.info("F_PRODUCT_WORK in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK());
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getULT_WORK_REF());
            } else {
                Log.info("F_PRODUCT_WORK in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getF_PRODUCT_WORK());
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getF_PRODUCT_WORK(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getULT_WORK_REF());
            }


            //TAX_CODE
            Log.info("TAX_CODE in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTAX_CODE());

            if (fType.equals("PKG"))
                assertNull(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTAX_CODE());
            else if (fType.equals("OAA") || fType.equals("JAS"))
                assertEquals("S001", dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTAX_CODE());
            else if (fType.equals("SUB") || fType.equals("JBS") || fType.equals("BKF") || fType.equals("RPR")) {
                if (type.equals("book"))
                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getELSEVIER_TAX_CODE(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTAX_CODE());
                else
                    assertEquals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getELSEVIER_TAX_CODE(), dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTAX_CODE());
            }



           //DQ_ERR
            Log.info("DQ_ERR in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getDQ_ERR());

            assertEquals("N", dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getDQ_ERR());
        });
    }

    @And("^Compare the records in EPH STG DQ and EPH SA for (.*)$")
    public void compareProductsDataBetweenSTGDQAndSA(String type) {
        Log.info("Compare the records in EPH STG DQ and EPH SA for " + type + " ..");

        dataQualityContext.productDataObjectsFromEPHSTGDQ.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
        dataQualityContext.productDataObjectsFromEPHSA.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));

        assertTrue(dataQualityContext.productDataObjectsFromEPHSTGDQ.size() == dataQualityContext.productDataObjectsFromEPHSA.size());

        IntStream.range(0, dataQualityContext.productDataObjectsFromEPHSA.size()).forEach(i -> {

            //verify B_CLASSNAME
            Log.info("B_CLASSNAME in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME());

            Log.info("Expecting B_CLASSNAME in EPH SA to be Product");

            assertEquals("Product", dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME());


            //verify PMX_SOURCE_REFERENCE
            Log.info("PMX_SOURCE_REFERENCE in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPMX_SOURCE_REFERENCE());
            Log.info("PMX_SOURCE_REFERENCE in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());

            Log.info("Expecting PMX_SOURCE_REFERENCE in EPH STG DQ and EPH SA is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPMX_SOURCE_REFERENCE(), dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());


            //PRODUCT_NAME
            Log.info("PRODUCT_NAME in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME());
            Log.info("PRODUCT_NAME in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME());

            Log.info("Expecting PRODUCT_NAME in EPH STG DQ and EPH SA is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_NAME(), dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME());


            //PRODUCT_SHORT_NAME
            Log.info("PRODUCT_SHORT_NAME in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_SHORT_NAME());
            Log.info("PRODUCT_SHORT_NAME in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME());

            Log.info("Expecting PRODUCT_SHORT_NAME in EPH STG DQ and EPH SA is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_SHORT_NAME(), dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME());


            //SEPARATELY_SALE_IND
            Log.info("SEPARATELY_SALE_IND in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getSEPARATELY_SALEABLE_IND());
            Log.info("SEPARATELY_SALE_IND in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND());

            Log.info("Expecting SEPARATELY_SALE_IND in EPH STG DQ and EPH SA is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getSEPARATELY_SALEABLE_IND(), dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND());


            //verify TRIAL_ALLOWED_IND
            Log.info("TRIAL_ALLOWED_IND in EPH STG DQ: " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTRIAL_ALLOWED_IND());
            Log.info("TRIAL_ALLOWED_IND in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getTRIAL_ALLOWED_IND());

            Log.info("Expecting TRIAL_ALLOWED_IND in EPH STG DQ and EPH SA are consistent for ");

            if (dataQualityContext.productDataObjectsFromEPHSTGDQ.get(0).getTRIAL_ALLOWED_IND() == null)
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTRIAL_ALLOWED_IND(), dataQualityContext.productDataObjectsFromEPHSA.get(i).getTRIAL_ALLOWED_IND());


            //verify FIRST_PUB_DATE

            Log.info("FIRST_PUB_DATE in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getFIRST_PUB_DATE());
            Log.info("FIRST_PUB_DATE in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE());

            Log.info("Expecting FIRST_PUB_DATE in EPH Staging And EPH SA are consistent for ");

            if (dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getFIRST_PUB_DATE() != null)
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getFIRST_PUB_DATE(), dataQualityContext.productDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE());


            //verify F_TYPE
            Log.info("F_TYPE in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_TYPE());
            Log.info("F_TYPE in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE());

            Log.info("Expecting F_TYPE in EPH STG DQ And EPH SA are consistent for ");

            if (dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_TYPE() != null)
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_TYPE(), dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE());


            //F_STATUS
            Log.info("F_STATUS in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_STATUS());
            Log.info("F_STATUS in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS());

            Log.info("Expecting F_STATUS in EPH SA is correct");

            if (dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_STATUS() != null)
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_STATUS(), dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS());


            //F_REVENUE_MODEL
            Log.info("F_REVENUE_MODEL in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_REVENUE_MODEL());
            Log.info("F_REVENUE_MODEL in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL());

            Log.info("Expecting F_STATUS in EPH SA is correct");

            if (dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_REVENUE_MODEL() != null)
                assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getF_REVENUE_MODEL(), dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL());


            //verify F_WWORK (F_PRODUCT_WORK)
            if (type.equals("package")) {
                Log.info("F_PRODUCT_WORK in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_WORK());

                assertNull(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_WORK());
            }


            //verify F_MANIFESTATION (F_PRODUCT_MANIFESTATION_TYP)
            if (type.equals("package")) {
                Log.info("F_PRODUCT_MANIFESTATION_TYP in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_WORK());

                assertNull(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            }

            //TAX_CODE
            Log.info("TAX_CODE in EPH STG DQ : " + dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTAX_CODE());
            Log.info("TAX_CODE in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getTAX_CODE());

            Log.info("Expecting TAX_CODE in  EPH STG DQ and EPH SA is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSTGDQ.get(i).getTAX_CODE(), dataQualityContext.productDataObjectsFromEPHSA.get(i).getTAX_CODE());

        });
    }

    @And("^Compare the products data between EPH SA and EPH GD for (.*)$")
    public void compareProductsDataBetweenSAndGD(String type) {
        Log.info("Compare the products data between EPH SA and EPH GD for " + type);

        //sort the lists before comparison
        dataQualityContext.productDataObjectsFromEPHSA.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
        dataQualityContext.productDataObjectsFromEPHGD.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));

        int bound = dataQualityContext.productDataObjectsFromEPHSA.size();

        assertEquals("Count of records in SA and GD is equal for " + type, dataQualityContext.productDataObjectsFromEPHSA.size(), dataQualityContext.productDataObjectsFromEPHGD.size());

        for (int i = 0; i < bound; i++) {

            // F_EVENT
            Log.info("F_EVENT in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_EVENT());
            Log.info("F_EVENT in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_EVENT());

            Log.info("Expecting F_EVENT in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_EVENT(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_EVENT());

            // B_CLASSNAME
            Log.info("B_CLASSNAME in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
            Log.info("B_CLASSNAME in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getB_CLASSNAME());

            Log.info("Expecting B_CLASSNAME in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getB_CLASSNAME());

            // PRODUCT_ID
            Log.info("PRODUCT_ID in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_ID());
            Log.info("PRODUCT_ID in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_ID());

            Log.info("Expecting PRODUCT_ID in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_ID(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_ID());

            // PMX_SOURCE_REFERENCE
            Log.info("PMX_SOURCE_REFERENCE in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());
            Log.info("PMX_SOURCE_REFERENCE in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE());

            Log.info("Expecting PMX_SOURCE_REFERENCE in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE());

            // PRODUCT_NAME
            Log.info("PRODUCT_NAME in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME());
            Log.info("PRODUCT_NAME in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_NAME());

            Log.info("Expecting PRODUCT_NAME in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_NAME());

            // PRODUCT_SHORT_NAME
            Log.info("PRODUCT_SHORT_NAME in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME());
            Log.info("PRODUCT_SHORT_NAME in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_SHORT_NAME());

            Log.info("Expecting PRODUCT_SHORT_NAME in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_SHORT_NAME());


            // SEPARATELY_SALEABLE_IND
            Log.info("SEPARATELY_SALEABLE_IND in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND());
            Log.info("SEPARATELY_SALEABLE_IND in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getSEPARATELY_SALEABLE_IND());

            Log.info("Expecting SEPARATELY_SALEABLE_IND in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getSEPARATELY_SALEABLE_IND());

            // TRIAL_ALLOWED_IND
            Log.info("TRIAL_ALLOWED_IND in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getTRIAL_ALLOWED_IND());
            Log.info("TRIAL_ALLOWED_IND in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getTRIAL_ALLOWED_IND());

            Log.info("Expecting TRIAL_ALLOWED_IND in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getTRIAL_ALLOWED_IND(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getTRIAL_ALLOWED_IND());


            // FIRST_PUB_DATE
            Log.info("FIRST_PUB_DATE in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE());
            Log.info("FIRST_PUB_DATE in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getFIRST_PUB_DATE());

            Log.info("Expecting FIRST_PUB_DATE in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getFIRST_PUB_DATE());


            // F_TYPE
            Log.info("F_TYPE in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE());
            Log.info("F_TYPE in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_TYPE());

            Log.info("Expecting F_TYPE in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_TYPE());


            // F_STATUS
            Log.info("F_STATUS in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS());
            Log.info("F_STATUS in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_STATUS());

            Log.info("Expecting F_STATUS in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_STATUS());


            // F_REVENUE_MODEL
            Log.info("F_REVENUE_MODEL in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL());
            Log.info("F_REVENUE_MODEL in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_REVENUE_MODEL());

            Log.info("Expecting F_REVENUE_MODEL in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_REVENUE_MODEL());


            // F_PRODUCT_WORK
            Log.info("F_PRODUCT_WORK in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_WORK());
            Log.info("F_PRODUCT_WORK in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_PRODUCT_WORK());

            Log.info("Expecting F_PRODUCT_WORK in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_WORK(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_PRODUCT_WORK());

            // F_PRODUCT_MANIFESTATION_TYP
            Log.info("F_PRODUCT_MANIFESTATION_TYP in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            Log.info("F_PRODUCT_MANIFESTATION_TYP in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            Log.info("Expecting F_PRODUCT_MANIFESTATION_TYP in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_MANIFESTATION_TYP(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            //TAX_CODE
            Log.info("TAX_CODE in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getTAX_CODE());
            Log.info("TAX_CODE in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getTAX_CODE());

            Log.info("Expecting TAX_CODE in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getTAX_CODE(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getTAX_CODE());


        }
    }

    @Given("^Check the db for duplicate products$")
    public void checkForDuplicateProducts() {
        Log.info("Check for duplicate products..");
        sql = ProductDataSQL.SELECT_DUPLICATE_PRODUCT_IDS;
        Log.info(sql);

        List<Map<?, ?>> results = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        if (!results.isEmpty()) {
            List<String> idsDuplicateProducts = results.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
            Log.info(idsDuplicateProducts.toString());
        } else
            Log.info("No duplicate products found.");

        assertTrue(results.isEmpty());
    }
}