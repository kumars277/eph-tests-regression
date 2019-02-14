package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


/**
 * Created by Bistra Drazheva on 11/02/2019
 */
public class ProductDataMappingCheck {

    @StaticInjection
    public DataQualityContext dataQualityContext;
    private String sql;
    public static List<String> ids;
    public static List<String> idsSA;
    public static List<String> workIds;

    @Given("^We get (.*) random ids for (.*)$")
    public void getRandomProductManifestationIdsForBooks(String numberOfRecords, String type) {

        sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_BOOKS, numberOfRecords);

        List<Map<?, ?>> randomProductManifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);

        ids = randomProductManifestationIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());

        sql.length();
    }


    @Given("^We get (.*) ids of journals for (.*) with (.*)$")
    public void getRandomProductManifestationIdsForJournals(String numberOfRecords, String type, String open_access) {
        switch (type) {
            case "journal":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_JOURNALS, numberOfRecords);
                break;
            case "print_journal":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_PRINT_JOURNALS, open_access, numberOfRecords);
                break;
            case "electronic_journal":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_ELECTRONIC_JOURNALS, numberOfRecords);
                break;
            case "non_print_or_electronic_journal":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_NON_PRINT_OR_ELECTRONIC_JOURNALS, numberOfRecords);
                break;
            case "book":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_BOOKS, numberOfRecords);
                break;
            default:
                break;
        }

        List<Map<?, ?>> randomProductManifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);

        ids = randomProductManifestationIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());

        sql.length();
    }

    @When("^We get the corresponding records from PMX$")
    public void getProductsDataFromPMX() {
        sql = String.format(ProductDataSQL.PMX_PRODUCT_EXTRACT, Joiner.on("','").join(ids));

        dataQualityContext.productDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.PMX_SIT_URL);
        sql.length();
    }

    @Then("^We get the data from EPH STG$")
    public void getProductsDataFromEPHSTG() {
        sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT, Joiner.on("','").join(ids));

        dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
        sql.length();
    }

    @And("^Compare the records in PMX and EPH STG for (.*)$")
    public void compareProductsDataBetweenPMXAndEPHSTG(String type) {

        IntStream.range(0, dataQualityContext.productDataObjectsFromPMX.size()).forEach(i -> {

            //verify PRODUCT_ID
            System.out.println("\"Expecting Product IDs in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nPRODUCT_ID in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_ID());
            System.out.println("\nPRODUCT_ID in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_ID());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_ID(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_ID()));

            //PRODUCT_NAME
            System.out.println("\"Expecting PRODUCT_NAME in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nPRODUCT_ID in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_ID());
            System.out.println("\nPRODUCT_ID in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_ID());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_NAME(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_NAME()));


            //PRODUCT_SHORT_NAME
            System.out.println("\"Expecting PRODUCT_SHORT_NAME in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nPRODUCT_SHORT_NAME in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_SHORT_NAME());
            System.out.println("\nPRODUCT_SHORT_NAME in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_SHORT_NAME(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME()));

            //TRIAL_ALLOWED_IND
            System.out.println("\nExpecting TRIAL_ALLOWED_IND in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nTRIAL_ALLOWED_IND in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getTRIAL_ALLOWED_IND());
            System.out.println("\nTRIAL_ALLOWED_IND in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getTRIAL_ALLOWED_IND(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND()));

            //FIRST_PUB_DATE
            System.out.println("\nExpecting FIRST_PUB_DATE in PMX and EPH Staging are consistent for " + type);


            if (dataQualityContext.productDataObjectsFromPMX.get(i).getFIRST_PUB_DATE() != null)
                try {
                    Date pmxFirstPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.productDataObjectsFromPMX.get(i).getFIRST_PUB_DATE());
                    System.out.println("\nFIRST_PUB_DATE in PMX : " + pmxFirstPubDate);

                    Date ephStgFirstPubDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.productDataObjectsFromPMX.get(i).getFIRST_PUB_DATE());
                    System.out.println("\nFIRST_PUB_DATE in EPH STG : " + ephStgFirstPubDate);


                    assertTrue(pmxFirstPubDate.compareTo(ephStgFirstPubDate) == 0);

                } catch (ParseException e) {
                    e.printStackTrace();
                }

            //ELSEVIER_TAX_CODE
            System.out.println("\nExpecting ELSEVIER_TAX_CODE in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nELSEVIER_TAX_CODE in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getELSEVIER_TAX_CODE());
            System.out.println("\nELSEVIER_TAX_CODE in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getELSEVIER_TAX_CODE());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getELSEVIER_TAX_CODE(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getELSEVIER_TAX_CODE()));

            // PRODUCT_MANIFESTATION_ID
            System.out.println("\nExpecting PRODUCT_MANIFESTATION_ID in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nPRODUCT_MANIFESTATION_ID in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getELSEVIER_TAX_CODE());
            System.out.println("\nPRODUCT_MANIFESTATION_ID in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getELSEVIER_TAX_CODE());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_MANIFESTATION_ID(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_MANIFESTATION_ID()));

            //F_PRODUCT_WORK
            System.out.println("\nExpecting F_PRODUCT_WORK in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nF_PRODUCT_WORK in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_WORK());
            System.out.println("\nF_PRODUCT_WORK in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK());

            assertTrue(Objects.equals(Integer.parseInt(dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_WORK()),
                    Integer.parseInt(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK())));

            //F_PRODUCT_MANIFESTATION_TYP
            System.out.println("\nExpecting F_PRODUCT_MANIFESTATION_TYP in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nF_PRODUCT_MANIFESTATION_TYP in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            System.out.println("\nF_PRODUCT_MANIFESTATION_TYP in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            assertTrue(Objects.equals(Integer.parseInt(dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP()),
                    Integer.parseInt(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_MANIFESTATION_TYP())));

            //SUBSCRIPTION
            System.out.println("\nExpecting SUBSCRIPTION in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nSUBSCRIPTION in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getSUBSCRIPTION());
            System.out.println("\nSUBSCRIPTION in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSUBSCRIPTION());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getSUBSCRIPTION(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSUBSCRIPTION()));

            //BULK_SALES
            System.out.println("\nExpecting BULK_SALES in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nBULK_SALES in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getBULK_SALES());
            System.out.println("\nBULK_SALES in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBULK_SALES());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getBULK_SALES(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBULK_SALES()));

            //BACK_FILES
            System.out.println("\nExpecting BACK_FILES in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nBACK_FILES in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getBACK_FILES());
            System.out.println("\nBACK_FILES in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBACK_FILES());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getBACK_FILES(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBACK_FILES()));

            //OPEN_ACCESS
            System.out.println("\nExpecting OPEN_ACCESS in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nOPEN_ACCESS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getOPEN_ACCESS());
            System.out.println("\nOPEN_ACCESS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getOPEN_ACCESS());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getOPEN_ACCESS(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getOPEN_ACCESS()));

            //REPRINTS
            System.out.println("\nExpecting REPRINTS in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nREPRINTS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getREPRINTS());
            System.out.println("\nREPRINTS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getREPRINTS());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getREPRINTS(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getREPRINTS()));

            //AUTHOR_CHARGES
            System.out.println("\nExpecting AUTHOR_CHARGES in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nAUTHOR_CHARGES in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getAUTHOR_CHARGES());
            System.out.println("\nAUTHOR_CHARGES in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAUTHOR_CHARGES());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getAUTHOR_CHARGES(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAUTHOR_CHARGES()));

            //ONE_OFF_ACCESS
            System.out.println("\nExpecting ONE_OFF_ACCESS in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nONE_OFF_ACCESS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getONE_OFF_ACCESS());
            System.out.println("\nONE_OFF_ACCESS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getONE_OFF_ACCESS());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getONE_OFF_ACCESS(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getONE_OFF_ACCESS()));

            //AVAILABILITY_STATUS
            System.out.println("\nExpecting AVAILABILITY_STATUS in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nAVAILABILITY_STATUS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getAVAILABILITY_STATUS());
            System.out.println("\nAVAILABILITY_STATUS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAVAILABILITY_STATUS());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getAVAILABILITY_STATUS(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAVAILABILITY_STATUS()));

            //WORK_TITLE
            System.out.println("\nExpecting WORK_TITLE in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nWORK_TITLE in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getWORK_TITLE());
            System.out.println("\nWORK_TITLE in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getWORK_TITLE());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getWORK_TITLE(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getWORK_TITLE()));

        });
    }


    @And("^We get the data from EPH SA$")
    public void getProductsDataFromEPHSA() {
        //for books
        idsSA = ids.stream().collect(Collectors.toList());
        IntStream.range(0, idsSA.size()).forEach(i -> idsSA.set(i, idsSA.get(i) + "-OOA"));
        idsSA.size();

        sql = String.format(ProductDataSQL.EPH_SA_PRODUCT_EXTRACT, Joiner.on("','").join(idsSA));

        dataQualityContext.productDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
        sql.length();
    }

    @And("^We get the data from EPH GD$")
    public void getProductsDataFromEPHGD() {
        sql = String.format(ProductDataSQL.EPH_GD_PRODUCT_EXTRACT, Joiner.on("','").join(idsSA));

        dataQualityContext.productDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
        sql.length();
    }

    @And("^We get the data from EPH SA for journals$")
    public void getProductsDataFromEPHSAForJournals() {
        //get F_ProductWorkIds
        sql = String.format(ProductDataSQL.SELECT_F_PRODUCT_WORK_IDS_FOR_GIVEN_MANIFESTATION_IDS, Joiner.on("','").join(ids));
        List<Map<?, ?>> fProductWorkIds = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);

        workIds = fProductWorkIds.stream().map(m -> (BigDecimal) m.get("F_PRODUCT_WORK")).map(String::valueOf).collect(Collectors.toList());

        idsSA = Stream.concat(ids.stream(), workIds.stream()).collect(Collectors.toList());
        IntStream.range(0, idsSA.size()).forEach(i -> idsSA.set(i, idsSA.get(i) + "%"));

        sql = String.format(ProductDataSQL.EPH_SA_PRODUCT_EXTRACT_JOURNALS, Joiner.on("|").join(idsSA));

        dataQualityContext.productDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
        sql.length();
    }

    @And("^We get the data from EPH GD for journals$")
    public void getProductsDataFromEPHGDForJournals() {
        sql = String.format(ProductDataSQL.EPH_GD_PRODUCT_EXTRACT_JOURNALS, Joiner.on("|").join(idsSA));

        dataQualityContext.productDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
        sql.length();

    }

    @And("^We check that mandatory columns are populated$")
    public void checkMandatoryColumnsForProductsInSAArePopulated() {
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

    @And("^Depends on the flags of every record from Staging check if we have the expected number of records in SA$")
    public void checkCountOfRecordsInSAIsApplicableToDataInSTG() {
        int expectedNumberOfRecordsInSA = 0;

        for (int i = 0; i < dataQualityContext.productDataObjectsFromEPHSTG.size(); i++) {
            // subscription flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSUBSCRIPTION().equals("Y"))
                expectedNumberOfRecordsInSA++;
            // bulk_sale flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBULK_SALES().equals("Y"))
                expectedNumberOfRecordsInSA++;
            // back_files flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBACK_FILES().equals("Y"))
                expectedNumberOfRecordsInSA++;
            //open_access flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getOPEN_ACCESS().equals("Y"))
                expectedNumberOfRecordsInSA++;
            //reprints flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getREPRINTS().equals("Y"))
                expectedNumberOfRecordsInSA++;
            //author_charges flag
            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAUTHOR_CHARGES().equals("Y"))
                expectedNumberOfRecordsInSA++;
        }

        System.out.println("\nExpected number of records in SA is : " + expectedNumberOfRecordsInSA);
        System.out.println("\nNumber of records in SA is : " + dataQualityContext.productDataObjectsFromEPHSA.size());

        Assert.assertEquals(expectedNumberOfRecordsInSA, dataQualityContext.productDataObjectsFromEPHSA.size());
    }

    @And("^Compare the records in EPH STG and EPH SA for books$")
    public void compareProductsDataBetweenSTGAndSA() {
        //sort the lists before comparison
        dataQualityContext.productDataObjectsFromEPHSTG.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
        dataQualityContext.productDataObjectsFromEPHSA.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));

        IntStream.range(0, dataQualityContext.productDataObjectsFromEPHSTG.size()).forEach(i -> {

            //verify B_CLASSNAME
            System.out.println("\nExpecting B_CLASSNAME in EPH SA for books to be Product");

            System.out.println("\nB_CLASSNAME in EPH SA for book: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME());


            assertTrue(Objects.equals("Product",
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME()));

            //verify PMX_SOURCE_REFERENCE
            System.out.println("\"Expecting PMX_SOURCE_REFERENCE in EPH Staging and EPH SA are consistent for ");

            System.out.println("\nPRODUCT_MANIFESTATION_ID in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_MANIFESTATION_ID());
            System.out.println("\nPRODUCT_SHORT_NAME in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_MANIFESTATION_ID() + "-OOA",
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE()));

            //verify PRODUCT_NAME
            System.out.println("\"Expecting PRODUCT_NAME in EPH Staging and EPH SA are consistent for ");

            System.out.println("\nPRODUCT_NAME in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_NAME());
            System.out.println("\nPRODUCT_NAME in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_NAME() + "  " + "Purchase",
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME()));


            //verify PRODUCT_SHORT_NAME
            System.out.println("\"Expecting PRODUCT_SHORT_NAME in EPH Staging and EPH SA are consistent for ");

            System.out.println("\nPRODUCT_SHORT_NAME in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME());
            System.out.println("\nPRODUCT_SHORT_NAME in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME());


            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME()));


            //verify SEPARATELY_SALEABLE_IND
            System.out.println("\"Expecting SEPARATELY_SALEABLE_IND in EPH Staging and EPH SA are consistent for ");

            System.out.println("\nSEPARATELY_SALEABLE_IND in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSEPARATELY_SALEABLE_IND());


            String availability_status = dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAVAILABILITY_STATUS();


            if (availability_status.equals("PNS"))
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND().equals("f"));
            else
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND().equals("t"));


            //verify TRIAL_ALLOWED_IND
            System.out.println("\"Expecting TRIAL_ALLOWED_IND in EPH Staging and EPH SA are consistent for ");

            System.out.println("\nTRIAL_ALLOWED_IND in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND());
            System.out.println("\nTRIAL_ALLOWED_IND in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getTRIAL_ALLOWED_IND());


            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND() == null)
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getTRIAL_ALLOWED_IND().equals("f"));
            else
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getTRIAL_ALLOWED_IND().equals("t"));


            //verify FIRST_PUB_DATE
            System.out.println("\nExpecting FIRST_PUB_DATE in EPH Staging And EPH SA are consistent for ");


            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getFIRST_PUB_DATE() != null)
                System.out.println("\nFIRST_PUB_DATE in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getFIRST_PUB_DATE());

            System.out.println("\nFIRST_PUB_DATE in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getFIRST_PUB_DATE(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE()));


            //verify F_TYPE
            System.out.println("\"Expecting F_TYPE in EPH SA is correct" );

            System.out.println("\nF_TYPE in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE());

            String pmx_source_reference = dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE();

            assertTrue(Objects.equals(pmx_source_reference.substring(pmx_source_reference.length() - 3),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE()));


            //verify F_STATUS
            System.out.println("\"Expecting F_STATUS in EPH Staging and EPH SA are consistent for ");

            System.out.println("\nF_STATUS in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_STATUS());

            if (availability_status.equals("PSTB"))
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS().equals("PST"));
            else
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS().equals(availability_status));


            //verify F_REVENUE_MODEL
            System.out.println("\"Expecting F_REVENUE_MODEL in EPH Staging and EPH SA are consistent for ");

            System.out.println("\nF_TYPE in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_TYPE());
            System.out.println("\nF_REVENUE_MODEL in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL());


            String fType = dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE();
            if (fType.equals("OOA") || fType.equals("JAS") || fType.equals("JBS") || fType.equals("JBF") || fType.equals("RPR"))
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL().equals("ONE"));
            else if (fType.equals("OAA"))
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL().equals("EVE"));
            else if (fType.equals("SUB"))
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL().equals("EVE"));
            else
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL().equals("SUB"));


//            //verify F_PRODUCT_WORK
//            System.out.println("\"Expecting F_PRODUCT_WORK in EPH Staging and EPH SA are consistent for " );
//
//            System.out.println("\nF_PRODUCT_WORK in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK());
//            System.out.println("\nF_PRODUCT_WORK in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_WORK());
//
//
//            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK(),
//                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_WORK()));
        });
    }

    @And("^Compare the records in EPH STG and EPH SA for journals with (.*)$")
    public void compareProductsDataBetweenSTGAndSAPrintJournals(String type) {
        IntStream.range(0, dataQualityContext.productDataObjectsFromEPHSA.size()).forEach(i -> {

            //verify B_CLASSNAME
            System.out.println("\nExpecting B_CLASSNAME in EPH SA for journals to be Product");

            System.out.println("\nB_CLASSNAME in EPH SA for journal: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME());

            Assert.assertTrue(Objects.equals("Product", dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME()));

            //verify PMX_SOURCE_REFERENCE and get the manifestation or work id
            String id;
            String pmxSourceReference = dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE();
            System.out.println("\n PMX_SOURCE_REFERENCE in EPH SA for journal is " + pmxSourceReference);

            if (pmxSourceReference.contains("SUB")) {
                //get the id

                id = pmxSourceReference.replace("-SUB", "");

                if (type.equals("print_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 1);
                else if (type.equals("electronic_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 2);
                else if (type.equals("non_print_or_electronic_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL_NOT_PRINT_OR_ELECTRONIC, id);

                dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                        .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
                System.out.println("\n PMX_SOURCE_REFERENCE in EPH STG for journal is " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID());

                //assert pmxSourceReference value is correct
                Assert.assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID() + "-SUB", pmxSourceReference));

            } else if (pmxSourceReference.contains("RPR")) {
                //get the id
                id = pmxSourceReference.replace("-RPR", "");

                if (type.equals("print_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 1);
                else if (type.equals("electronic_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 2);
                else if (type.equals("non_print_or_electronic_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL_NOT_PRINT_OR_ELECTRONIC, id);

                dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                        .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
                System.out.println("\n PMX_SOURCE_REFERENCE in EPH STG for journal is " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID());

                //assert pmxSourceReference value is correct
                Assert.assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID() + "-RPR", pmxSourceReference));

            } else if (pmxSourceReference.contains("JBS")) {

                //get the id
                id = pmxSourceReference.replace("-JBS", "");

                if (type.equals("print_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 1);
                else if (type.equals("electronic_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL, id, 2);
                else if (type.equals("non_print_or_electronic_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_JOURNAL_NOT_PRINT_OR_ELECTRONIC, id);

                dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                        .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
                System.out.println("\n PMX_SOURCE_REFERENCE in EPH STG for journal is " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID());

                //assert pmxSourceReference value is correct
                Assert.assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_MANIFESTATION_ID() + "-JBS", pmxSourceReference));

            } else if (pmxSourceReference.contains("JAS")) {
                //get the id
                id = pmxSourceReference.replace("-JAS", "");

                if (type.equals("print_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK, id, 1);
                else if (type.equals("electronic_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK, id, 2);
                else if (type.equals("non_print_or_electronic_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_NOT_PRINT_OR_ELECTRONIC, id);


                dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                        .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
                System.out.println("\n PMX_SOURCE_REFERENCE in EPH STG for journal is " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getF_PRODUCT_WORK());

                //assert pmxSourceReference value is correct
                Assert.assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getF_PRODUCT_WORK() + "-JAS", pmxSourceReference));
            } else if (pmxSourceReference.contains("OAA")) {

                //get the id
                id = pmxSourceReference.replace("-OAA", "");

                if (type.equals("print_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK, id, 1);
                else if (type.equals("electronic_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK, id, 2);
                else if (type.equals("non_print_or_electronic_journal"))
                    sql = String.format(ProductDataSQL.EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_NOT_PRINT_OR_ELECTRONIC, id);


                dataQualityContext.productDataObjectsFromEPHSTG = DBManager
                        .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
                System.out.println("\n PMX_SOURCE_REFERENCE in EPH STG for journal is " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getF_PRODUCT_WORK());

                //assert pmxSourceReference value is correct
                Assert.assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getF_PRODUCT_WORK() + "-OAA", pmxSourceReference));
            }


            //PRODUCT_NAME
            System.out.println("\nExpecting PRODUCT_NAME in EPH STH and EPH SA for journals is consistent");

            System.out.println("\nPRODUCT_NAME in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME());
            System.out.println("\nPRODUCT_NAME in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME());

            String suffix = null;
            if (pmxSourceReference.contains("SUB")) {
                suffix = "Subscription";
                Assert.assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME() + " " + suffix, dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME()));
            } else if (pmxSourceReference.contains("JBS")) {
                suffix = " Bulk Sales";
                Assert.assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME() + " " + suffix, dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME()));
            } else if (pmxSourceReference.contains("BKF")) {
                suffix = " Back Files";
                Assert.assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME() + " " + suffix, dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME()));
            } else if (pmxSourceReference.contains("RPR")) {
                suffix = " Reprints";
                Assert.assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME() + " " + suffix, dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME()));
            } else if (pmxSourceReference.contains("OOA")) {
                suffix = " One off Access";
                Assert.assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME() + " " + suffix, dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME()));
            } else if (pmxSourceReference.contains("OAA")) {
                suffix = " Open Access";
                String name = dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME();
                if (name.contains("Print"))
                    Assert.assertTrue(Objects.equals(name.replace(" (Print)", "") + " " + suffix, dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME()));
                else if (name.contains("Online"))
                    Assert.assertTrue(Objects.equals(name.replace(" (Online)", "") + " " + suffix, dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME()));

            } else if (pmxSourceReference.contains("JAS")) {
                suffix = " Author Charges";
                String name = dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_NAME();
                if (name.contains("Print"))
                    Assert.assertTrue(Objects.equals(name.replace(" (Print)", "") + " " + suffix, dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME()));
                else if (name.contains("Online"))
                    Assert.assertTrue(Objects.equals(name.replace(" (Online)", "") + " " + suffix, dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME()));
            }


            //PRODUCT_SHORT_NAME
            System.out.println("\nExpecting PRODUCT_SHORT_NAME in EPH STH and EPH SA for journals is consistent");

            System.out.println("\nPRODUCT_SHORT_NAME in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_SHORT_NAME());
            System.out.println("\nPRODUCT_SHORT_NAME in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME());

            Assert.assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).getPRODUCT_SHORT_NAME(), dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME()));


            //SEPARATELY_SALE_IND
            String availability_status = dataQualityContext.productDataObjectsFromEPHSTG.get(0).getAVAILABILITY_STATUS();
            System.out.println("\n Availability status: " + availability_status);
            System.out.println("\n SEPARATELY_SALE_IND : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND());


            if (availability_status.equals("PNS"))
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND().equals("f"));
            else
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND().equals("t"));

            //F_TYPE
            System.out.println("\"Expecting F_TYPE in EPH SA is correct" );

            System.out.println("\nF_TYPE in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE());

            String pmx_source_reference = dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE();

            assertTrue(Objects.equals(pmx_source_reference.substring(pmx_source_reference.length() - 3),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE()));

            //F_STATUS
            System.out.println("\nF_STATUS in EPH SA: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS());

            if (pmxSourceReference.contains("SUB") || pmxSourceReference.contains("JBS") || pmxSourceReference.contains("OAA") || pmxSourceReference.contains("JAS")) {
                if (availability_status.equals("PSTB"))
                    assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS().equals("PST"));
                else if (availability_status.equals("PSTB"))
                    assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS().equals(availability_status));
            } else if (pmxSourceReference.contains("BKF") || pmxSourceReference.contains("RPR")) {
                if (availability_status.equals("PSTB"))
                    assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS().equals("PAS"));
                else
                    assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS().equals(availability_status));
            }

            //F_REVENUE_MODEL
            String fType = dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE();
            if (fType.equals("OOA") || fType.equals("JAS") || fType.equals("JBS") || fType.equals("JBF") || fType.equals("RPR"))
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL().equals("ONE"));
            else if (fType.equals("OAA"))
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL().equals("EVE"));
            else if (fType.equals("SUB") && type.equals("print_journal"))
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL().equals("EVE"));
            else
                assertTrue(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL().equals("SUB"));


        });
    }

    @And("^Compare the products data between EPH SA and EPH GD$")
    public void compareProductsDataBetweenSAndGD() {
        //sort the lists before comparison
        dataQualityContext.productDataObjectsFromEPHSA.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
        dataQualityContext.productDataObjectsFromEPHGD.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));

        int bound = dataQualityContext.productDataObjectsFromEPHSA.size();
        for (int i = 0; i < bound; i++) {

            // F_EVENT
            System.out.println("\nExpecting F_EVENT in EPH SA and EPH GD is consistent");

            System.out.println("\nF_EVENT in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_EVENT());
            System.out.println("\nF_EVENT in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_EVENT());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_EVENT(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_EVENT()));

            // B_CLASSNAME
            System.out.println("\nExpecting B_CLASSNAME in EPH SA and EPH GD is consistent");

            System.out.println("\nB_CLASSNAME in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
            System.out.println("\nB_CLASSNAME in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getB_CLASSNAME());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getB_CLASSNAME()));

            // PRODUCT_ID
            System.out.println("\nExpecting PRODUCT_ID in EPH SA and EPH GD is consistent");

            System.out.println("\nPRODUCT_ID in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_ID());
            System.out.println("\nPRODUCT_ID in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_ID());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_ID(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_ID()));

            // PMX_SOURCE_REFERENCE
            System.out.println("\nExpecting PMX_SOURCE_REFERENCE in EPH SA and EPH GD is consistent");

            System.out.println("\nPMX_SOURCE_REFERENCE in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());
            System.out.println("\nPMX_SOURCE_REFERENCE in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE()));

            // PRODUCT_NAME
            System.out.println("\nExpecting PRODUCT_NAME in EPH SA and EPH GD is consistent");

            System.out.println("\nPRODUCT_NAME in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME());
            System.out.println("\nPRODUCT_NAME in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_NAME());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_NAME()));

            // PRODUCT_SHORT_NAME
            System.out.println("\nExpecting PRODUCT_SHORT_NAME in EPH SA and EPH GD is consistent");

            System.out.println("\nPRODUCT_SHORT_NAME in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME());
            System.out.println("\nPRODUCT_SHORT_NAME in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_SHORT_NAME());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_SHORT_NAME()));


            // SEPARATELY_SALEABLE_IND
            System.out.println("\nExpecting SEPARATELY_SALEABLE_IND in EPH SA and EPH GD is consistent");

            System.out.println("\nSEPARATELY_SALEABLE_IND in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND());
            System.out.println("\nSEPARATELY_SALEABLE_IND in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getSEPARATELY_SALEABLE_IND());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getSEPARATELY_SALEABLE_IND()));

            // TRIAL_ALLOWED_IND
            System.out.println("\nExpecting TRIAL_ALLOWED_IND in EPH SA and EPH GD is consistent");

            System.out.println("\nTRIAL_ALLOWED_IND in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getTRIAL_ALLOWED_IND());
            System.out.println("\nTRIAL_ALLOWED_IND in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getTRIAL_ALLOWED_IND());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getTRIAL_ALLOWED_IND(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getTRIAL_ALLOWED_IND()));


            // FIRST_PUB_DATE
            System.out.println("\nExpecting FIRST_PUB_DATE in EPH SA and EPH GD is consistent");

            System.out.println("\nFIRST_PUB_DATE in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE());
            System.out.println("\nFIRST_PUB_DATE in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getFIRST_PUB_DATE());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getFIRST_PUB_DATE()));


            // F_TYPE
            System.out.println("\nExpecting F_TYPE in EPH SA and EPH GD is consistent");

            System.out.println("\nF_TYPE in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE());
            System.out.println("\nF_TYPE in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_TYPE());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_TYPE()));


            // F_STATUS
            System.out.println("\nExpecting F_STATUS in EPH SA and EPH GD is consistent");

            System.out.println("\nF_STATUS in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS());
            System.out.println("\nF_STATUS in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_STATUS());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_STATUS()));


            // F_REVENUE_MODEL
            System.out.println("\nExpecting F_REVENUE_MODEL in EPH SA and EPH GD is consistent");

            System.out.println("\nF_REVENUE_MODEL in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL());
            System.out.println("\nF_REVENUE_MODEL in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_REVENUE_MODEL());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_REVENUE_MODEL()));


            // F_PRODUCT_WORK
            System.out.println("\nExpecting F_PRODUCT_WORK in EPH SA and EPH GD is consistent");

            System.out.println("\nF_PRODUCT_WORK in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_WORK());
            System.out.println("\nF_PRODUCT_WORK in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_PRODUCT_WORK());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_WORK(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_PRODUCT_WORK()));

            // F_PRODUCT_MANIFESTATION_TYP
            System.out.println("\nExpecting F_PRODUCT_MANIFESTATION_TYP in EPH SA and EPH GD is consistent");

            System.out.println("\nF_PRODUCT_MANIFESTATION_TYP in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            System.out.println("\nF_PRODUCT_MANIFESTATION_TYP in EPH GD: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            assertTrue(Objects.equals(dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_MANIFESTATION_TYP(), dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_PRODUCT_MANIFESTATION_TYP()));


        }
    }
}