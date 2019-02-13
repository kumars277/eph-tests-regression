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

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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


    @Given("^We get (.*) random ids for (.*)$")
    public void getRandomProductManifestationIds(String numberOfRecords, String type) {
        switch (type) {
            case "journal":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_JOURNALS, numberOfRecords);
                break;
            case "print_journal":
                sql = String.format(ProductDataSQL.SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_PRINT_JOURNALS, numberOfRecords);
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

            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_ID(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_ID());

            //PRODUCT_NAME
            System.out.println("\"Expecting PRODUCT_NAME in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nPRODUCT_ID in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_ID());
            System.out.println("\nPRODUCT_ID in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_ID());


            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_NAME(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_NAME());


            //PRODUCT_SHORT_NAME
            System.out.println("\"Expecting PRODUCT_SHORT_NAME in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nPRODUCT_SHORT_NAME in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_SHORT_NAME());
            System.out.println("\nPRODUCT_SHORT_NAME in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME());


            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_SHORT_NAME(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME());

            //TRIAL_ALLOWED_IND
            System.out.println("\nExpecting TRIAL_ALLOWED_IND in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nTRIAL_ALLOWED_IND in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getTRIAL_ALLOWED_IND());
            System.out.println("\nTRIAL_ALLOWED_IND in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND());


            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getTRIAL_ALLOWED_IND(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND());

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


            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getELSEVIER_TAX_CODE(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getELSEVIER_TAX_CODE());

            // PRODUCT_MANIFESTATION_ID
            System.out.println("\nExpecting PRODUCT_MANIFESTATION_ID in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nPRODUCT_MANIFESTATION_ID in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getELSEVIER_TAX_CODE());
            System.out.println("\nPRODUCT_MANIFESTATION_ID in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getELSEVIER_TAX_CODE());


            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getPRODUCT_MANIFESTATION_ID(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_MANIFESTATION_ID());

            //F_PRODUCT_WORK
            System.out.println("\nExpecting F_PRODUCT_WORK in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nF_PRODUCT_WORK in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_WORK());
            System.out.println("\nF_PRODUCT_WORK in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK());

            Objects.equals(Integer.parseInt(dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_WORK()),
                    Integer.parseInt(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK()));

            //F_PRODUCT_MANIFESTATION_TYP
            System.out.println("\nExpecting F_PRODUCT_MANIFESTATION_TYP in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nF_PRODUCT_MANIFESTATION_TYP in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            System.out.println("\nF_PRODUCT_MANIFESTATION_TYP in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_MANIFESTATION_TYP());

            Objects.equals(Integer.parseInt(dataQualityContext.productDataObjectsFromPMX.get(i).getF_PRODUCT_MANIFESTATION_TYP()),
                    Integer.parseInt(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_MANIFESTATION_TYP()));

            //SUBSCRIPTION
            System.out.println("\nExpecting SUBSCRIPTION in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nSUBSCRIPTION in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getSUBSCRIPTION());
            System.out.println("\nSUBSCRIPTION in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSUBSCRIPTION());


            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getSUBSCRIPTION(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSUBSCRIPTION());

            //BULK_SALES
            System.out.println("\nExpecting BULK_SALES in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nBULK_SALES in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getBULK_SALES());
            System.out.println("\nBULK_SALES in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBULK_SALES());

            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getBULK_SALES(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBULK_SALES());

            //BACK_FILES
            System.out.println("\nExpecting BACK_FILES in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nBACK_FILES in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getBACK_FILES());
            System.out.println("\nBACK_FILES in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBACK_FILES());

            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getBACK_FILES(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getBACK_FILES());

            //OPEN_ACCESS
            System.out.println("\nExpecting OPEN_ACCESS in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nOPEN_ACCESS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getOPEN_ACCESS());
            System.out.println("\nOPEN_ACCESS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getOPEN_ACCESS());

            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getOPEN_ACCESS(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getOPEN_ACCESS());

            //REPRINTS
            System.out.println("\nExpecting REPRINTS in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nREPRINTS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getREPRINTS());
            System.out.println("\nREPRINTS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getREPRINTS());


            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getREPRINTS(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getREPRINTS());

            //AUTHOR_CHARGES
            System.out.println("\nExpecting AUTHOR_CHARGES in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nAUTHOR_CHARGES in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getAUTHOR_CHARGES());
            System.out.println("\nAUTHOR_CHARGES in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAUTHOR_CHARGES());

            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getAUTHOR_CHARGES(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAUTHOR_CHARGES());

            //ONE_OFF_ACCESS
            System.out.println("\nExpecting ONE_OFF_ACCESS in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nONE_OFF_ACCESS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getONE_OFF_ACCESS());
            System.out.println("\nONE_OFF_ACCESS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getONE_OFF_ACCESS());

            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getONE_OFF_ACCESS(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getONE_OFF_ACCESS());

            //AVAILABILITY_STATUS
            System.out.println("\nExpecting AVAILABILITY_STATUS in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nAVAILABILITY_STATUS in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getAVAILABILITY_STATUS());
            System.out.println("\nAVAILABILITY_STATUS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAVAILABILITY_STATUS());


            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getAVAILABILITY_STATUS(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getAVAILABILITY_STATUS());

            //WORK_TITLE
            System.out.println("\nExpecting WORK_TITLE in PMX and EPH Staging are consistent for " + type);

            System.out.println("\nWORK_TITLE in PMX: " + dataQualityContext.productDataObjectsFromPMX.get(i).getWORK_TITLE());
            System.out.println("\nWORK_TITLE in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getWORK_TITLE());


            Objects.equals(dataQualityContext.productDataObjectsFromPMX.get(i).getWORK_TITLE(),
                    dataQualityContext.productDataObjectsFromEPHSTG.get(i).getWORK_TITLE());

        });
    }

    @When("^we get the corresponding pmx_source_reference ids for the random ids for (.*)$")
    public void getListOfPmxSourceReferenceIds(String type) {
        switch (type) {
            case "book":
                idsSA = ids.stream().collect(Collectors.toList());
                IntStream.range(0, idsSA.size()).forEach(i -> idsSA.set(i, idsSA.get(i) + "-OOA"));
                idsSA.size();
                break;

            case "print_journal":
                idsSA = new ArrayList<>();
                for (int i = 0; i < ids.size(); i++) {
                    idsSA.add(ids.get(i) + "-JBS");
                    idsSA.add(ids.get(i) + "-SUB");
                    idsSA.add(ids.get(i) + "-RPR");
                }
                break;

            case "electronic_journal":
                idsSA = new ArrayList<>();
                for (int i = 0; i < ids.size(); i++) {
                    idsSA.add(ids.get(i) + "-SUB");
                    idsSA.add(ids.get(i) + "-BKF");
                }
                break;

            case "non_print_or_electronic_journal":
                idsSA = new ArrayList<>();
                for (int i = 0; i < ids.size(); i++) {
                    idsSA.add(ids.get(i) + "-SUB");
                    idsSA.add(ids.get(i) + "-JAS");
                }
                break;
            default:
                break;
        }

    }

    @And("^We get the data from EPH SA$")
    public void getProductsDataFromEPHSA() {
        sql = String.format(ProductDataSQL.EPH_SA_PRODUCT_EXTRACT, Joiner.on("','").join(idsSA));

        dataQualityContext.productDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
        sql.length();
    }

    @And("^Compare the records in EPH STG and EPH SA for (.*)$")
    public void compareProductsDataBetweenSTGAndSA(String type) {
        IntStream.range(0, dataQualityContext.productDataObjectsFromEPHSTG.size()).forEach(i -> {

            //verify B_CLASSNAME
            System.out.println("\nExpecting B_CLASSNAME in EPH SA for books to be Product");

            System.out.println("\nB_CLASSNAME in EPH SA for book: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME());


            Objects.equals("Product",
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getB_CLASSNAME());

            //verify PMX_SOURCE_REFERENCE
            System.out.println("\"Expecting PMX_SOURCE_REFERENCE in EPH Staging and EPH SA are consistent for " + type);

            System.out.println("\nPRODUCT_MANIFESTATION_ID in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_MANIFESTATION_ID());
            System.out.println("\nPRODUCT_SHORT_NAME in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());


            Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_MANIFESTATION_ID() + "OOA",
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getPMX_SOURCE_REFERENCE());

            //verify PRODUCT_NAME
            System.out.println("\"Expecting PRODUCT_NAME in EPH Staging and EPH SA are consistent for " + type);

            System.out.println("\nPRODUCT_NAME in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_NAME());
            System.out.println("\nPRODUCT_NAME in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME());


            Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_NAME(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_NAME());


            //verify PRODUCT_SHORT_NAME
            System.out.println("\"Expecting PRODUCT_SHORT_NAME in EPH Staging and EPH SA are consistent for " + type);

            System.out.println("\nPRODUCT_SHORT_NAME in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME());
            System.out.println("\nPRODUCT_SHORT_NAME in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME());


            Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_SHORT_NAME(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getPRODUCT_SHORT_NAME());


            //verify SEPARATELY_SALEABLE_IND
            System.out.println("\"Expecting SEPARATELY_SALEABLE_IND in EPH Staging and EPH SA are consistent for " + type);

            System.out.println("\nSEPARATELY_SALEABLE_IND in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSEPARATELY_SALEABLE_IND());
            System.out.println("\nSEPARATELY_SALEABLE_IND in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND());


            Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getSEPARATELY_SALEABLE_IND(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getSEPARATELY_SALEABLE_IND());


            //verify TRIAL_ALLOWED_IND
            System.out.println("\"Expecting TRIAL_ALLOWED_IND in EPH Staging and EPH SA are consistent for " + type);

            System.out.println("\nTRIAL_ALLOWED_IND in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND());
            System.out.println("\nTRIAL_ALLOWED_IND in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getTRIAL_ALLOWED_IND());


            Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getTRIAL_ALLOWED_IND(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getTRIAL_ALLOWED_IND());

            //verify FIRST_PUB_DATE
            System.out.println("\nExpecting FIRST_PUB_DATE in EPH Staging And EPH SA are consistent for " + type);


            if (dataQualityContext.productDataObjectsFromEPHSTG.get(i).getFIRST_PUB_DATE() != null)
                System.out.println("\nFIRST_PUB_DATE in EPH STG : " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getFIRST_PUB_DATE());

            System.out.println("\nFIRST_PUB_DATE in EPH SA : " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE());

            Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getFIRST_PUB_DATE(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getFIRST_PUB_DATE());


            //verify F_TYPE
            System.out.println("\"Expecting F_TYPE in EPH Staging and EPH SA are consistent for " + type);

            System.out.println("\nF_TYPE in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_TYPE());
            System.out.println("\nF_TYPE in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE());


            Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_TYPE(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_TYPE());

            //verify F_STATUS
            System.out.println("\"Expecting F_STATUS in EPH Staging and EPH SA are consistent for " + type);

            System.out.println("\nF_STATUS in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_STATUS());
            System.out.println("\nF_STATUS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS());


            Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_STATUS(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS());

            //verify F_STATUS
            System.out.println("\"Expecting F_STATUS in EPH Staging and EPH SA are consistent for " + type);

            System.out.println("\nF_STATUS in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_STATUS());
            System.out.println("\nF_STATUS in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS());


            Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_STATUS(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_STATUS());

            //verify F_REVENUE_MODEL
            System.out.println("\"Expecting F_REVENUE_MODEL in EPH Staging and EPH SA are consistent for " + type);

            System.out.println("\nF_TYPE in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_TYPE());
            System.out.println("\nF_REVENUE_MODEL in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL());


            Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_TYPE(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_REVENUE_MODEL());

            //verify F_PRODUCT_WORK
            System.out.println("\"Expecting F_PRODUCT_WORK in EPH Staging and EPH SA are consistent for " + type);

            System.out.println("\nF_PRODUCT_WORK in EPH STG: " + dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK());
            System.out.println("\nF_PRODUCT_WORK in EPH Staging: " + dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_WORK());


            Objects.equals(dataQualityContext.productDataObjectsFromEPHSTG.get(i).getF_PRODUCT_WORK(),
                    dataQualityContext.productDataObjectsFromEPHSA.get(i).getF_PRODUCT_WORK());
        });
    }

    @And("^Compare the records in EPH STG and EPH SA for printed journals(.*)$")
    public void compareProductsDataBetweenSTGAndSAPrintJournals(String type) {

        //assert count of records in SA
        assertTrue(dataQualityContext.productDataObjectsFromEPHSTG.size() * 3 == dataQualityContext.productDataObjectsFromEPHSA.size());

        for (int i = 0; i < dataQualityContext.productDataObjectsFromEPHSA.size(); i++) {
            dataQualityContext.productDataObjectsFromEPHSTG.get(i).getPRODUCT_ID();
        }
    }
    }