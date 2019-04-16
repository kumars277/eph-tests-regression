package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.AccountableProductDataObject;
import com.eph.automation.testing.services.db.sql.AccountableProductSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Bistra Drazheva on 03/04/2019
 */
public class AccountableProductDataQualityCheckSteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    private String sql;
    private static int countAccountableProductsPMX;
    private static int countAccountableProductsEPHSTG;
    private static int countAccountableProductsEPHSA;
    private static int countAccountableProductsEPHGD;
    private static List<String> ids;
    private static List<String> idsPMX;
    private static List<String> idsSourceRef;
    private static List<String> idsSA;

    @Given("^We get the count of accountable product data from PMX$")
    public void getCountAccountableProductsPMX() {
        Log.info("When We get the count of accountable product data in PMX ..");
        sql = AccountableProductSQL.SELECT_COUNT_ACCOUNTABLE_PRODUCT_PMX;
        Log.info(sql);
        List<Map<String, Object>> accountableProductsNumber = DBManager.getDBResultMap(sql, Constants.PMX_URL);
        countAccountableProductsPMX = ((BigDecimal) accountableProductsNumber.get(0).get("count")).intValue();
        Log.info("Count of accountable product data in PMX is: " + countAccountableProductsPMX);
    }

    @When("^We get the count of accountable product data from EPH STG$")
    public void getCountAccountableProductsEPHSTGFromPMX() {
        Log.info("When We get the count of accountable product data in EPH STG ..");
        sql = AccountableProductSQL.SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_FROM_PMX;
        Log.info(sql);
        List<Map<String, Object>> accountableProductsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countAccountableProductsEPHSTG = ((Long) accountableProductsNumber.get(0).get("count")).intValue();
        Log.info("Count of accountable product data in EPH STG is: " + countAccountableProductsEPHSTG);
    }

    @When("^We get the count of accountable product data from EPH STG processed to SA$")
    public void getCountAccountableProductsEPHSTG() {
        Log.info("When We get the count of accountable product data in EPH STG ..");
        sql = AccountableProductSQL.SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_THAT_WILL_BE_PROCESSED_TO_SA;
        Log.info(sql);
        List<Map<String, Object>> accountableProductsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countAccountableProductsEPHSTG = ((Long) accountableProductsNumber.get(0).get("count")).intValue();
        Log.info("Count of accountable product data in EPH STG is: " + countAccountableProductsEPHSTG);
    }

    @When("^We check the count of accountable product data received from PMX to EPH STG$")
    public void getCountAccountableProductsEPHSTGReceivedFromPMX() {
        Log.info("When We get the count of accountable product data in EPH STG ..");
        sql = AccountableProductSQL.SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_FROM_PMX;
        Log.info(sql);
        List<Map<String, Object>> accountableProductsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countAccountableProductsEPHSTG = ((Long) accountableProductsNumber.get(0).get("count")).intValue();
        Log.info("Count of accountable product data in EPH STG is: " + countAccountableProductsEPHSTG);
    }

    @When("^We get the count of accountable product data from EPH SA$")
    public void getCountAccountableProductsEPHSA() {
        Log.info("When We get the count of accountable product data in EPH SA ..");
        sql = AccountableProductSQL.SELECT_COUNT_ACCOUNTABLE_PRODUCT_SA;
        Log.info(sql);
        List<Map<String, Object>> accountableProductsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countAccountableProductsEPHSA = ((Long) accountableProductsNumber.get(0).get("count")).intValue();
        Log.info("Count of accountable product data in EPH SA is: " + countAccountableProductsEPHSA);
    }

    @When("^We get the count of accountable product data from EPH GD$")
    public void getCountAccountableProductsEPHGD() {
        Log.info("When We get the count of accountable product data in EPH GD ..");
        sql = AccountableProductSQL.SELECT_COUNT_ACCOUNTABLE_PRODUCT_GD;
        Log.info(sql);
        List<Map<String, Object>> accountableProductsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countAccountableProductsEPHGD = ((Long) accountableProductsNumber.get(0).get("count")).intValue();
        Log.info("Count of accountable product data in EPH GD is: " + countAccountableProductsEPHGD);
    }

    @Then("^Compare the count of accountable product data in PMX and EPH STG$")
    public void compareCountAccountableProductsBetweenPMXAndEPHSTG() {
        Assert.assertEquals("\nAccountable product data count in PMX and EPH STG is not equal", countAccountableProductsPMX, countAccountableProductsEPHSTG);

    }

    @Then("^Compare the count of accountable product data in EPH STG and EPH SA$")
    public void compareCountAccountableProductsBetweenEPHSTGAndEPHSA() {
        Assert.assertEquals("\nAccountable product data count in EPH STG and EPH SA is not equal", countAccountableProductsEPHSTG, countAccountableProductsEPHSA);

    }

    @Then("^Compare the count of accountable product data in EPH SA and EPH GD$")
    public void compareCountAccountableProductsBetweenEPHSAAndEPHGD() {
        Assert.assertEquals("\nAccountable product data count in EPH SA and EPH GD is not equal", countAccountableProductsEPHSA, countAccountableProductsEPHGD);
    }

    @Given("^We get (.*) random ids of accountable product$")
    public void getRandomAccountableProductsIds(String numberOfRecords) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);

        Log.info("Get the product work ids for given random ids from Staging ..");

        sql = String.format(AccountableProductSQL.GET_RANDOM_WORK_IDS_FROM_STG, numberOfRecords);
        Log.info(sql);

        List<Map<?, ?>> productWorkIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        idsPMX = productWorkIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info(idsPMX.toString());

        Log.info("Get the ids from stg ...");
        sql = String.format(AccountableProductSQL.SELECT_IDS_STG, Joiner.on("','").join(idsPMX));
        Log.info(sql);

        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomIds.stream().map(m -> (String) m.get("ACC_PROD_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());

    }


    @When("^We get the accountable product data from PMX$")
    public void getAccountableProductsDataPMX() {



        Log.info("Get the accountable product data from PMX  ..");
        sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_PMX, Joiner.on("','").join(idsPMX));
        Log.info(sql);

        dataQualityContext.accountableProductDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.PMX_URL);
    }


    @Then("^We get the accountable product data from EPH STG$")
    public void getAccountableProductsDataEPHSTG() {
        Log.info("Get the accountable product data from EPH STG  ..");
        sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_STG, Joiner.on("','").join(idsPMX));
        Log.info(sql);

        dataQualityContext.accountableProductDataObjectsFromSTG = DBManager
                .getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);
        Log.info(sql);
    }

    @Then("^We get the accountable product data from EPH SA$")
    public void getAccountableProductsDataEPHSA() {
        Log.info("Get the accountable product ids from the lookup table  ..");
        idsSourceRef = new ArrayList<>(ids);
        IntStream.range(0, idsSourceRef.size()).forEach(i -> idsSourceRef.set(i, idsSourceRef.get(i).concat(dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD())));


        sql = String.format(AccountableProductSQL.GET_NUMERIC_ID_FROM_LOOKUP_AP, Joiner.on("','").join(idsSourceRef));
        Log.info(sql);
        List<Map<?, ?>> lookupResults = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        idsSA = lookupResults.stream().map(m -> (BigDecimal) m.get("NUMERIC_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("numeric ids :" + idsSA.toString());

        Log.info("Get the accountable product data from EPH SA  ..");
        sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_SA, Joiner.on("','").join(idsSA));
        Log.info(sql);

        dataQualityContext.accountableProductDataObjectsFromSA = DBManager
                .getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);
    }

    @Then("^We get the accountable product data from EPH GD$")
    public void getAccountableProductsDataEPHGD() {
        Log.info("Get the accountable product data from EPH SA  ..");
        sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_GD, Joiner.on("','").join(idsSA));
        Log.info(sql);

        dataQualityContext.accountableProductDataObjectsFromGD = DBManager
                .getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);
    }

    @And("^Compare the accountable product data in PMX and EPH STG$")
    public void compareAccountableProductsDataPMXAndSTG() {
        Log.info("And the accountable product data in PMX and EPH STG ..");
        dataQualityContext.accountableProductDataObjectsFromPMX.sort(Comparator.comparing(AccountableProductDataObject::getPRODUCT_WORK_ID));
        dataQualityContext.accountableProductDataObjectsFromSTG.sort(Comparator.comparing(AccountableProductDataObject::getPRODUCT_WORK_ID));


        IntStream.range(0, dataQualityContext.accountableProductDataObjectsFromSTG.size()).forEach(i -> {

            //PRODUCT_WORK_ID
            Log.info("PRODUCT_WORK_ID in PMX: " + dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_WORK_ID());
            Log.info("PRODUCT_WORK_ID in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPRODUCT_WORK_ID());

            assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_WORK_ID(),dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPRODUCT_WORK_ID());

            //ACC_PROD_ID
            Log.info("ACC_PROD_ID in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_ID());

            if(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_GROUP_TYPE_NAME().equals("Journals"))
                assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getWORK_ELS_PROD_ID(),dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_ID());
            else
                assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID(),dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_ID());


            //ACC_PROD_NAME
            Log.info("ACC_PROD_NAME in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_NAME());

            if(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_GROUP_TYPE_NAME().equals("Journals"))
                assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_WORK_TITLE(),dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_NAME());
            else
                assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getACC_PROD_NAME(),dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_NAME());


            //PARENT_ACC_PROD
            Log.info("PARENT_ACC_PROD in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());

            if(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_GROUP_TYPE_NAME().equals("Journals"))
                assertEquals(dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD(),dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getACC_PROD_HIERARHY());
            else if(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID().equals("1000000"))
                assertEquals("PBKSOTH",dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
            else if(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID().equals("1000001"))
                assertEquals("PBKSTEX",dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
            else if(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID().equals("1000002"))
                assertEquals("PBKSSER",dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
            else if(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID().equals("1000003"))
                assertEquals("PBKSMRW",dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
            else if(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID().equals("1000004"))
                assertEquals("PBKSREF",dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
            else
                assertTrue(dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD() == null);


            //PRODUCT_GROUP_TYPE_NAME
            Log.info("PRODUCT_GROUP_TYPE_NAME in PMX: " + dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_GROUP_TYPE_NAME());
            Log.info("PRODUCT_GROUP_TYPE_NAME in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPRODUCT_GROUP_TYPE_NAME());

            assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_WORK_ID(),dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPRODUCT_WORK_ID());

        });


    }

    @And("^Compare the accountable product data in EPH STG and EPH SA$")
    public void compareAccountableProductsDataSTGAndSA() {
        Log.info("And the accountable product data in STG and SA ..");
        dataQualityContext.accountableProductDataObjectsFromSTG.sort(Comparator.comparing(AccountableProductDataObject::getPRODUCT_WORK_ID));
        dataQualityContext.accountableProductDataObjectsFromSA.sort(Comparator.comparing(AccountableProductDataObject::getPRODUCT_WORK_ID));


        IntStream.range(0, dataQualityContext.accountableProductDataObjectsFromSTG.size()).forEach(i -> {
            Log.info("Get the accountable product ids from the lookup table  ..");
            String idsSourceRef = dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_ID().concat(dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
            sql = String.format(AccountableProductSQL.GET_NUMERIC_ID_FROM_LOOKUP_AP, idsSourceRef);
            Log.info(sql);
            List<Map<?, ?>> lookupResults = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            idsSA = lookupResults.stream().map(m -> (BigDecimal) m.get("NUMERIC_ID")).map(String::valueOf).collect(Collectors.toList());
            Log.info("numeric ids :" + idsSA.toString());

            Log.info("Get the accountable product data from EPH SA  ..");
            sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_SA, Joiner.on("','").join(idsSA));
            Log.info(sql);

            dataQualityContext.accountableProductDataObjectsFromSA = DBManager
                    .getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);

            //B_CLASSNAME
            Log.info("B_CLASSNAME in EPH: " + dataQualityContext.accountableProductDataObjectsFromSA.get(0).getB_CLASSNAME());

            assertEquals("AccountableProduct",dataQualityContext.accountableProductDataObjectsFromSA.get(0).getB_CLASSNAME());

            //GL_PRODUCT_SEGMENT_CODE
            Log.info("GL_PRODUCT_SEGMENT_CODE in EPH SА: " + dataQualityContext.accountableProductDataObjectsFromSA.get(0).getGL_PRODUCT_SEGMENT_CODE());

                assertEquals(dataQualityContext.accountableProductDataObjectsFromSA.get(0).getGL_PRODUCT_SEGMENT_CODE(),dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_ID());


            //GL_PRODUCT_SEGMENT_NAME
            Log.info("GL_PRODUCT_SEGMENT_NAME in EPH SA: " + dataQualityContext.accountableProductDataObjectsFromSA.get(0).getGL_PRODUCT_SEGMENT_NAME());

                assertEquals(dataQualityContext.accountableProductDataObjectsFromSA.get(0).getGL_PRODUCT_SEGMENT_NAME(),dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_NAME());


            //F_GL_PRODUCT_SEGMENT_PARENT
            Log.info("F_GL_PRODUCT_SEGMENT_PARENT in EPH SA: " + dataQualityContext.accountableProductDataObjectsFromSA.get(0).getF_GL_PRODUCT_SEGMENT_PARENT());

                assertEquals(dataQualityContext.accountableProductDataObjectsFromSA.get(0).getF_GL_PRODUCT_SEGMENT_PARENT(),dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());

        });
    }


    @And("^Compare the accountable product data in EPH SA and EPH GD$")
    public void compareAccountableProductsDataSAAndGD() {
        Log.info("And the accountable product data in SA and EPH GD ..");
        dataQualityContext.accountableProductDataObjectsFromSA.sort(Comparator.comparing(AccountableProductDataObject::getACCOUNTABLE_PRODUCT_ID));
        dataQualityContext.accountableProductDataObjectsFromGD.sort(Comparator.comparing(AccountableProductDataObject::getACCOUNTABLE_PRODUCT_ID));


        IntStream.range(0, dataQualityContext.accountableProductDataObjectsFromSA.size()).forEach(i -> {

            //B_CLASSNAME
            Log.info("B_CLASSNAME in EPH SA: " + dataQualityContext.accountableProductDataObjectsFromSA.get(i).getB_CLASSNAME());
            Log.info("B_CLASSNAME in EPH GD: " + dataQualityContext.accountableProductDataObjectsFromGD.get(i).getB_CLASSNAME());

            assertEquals(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getB_CLASSNAME(), dataQualityContext.accountableProductDataObjectsFromGD.get(i).getB_CLASSNAME());

            //GL_PRODUCT_SEGMENT_CODE
            Log.info("GL_PRODUCT_SEGMENT_CODE in EPH SA: " + dataQualityContext.accountableProductDataObjectsFromSA.get(i).getGL_PRODUCT_SEGMENT_CODE());
            Log.info("GL_PRODUCT_SEGMENT_CODE in EPH GD: " + dataQualityContext.accountableProductDataObjectsFromGD.get(i).getGL_PRODUCT_SEGMENT_CODE());

            assertEquals(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getGL_PRODUCT_SEGMENT_CODE(), dataQualityContext.accountableProductDataObjectsFromGD.get(i).getGL_PRODUCT_SEGMENT_CODE());


            //GL_PRODUCT_SEGMENT_NAME
            Log.info("GL_PRODUCT_SEGMENT_NAME in EPH SA: " + dataQualityContext.accountableProductDataObjectsFromSA.get(i).getGL_PRODUCT_SEGMENT_NAME());
            Log.info("GL_PRODUCT_SEGMENT_NAME in EPH GD: " + dataQualityContext.accountableProductDataObjectsFromGD.get(i).getGL_PRODUCT_SEGMENT_NAME());

            assertEquals(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getGL_PRODUCT_SEGMENT_NAME(), dataQualityContext.accountableProductDataObjectsFromGD.get(i).getGL_PRODUCT_SEGMENT_NAME());

            //F_GL_PRODUCT_SEGMENT_PARENT
            Log.info("F_GL_PRODUCT_SEGMENT_PARENT in EPH SA: " + dataQualityContext.accountableProductDataObjectsFromSA.get(i).getF_GL_PRODUCT_SEGMENT_PARENT());
            Log.info("F_GL_PRODUCT_SEGMENT_PARENT in EPH GD: " + dataQualityContext.accountableProductDataObjectsFromGD.get(i).getF_GL_PRODUCT_SEGMENT_PARENT());

            assertEquals(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getF_GL_PRODUCT_SEGMENT_PARENT(), dataQualityContext.accountableProductDataObjectsFromGD.get(i).getF_GL_PRODUCT_SEGMENT_PARENT());

        });

    }


    @And("^Check the mandatory columns are populated for accountable products role$")
    public void checkMandatoryColumnsForAccountableProductsRoleInSAArePopulated() {
        Log.info("We check that mandatory columns are populated ...");

        IntStream.range(0, dataQualityContext.accountableProductDataObjectsFromSA.size()).forEach(i -> {
            //verify F_EVENT is not null
            assertNotNull(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getF_EVENT());
            //verify ACCOUNTABLE_PRODUCT_ID is not null
            assertNotNull(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getACCOUNTABLE_PRODUCT_ID());
            //verify GL_PRODUCT_SEGMENT_CODE
            assertNotNull(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getGL_PRODUCT_SEGMENT_CODE());
            //verify GL_PRODUCT_SEGMENT_NAME
            assertNotNull(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getGL_PRODUCT_SEGMENT_NAME());
            //verify F_GL_PRODUCT_SEGMENT_PARENT
            assertNotNull(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getF_GL_PRODUCT_SEGMENT_PARENT());


        });
    }



}
