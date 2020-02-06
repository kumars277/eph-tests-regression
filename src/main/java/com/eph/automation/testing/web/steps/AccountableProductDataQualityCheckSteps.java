package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.AccountableProductDataObject;
import com.eph.automation.testing.services.db.sql.AccountableProductSQL;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Created by Bistra Drazheva on 03/04/2019
 */
public class AccountableProductDataQualityCheckSteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    private String sql;
    private static int countAccountableProductsPMX;
    private static int countAccountableProductsEPHSTG;
    private static int countAccountableProductsEPHDQ;
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


    @When("^We get the count of accountable product data from EPH STG going to DQ$")
    public void getCountAccountableProductsEPHSTGGoingToDQ() {
        Log.info("When We get the count of accountable product data in EPH STG going to DQ..");
            if (System.getProperty("LOAD") == null || System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                sql = AccountableProductSQL.SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_GOING_TO_DQ;
                Log.info(sql);
            } else {
        sql = WorkCountSQL.GET_REFRESH_DATE;
        Log.info(sql);
        List<Map<String, Object>> refreshDateNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String refreshDate = (String) refreshDateNumber.get(1).get("refresh_timestamp");
        Log.info("refreshDate : " + refreshDate);

        sql = String.format(AccountableProductSQL.SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_GOING_TO_DQ_DELTA, refreshDate );
        Log.info(sql);

            }

        List<Map<String, Object>> accountableProductsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countAccountableProductsEPHSTG = ((Long) accountableProductsNumber.get(0).get("count")).intValue();
        Log.info("Count of accountable product data in EPH STG is: " + countAccountableProductsEPHSTG);

//        Log.info("Assert count of records is not null");
//
//        assertTrue("No data found in EPH STG for product relationships", countAccountableProductsEPHSTG != 0);
    }


    @When("^We get the count of accountable product data from EPH DQ$")
    public void getCountAccountableProductsEPHDQ() {
        Log.info("When We get the count of accountable product data in EPH DQ ..");
        sql = AccountableProductSQL.SELECT_COUNT_ACCOUNTABLE_PRODUCT_DQ;
        Log.info(sql);
        List<Map<String, Object>> accountableProductsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countAccountableProductsEPHDQ = ((Long) accountableProductsNumber.get(0).get("count")).intValue();
        Log.info("Count of accountable product data in EPH DQ is: " + countAccountableProductsEPHDQ);
    }



    @When("^We get the count of accountable product data from EPH DQ processed to SA$")
    public void getCountAccountableProductsEPHSTG() {
        Log.info("When We get the count of accountable product data in EPH DQ going to SA ..");
        sql = AccountableProductSQL.SELECT_COUNT_ACCOUNTABLE_PRODUCT_DQ_GOING_TO_SA;
        Log.info(sql);

        List<Map<String, Object>> accountableProductsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countAccountableProductsEPHDQ = ((Long) accountableProductsNumber.get(0).get("count")).intValue();
        Log.info("Count of accountable product data in EPH DQ going to SA is: " + countAccountableProductsEPHDQ);
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

    @Then("^Compare the count of accountable product data in EPH STG and EPH DQ$")
    public void compareCountAccountableProductsBetweenEPHSTGAndEPHDQ() {
        Assert.assertEquals("\nAccountable product data count in EPH STG and EPH DQ is not equal", countAccountableProductsEPHSTG, countAccountableProductsEPHDQ);

    }

    @Then("^Compare the count of accountable product data in EPH DQ and EPH SA$")
    public void compareCountAccountableProductsBetweenEPHDQAndEPHSA() {
        Assert.assertEquals("\nAccountable product data count in EPH DQ and EPH SA is not equal", countAccountableProductsEPHDQ, countAccountableProductsEPHSA);

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

        sql = String.format(AccountableProductSQL.GET_RANDOM_IDS_STG, numberOfRecords);
        Log.info(sql);

        List<Map<?, ?>> productWorkIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        idsPMX = productWorkIds.stream().map(m ->  m.get("PRODUCT_WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Product work ids : " + idsPMX.toString());

        ids = productWorkIds.stream().map(m ->  m.get("PMX_SOURCE_REFERENCE")).map(String::valueOf).collect(Collectors.toList());
        Log.info("External ref : "  + ids.toString());


    }

    @Given("^We get (.*) random pmx source ref ids of accountable product$")
    public void getRandomAccountableProductsPMXSourceRef(String numberOfRecords) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);

        Log.info("Get the product work ids for given random ids from Staging ..");

        sql = String.format(AccountableProductSQL.GET_RANDOM_PMX_SOURCE_REF_IDS_STG, numberOfRecords);
        Log.info(sql);

        List<Map<?, ?>> productWorkIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = productWorkIds.stream().map(m ->  m.get("PMX_SOURCE_REFERENCE")).map(String::valueOf).collect(Collectors.toList());
        Log.info("External ref : "  + ids.toString());


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
        if(ids.isEmpty())
            sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_STG, '0');
        else
        sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_STG, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.accountableProductDataObjectsFromSTG = DBManager
                .getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);
        Log.info(sql);
    }

    @Then("^We get the accountable product data from EPH STG coming from PMX$")
    public void getAccountableProductsDataEPHSTGComingFromPMX() {
        Log.info("Get the accountable product data from EPH STG  ..");
        if(ids.isEmpty())
            sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_STG_PMX, '0');
        else
        sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_STG_PMX, Joiner.on("','").join(idsPMX));
        Log.info(sql);

        dataQualityContext.accountableProductDataObjectsFromSTG = DBManager
                .getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);
        Log.info(sql);
    }

    @Then("^We get the accountable product data from EPH DQ$")
    public void getAccountableProductsDataEPHDQ() {
        Log.info("Get the accountable product data from EPH STG  ..");
        sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_DQ, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.accountableProductDataObjectsFromSTGDQ = DBManager
                .getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);
        Log.info(sql);
    }

    @Then("^We get the accountable product data from EPH SA$")
    public void getAccountableProductsDataEPHSA() {
        Log.info("Get the accountable product data from EPH SA  ..");
        sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_SA, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.accountableProductDataObjectsFromSA = DBManager
                .getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);
    }

    @Then("^We get the accountable product data from EPH GD$")
    public void getAccountableProductsDataEPHGD() {
        Log.info("Get the accountable product data from EPH GD  ..");
        sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_GD, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.accountableProductDataObjectsFromGD = DBManager
                .getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);
    }

    @And("^Compare the accountable product data in PMX and EPH STG$")
    public void compareAccountableProductsDataPMXAndSTG() {
        Log.info("And the accountable product data in PMX and EPH STG ..");
        if ( CollectionUtils.isEmpty(dataQualityContext.accountableProductDataObjectsFromSTG) && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Accountable Products");
        } else {
            dataQualityContext.accountableProductDataObjectsFromPMX.sort(Comparator.comparing(AccountableProductDataObject::getPRODUCT_WORK_ID));
            dataQualityContext.accountableProductDataObjectsFromSTG.sort(Comparator.comparing(AccountableProductDataObject::getPRODUCT_WORK_ID));

            IntStream.range(0, dataQualityContext.accountableProductDataObjectsFromSTG.size()).forEach(i -> {

                //PRODUCT_WORK_ID
                Log.info("PRODUCT_WORK_ID in PMX: " + dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_WORK_ID());
                Log.info("PRODUCT_WORK_ID in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPRODUCT_WORK_ID());

                assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_WORK_ID(), dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPRODUCT_WORK_ID());

                //ACC_PROD_ID
                Log.info("ACC_PROD_ID in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_ID());

                if (dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_GROUP_TYPE_NAME().equals("Journals"))
                    assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getWORK_ELS_PROD_ID(), dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_ID());
                else
                    assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID(), dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_ID());


                //ACC_PROD_NAME
                Log.info("ACC_PROD_NAME in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_NAME());


                if (dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_GROUP_TYPE_NAME().equals("Journals"))
                    assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_WORK_TITLE(), dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_NAME());
                else
                    assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getACC_PROD_NAME(), dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_NAME());


                //PARENT_ACC_PROD
                Log.info("PARENT_ACC_PROD in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());

                if (dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_GROUP_TYPE_NAME().equals("Journals"))
                    assertEquals(dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD(), dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getACC_PROD_HIERARHY());
                else if (dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID().equals("1000000"))
                    assertEquals("PBKSOTH", dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
                else if (dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID().equals("1000001"))
                    assertEquals("PBKSTEX", dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
                else if (dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID().equals("1000002"))
                    assertEquals("PBKSSER", dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
                else if (dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID().equals("1000003"))
                    assertEquals("PBKSMRW", dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
                else if (dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getAC_ELS_PROD_ID().equals("1000004"))
                    assertEquals("PBKSREF", dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
                else
                    assertTrue(dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD() == null);


                //PRODUCT_GROUP_TYPE_NAME
                Log.info("PRODUCT_GROUP_TYPE_NAME in PMX: " + dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_GROUP_TYPE_NAME());
                Log.info("PRODUCT_GROUP_TYPE_NAME in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPRODUCT_GROUP_TYPE_NAME());

                assertEquals(dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_WORK_ID(), dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPRODUCT_WORK_ID());


                //UPDATED
                Log.info("UPDATED in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getUPDATED());


                sql = String.format(AccountableProductSQL.SELECT_UPDATED_VALUE, dataQualityContext.accountableProductDataObjectsFromPMX.get(i).getPRODUCT_WORK_ID());
                List<Map<String, Object>> updatedNumber = DBManager.getDBResultMap(sql, Constants.PMX_URL);
                String updated = (String) updatedNumber.get(0).get("UPDATED");

                assertEquals(updated, dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getUPDATED());

            });
        }
    }

    @And("^Compare the accountable product data in EPH STG and EPH DQ$")
    public void compareAccountableProductsDataSTGAndDQ() {
        Log.info("And the accountable product data in STG and DQ ..");


        if ( CollectionUtils.isEmpty(dataQualityContext.accountableProductDataObjectsFromSTGDQ) && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Accountable products in DQ");
        } else {
            dataQualityContext.accountableProductDataObjectsFromSTG.sort(Comparator.comparing(AccountableProductDataObject::getPRODUCT_WORK_ID));
            dataQualityContext.accountableProductDataObjectsFromSTGDQ.sort(Comparator.comparing(AccountableProductDataObject::getPRODUCT_WORK_ID));

            IntStream.range(0, dataQualityContext.accountableProductDataObjectsFromSTG.size()).forEach(i -> {

                //   Log.info("Get the dq data  ..");
                //    String pmxSourceRef = dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_ID().concat(dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());

                //   sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_DQ, Joiner.on("','").join(idsPMX));

                // sql = String.format(AccountableProductSQL.SELECT_DATA_ACCOUNTABLE_PRODUCT_DQ, pmxSourceRef);

                //    Log.info(sql);

                //  dataQualityContext.accountableProductDataObjectsFromSTGDQ = DBManager
                //         .getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);

                //ACC_PROD_ID
                Log.info("ACC_PROD_ID in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_ID());
                Log.info("ACC_PROD_ID in EPH DQ: " + dataQualityContext.accountableProductDataObjectsFromSTGDQ.get(i).getACC_PROD_ID());

                assertEquals(dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_ID(), dataQualityContext.accountableProductDataObjectsFromSTGDQ.get(i).getACC_PROD_ID());


                //ACC_PROD_NAME
                Log.info("ACC_PROD_NAME in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_NAME());
                Log.info("ACC_PROD_NAME in EPH DQ: " + dataQualityContext.accountableProductDataObjectsFromSTGDQ.get(i).getACC_PROD_NAME());

                assertEquals(dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getACC_PROD_NAME(), dataQualityContext.accountableProductDataObjectsFromSTGDQ.get(i).getACC_PROD_NAME());


                //PARENT_ACC_PROD
                Log.info("PARENT_ACC_PROD in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD());
                Log.info("PARENT_ACC_PROD in EPH DQ: " + dataQualityContext.accountableProductDataObjectsFromSTGDQ.get(i).getPARENT_ACC_PROD());

                assertEquals(dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getPARENT_ACC_PROD(), dataQualityContext.accountableProductDataObjectsFromSTGDQ.get(i).getPARENT_ACC_PROD());

                //DQ_ERR
                Log.info("DQ_ERR in EPH STG: " + dataQualityContext.accountableProductDataObjectsFromSTG.get(i).getDQ_ERR());

                assertEquals(Character.toString('N'), dataQualityContext.accountableProductDataObjectsFromSTGDQ.get(i).getDQ_ERR());

            });
        }
    }


    @And("^Compare the accountable product data in EPH DQ and EPH SA$")
    public void compareAccountableProductsDataDQAndSA() {
        Log.info("And the accountable product data in DQ and SA ..");


        if ( CollectionUtils.isEmpty(dataQualityContext.accountableProductDataObjectsFromSA) && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Accountable products in SA");
        } else {
            dataQualityContext.accountableProductDataObjectsFromSTGDQ.sort(Comparator.comparing(AccountableProductDataObject::getPMX_SOURCE_REFERENCE));
            dataQualityContext.accountableProductDataObjectsFromSA.sort(Comparator.comparing(AccountableProductDataObject::getEXTERNAL_REFERENCE));

            IntStream.range(0, dataQualityContext.accountableProductDataObjectsFromSTGDQ.size()).forEach(i -> {


                //B_CLASSNAME
                Log.info("B_CLASSNAME in EPH: " + dataQualityContext.accountableProductDataObjectsFromSA.get(i).getB_CLASSNAME());

                assertEquals("AccountableProduct", dataQualityContext.accountableProductDataObjectsFromSA.get(i).getB_CLASSNAME());


                //GL_PRODUCT_SEGMENT_CODE
                Log.info("GL_PRODUCT_SEGMENT_CODE in EPH SA: " + dataQualityContext.accountableProductDataObjectsFromSA.get(i).getGL_PRODUCT_SEGMENT_CODE());

                assertEquals(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getGL_PRODUCT_SEGMENT_CODE(), dataQualityContext.accountableProductDataObjectsFromSTGDQ.get(i).getACC_PROD_ID());


                //GL_PRODUCT_SEGMENT_NAME
                Log.info("GL_PRODUCT_SEGMENT_NAME in EPH SA: " + dataQualityContext.accountableProductDataObjectsFromSA.get(i).getGL_PRODUCT_SEGMENT_NAME());

                assertEquals(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getGL_PRODUCT_SEGMENT_NAME(), dataQualityContext.accountableProductDataObjectsFromSTGDQ.get(i).getACC_PROD_NAME());


                //F_GL_PRODUCT_SEGMENT_PARENT
                Log.info("F_GL_PRODUCT_SEGMENT_PARENT in EPH SA: " + dataQualityContext.accountableProductDataObjectsFromSA.get(i).getF_GL_PRODUCT_SEGMENT_PARENT());

                assertEquals(dataQualityContext.accountableProductDataObjectsFromSA.get(i).getF_GL_PRODUCT_SEGMENT_PARENT(), dataQualityContext.accountableProductDataObjectsFromSTGDQ.get(i).getPARENT_ACC_PROD());

            });
        }
    }


    @And("^Compare the accountable product data in EPH SA and EPH GD$")
    public void compareAccountableProductsDataSAAndGD() {
        Log.info("And the accountable product data in SA and EPH GD ..");


        if ( CollectionUtils.isEmpty(dataQualityContext.accountableProductDataObjectsFromSA) && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Accountable Product Data");
        } else {
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
    }


    @And("^Check the mandatory columns are populated for accountable products role$")
    public void checkMandatoryColumnsForAccountableProductsRoleInSAArePopulated() {
        Log.info("We check that mandatory columns are populated ...");
        if ( CollectionUtils.isEmpty(dataQualityContext.accountableProductDataObjectsFromSA) && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Accountable Product Data");
        } else {
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



}
