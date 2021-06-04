package com.eph.automation.testing.web.steps.JM;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JMETL.JMContext;
import com.eph.automation.testing.models.dao.JMDataLake.JMETLObject;
import com.eph.automation.testing.services.db.JMDataLakeSQL.JMExtendedCountCheckSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JM_ExtendedCountAndDataCheckSteps {
    private static List<String> Ids;
    String sql;
    String JMSQL;
    String PRODSQL;
    private static int JM_ExtendedCount;
    private static int ProdCount;

    @Given("We know the count of JM Extended data in JM")
    public void getJM_ExtendedCount() {
        JMSQL = String.format(JMExtendedCountCheckSQL.GET_JMExtendedCount);
        Log.info(JMSQL);
        List<Map<String,Object>> JMTableCount = DBManager.getDLResultMap(JMSQL, Constants.AWS_URL);
        JM_ExtendedCount = ((Long) JMTableCount.get(0).get("Total_Count")).intValue();
        Log.info("JM_Extended Count: " + JM_ExtendedCount);
    }

    @Then("^Get the count for Product Extended DB$")
    public void getProdExtendedCount() {
        PRODSQL= String.format(JMExtendedCountCheckSQL.GET_Product_Extended);
        Log.info(PRODSQL);
        List<Map<String,Object>> PromisCurrentTableCount = DBManager.getDLResultMap(PRODSQL,Constants.AWS_URL);
        ProdCount = ((Long) PromisCurrentTableCount.get(0).get("Total_Count")).intValue();
        Log.info("Product_Extended Count: " + ProdCount);
    }

    @And("^Compare the count for JM Extended Data between JM and Product Extended$")
    public void JMcomparetoPRODExtDB() {
        Log.info("The JM Table Count: "+JM_ExtendedCount + " and ProdExtended Count: "+ProdCount);
        Assert.assertEquals("The counts for JM and PROD_Extended is not equal", JM_ExtendedCount, ProdCount);
    }

    @Given("^We get the (.*) random JM Extended ids$")
    public void getRandomJMExtendedIds(String numberOfRecords) {
//  numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random IDs...");
        sql = String.format(JMExtendedCountCheckSQL.GET_JM_EXTENDED_IDs, numberOfRecords);
        List<Map<?, ?>> randomWorkBusinessIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomWorkBusinessIds.stream().map(m -> (String) m.get("EPR_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the JM Extended DB records$")
    public void getJMExtendedDBRecords() {
        Log.info("We get the records from JM Extended DB..");
        sql = String.format(JMExtendedCountCheckSQL.GET_JM_EXTENDED_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        JMContext.JMObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.AWS_URL);
        System.out.println(JMContext.JMObjectsFromDL.size());
    }

    @Then("^We get the ProductExtended records$")
    public void getJMInboundRecords() {
        Log.info("We get the Prod Extended records..");
        sql = String.format(JMExtendedCountCheckSQL.GET_PRODUCT_EXTENDED_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        JMContext.JMTransformObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JM records in between JM Extended DB and Product Extended DB$")
    public void compareJMExtendedtoProductExtended() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JMContext.JMObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the JM Extended and Product Extended ..");
            for (int i = 0; i < JMContext.JMObjectsFromDL.size(); i++) {
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getEPR_ID)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getEPR_ID));

                        String[] JMF_WorkColumnName = {"getEPR_ID","getPRODUCT_TYPE","getAPPLICATION_NAME","getDELTA_ANSWER_CODE_UK","getDELTA_ANSWER_CODE_US","getPUBLICATION_STATUS_ANZ","getAVAILABILITY_FORMAT"};

                        for (String strTemp : JMF_WorkColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("EPR_ID => " + JMContext.JMObjectsFromDL.get(i).getEPR_ID() +
                                    " " + strTemp + " => JM Extended=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Product Extended=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
            }
        }
    }
}
