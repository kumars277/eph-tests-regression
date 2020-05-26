package com.eph.automation.testing.web.steps.JRBIDataLakeAccess;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIDataLakeCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class JRBICountChecksSteps {
    private static String JRBISQLSourceCount;
    private static int JRBISourceCount;
    private static String JRBICurrentSQLCount;
    private static int JRBICurrentCount;

    @Given("^We know the total count of JRBI data from Source table (.*)$")
    public void getJRBISourceCount(String tableName) {
        switch (tableName){
            case "jrbi_transform_current_work":
                Log.info("Getting Source Table Count for Work...");
                JRBISQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_SOURCE_COUNT;
                break;
            case "jrbi_transform_current_manifestation":
                Log.info("Getting Source Table Count for Manif...");
                JRBISQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_SOURCE_COUNT;
                break;
            case "jrbi_transform_current_person":
                Log.info("Getting Source Table Count for Person...");
                JRBISQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PERSON_SOURCE_COUNT;
                break;
        }
        Log.info(JRBISQLSourceCount);
        List<Map<String, Object>> JRBISourceTableCount = DBManager.getDBResultMap(JRBISQLSourceCount, Constants.AWS_URL);
        JRBISourceCount = ((Long) JRBISourceTableCount.get(0).get("Source_Count")).intValue();
        Log.info("Source table count of JRBI Access has the Count: " + JRBISourceCount);
    }

    @Then("^Get the JRBI (.*) work data count in the DL$")
    public void getJRBICurrentTableCount(String tableName){
        switch (tableName){
            case "jrbi_transform_current_work":
                 Log.info("Getting Current Work Table Count...");
                 JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_WORK_COUNT;
                break;

            case "jrbi_transform_current_manifestation":
                Log.info("Getting Current Work Table Count...");
                JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_MANIF_COUNT;
                break;

            case "jrbi_transform_current_person":
                Log.info("Getting Current Person Table Count...");
                JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_PERSON_COUNT;
                break;
        }
        Log.info(JRBICurrentSQLCount);
        List<Map<String, Object>> JRBICurrentTableCount = DBManager.getDBResultMap(JRBICurrentSQLCount, Constants.AWS_URL);
        JRBICurrentCount = ((Long) JRBICurrentTableCount.get(0).get("Current_Count")).intValue();
        Log.info(tableName+" of JRBI Access has the Count: " + JRBICurrentCount);
    }
    @And("^Compare count (.*) table and JRBI Source table are identical$")
    public void compareCurrentandSourceCount(String tableName){
        Log.info("The count for table " + tableName + " in Source: " + JRBISourceCount + " and in transform_Current: " + JRBICurrentCount);
        Assert.assertEquals("The counts are not equal when compared with source and "+tableName, JRBICurrentCount, JRBISourceCount);

    }


}
