package com.eph.automation.testing.web.steps.PROMISDataLake;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.PROMISDataLakeSQL.PRMtoDataLakeTableCountChecksSQL;
import cucumber.api.java.en.*;
import org.junit.*;

import java.math.BigDecimal;
import java.util.*;

public class PRMtoDatalakeTableCountChecksSteps {
    private static String sqlDL;
    private static String sqlPRM;
    private static int PRM_OracleCount;
    private static int PRM_DLCount;

    @Given("^We know the number of PRM (.*) data in Oracle")
    public void getPRMoracleCount(String tableName) {
        switch (tableName){
            case "PRMAUTPUBT":
                sqlPRM = PRMtoDataLakeTableCountChecksSQL.GET_PRMAUTPUBT_COUNT_ORACLE;
                break;

        }
        Log.info(sqlPRM);
        List<Map<String, Object>> PRMTableCount = DBManager.getDBResultMap(sqlPRM, Constants.PROMIS_URL);
        PRM_OracleCount = ((BigDecimal) PRMTableCount.get(0).get("Total_Count")).intValue();
        Log.info(tableName + " table in PRM Oracle has the Count: " + PRM_OracleCount);
    }

    @Then("^Get the PRM (.*) data is in the DL$")
    public void getPRMDLCount(String tableName) {
        switch (tableName){
            case "PRMAUTPUBT":
                sqlDL = PRMtoDataLakeTableCountChecksSQL.GET_PRMAUTPUBT_COUNT_DL;
                break;
        }
        Log.info(sqlDL);
        List<Map<String, Object>> DLCount = DBManager.getDLResultMap(sqlDL,Constants.AWS_URL);
        PRM_DLCount = ((Long) DLCount.get(0).get("Total_count")).intValue();
        Log.info(tableName + " table PRM DL has the count: " + PRM_DLCount);
    }

    @And("^Compare the PRM count for (.*) table between Oracle and DL are equal$")
    public void PRMcomparetoDL(String tableName) {
        Log.info("The count for " + tableName + " table in PRM Oracle: " + PRM_OracleCount + " and DL: " + PRM_DLCount);
        Assert.assertEquals("The counts between " + tableName + " is not equal", PRM_OracleCount, PRM_DLCount);

    }
}
