package com.eph.automation.testing.web.steps.d_PROMIS;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.PROMISDataLakeSQL.PRMTableOutboundCountChecksSQL;
import cucumber.api.java.en.*;
import org.junit.*;
import java.math.BigDecimal;
import java.util.*;

public class PRMtoDatalakeTableCountChecksSteps {
    private static String PRMDL;
    private static String PRMOracle;
    private static int PRM_OracleCount;
    private static int PRM_DLCount;

    @Given("^We know the number of PRM (.*) data in Oracle")
    public void getPRMoracleCount(String tableName) {
        switch (tableName){
            case "PRMAUTPUBT":
                PRMOracle = PRMTableOutboundCountChecksSQL.GET_PRMAUTPUBT_COUNT_ORACLE;
                break;
            case "PRMCLSCODT":
                PRMOracle = PRMTableOutboundCountChecksSQL.GET_PRMCLSCODT_COUNT_ORACLE;
                break;

            case "PRMCLST":
                PRMOracle = PRMTableOutboundCountChecksSQL.GET_PRMCLST_COUNT_ORACLE;
                break;

            case "PRMLONDEST":
                PRMOracle = PRMTableOutboundCountChecksSQL.GET_PRMLONDEST_COUNT_ORACLE;
                break;

            case "PRMPRICEST":
                PRMOracle = PRMTableOutboundCountChecksSQL.GET_PRMPRICEST_COUNT_ORACLE;
                break;

            case "PRMPUBINFT":
                PRMOracle = PRMTableOutboundCountChecksSQL.GET_PRMPUBINFT_COUNT_ORACLE;
                break;

            case "PRMPUBRELT":
                PRMOracle = PRMTableOutboundCountChecksSQL.GET_PRMPUBRELT_COUNT_ORACLE;
                break;

        }
        Log.info(PRMOracle);
        List<Map<String, Object>> PRMTableOracleCount = DBManager.getDBResultMap(PRMOracle, Constants.PROMIS_URL);
        PRM_OracleCount = ((BigDecimal) PRMTableOracleCount.get(0).get("Total_Count")).intValue();
        Log.info(tableName + " table in PRM Oracle has the Count: " + PRM_OracleCount);
    }

    @Then("^Get the PRM (.*) data is in the DL$")
    public void getPRMDLCount(String tableName) {
        switch (tableName){
            case "PRMAUTPUBT":
                PRMDL = PRMTableOutboundCountChecksSQL.GET_PRMAUTPUBT_COUNT_DL;
                break;

            case "PRMCLSCODT":
                PRMDL = PRMTableOutboundCountChecksSQL.GET_PRMCLSCODT_COUNT_DL;
                break;

            case "PRMCLST":
                PRMDL = PRMTableOutboundCountChecksSQL.GET_PRMCLST_COUNT_DL;
                break;

            case "PRMLONDEST":
                PRMDL = PRMTableOutboundCountChecksSQL.GET_PRMLONDEST_COUNT_DL;
                break;

            case "PRMPRICEST":
                PRMDL = PRMTableOutboundCountChecksSQL.GET_PRMPRICEST_COUNT_DL;
                break;

            case "PRMPUBINFT":
                PRMDL = PRMTableOutboundCountChecksSQL.GET_PRMPUBINFT_COUNT_DL;
                break;

            case "PRMPUBRELT":
                PRMDL = PRMTableOutboundCountChecksSQL.GET_PRMPUBRELT_COUNT_DL;
                break;

        }
        Log.info(PRMDL);
        List<Map<String, Object>> PRMDLCount = DBManager.getDLResultMap(PRMDL,Constants.AWS_URL);
        PRM_DLCount = ((Long) PRMDLCount.get(0).get("Total_count")).intValue();
        Log.info(tableName + " table PRM DL has the count: " + PRM_DLCount);
    }

    @And("^Compare the PRM count for (.*) table between Oracle and DL are equal$")
    public void PRMcomparetoDL(String tableName) {
        Log.info("The count for table" + tableName + " in PRM Oracle: " + PRM_OracleCount + " and DL: " + PRM_DLCount);
        Assert.assertEquals("The counts for table " + tableName + " is not equal", PRM_OracleCount, PRM_DLCount);

    }
}
