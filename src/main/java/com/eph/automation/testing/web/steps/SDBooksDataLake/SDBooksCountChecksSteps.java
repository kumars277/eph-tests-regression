package com.eph.automation.testing.web.steps.SDBooksDataLake;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.SDBooksDataLakeSQL.SDDataLakeCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class SDBooksCountChecksSteps {
    private static String SDFullSQLSourceCount;
    private static int SDFullSourceCount;
    private static String SDCurrentSQLCount;
    private static String SDPreviousSQLCount;
    private static String SDCurrentHistSQLCount;
    private static String SDPreviousHistSQLCount;
    private static String SDDeltaCurrentSQLCount;
    private static int SDCurrentCount;
    private static int SDPreviousCount;
    private static int SDCurrentHistCount;
    private static int SDPreviousHistCount;
    private static int SDDeltaCurrentCount;
    private static int SDCurrPrevCount;
    private static String SDCurrPrevSQLCount;


    @Given("^Get the total count of SD Data from Full Load (.*)$")
    public void getSDFullLoadCount (String tableName) {
        switch (tableName){
            case "sdbooks_transform_current_urls":
                Log.info("Getting Source Table Count for URL...");
                SDFullSQLSourceCount = SDDataLakeCountChecksSQL.GET_SD_URL_SOURCE_COUNT;
                break;
        }
        Log.info(SDFullSQLSourceCount);
        List<Map<String, Object>> SDFullSourceTableCount = DBManager.getDBResultMap(SDFullSQLSourceCount, Constants.AWS_URL);
        SDFullSourceCount = ((Long) SDFullSourceTableCount.get(0).get("Source_count")).intValue();
    }

    @Given("^We know the total count of Current SD data from (.*)$")
    public void getCountfromCurrentTables(String tableName){
        switch (tableName){
            case "sdbooks_transform_current_urls":
                Log.info("Getting Current URL Table Count...");
                SDCurrentSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_CURRENT_COUNT;
                break;
        }
        Log.info(SDCurrentSQLCount);
        List<Map<String, Object>> SDCurrentTableCount = DBManager.getDBResultMap(SDCurrentSQLCount, Constants.AWS_URL);
        SDCurrentCount = ((Long) SDCurrentTableCount.get(0).get("Target_count")).intValue();
    }

    @And("^Compare count of SD Full load with current (.*) table are identical$")
    public void compareFullAndCurrentCounts(String tableName){
        Log.info("The count for table sdbooks_inbound_part => " + SDFullSourceCount + " and in "+tableName+" => " + SDCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with sdbooks_inbound_part and "+tableName, SDCurrentCount, SDFullSourceCount);
    }

    @Then("^Get the count of SD transform_current_history (.*)$")
    public void getCountfromCurrentHistoryTables(String tableName){
        switch (tableName){
            case "sdbooks_transform_history_urls_part":
                Log.info("Getting Current History URL Table Count...");
                SDCurrentHistSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_CURRENT_HISTORY_COUNT;
                break;
        }
        Log.info(SDCurrentHistSQLCount);
        List<Map<String, Object>> SDCurrentHistTableCount = DBManager.getDBResultMap(SDCurrentHistSQLCount, Constants.AWS_URL);
        SDCurrentHistCount = ((Long) SDCurrentHistTableCount.get(0).get("History_Count")).intValue();
    }

    @And("^Check count of SD current table (.*) and SD current history (.*) are identical$")
    public void compareCurrentAndHistCounts(String srcTable, String trgtTable){
        Log.info("The count for table "+srcTable+" => " + SDCurrentCount + " and in "+trgtTable+" => " + SDCurrentHistCount);
        Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and "+trgtTable, SDCurrentHistCount, SDCurrentCount);
    }

    @Given("^We know the total count of Previous SD data from (.*)$")
    public void getCountfromPreviousTables(String tableName){
        switch (tableName){
            case "sdbooks_transform_previous_urls":
                Log.info("Getting Previous URL Table Count...");
                SDPreviousSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_PREVIOUS_COUNT;
                break;
        }
        Log.info(SDPreviousSQLCount);
        List<Map<String, Object>> SDPreviousTableCount = DBManager.getDBResultMap(SDPreviousSQLCount, Constants.AWS_URL);
        SDPreviousCount = ((Long) SDPreviousTableCount.get(0).get("Previous_count")).intValue();
    }

    @Then("^Get the count of SD transform_previous_history (.*)$")
    public void getCountfromPreviousHistoryTables(String tableName){
        switch (tableName){
            case "sdbooks_transform_history_urls_part_previous":
                Log.info("Getting Previous History URL Table Count...");
                SDPreviousHistSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_PREVIOUS_HISTORY_COUNT;
                break;
        }
        Log.info(SDPreviousHistSQLCount);
        List<Map<String, Object>> SDPreviousHistTableCount = DBManager.getDBResultMap(SDPreviousHistSQLCount, Constants.AWS_URL);
        SDPreviousHistCount = ((Long) SDPreviousHistTableCount.get(0).get("Previous_History_Count")).intValue();
    }

    @And("^Check count of SD previous table (.*) and SD previous history (.*) are identical$")
    public void comparePreviousAndHistCounts(String srcTable, String trgtTable){
        Log.info("The count for table "+srcTable+" => " + SDPreviousCount + " and in "+trgtTable+" => " + SDPreviousHistCount);
        Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and "+trgtTable, SDPreviousHistCount, SDPreviousCount);
    }

    @Given("^Get the difference of total count between current and previous of SDbooks data (.*)$")
    public void getCountDifffromCurrentPrevTables(String tableName){
        switch (tableName){
            case "sdbooks_delta_current_urls":
                Log.info("Getting Current and Previous URL Table diff Count...");
                SDCurrPrevSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_DIFF_CURR_PREV_COUNT;
                break;
        }
        Log.info(SDCurrPrevSQLCount);
        List<Map<String, Object>> SDCurrPrevTableCount = DBManager.getDBResultMap(SDCurrPrevSQLCount, Constants.AWS_URL);
        SDCurrPrevCount = ((Long) SDCurrPrevTableCount.get(0).get("source_count")).intValue();
    }

    @Then("^We know the delta current count for Sdbooks tables (.*)$")
    public void getDeltaCurrentTables(String tableName){
        switch (tableName){
            case "sdbooks_delta_current_urls":
                Log.info("Getting Delta Current URL Table Count...");
                SDDeltaCurrentSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_DELTA_CURRENT_COUNT;
                break;
        }
        Log.info(SDDeltaCurrentSQLCount);
        List<Map<String, Object>> SDDeltaCurrentTableCount = DBManager.getDBResultMap(SDDeltaCurrentSQLCount, Constants.AWS_URL);
        SDDeltaCurrentCount = ((Long) SDDeltaCurrentTableCount.get(0).get("Delta_Count")).intValue();
    }

    @And("^Compare SDbooks delta count of (.*) and (.*) with (.*) are identical$")
    public void compareDeltaCounts(String srcTable1,String srcTable2, String trgtTable){
        Log.info("The Diff of count for table "+srcTable1+" and "+srcTable2+" => " + SDCurrPrevCount + " and in "+trgtTable+" => " + SDDeltaCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with Diff of "+srcTable1+" and "+srcTable2+"with "+trgtTable, SDPreviousHistCount, SDPreviousCount);
    }

}
