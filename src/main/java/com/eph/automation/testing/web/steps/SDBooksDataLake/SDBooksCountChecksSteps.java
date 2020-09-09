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
    private static String SDDeltaCurrentHistSQLCount;
    private static int SDCurrentCount;
    private static int SDPreviousCount;
    private static int SDCurrentHistCount;
    private static int SDPreviousHistCount;
    private static int SDDeltaCurrentCount;
    private static int SDDeltaCurrentHistCount;
    private static int SDCurrPrevCount;
    private static String SDCurrPrevSQLCount;
    private static String SDDeltaExclSQLCount;
    private static int SDDeltaExclCount;
    private static String SDLatestSQLCount;
    private static int SDLatestCount;
    private static String SDDeltaCurrHistSQLCount;
    private static int SDDeltaCurrHistCount;
    private static String SDExclSQLCount;
    private static int SDExclCount;
    private static int SDDuplicateLatestCount;
    private static String SDDuplicateLatestSQLCount;



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
        Assert.assertEquals("The counts are not equal when compared with Diff of "+srcTable1+" and "+srcTable2+"with "+trgtTable, SDDeltaCurrentCount, SDCurrPrevCount);
    }

    @Then("^Get the count of SDBook delta current history (.*) table$")
    public void getDeltaHistCurrTables(String tableName){
        switch (tableName){
            case "sdbooks_delta_history_urls_part":
                Log.info("Getting Delta Current History URL Table Count...");
                SDDeltaCurrentHistSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_DELTA_CURR_HIST_COUNT;
                break;
        }
        Log.info(SDDeltaCurrentHistSQLCount);
        List<Map<String, Object>> SDDeltaCurrentHistTableCount = DBManager.getDBResultMap(SDDeltaCurrentHistSQLCount, Constants.AWS_URL);
        SDDeltaCurrentHistCount = ((Long) SDDeltaCurrentHistTableCount.get(0).get("Delta_History_Count")).intValue();
    }

    @And("^Compare SD delta current (.*) table and delta history (.*) are identical$")
    public void compareDeltaHistCounts(String srcTable,String trgtTable){
        Log.info("The count for table "+srcTable+" => " + SDDeltaCurrentCount + " and in "+trgtTable+" => " + SDDeltaCurrentHistCount);
        Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and "+trgtTable, SDDeltaCurrentCount, SDDeltaCurrentHistCount);
    }


    @Given("^Get the sum of total count between SDBooks delta current and and Current_Exclude Table (.*)$")
    public void getCountSumofDeltaExclTables(String tableName){
        switch (tableName){
            case "sdbooks_transform_latest_urls":
                Log.info("Getting Sum of Delta and Exclude URL Table Count...");
                SDDeltaExclSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_SUM_DELTA_EXCL_COUNT;
                break;
        }
        Log.info(SDDeltaExclSQLCount);
        List<Map<String, Object>> SDDeltaExclTableCount = DBManager.getDBResultMap(SDDeltaExclSQLCount, Constants.AWS_URL);
        SDDeltaExclCount = ((Long) SDDeltaExclTableCount.get(0).get("source_count")).intValue();
    }

    @Then("^Get the SDbooks (.*) latest data count$")
    public void getLatestCurrentTables(String tableName){
        switch (tableName){
            case "sdbooks_transform_latest_urls":
                Log.info("Getting Latest Current URL Table Count...");
                SDLatestSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_LATEST_CURRENT_COUNT;
                break;
        }
        Log.info(SDLatestSQLCount);
        List<Map<String, Object>> SDLatestTableCount = DBManager.getDBResultMap(SDLatestSQLCount, Constants.AWS_URL);
        SDLatestCount = ((Long) SDLatestTableCount.get(0).get("latest_count")).intValue();
    }

    @And("^Compare SDBooks latest counts of (.*) and (.*) with (.*) are identical$")
    public void compareLatestCounts(String srcTable1,String srcTable2, String trgtTable){
        Log.info("The sum of count for table "+srcTable1+" and "+srcTable2+" => " + SDDeltaExclCount + " and in "+trgtTable+" => " + SDLatestCount);
        Assert.assertEquals("The counts are not equal when compared with sum of "+srcTable1+" and "+srcTable2+"with "+trgtTable, SDLatestCount, SDDeltaExclCount);
    }

    @Given("^Get the SDBooks total count difference between delta current and transform current history Table (.*)$")
    public void getCountDiffofDeltaAndCurrHistTables(String tableName){
        switch (tableName){
            case "sdbooks_transform_history_excl_delta":
                Log.info("Getting Diff of Delta and Current History URL Table Count...");
                SDDeltaCurrHistSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_DIFF_DELTA_CURR_HIST_COUNT;
                break;
        }
        Log.info(SDDeltaCurrHistSQLCount);
        List<Map<String, Object>> SDDeltaCurrHistTableCount = DBManager.getDBResultMap(SDDeltaCurrHistSQLCount, Constants.AWS_URL);
        SDDeltaCurrHistCount = ((Long) SDDeltaCurrHistTableCount.get(0).get("source_count")).intValue();
    }

    @Then("^Get the SDBooks (.*) exclude data count$")
    public void getExclCurrentTables(String tableName){
        switch (tableName){
            case "sdbooks_transform_history_excl_delta":
                Log.info("Getting Exclude Current URL Table Count...");
                SDExclSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_EXCLUDE_CURRENT_COUNT;
                break;
        }
        Log.info(SDExclSQLCount);
        List<Map<String, Object>> SDExclTableCount = DBManager.getDBResultMap(SDExclSQLCount, Constants.AWS_URL);
        SDExclCount = ((Long) SDExclTableCount.get(0).get("excl_count")).intValue();
    }

    @And("^Compare SDBooks exclude count of (.*) and (.*) with (.*) are identical$")
    public void compareExcludeCounts(String srcTable1,String srcTable2, String trgtTable){
        Log.info("The diff of count for table "+srcTable1+" and "+srcTable2+" => " + SDDeltaCurrHistCount + " and in "+trgtTable+" => " + SDExclCount);
        Assert.assertEquals("The counts are not equal when compared with Diff of "+srcTable1+" and "+srcTable2+"with "+trgtTable, SDExclCount, SDDeltaCurrHistCount);
    }

    @Given("^Get the SDBooks Duplicate count in (.*) table$")
    public void getDuplicateCount(String tableName){
        switch (tableName){
            case "sdbooks_transform_latest_urls":
                Log.info("Getting Duplicate URL Latest Table Count...");
                SDDuplicateLatestSQLCount = SDDataLakeCountChecksSQL.GET_SD_DUPLICATES_LATEST_URL_COUNT;
                break;
        }
        Log.info(SDDuplicateLatestSQLCount);
        List<Map<String, Object>> SDDupLatestTableCount = DBManager.getDBResultMap(SDDuplicateLatestSQLCount, Constants.AWS_URL);
        SDDuplicateLatestCount = ((Long) SDDupLatestTableCount.get(0).get("Duplicate_Count")).intValue();
    }

    @Then("^Check the SDBooks count should be equal to Zero (.*)$")
    public void checkDupCountZero(String tableName){
        Log.info("The Duplicate count for "+tableName+" => " + SDDuplicateLatestCount);
        Assert.assertEquals("There are Duplicate Count of ISBN in "+tableName,0,SDDuplicateLatestCount);

    }

}
