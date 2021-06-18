package com.eph.automation.testing.steps.sdBooks;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.SDBooksDataLakeSQL.SDDataLakeCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class SDBooksCountChecksSteps {
    private static String SDFullSQLSourceCount;
    private static int SDFullSourceCount;
    private static String SDCurrentSQLCount;
    private static String SDPreviousSQLCount;
    private static String SDCurrentHistSQLCount;
    private static String SDTransform_FileSQLCount;
    private static String SDDeltaCurrentSQLCount;
    private static String SDDeltaCurrentHistSQLCount;
    private static int SDCurrentCount;
    private static int SDPreviousCount;
    private static int SDCurrentHistCount;
    private static int SDTransformFileCount;
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



    @Given("^Get the total count of SD Data from Full Load$")
    public void getSDFullLoadCount () {
                SDFullSQLSourceCount = SDDataLakeCountChecksSQL.GET_SD_URL_SOURCE_COUNT;
        Log.info(SDFullSQLSourceCount);
        List<Map<String, Object>> SDFullSourceTableCount = DBManager.getDBResultMap(SDFullSQLSourceCount, Constants.AWS_URL);
        SDFullSourceCount = ((Long) SDFullSourceTableCount.get(0).get("Source_count")).intValue();
    }

    @Given("^We know the total count of Current SD data$")
    public void getCountfromCurrentTables(){
                Log.info("Getting Current URL Table Count...");
                SDCurrentSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_CURRENT_COUNT;
        Log.info(SDCurrentSQLCount);
        List<Map<String, Object>> SDCurrentTableCount = DBManager.getDBResultMap(SDCurrentSQLCount, Constants.AWS_URL);
        SDCurrentCount = ((Long) SDCurrentTableCount.get(0).get("Target_count")).intValue();
    }

    @And("^Compare count of SD Full load with current table are identical$")
    public void compareFullAndCurrentCounts(){
        Log.info("The count for table sdbooks_inbound_part => " + SDFullSourceCount + " and in Current_URL => " + SDCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with sdbooks_inbound_part and in Current_URL", SDFullSourceCount,SDCurrentCount );
    }

    @Then("^Get the count of SD transform_current_history$")
    public void getCountfromCurrentHistoryTables(){
                  Log.info("Getting Current History URL Table Count...");
                SDCurrentHistSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_CURRENT_HISTORY_COUNT;
         Log.info(SDCurrentHistSQLCount);
        List<Map<String, Object>> SDCurrentHistTableCount = DBManager.getDBResultMap(SDCurrentHistSQLCount, Constants.AWS_URL);
        SDCurrentHistCount = ((Long) SDCurrentHistTableCount.get(0).get("History_Count")).intValue();
    }

    @And("^Check count of SD current table and SD current history are identical$")
    public void compareCurrentAndHistCounts(){
        Log.info("The count for table currentUrL => " + SDCurrentCount + " and in currentUrlHistory => " + SDCurrentHistCount);
        Assert.assertEquals("The counts are not equal when compared with currentUrL and currentUrlHistory", SDCurrentCount,SDCurrentHistCount );
    }


    @Then("^Get the count of SD transform_file$")
    public void getCountfromtransform_fileTables(){
                Log.info("Getting transform_file URL Table Count...");
                SDTransform_FileSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_TRANSFORM_FILE;
        Log.info(SDTransform_FileSQLCount);
        List<Map<String, Object>> SDTransformFileTableCount = DBManager.getDBResultMap(SDTransform_FileSQLCount, Constants.AWS_URL);
        SDTransformFileCount = ((Long) SDTransformFileTableCount.get(0).get("Transform_Count")).intValue();
    }

    @And("^Check count of SD current table and SD tranform_file are identical$")
    public void compareCurrntAndTransformCounts(){
        Log.info("The count for table currentUrl => " + SDCurrentCount + " and in url tranform_file => " + SDTransformFileCount);
        Assert.assertEquals("The counts are not equal when compared with currentUrl and tranform_file", SDCurrentCount,SDTransformFileCount );
    }

    @Given("^Get the difference of total count between current and previous time stamps of transform_file of SDbooks data$")
    public void getCountDifffromCurrentPrevTables(){
                Log.info("Getting Current and Previous transform_file toime stamp URL Table ...");
                SDCurrPrevSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_DIFF_CURR_PREV_COUNT;
        Log.info(SDCurrPrevSQLCount);
        List<Map<String, Object>> SDCurrPrevTableCount = DBManager.getDBResultMap(SDCurrPrevSQLCount, Constants.AWS_URL);
        SDCurrPrevCount = ((Long) SDCurrPrevTableCount.get(0).get("source_count")).intValue();
    }

    @Then("^We know the delta current count for Sdbooks tables$")
    public void getDeltaCurrentTables(String tableName){
                Log.info("Getting Delta Current URL Table Count...");
                SDDeltaCurrentSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_DELTA_CURRENT_COUNT;
        Log.info(SDDeltaCurrentSQLCount);
        List<Map<String, Object>> SDDeltaCurrentTableCount = DBManager.getDBResultMap(SDDeltaCurrentSQLCount, Constants.AWS_URL);
        SDDeltaCurrentCount = ((Long) SDDeltaCurrentTableCount.get(0).get("Delta_Count")).intValue();
    }

    @And("^Compare SDbooks delta count with tranform_file identical$")
    public void compareDeltaCounts(){
        Log.info("The Diff of current and prev time stamp count for table tranform_file => " + SDCurrPrevCount + " delta_current => " + SDDeltaCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with Diff of current and prev time stamp count for table tranform_file with delta_current", SDDeltaCurrentCount, SDCurrPrevCount);
    }

    @Then("^Get the count of SDBook delta current history table$")
    public void getDeltaHistCurrTables(){
                Log.info("Getting Delta Current History URL Table Count...");
                SDDeltaCurrentHistSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_DELTA_CURR_HIST_COUNT;
       Log.info(SDDeltaCurrentHistSQLCount);
        List<Map<String, Object>> SDDeltaCurrentHistTableCount = DBManager.getDBResultMap(SDDeltaCurrentHistSQLCount, Constants.AWS_URL);
        SDDeltaCurrentHistCount = ((Long) SDDeltaCurrentHistTableCount.get(0).get("Delta_History_Count")).intValue();
    }

    @And("^Compare SD delta current table and delta history are identical$")
    public void compareDeltaHistCounts(){
        Log.info("The count for table deltcurrent => " + SDDeltaCurrentCount + " and in deltaHistory => " + SDDeltaCurrentHistCount);
        Assert.assertEquals("The counts are not equal when compared with deltcurrent and deltaHistory", SDDeltaCurrentCount, SDDeltaCurrentHistCount);
    }

    @Given("^Get the sum of total count between SDBooks delta current and and Current_Exclude Table$")
    public void getCountSumofDeltaExclTables(){
                Log.info("Getting Sum of Delta and Exclude URL Table Count...");
                SDDeltaExclSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_SUM_DELTA_EXCL_COUNT;
      Log.info(SDDeltaExclSQLCount);
        List<Map<String, Object>> SDDeltaExclTableCount = DBManager.getDBResultMap(SDDeltaExclSQLCount, Constants.AWS_URL);
        SDDeltaExclCount = ((Long) SDDeltaExclTableCount.get(0).get("source_count")).intValue();
    }

    @Then("^Get the SDbooks latest data count$")
    public void getLatestCurrentTables(){
               Log.info("Getting Latest Current URL Table Count...");
                SDLatestSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_LATEST_CURRENT_COUNT;

        Log.info(SDLatestSQLCount);
        List<Map<String, Object>> SDLatestTableCount = DBManager.getDBResultMap(SDLatestSQLCount, Constants.AWS_URL);
        SDLatestCount = ((Long) SDLatestTableCount.get(0).get("latest_count")).intValue();
    }

    @And("^Compare SDBooks latest counts and exclude with delta are identical$")
    public void compareLatestCounts(){
        Log.info("The sum of count for table delta and exclude => " + SDDeltaExclCount + " and in latest => " + SDLatestCount);
        Assert.assertEquals("The counts are not equal when compared with sum of delta with exclude and latest", SDLatestCount, SDDeltaExclCount);
    }

    @Given("^Get the SDBooks total count difference between delta current and transform current history Table$")
    public void getCountDiffofDeltaAndCurrHistTables(){
                 Log.info("Getting Diff of Delta and Current History URL Table Count...");
                SDDeltaCurrHistSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_DIFF_DELTA_CURR_HIST_COUNT;
        Log.info(SDDeltaCurrHistSQLCount);
        List<Map<String, Object>> SDDeltaCurrHistTableCount = DBManager.getDBResultMap(SDDeltaCurrHistSQLCount, Constants.AWS_URL);
        SDDeltaCurrHistCount = ((Long) SDDeltaCurrHistTableCount.get(0).get("source_count")).intValue();
    }

    @Then("^Get the SDBooks exclude data count$")
    public void getExclCurrentTables(){
                Log.info("Getting Exclude Current URL Table Count...");
                SDExclSQLCount = SDDataLakeCountChecksSQL.GET_SD_URL_EXCLUDE_CURRENT_COUNT;
        Log.info(SDExclSQLCount);
        List<Map<String, Object>> SDExclTableCount = DBManager.getDBResultMap(SDExclSQLCount, Constants.AWS_URL);
        SDExclCount = ((Long) SDExclTableCount.get(0).get("excl_count")).intValue();
    }

    @And("^Compare SDBooks exclude count and delta current with current history are identical$")
    public void compareExcludeCounts(){
        Log.info("The diff of count for table delta current with current history => " + SDDeltaCurrHistCount + " and in exclude count => " + SDExclCount);
        Assert.assertEquals("The counts are not equal when compared with Diff of delta current with current history with exclude count", SDExclCount, SDDeltaCurrHistCount);
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
