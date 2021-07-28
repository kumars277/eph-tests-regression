package com.eph.automation.testing.steps.sdbooks;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.sdbookssql.SDDataLakeCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class SDBooksCountChecksSteps {
    private static int sdFullSrcCount;
    private static int sdCurrCount;
    private static int sdCurrHistCount;
    private static int sdTransformFileCount;
    private static int sdDeltaCurrCount;
    private static int sdDeltaCurrHistoryCount;
    private static int sdCurrPrevCount;
    private static int sdDeltaExclCount;
    private static int sdLatestCnt;
    private static int sdDeltaCurrHistCount;
    private static int sdExclCount;
    private static int sdDuplicateCnt;

    @Given("^Get the total count of SD Data from Full Load$")
    public static void getSDFullLoadCount () {
        String sdFullSqlSourceCount = SDDataLakeCountChecksSQL.GET_SD_URL_SOURCE_COUNT;
        Log.info(sdFullSqlSourceCount);
        List<Map<String, Object>> sdFullSrcTableCnt = DBManager.getDBResultMap(sdFullSqlSourceCount, Constants.AWS_URL);
        sdFullSrcCount = ((Long) sdFullSrcTableCnt.get(0).get("Source_count")).intValue();
    }

    @Given("^We know the total count of Current SD data$")
    public static void getCountfromCurrentTables(){
        Log.info("Getting Current URL Table Count...");
        String sdCurrSqlCount = SDDataLakeCountChecksSQL.GET_SD_URL_CURRENT_COUNT;
        Log.info(sdCurrSqlCount);
        List<Map<String, Object>> sdCurrTableCount = DBManager.getDBResultMap(sdCurrSqlCount, Constants.AWS_URL);
        sdCurrCount = ((Long) sdCurrTableCount.get(0).get("Target_count")).intValue();
    }

    @And("^Compare count of SD Full load with current table are identical$")
    public void compareFullAndCurrentCounts(){
        Log.info("The count for table sdbooks_inbound_part => " + sdFullSrcCount + " and in Current_URL => " + sdCurrCount);
        Assert.assertEquals("The counts are not equal when compared with sdbooks_inbound_part and in Current_URL", sdFullSrcCount,sdCurrCount );
    }

    @Then("^Get the count of SD transform_current_history$")
    public static void getCountfromCurrentHistoryTables(){
        Log.info("Getting Current History URL Table Count...");
        String sdCurrHistSqlCount = SDDataLakeCountChecksSQL.GET_SD_URL_CURRENT_HISTORY_COUNT;
        Log.info(sdCurrHistSqlCount);
        List<Map<String, Object>> sdCurrHistTableCount = DBManager.getDBResultMap(sdCurrHistSqlCount, Constants.AWS_URL);
        sdCurrHistCount = ((Long) sdCurrHistTableCount.get(0).get("History_Count")).intValue();
    }

    @And("^Check count of SD current table and SD current history are identical$")
    public void compareCurrentAndHistCounts(){
        Log.info("The count for table currentUrL => " + sdCurrCount + " and in currentUrlHistory => " + sdCurrHistCount);
        Assert.assertEquals("The counts are not equal when compared with currentUrL and currentUrlHistory", sdCurrCount,sdCurrHistCount );
    }


    @Then("^Get the count of SD transform_file$")
    public static void getCountfrmtransfileTables(){
        Log.info("Getting transform_file URL Table Count...");
        String sdTransformFileSqlCount = SDDataLakeCountChecksSQL.GET_SD_URL_TRANSFORM_FILE;
        Log.info(sdTransformFileSqlCount);
        List<Map<String, Object>> sdTransformFileTableCnt = DBManager.getDBResultMap(sdTransformFileSqlCount, Constants.AWS_URL);
        sdTransformFileCount = ((Long) sdTransformFileTableCnt.get(0).get("Transform_Count")).intValue();
    }

    @And("^Check count of SD current table and SD tranform_file are identical$")
    public void compareCurrntAndTransformCounts(){
        Log.info("The count for table currentUrl => " + sdCurrCount + " and in url tranform_file => " + sdTransformFileCount);
        Assert.assertEquals("The counts are not equal when compared with currentUrl and tranform_file", sdCurrCount,sdTransformFileCount );
    }

    @Given("^Get the difference of total count between current and previous time stamps of transform_file of SDbooks data$")
    public static void getCountDifffromCurrentPrevTables(){
        Log.info("Getting Current and Previous transform_file toime stamp URL Table ...");
        String sdCurrPrevSqlCount = SDDataLakeCountChecksSQL.GET_SD_URL_DIFF_CURR_PREV_COUNT;
        Log.info(sdCurrPrevSqlCount);
        List<Map<String, Object>> sdCurrPrevTableCnt = DBManager.getDBResultMap(sdCurrPrevSqlCount, Constants.AWS_URL);
        sdCurrPrevCount = ((Long) sdCurrPrevTableCnt.get(0).get("source_count")).intValue();
    }

    @Then("^We know the delta current count for Sdbooks tables$")
    public static void getDeltaCurrentTables(String tableName){
        Log.info("Getting Delta Current URL Table Count...");
        String sdDeltaCurrSqlCount = SDDataLakeCountChecksSQL.GET_SD_URL_DELTA_CURRENT_COUNT;
        Log.info(sdDeltaCurrSqlCount);
        List<Map<String, Object>> sdDeltaCurrTableCnt = DBManager.getDBResultMap(sdDeltaCurrSqlCount, Constants.AWS_URL);
        sdDeltaCurrCount = ((Long) sdDeltaCurrTableCnt.get(0).get("Delta_Count")).intValue();
    }

    @And("^Compare SDbooks delta count with tranform_file identical$")
    public void compareDeltaCounts(){
        Log.info("The Diff of current and prev time stamp count for table tranform_file => " + sdCurrPrevCount + " delta_current => " + sdDeltaCurrCount);
        Assert.assertEquals("The counts are not equal when compared with Diff of current and prev time stamp count for table tranform_file with delta_current", sdDeltaCurrCount, sdCurrPrevCount);
    }

    @Then("^Get the count of SDBook delta current history table$")
    public static void getDeltaHistCurrTables(){
        Log.info("Getting Delta Current History URL Table Count...");
        String sdDeltaCurrHistSqlCounts = SDDataLakeCountChecksSQL.GET_SD_URL_DELTA_CURR_HIST_COUNT;
        Log.info(sdDeltaCurrHistSqlCounts);
        List<Map<String, Object>> sdDeltaCurrHistTableCnt = DBManager.getDBResultMap(sdDeltaCurrHistSqlCounts, Constants.AWS_URL);
        sdDeltaCurrHistoryCount = ((Long) sdDeltaCurrHistTableCnt.get(0).get("Delta_History_Count")).intValue();
    }

    @And("^Compare SD delta current table and delta history are identical$")
    public void compareDeltaHistCounts(){
        Log.info("The count for table deltcurrent => " + sdDeltaCurrCount + " and in deltaHistory => " + sdDeltaCurrHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with deltcurrent and deltaHistory", sdDeltaCurrCount, sdDeltaCurrHistoryCount);
    }

    @Given("^Get the sum of total count between SDBooks delta current and and Current_Exclude Table$")
    public static void getCountSumofDeltaExclTables(){
        Log.info("Getting Sum of Delta and Exclude URL Table Count...");
        String sdDeltaExclSqlCount = SDDataLakeCountChecksSQL.GET_SD_URL_SUM_DELTA_EXCL_COUNT;
        Log.info(sdDeltaExclSqlCount);
        List<Map<String, Object>> sdDeltaExclTableCnt = DBManager.getDBResultMap(sdDeltaExclSqlCount, Constants.AWS_URL);
        sdDeltaExclCount = ((Long) sdDeltaExclTableCnt.get(0).get("source_count")).intValue();
    }

    @Then("^Get the SDbooks latest data count$")
    public static void getLatestCurrentTables(){
        Log.info("Getting Latest Current URL Table Count...");
        String sdLatestSqlCount = SDDataLakeCountChecksSQL.GET_SD_URL_LATEST_CURRENT_COUNT;
        Log.info(sdLatestSqlCount);
        List<Map<String, Object>> sdLatestTableCount = DBManager.getDBResultMap(sdLatestSqlCount, Constants.AWS_URL);
        sdLatestCnt = ((Long) sdLatestTableCount.get(0).get("latest_count")).intValue();
    }

    @And("^Compare SDBooks latest counts and exclude with delta are identical$")
    public void compareLatestCounts(){
        Log.info("The sum of count for table delta and exclude => " + sdDeltaExclCount + " and in latest => " + sdLatestCnt);
        Assert.assertEquals("The counts are not equal when compared with sum of delta with exclude and latest", sdLatestCnt, sdDeltaExclCount);
    }

    @Given("^Get the SDBooks total count difference between delta current and transform current history Table$")
    public static void getCountDiffofDeltaAndCurrHistTables(){
        Log.info("Getting Diff of Delta and Current History URL Table Count...");
        String sdDeltaCurrHistSqlCount = SDDataLakeCountChecksSQL.GET_SD_URL_DIFF_DELTA_CURR_HIST_COUNT;
        Log.info(sdDeltaCurrHistSqlCount);
        List<Map<String, Object>> sdDeltaCurrHistTableCount = DBManager.getDBResultMap(sdDeltaCurrHistSqlCount, Constants.AWS_URL);
        sdDeltaCurrHistCount = ((Long) sdDeltaCurrHistTableCount.get(0).get("source_count")).intValue();
    }

    @Then("^Get the SDBooks exclude data count$")
    public static void getExclCurrentTables(){
        Log.info("Getting Exclude Current URL Table Count...");
        String sdExclSqlCount = SDDataLakeCountChecksSQL.GET_SD_URL_EXCLUDE_CURRENT_COUNT;
        Log.info(sdExclSqlCount);
        List<Map<String, Object>> sdExcludeTableCnt = DBManager.getDBResultMap(sdExclSqlCount, Constants.AWS_URL);
        sdExclCount = ((Long) sdExcludeTableCnt.get(0).get("excl_count")).intValue();
    }

    @And("^Compare SDBooks exclude count and delta current with current history are identical$")
    public void compareExcludeCounts(){
        Log.info("The diff of count for table delta current with current history => " + sdDeltaCurrHistCount + " and in exclude count => " + sdExclCount);
        Assert.assertEquals("The counts are not equal when compared with Diff of delta current with current history with exclude count", sdExclCount, sdDeltaCurrHistCount);
    }

    @Given("^Get the SDBooks Duplicate count in (.*) table$")
    public static void getDuplicateCount(String tableName){
        Log.info("Getting Duplicate URL Latest Table Count...");
        String sdDuplicateSqlCnt = SDDataLakeCountChecksSQL.GET_SD_DUPLICATES_LATEST_URL_COUNT;
        Log.info(sdDuplicateSqlCnt);
        List<Map<String, Object>> sdDuplicateCount = DBManager.getDBResultMap(sdDuplicateSqlCnt, Constants.AWS_URL);
        sdDuplicateCnt = ((Long) sdDuplicateCount.get(0).get("Duplicate_Count")).intValue();
    }

    @Then("^Check the SDBooks count should be equal to Zero (.*)$")
    public void checkDupCountZero(String tableName){
        Log.info("The Duplicate count for "+tableName+" => " + sdDuplicateCnt);
        Assert.assertEquals("There are Duplicate Count of ISBN in "+tableName,0,sdDuplicateCnt);

    }

}
