package com.eph.automation.testing.steps.sdrm;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.sdrmsql.SDRMDataChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import java.util.List;
import java.util.Map;

public class SDRMCountChecksSteps {

    private static String sdrmCurrCountSql;
    private static int sdrmSourceCount;
    private static int sdrmCurrCount;
    private static int sdrmProductHistCount;
    private static int sdrmProdFileHistCount;
    private static int sdrmDeltaCurrCount;
    private static int sdrmDeltaHistCount;
    private static int sdrmDeltaCurrProdHistCount;
    private static int sdrmHistExclDeltaCount;
    private static int sdrmDeltaHistExclDeltaCount;
    private static int sdrmLatestProCount;
    private static int sdrmDupLatestProdCount;
    private static int sdrmFileHistDiffCount;
    private static int sdrmDeltaCurrProdCount;


    @Given("^Get the total count of SDRM Data from Inbound Load$")
    public static void getSDRMInboundount () {
        Log.info("Getting sdrm inbound table count...");
        String sdrmSourceCountSQL;
        sdrmSourceCountSQL= SDRMDataChecksSQL.GET_SDRM_INBOUND_SOURCE_COUNT;
        List<Map<String, Object>> sdrmInboundSrcTableCount = DBManager.getDLResultMap(sdrmSourceCountSQL, Constants.AWS_URL);
        sdrmSourceCount = ((Long) sdrmInboundSrcTableCount.get(0).get("source_count")).intValue();
        Log.info(sdrmSourceCountSQL);
    }

    @Given("^We know the total count Current SDRM data from Current Product Availability$")
    public static void getCountCurrentTables(){
        Log.info("Getting sdrm current product availability table count...");
        sdrmCurrCountSql=SDRMDataChecksSQL.GET_SDRM_CURRENT_COUNT;
        List<Map<String, Object>> sdrmCurrentTableCount = DBManager.getDLResultMap(sdrmCurrCountSql, Constants.AWS_URL);
        sdrmCurrCount = ((Long) sdrmCurrentTableCount.get(0).get("source_count")).intValue();
        Log.info(sdrmCurrCountSql);
    }

    @And("^Compare count of SDRM Inbound load with current Product Availability table are identical$")
    public void compareFullAndCurrentCounts(){
        Log.info("The count for table sdrm Source_inbound_part => " + sdrmSourceCount + " and in current Product Availability => " + sdrmCurrCount);
        Assert.assertEquals("The counts are not equal when compared with sdrm_inbound_part and in current Product Availability", sdrmCurrCount, sdrmSourceCount);
    }

    @Given("^We know the total SDRM Current product availability data$")
    public static void getCountfromCurrentHistoryTables(){
        Log.info("Getting sdrm Current Product Table Count...");
        sdrmCurrCountSql=SDRMDataChecksSQL.GET_SDRM_CURRENT_COUNT;
        List<Map<String, Object>> sdrmCurrentTableCount = DBManager.getDLResultMap(sdrmCurrCountSql, Constants.AWS_URL);
        sdrmCurrCount = ((Long) sdrmCurrentTableCount.get(0).get("source_count")).intValue();
        Log.info(sdrmCurrCountSql);
    }

    @Then("^Get the count of SDRM transform product availability history$")
    public static void getCountfromtransformfile(String tableName){
         Log.info("Getting sdrm Product history Table Count...");
         String sdrmProdHistSqlCount;
         sdrmProdHistSqlCount = SDRMDataChecksSQL.GET_SDRM_PRODUCT_HISTORY_COUNT;
         Log.info(sdrmProdHistSqlCount);
        List<Map<String, Object>> sdrmTransformFileTableCount = DBManager.getDBResultMap(sdrmProdHistSqlCount, Constants.AWS_URL);
        sdrmProductHistCount = ((Long) sdrmTransformFileTableCount.get(0).get("target_count")).intValue();
    }

    @And("^Check count of current product availability and product history availability are identical$")
    public void compareCurrentAndHistCounts(){
        Log.info("The count for table current product availability => " + sdrmCurrCount + " and in product history availability => " + sdrmProductHistCount);
        Assert.assertEquals("The counts are not equal when compared with current product availability and in product history availability", sdrmCurrCount, sdrmProductHistCount);
    }

    @Then("^Get the count of SDRM transform product file history$")
    public static void getCountfrmTransformFileHistTables(){
         Log.info("Getting sdrm Product file history Table Count...");
         String sdrmProdFileHistSqlCount;
       sdrmProdFileHistSqlCount = SDRMDataChecksSQL.GET_SDRM_PRODUCT_FILE_HISTORY_COUNT;
       Log.info(sdrmProdFileHistSqlCount);
        List<Map<String, Object>> sdrmTransformFileTableCount = DBManager.getDBResultMap(sdrmProdFileHistSqlCount, Constants.AWS_URL);
        sdrmProdFileHistCount = ((Long) sdrmTransformFileTableCount.get(0).get("target_count")).intValue();
    }

    @And("^Check count of SDRM current product availability table and SDRM product availability file history are identical$")
    public void compareCurrentAndFileHistCounts(){
        Log.info("The count for table current product availability table => " + sdrmCurrCount + " and in product availability file history => " + sdrmProdFileHistCount);
        Assert.assertEquals("The counts are not equal when compared with current product availability and in product availability file history", sdrmCurrCount, sdrmProdFileHistCount);


    }
    @Given("^We know the total count of difference between Current and Previous timestamps of the SDRM transform product availability file history$")
    public static void getCountfromDifferencebetweenCurrentAndPreviousFileHistory(){
        Log.info("Getting Difference between current and previous timestamps of the sdrm File history...");
        String sdrmFileHistDiffSqlCount;
        sdrmFileHistDiffSqlCount=SDRMDataChecksSQL.GET_SDRM_FILEHISTORY_COUNT;
        List<Map<String, Object>> sdrmDeltaProdCount = DBManager.getDLResultMap(sdrmFileHistDiffSqlCount, Constants.AWS_URL);
        sdrmFileHistDiffCount = ((Long) sdrmDeltaProdCount.get(0).get("source_count")).intValue();
        Log.info(sdrmFileHistDiffSqlCount);
    }

    @Then("^Get the count of SDRM delta current product availability$")
    public static void getCountfromSDRMDeltaCurrentTables(){
        Log.info("Getting sdrm Delta Current product Table Count...");
        String sdrmDeltaCurrProdSqlCount;
        sdrmDeltaCurrProdSqlCount = SDRMDataChecksSQL.GET_SDRM_DELTA_CURRENT_PRODUCT_COUNT;
        List<Map<String, Object>> sdrmDeltaProdHistCount = DBManager.getDBResultMap(sdrmDeltaCurrProdSqlCount, Constants.AWS_URL);
        sdrmDeltaCurrProdCount = ((Long) sdrmDeltaProdHistCount.get(0).get("source_count")).intValue();
        Log.info(sdrmDeltaCurrProdSqlCount);
    }

    @And("^Check count of difference between current and previous timestamps of the SDRM transform file history product availability and SDRM delta current product availability are identical$")
    public void compareFileHistoryAndDeltaCurrentProductCounts(){
        Log.info("The count for table srcTable => " + sdrmFileHistDiffCount + " and in delta current product availability => " + sdrmDeltaCurrProdCount);
        Assert.assertEquals("The counts are not equal when compared with srcTable and delta current product availability", sdrmFileHistDiffCount, sdrmDeltaCurrProdCount);
    }



    @Given("^We know the total count of SDRM Delta Current product availability data$")
    public static void getCountfromDeltaCurrentProductTables(){
        Log.info("Getting sdrm Delta Current Product Table Count...");
        String sdrmDeltaCurrCountSQL;
        sdrmDeltaCurrCountSQL=SDRMDataChecksSQL.GET_SDRM_DELTA_CURRENT_PRODUCT_COUNT;
        List<Map<String, Object>> sdrmDeltaProdCount = DBManager.getDLResultMap(sdrmDeltaCurrCountSQL, Constants.AWS_URL);
        sdrmDeltaCurrCount = ((Long) sdrmDeltaProdCount.get(0).get("source_count")).intValue();
        Log.info(sdrmDeltaCurrCountSQL);
    }

    @Then("^Get the count of SDRM delta product availability history$")
    public static void getCountfromDeltaHistoryTables(){
        Log.info("Getting sdrm Delta Product history Table Count...");
        String sdrmDeltaHistCountSql;
        sdrmDeltaHistCountSql = SDRMDataChecksSQL.GET_SDRM_DELTA_PRODUCT_HISTORY_COUNT;
        Log.info(sdrmDeltaHistCountSql);
        List<Map<String, Object>> sdrmDeltaProdHistCount = DBManager.getDBResultMap(sdrmDeltaHistCountSql, Constants.AWS_URL);
        sdrmDeltaHistCount = ((Long) sdrmDeltaProdHistCount.get(0).get("target_count")).intValue();
    }

    @And("^Check count of SDRM Delta current product availability table and SDRM delta product availability history are identical$")
    public void compareCurrentDeltaAndDeltaHistCounts(){
            Log.info("The count for table Delta current product availability table => " + sdrmDeltaCurrCount + " and in delta product availability history => " + sdrmDeltaHistCount);
            Assert.assertEquals("The counts are not equal when compared with current product availability table and delta product availability history", sdrmDeltaCurrCount, sdrmDeltaHistCount);
    }

    @Given("^We know the total count of difference between SDRM Delta Current product availability data and SDRM Product availability History$")
    public static void getCountfromDeltaCurrentProductAndProductHistoryTables(String tableName){
        Log.info("Getting difference between sdrm Delta Current Product Table Count and sdrm Product History table...");
        String sdrmDeltaProdCountSql;
        sdrmDeltaProdCountSql=SDRMDataChecksSQL.GET_SDRM_CURRENT_PRODUCT_AND_PRODUCT_HISTORY_COUNT;
        List<Map<String, Object>> sdrmDeltaProdCount = DBManager.getDLResultMap(sdrmDeltaProdCountSql, Constants.AWS_URL);
        sdrmDeltaCurrProdHistCount = ((Long) sdrmDeltaProdCount.get(0).get("source_count")).intValue();
        Log.info(sdrmDeltaProdCountSql);
    }

    @Then("^Get the count of SDRM transform product availability history excl delta$")
    public static void getCountfromHistoryExclDeltaTables(String tableName){
        Log.info("Getting sdrm Transform history excl delta Table Count...");
        String sdrmHistExclDeltaCountSQL;
        sdrmHistExclDeltaCountSQL = SDRMDataChecksSQL.GET_SDRM_HISTORY_EXCL_DELTA_COUNT;
        List<Map<String, Object>> sdrmDeltaProdHistCount = DBManager.getDBResultMap(sdrmHistExclDeltaCountSQL, Constants.AWS_URL);
        sdrmHistExclDeltaCount = ((Long) sdrmDeltaProdHistCount.get(0).get("target_count")).intValue();
        Log.info(sdrmHistExclDeltaCountSQL);
    }

    @And("^Check count of between SDRM Delta Current product availability data and SDRM Product availability History and SDRM transform availability history excl delta are identical$")
    public void compareCurrDelProdHistAndHistExclDeltaCounts(String srcTable, String trgtTable){
        Log.info("The count for table sourceTable => " + sdrmDeltaCurrProdHistCount + " and in product availability excl => " + sdrmHistExclDeltaCount);
        Assert.assertEquals("The counts are not equal when compared with srcTable and in product availability excl", sdrmDeltaCurrProdHistCount, sdrmHistExclDeltaCount);
    }

    @Given("^We know the total SDRM Delta Current product availability data and SDRM prod availability History Excl Delta$")
    public static void getCountfromDeltaCurrentProductAndSDRMHistoryExclDeltaTables(){
        Log.info("Getting combine of sdrm Delta Current Product Table Count and sdrm History Excl Delta table...");
        String sdrmExclCountSql;
        sdrmExclCountSql=SDRMDataChecksSQL.GET_SDRM_DELTA_CURRENT_AND_SDRM_HISTORY_EXCL_DELTA_COUNT;
        List<Map<String, Object>> sdrmDeltaProdCount = DBManager.getDLResultMap(sdrmExclCountSql, Constants.AWS_URL);
        sdrmDeltaHistExclDeltaCount = ((Long) sdrmDeltaProdCount.get(0).get("source_count")).intValue();
        Log.info(sdrmExclCountSql);
    }

    @Then("^Get the count of SDRM transform prod availability latest table$")
    public static void getCountfromSDRMTransformLatestProductTables(){
        Log.info("Getting sdrm Transform latest product Table Count...");
        String sdrmLatestProCountSQL;
        sdrmLatestProCountSQL = SDRMDataChecksSQL.GET_SDRM_LATEST_PRODUCT_COUNT;
        List<Map<String, Object>> sdrmDeltaProdHistCount = DBManager.getDBResultMap(sdrmLatestProCountSQL, Constants.AWS_URL);
        sdrmLatestProCount = ((Long) sdrmDeltaProdHistCount.get(0).get("target_count")).intValue();
        Log.info(sdrmLatestProCountSQL);
    }

    @And("^Check count of between SDRM Delta Current product availability data and SDRM Product availability History and SDRM transform latest product availability are identical$")
    public void compareCurrentDeltaAndsdrmHistExclDeltaCounts(){
        Log.info("The count for table srcTable => " + sdrmDeltaHistExclDeltaCount + " and in latest product availability => " + sdrmLatestProCount);
        Assert.assertEquals("The counts are not equal when compared with srcTable and latest product availability", sdrmDeltaHistExclDeltaCount, sdrmLatestProCount);

    }

    @Given("^Get the SDRM Duplicate count in (.*) table$")
    public static void getDuplicateCount(String tableName){
        Log.info("Getting Duplicate sdrm Latest Table Count...");
        String sdrmDupLatestProdCountSQL;
        sdrmDupLatestProdCountSQL = SDRMDataChecksSQL.GET_SDRM_DUPLICATES_LATEST_PRODUCUT_COUNT;
        List<Map<String, Object>> sdrmDupLatestTableCount = DBManager.getDBResultMap(sdrmDupLatestProdCountSQL, Constants.AWS_URL);
        sdrmDupLatestProdCount = ((Long) sdrmDupLatestTableCount.get(0).get("Duplicate_Count")).intValue();
        Log.info(sdrmDupLatestProdCountSQL);
    }

    @Then("^Check the SDRM count should be equal to Zero (.*)$")
    public void checkDupCountZero(String tableName){
        Log.info("The Duplicate count for "+tableName+" => " + sdrmDupLatestProdCount);
        Assert.assertEquals("There are Duplicate Count of sdrm in "+tableName,0,sdrmDupLatestProdCount);

    }

}
