package com.eph.automation.testing.steps.sdrm;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.SDRMDataLakeSQL.SDRMDataLakeCountCheckSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import java.util.List;
import java.util.Map;

public class SDRMCountChecksSteps {

    private static String SDRMSourceCountSQL, SDRMCurrentCountSQL, SDRMProductHistory_SQLCount, SDRMProductFileHistory_SQLCount;
    private static int SDRMSourceCount, SDRMCurrentCount, SDRMProductHistoryCount, SDRMProductFileHistoryCount;
    private static String SDRMDeltaCurrentCountSQL, SDRMDeltaHistoryCountSQL, SDRMDeltaCurrent_ProductHistoryCountSQL, SDRMHistoryExclDeltaCountSQL;
    private static int SDRMDeltaCurrentCount, SDRMDeltaHistoryCount, SDRMDeltaCurrent_ProductHistoryCount, SDRMHistoryExclDeltaCount;
    public static String SDRMDeltaCurrent_SDRMHistoryExclDeltaCountSQL, SDRMLatestProductCountSQL;
    public static int SDRMDeltaCurrent_SDRMHistoryExclDeltaCount, SDRMLatestProductCount;
    private static String SDRMDuplicatesLatestProductCountSQL;
    private static int SDRMDuplicatesLatestProductCount;
    private static String SDRMFileHistoryDifferenceSQLCount, SDRMDeltaCurrentProductSQLCount;
    private static int SDRMFileHistoryDifferenceCount, SDRMDeltaCurrentProductCount;


    @Given("^Get the total count of SDRM Data from Inbound Load$")
    public void getSDRMInboundount () {
        Log.info("Getting sdrm inbound table count...");
        SDRMSourceCountSQL= SDRMDataLakeCountCheckSQL.GET_SDRM_INBOUND_SOURCE_COUNT;
        List<Map<String, Object>> SDRMInboundSourceTableCount = DBManager.getDLResultMap(SDRMSourceCountSQL, Constants.AWS_URL);
        SDRMSourceCount = ((Long) SDRMInboundSourceTableCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMSourceCountSQL);
    }

    @Given("^We know the total count of Current SDRM data from Current Product Availability$")
    public void getCountfromCurrentTables(){
        Log.info("Getting sdrm current product availability table count...");
        SDRMCurrentCountSQL=SDRMDataLakeCountCheckSQL.GET_SDRM_CURRENT_COUNT;
        List<Map<String, Object>> SDRMCurrentTableCount = DBManager.getDLResultMap(SDRMCurrentCountSQL, Constants.AWS_URL);
        SDRMCurrentCount = ((Long) SDRMCurrentTableCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMCurrentCountSQL);
    }

    @And("^Compare count of SDRM Inbound load with current Product Availability table are identical$")
    public void compareFullAndCurrentCounts(){
        Log.info("The count for table SDRM Source_inbound_part => " + SDRMSourceCount + " and in current Product Availability => " + SDRMCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with sdrm_inbound_part and in current Product Availability", SDRMCurrentCount, SDRMSourceCount);
    }

    @Given("^We know the total count of SDRM Current product availability data$")
    public void getCountfromCurrentHistoryTables(){
        Log.info("Getting SDRM Current Product Table Count...");
        SDRMCurrentCountSQL=SDRMDataLakeCountCheckSQL.GET_SDRM_CURRENT_COUNT;
        List<Map<String, Object>> SDRMCurrentTableCount = DBManager.getDLResultMap(SDRMCurrentCountSQL, Constants.AWS_URL);
        SDRMCurrentCount = ((Long) SDRMCurrentTableCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMCurrentCountSQL);
    }

    @Then("^Get the count of SDRM transform product availability history$")
    public void getCountfromtransform_fileTables(String tableName){
         Log.info("Getting SDRM Product history Table Count...");
         SDRMProductHistory_SQLCount = SDRMDataLakeCountCheckSQL.GET_SDRM_PRODUCT_HISTORY_COUNT;
         Log.info(SDRMProductHistory_SQLCount);
        List<Map<String, Object>> SDTransformFileTableCount = DBManager.getDBResultMap(SDRMProductHistory_SQLCount, Constants.AWS_URL);
        SDRMProductHistoryCount = ((Long) SDTransformFileTableCount.get(0).get("target_count")).intValue();
    }

    @And("^Check count of current product availability and product history availability are identical$")
    public void compareCurrentAndHistCounts(){
        Log.info("The count for table current product availability => " + SDRMCurrentCount + " and in product history availability => " + SDRMProductHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with current product availability and in product history availability", SDRMCurrentCount, SDRMProductHistoryCount);
    }

    @Then("^Get the count of SDRM transform product file history$")
    public void getCountfromtransform_fileHistoryTables(){
         Log.info("Getting SDRM Product file history Table Count...");
       SDRMProductFileHistory_SQLCount = SDRMDataLakeCountCheckSQL.GET_SDRM_PRODUCT_FILE_HISTORY_COUNT;
       Log.info(SDRMProductFileHistory_SQLCount);
        List<Map<String, Object>> SDTransformFileTableCount = DBManager.getDBResultMap(SDRMProductFileHistory_SQLCount, Constants.AWS_URL);
        SDRMProductFileHistoryCount = ((Long) SDTransformFileTableCount.get(0).get("target_count")).intValue();
    }

    @And("^Check count of SDRM current product availability table and SDRM product availability file history are identical$")
    public void compareCurrentAndFileHistCounts(){
        Log.info("The count for table current product availability table => " + SDRMCurrentCount + " and in product availability file history => " + SDRMProductFileHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with current product availability and in product availability file history", SDRMCurrentCount, SDRMProductFileHistoryCount);


    }
    @Given("^We know the total count of difference between Current and Previous timestamps of the SDRM transform product availability file history$")
    public void getCountfromDifferencebetweenCurrentAndPreviousFileHistory(){
        Log.info("Getting Difference between current and previous timestamps of the SDRM File history...");
        SDRMFileHistoryDifferenceSQLCount=SDRMDataLakeCountCheckSQL.GET_SDRM_FILEHISTORY_COUNT;
        List<Map<String, Object>> SDRMDeltaProductCount = DBManager.getDLResultMap(SDRMFileHistoryDifferenceSQLCount, Constants.AWS_URL);
        SDRMFileHistoryDifferenceCount = ((Long) SDRMDeltaProductCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMFileHistoryDifferenceSQLCount);
    }

    @Then("^Get the count of SDRM delta current product availability$")
    public void getCountfromSDRMDeltaCurrentTables(){
        Log.info("Getting SDRM Delta Current product Table Count...");
        SDRMDeltaCurrentProductSQLCount = SDRMDataLakeCountCheckSQL.GET_SDRM_DELTA_CURRENT_PRODUCT_COUNT;
        List<Map<String, Object>> SDRMDeltaProductHistoryCount = DBManager.getDBResultMap(SDRMDeltaCurrentProductSQLCount, Constants.AWS_URL);
        SDRMDeltaCurrentProductCount = ((Long) SDRMDeltaProductHistoryCount.get(0).get("source_count")).intValue();
        Log.info(SDRMDeltaCurrentProductSQLCount);
    }

    @And("^Check count of difference between current and previous timestamps of the SDRM transform file history product availability and SDRM delta current product availability are identical$")
    public void compareFileHistoryAndDeltaCurrentProductCounts(){
        Log.info("The count for table srcTable => " + SDRMFileHistoryDifferenceCount + " and in delta current product availability => " + SDRMDeltaCurrentProductCount);
        Assert.assertEquals("The counts are not equal when compared with srcTable and delta current product availability", SDRMFileHistoryDifferenceCount, SDRMDeltaCurrentProductCount);
    }



    @Given("^We know the total count of SDRM Delta Current product availability data$")
    public void getCountfromDeltaCurrentProductTables(){
               Log.info("Getting SDRM Delta Current Product Table Count...");
                SDRMDeltaCurrentCountSQL=SDRMDataLakeCountCheckSQL.GET_SDRM_DELTA_CURRENT_PRODUCT_COUNT;

        List<Map<String, Object>> SDRMDeltaProductCount = DBManager.getDLResultMap(SDRMDeltaCurrentCountSQL, Constants.AWS_URL);
        SDRMDeltaCurrentCount = ((Long) SDRMDeltaProductCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMDeltaCurrentCountSQL);
    }

    @Then("^Get the count of SDRM delta product availability history$")
    public void getCountfromDeltaHistoryTables(){
                Log.info("Getting SDRM Delta Product history Table Count...");
                SDRMDeltaHistoryCountSQL = SDRMDataLakeCountCheckSQL.GET_SDRM_DELTA_PRODUCT_HISTORY_COUNT;
        Log.info(SDRMDeltaHistoryCountSQL);
        List<Map<String, Object>> SDRMDeltaProductHistoryCount = DBManager.getDBResultMap(SDRMDeltaHistoryCountSQL, Constants.AWS_URL);
        SDRMDeltaHistoryCount = ((Long) SDRMDeltaProductHistoryCount.get(0).get("target_count")).intValue();
    }

    @And("^Check count of SDRM Delta current product availability table and SDRM delta product availability history are identical$")
    public void compareCurrentDeltaAndDeltaHistCounts(){
            Log.info("The count for table Delta current product availability table => " + SDRMDeltaCurrentCount + " and in delta product availability history => " + SDRMDeltaHistoryCount);
            Assert.assertEquals("The counts are not equal when compared with current product availability table and delta product availability history", SDRMDeltaCurrentCount, SDRMDeltaHistoryCount);
    }

    @Given("^We know the total count of difference between SDRM Delta Current product availability data and SDRM Product availability History$")
    public void getCountfromDeltaCurrentProductAndProductHistoryTables(String tableName){
                Log.info("Getting difference between SDRM Delta Current Product Table Count and SDRM Product History table...");
                SDRMDeltaCurrent_ProductHistoryCountSQL=SDRMDataLakeCountCheckSQL.GET_SDRM_CURRENT_PRODUCT_AND_PRODUCT_HISTORY_COUNT;
        List<Map<String, Object>> SDRMDeltaProductCount = DBManager.getDLResultMap(SDRMDeltaCurrent_ProductHistoryCountSQL, Constants.AWS_URL);
        SDRMDeltaCurrent_ProductHistoryCount = ((Long) SDRMDeltaProductCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMDeltaCurrent_ProductHistoryCountSQL);
    }

    @Then("^Get the count of SDRM transform product availability history excl delta$")
    public void getCountfromHistoryExclDeltaTables(String tableName){
                Log.info("Getting SDRM Transform history excl delta Table Count...");
                SDRMHistoryExclDeltaCountSQL = SDRMDataLakeCountCheckSQL.GET_SDRM_HISTORY_EXCL_DELTA_COUNT;
                List<Map<String, Object>> SDRMDeltaProductHistoryCount = DBManager.getDBResultMap(SDRMHistoryExclDeltaCountSQL, Constants.AWS_URL);
        SDRMHistoryExclDeltaCount = ((Long) SDRMDeltaProductHistoryCount.get(0).get("target_count")).intValue();
        Log.info(SDRMHistoryExclDeltaCountSQL);
    }

    @And("^Check count of between SDRM Delta Current product availability data and SDRM Product availability History and SDRM transform availability history excl delta are identical$")
    public void compareCurrentDelta_ProductHistoryAndHistExclDeltaCounts(String srcTable, String trgtTable){
        Log.info("The count for table sourceTable => " + SDRMDeltaCurrent_ProductHistoryCount + " and in product availability excl => " + SDRMHistoryExclDeltaCount);
        Assert.assertEquals("The counts are not equal when compared with srcTable and in product availability excl", SDRMDeltaCurrent_ProductHistoryCount, SDRMHistoryExclDeltaCount);
    }

    @Given("^We know the total count of SDRM Delta Current product availability data and SDRM prod availability History Excl Delta$")
    public void getCountfromDeltaCurrentProductAndSDRMHistoryExclDeltaTables(){
               Log.info("Getting combine of SDRM Delta Current Product Table Count and SDRM History Excl Delta table...");
                SDRMDeltaCurrent_SDRMHistoryExclDeltaCountSQL=SDRMDataLakeCountCheckSQL.GET_SDRM_DELTA_CURRENT_AND_SDRM_HISTORY_EXCL_DELTA_COUNT;
        List<Map<String, Object>> SDRMDeltaProductCount = DBManager.getDLResultMap(SDRMDeltaCurrent_SDRMHistoryExclDeltaCountSQL, Constants.AWS_URL);
        SDRMDeltaCurrent_SDRMHistoryExclDeltaCount = ((Long) SDRMDeltaProductCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMDeltaCurrent_SDRMHistoryExclDeltaCountSQL);
    }

    @Then("^Get the count of SDRM transform prod availability latest table$")
    public void getCountfromSDRMTransformLatestProductTables(){
               Log.info("Getting SDRM Transform latest product Table Count...");
                SDRMLatestProductCountSQL = SDRMDataLakeCountCheckSQL.GET_SDRM_LATEST_PRODUCT_COUNT;

        List<Map<String, Object>> SDRMDeltaProductHistoryCount = DBManager.getDBResultMap(SDRMLatestProductCountSQL, Constants.AWS_URL);
        SDRMLatestProductCount = ((Long) SDRMDeltaProductHistoryCount.get(0).get("target_count")).intValue();
        Log.info(SDRMLatestProductCountSQL);
    }

    @And("^Check count of between SDRM Delta Current product availability data and SDRM Product availability History and SDRM transform latest product availability are identical$")
    public void compareCurrentDeltaAndSDRMHistoryExclDeltaCounts(){
        Log.info("The count for table srcTable => " + SDRMDeltaCurrent_SDRMHistoryExclDeltaCount + " and in latest product availability => " + SDRMLatestProductCount);
        Assert.assertEquals("The counts are not equal when compared with srcTable and latest product availability", SDRMDeltaCurrent_SDRMHistoryExclDeltaCount, SDRMLatestProductCount);

    }

    @Given("^Get the SDRM Duplicate count in (.*) table$")
    public void getDuplicateCount(String tableName){
        switch (tableName){
            case "sdrm_transform_latest_product_availability":
                Log.info("Getting Duplicate SDRM Latest Table Count...");
                SDRMDuplicatesLatestProductCountSQL = SDRMDataLakeCountCheckSQL.GET_SDRM_DUPLICATES_LATEST_PRODUCUT_COUNT;
                break;
        }

        List<Map<String, Object>> SDRMDupLatestTableCount = DBManager.getDBResultMap(SDRMDuplicatesLatestProductCountSQL, Constants.AWS_URL);
        SDRMDuplicatesLatestProductCount = ((Long) SDRMDupLatestTableCount.get(0).get("Duplicate_Count")).intValue();
        Log.info(SDRMDuplicatesLatestProductCountSQL);

    }

    @Then("^Check the SDRM count should be equal to Zero (.*)$")
    public void checkDupCountZero(String tableName){
        Log.info("The Duplicate count for "+tableName+" => " + SDRMDuplicatesLatestProductCount);
        Assert.assertEquals("There are Duplicate Count of SDRM in "+tableName,0,SDRMDuplicatesLatestProductCount);

    }

}
