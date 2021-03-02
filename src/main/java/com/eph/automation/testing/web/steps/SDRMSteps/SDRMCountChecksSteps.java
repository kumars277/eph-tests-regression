package com.eph.automation.testing.web.steps.SDRMSteps;


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


    @Given("^Get the total count of SDRM Data from Inbound Load (.*)$")
    public void getSDRMInboundount (String tableName) {
        Log.info("Getting sdrm inbound table count...");
        switch(tableName)
        {
            case "sdrm_transform_current_product_availability":
                SDRMSourceCountSQL= SDRMDataLakeCountCheckSQL.GET_SDRM_INBOUND_SOURCE_COUNT;
                break;
        }
        List<Map<String, Object>> SDRMInboundSourceTableCount = DBManager.getDLResultMap(SDRMSourceCountSQL, Constants.AWS_URL);
        SDRMSourceCount = ((Long) SDRMInboundSourceTableCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMSourceCountSQL);
    }

    @Given("^We know the total count of Current SDRM data from (.*)$")
    public void getCountfromCurrentTables(String tableName){
        Log.info("Getting sdrm current product availability table count...");
        switch(tableName)
        {
            case "sdrm_transform_current_product_availability":
                SDRMCurrentCountSQL=SDRMDataLakeCountCheckSQL.GET_SDRM_CURRENT_COUNT;
                break;
        }
        List<Map<String, Object>> SDRMCurrentTableCount = DBManager.getDLResultMap(SDRMCurrentCountSQL, Constants.AWS_URL);
        SDRMCurrentCount = ((Long) SDRMCurrentTableCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMCurrentCountSQL);
    }

    @And("^Compare count of SDRM Inbound load with current (.*) table are identical$")
    public void compareFullAndCurrentCounts(String tableName){
        Log.info("Compare "+tableName);
        Log.info("The count for table SDRM Source_inbound_part => " + SDRMSourceCount + " and in "+tableName+" => " + SDRMCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with sdrm_inbound_part and "+tableName, SDRMCurrentCount, SDRMSourceCount);
    }

    @Given("^We know the total count of SDRM Current product data from (.*)$")
    public void getCountfromCurrentHistoryTables(String tableName){
        switch (tableName){
            case "sdrm_transform_current_product_availability":
                Log.info("Getting SDRM Current Product Table Count...");
                SDRMCurrentCountSQL=SDRMDataLakeCountCheckSQL.GET_SDRM_CURRENT_COUNT;
                break;
        }
        List<Map<String, Object>> SDRMCurrentTableCount = DBManager.getDLResultMap(SDRMCurrentCountSQL, Constants.AWS_URL);
        SDRMCurrentCount = ((Long) SDRMCurrentTableCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMCurrentCountSQL);
    }

    @Then("^Get the count of SDRM transform product history (.*)$")
    public void getCountfromtransform_fileTables(String tableName){
        switch (tableName){
            case "sdrm_transform_history_product_availability_part":
                Log.info("Getting SDRM Product history Table Count...");
                SDRMProductHistory_SQLCount = SDRMDataLakeCountCheckSQL.GET_SDRM_PRODUCT_HISTORY_COUNT;
                break;
        }
        Log.info(SDRMProductHistory_SQLCount);
        List<Map<String, Object>> SDTransformFileTableCount = DBManager.getDBResultMap(SDRMProductHistory_SQLCount, Constants.AWS_URL);
        SDRMProductHistoryCount = ((Long) SDTransformFileTableCount.get(0).get("target_count")).intValue();
    }

    @And("^Check count of SDRM current product table (.*) and SDRM product history (.*) are identical$")
    public void compareCurrentAndHistCounts(String srcTable, String trgtTable){
        Log.info("The count for table "+srcTable+" => " + SDRMCurrentCount + " and in "+trgtTable+" => " + SDRMProductHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and "+trgtTable, SDRMCurrentCount, SDRMProductHistoryCount);
    }

    @Then("^Get the count of SDRM transform product file history (.*)$")
    public void getCountfromtransform_fileHistoryTables(String tableName){
        switch (tableName){
            case "sdrm_transform_file_history_product_availability_part":
                Log.info("Getting SDRM Product file history Table Count...");
                SDRMProductFileHistory_SQLCount = SDRMDataLakeCountCheckSQL.GET_SDRM_PRODUCT_FILE_HISTORY_COUNT;
                break;
        }
        Log.info(SDRMProductFileHistory_SQLCount);
        List<Map<String, Object>> SDTransformFileTableCount = DBManager.getDBResultMap(SDRMProductFileHistory_SQLCount, Constants.AWS_URL);
        SDRMProductFileHistoryCount = ((Long) SDTransformFileTableCount.get(0).get("target_count")).intValue();
    }

    @And("^Check count of SDRM current product table (.*) and SDRM product file history (.*) are identical$")
    public void compareCurrentAndFileHistCounts(String srcTable, String trgtTable){
        Log.info("The count for table "+srcTable+" => " + SDRMCurrentCount + " and in "+trgtTable+" => " + SDRMProductFileHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and "+trgtTable, SDRMCurrentCount, SDRMProductFileHistoryCount);


    }
    @Given("^We know the total count of difference between Current and Previous timestamps of the SDRM transform file history (.*)$")
    public void getCountfromDifferencebetweenCurrentAndPreviousFileHistory(String tableName){
        switch (tableName){
            case "sdrm_transform_file_history_product_availability_part":
                Log.info("Getting Difference between current and previous timestamps of the SDRM File history...");
                SDRMFileHistoryDifferenceSQLCount=SDRMDataLakeCountCheckSQL.GET_SDRM_FILEHISTORY_COUNT;
                break;
        }
        List<Map<String, Object>> SDRMDeltaProductCount = DBManager.getDLResultMap(SDRMFileHistoryDifferenceSQLCount, Constants.AWS_URL);
        SDRMFileHistoryDifferenceCount = ((Long) SDRMDeltaProductCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMFileHistoryDifferenceSQLCount);
    }

    @Then("^Get the count of SDRM delta current product (.*)$")
    public void getCountfromSDRMDeltaCurrentTables(String tableName){
        switch (tableName){
            case "sdrm_delta_current_product_availability":
                Log.info("Getting SDRM Delta Current product Table Count...");
                SDRMDeltaCurrentProductSQLCount = SDRMDataLakeCountCheckSQL.GET_SDRM_DELTA_CURRENT_PRODUCT_COUNT;
                break;
        }
        List<Map<String, Object>> SDRMDeltaProductHistoryCount = DBManager.getDBResultMap(SDRMDeltaCurrentProductSQLCount, Constants.AWS_URL);
        SDRMDeltaCurrentProductCount = ((Long) SDRMDeltaProductHistoryCount.get(0).get("source_count")).intValue();
        Log.info(SDRMDeltaCurrentProductSQLCount);
    }

    @And("^Check count of difference between current and previous timestamps of the SDRM transform file history product (.*) and SDRM delta current product (.*) are identical$")
    public void compareFileHistoryAndDeltaCurrentProductCounts(String srcTable, String trgtTable){
        Log.info("The count for table "+srcTable+" => " + SDRMFileHistoryDifferenceCount + " and in "+trgtTable+" => " + SDRMDeltaCurrentProductCount);
        Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and "+trgtTable, SDRMFileHistoryDifferenceCount, SDRMDeltaCurrentProductCount);
    }



    @Given("^We know the total count of SDRM Delta Current product data from (.*)$")
    public void getCountfromDeltaCurrentProductTables(String tableName){
        switch (tableName){
            case "sdrm_delta_current_product_availability":
                Log.info("Getting SDRM Delta Current Product Table Count...");
                SDRMDeltaCurrentCountSQL=SDRMDataLakeCountCheckSQL.GET_SDRM_DELTA_CURRENT_PRODUCT_COUNT;
                break;
        }
        List<Map<String, Object>> SDRMDeltaProductCount = DBManager.getDLResultMap(SDRMDeltaCurrentCountSQL, Constants.AWS_URL);
        SDRMDeltaCurrentCount = ((Long) SDRMDeltaProductCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMDeltaCurrentCountSQL);
    }

    @Then("^Get the count of SDRM delta product history (.*)$")
    public void getCountfromDeltaHistoryTables(String tableName){
        switch (tableName){
            case "sdrm_delta_history_product_availability_part":
                Log.info("Getting SDRM Delta Product history Table Count...");
                SDRMDeltaHistoryCountSQL = SDRMDataLakeCountCheckSQL.GET_SDRM_DELTA_PRODUCT_HISTORY_COUNT;
                break;
        }
        Log.info(SDRMDeltaHistoryCountSQL);
        List<Map<String, Object>> SDRMDeltaProductHistoryCount = DBManager.getDBResultMap(SDRMDeltaHistoryCountSQL, Constants.AWS_URL);
        SDRMDeltaHistoryCount = ((Long) SDRMDeltaProductHistoryCount.get(0).get("target_count")).intValue();
    }

    @And("^Check count of SDRM Delta current product table (.*) and SDRM delta product history (.*) are identical$")
    public void compareCurrentDeltaAndDeltaHistCounts(String srcTable, String trgtTable){
            Log.info("The count for table "+srcTable+" => " + SDRMDeltaCurrentCount + " and in "+trgtTable+" => " + SDRMDeltaHistoryCount);
            Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and "+trgtTable, SDRMDeltaCurrentCount, SDRMDeltaHistoryCount);
    }

    @Given("^We know the total count of difference between SDRM Delta Current product data and SDRM Product History (.*)$")
    public void getCountfromDeltaCurrentProductAndProductHistoryTables(String tableName){
        switch (tableName){
            case "sdrm_delta_current_product_availability":
                Log.info("Getting difference between SDRM Delta Current Product Table Count and SDRM Product History table...");
                SDRMDeltaCurrent_ProductHistoryCountSQL=SDRMDataLakeCountCheckSQL.GET_SDRM_CURRENT_PRODUCT_AND_PRODUCT_HISTORY_COUNT;
                break;
        }
        List<Map<String, Object>> SDRMDeltaProductCount = DBManager.getDLResultMap(SDRMDeltaCurrent_ProductHistoryCountSQL, Constants.AWS_URL);
        SDRMDeltaCurrent_ProductHistoryCount = ((Long) SDRMDeltaProductCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMDeltaCurrent_ProductHistoryCountSQL);
    }

    @Then("^Get the count of SDRM transform history excl delta (.*)$")
    public void getCountfromHistoryExclDeltaTables(String tableName){
        switch (tableName){
            case "sdrm_transform_history_excl_delta":
                Log.info("Getting SDRM Transform history excl delta Table Count...");
                SDRMHistoryExclDeltaCountSQL = SDRMDataLakeCountCheckSQL.GET_SDRM_HISTORY_EXCL_DELTA_COUNT;
                break;
     }
        List<Map<String, Object>> SDRMDeltaProductHistoryCount = DBManager.getDBResultMap(SDRMHistoryExclDeltaCountSQL, Constants.AWS_URL);
        SDRMHistoryExclDeltaCount = ((Long) SDRMDeltaProductHistoryCount.get(0).get("target_count")).intValue();
        Log.info(SDRMHistoryExclDeltaCountSQL);
    }

    @And("^Check count of between SDRM Delta Current product data and SDRM Product History (.*) and SDRM transform history excl delta (.*) are identical$")
    public void compareCurrentDelta_ProductHistoryAndHistExclDeltaCounts(String srcTable, String trgtTable){
        Log.info("The count for table "+srcTable+" => " + SDRMDeltaCurrent_ProductHistoryCount + " and in "+trgtTable+" => " + SDRMHistoryExclDeltaCount);
        Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and "+trgtTable, SDRMDeltaCurrent_ProductHistoryCount, SDRMHistoryExclDeltaCount);
    }

    @Given("^We know the total count of SDRM Delta Current product data and SDRM History Excl Delta (.*)$")
    public void getCountfromDeltaCurrentProductAndSDRMHistoryExclDeltaTables(String tableName){
        switch (tableName){
            case "sdrm_delta_current_product_availability":
                Log.info("Getting combine of SDRM Delta Current Product Table Count and SDRM History Excl Delta table...");
                SDRMDeltaCurrent_SDRMHistoryExclDeltaCountSQL=SDRMDataLakeCountCheckSQL.GET_SDRM_DELTA_CURRENT_AND_SDRM_HISTORY_EXCL_DELTA_COUNT;
                break;
        }
        List<Map<String, Object>> SDRMDeltaProductCount = DBManager.getDLResultMap(SDRMDeltaCurrent_SDRMHistoryExclDeltaCountSQL, Constants.AWS_URL);
        SDRMDeltaCurrent_SDRMHistoryExclDeltaCount = ((Long) SDRMDeltaProductCount.get(0).get("source_Count")).intValue();
        Log.info(SDRMDeltaCurrent_SDRMHistoryExclDeltaCountSQL);
    }

    @Then("^Get the count of SDRM transform latest product (.*)$")
    public void getCountfromSDRMTransformLatestProductTables(String tableName){
        switch (tableName){
            case "sdrm_transform_latest_product_availability":
                Log.info("Getting SDRM Transform latest product Table Count...");
                SDRMLatestProductCountSQL = SDRMDataLakeCountCheckSQL.GET_SDRM_LATEST_PRODUCT_COUNT;
                break;
        }

        List<Map<String, Object>> SDRMDeltaProductHistoryCount = DBManager.getDBResultMap(SDRMLatestProductCountSQL, Constants.AWS_URL);
        SDRMLatestProductCount = ((Long) SDRMDeltaProductHistoryCount.get(0).get("target_count")).intValue();
        Log.info(SDRMLatestProductCountSQL);
    }

    @And("^Check count of between SDRM Delta Current product data and SDRM Product History (.*) and SDRM transform latest product (.*) are identical$")
    public void compareCurrentDeltaAndSDRMHistoryExclDeltaCounts(String srcTable, String trgtTable){
        Log.info("The count for table "+srcTable+" => " + SDRMDeltaCurrent_SDRMHistoryExclDeltaCount + " and in "+trgtTable+" => " + SDRMLatestProductCount);
        Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and "+trgtTable, SDRMDeltaCurrent_SDRMHistoryExclDeltaCount, SDRMLatestProductCount);


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
