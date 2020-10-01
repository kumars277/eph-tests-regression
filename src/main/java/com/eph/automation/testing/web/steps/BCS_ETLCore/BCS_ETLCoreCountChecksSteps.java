package com.eph.automation.testing.web.steps.BCS_ETLCore;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.BCS_ETLCoreSQL.BCS_ETLCoreCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class BCS_ETLCoreCountChecksSteps {
    private static String BCSCoreSQLCurrentCount;
    private static int BCSCoreCurrentCount;
    private static String BCSInboundCurrentSQLCount;
    private static String SDPreviousSQLCount;
    private static String SDCurrentHistSQLCount;
    private static String SDPreviousHistSQLCount;
    private static String SDDeltaCurrentSQLCount;
    private static String SDDeltaCurrentHistSQLCount;
    private static int BCSInboundCurrentCount;
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



    @Given("^Get the total count of BCS Core from Inbound Tables (.*)$")
    public void getBCSCoreCount (String tableName) {
        switch (tableName){
            case "etl_accountable_product_current_v":
                Log.info("Getting BCS Core Accountable Product Current Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_ACC_PROD_CURR_COUNT;
                break;
            case "etl_manifestation_current_v":
                Log.info("Getting BCS Core Manifestation Current Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_MANIF_CURR_COUNT;
                break;
            case "etl_person_current_v":
                Log.info("Getting BCS Core Person Current Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_PERSON_CURR_COUNT;
                break;
            case "etl_product_current_v":
                Log.info("Getting BCS Core Product Current Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_PRODUCT_CURR_COUNT;
                break;
            case "etl_work_person_role_current_v":
                Log.info("Getting BCS Core work person Current Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_PERS_CURR_COUNT;
                break;
            case "etl_work_relationship_current_v":
                Log.info("Getting BCS Core work Relation Current Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_RELT_CURR_COUNT;
                break;
            case "etl_work_current_v":
                Log.info("Getting BCS Core work Current Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_CURR_COUNT;
                break;
            case "etl_work_identifier_current_v":
                Log.info("Getting BCS Core work identifier Current Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_COUNT;
                break;
            case "etl_manifestation_identifier_current_v":
                Log.info("Getting BCS Core manif identifier Current Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_COUNT;
                break;
            case "etl_accountable_product_previous_v":
                Log.info("Getting BCS Core Accountable Product Previous Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_ACC_PROD_PREV_COUNT;
                break;
            case "etl_manifestation_previous_v":
                Log.info("Getting BCS Core Manifestation Previous Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_MANIF_PREV_COUNT;
                break;
            case "etl_person_previous_v":
                Log.info("Getting BCS Core Person Previous Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_PERSON_PREV_COUNT;
                break;
            case "etl_product_previous_v":
                Log.info("Getting BCS Core Product Previous Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_PRODUCT_PREV_COUNT;
                break;
            case "etl_work_person_role_previous_v":
                Log.info("Getting BCS Core work person Previous Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_PERS_PREV_COUNT;
                break;
            case "etl_work_relationship_previous_v":
                Log.info("Getting BCS Core work Relation Previous Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_RELT_PREV_COUNT;
                break;
            case "etl_work_previous_v":
                Log.info("Getting BCS Core work Previous Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_PREV_COUNT;
                break;
            case "etl_work_identifier_previous_v":
                Log.info("Getting BCS Core work identifier Previous Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_IDENTIF_PREV_COUNT;
                break;
            case "etl_manifestation_identifier_previous_v":
                Log.info("Getting BCS Core manif identifier Previous Table Count...");
                BCSCoreSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_MANIF_IDENTIF_PREV_COUNT;
                break;

        }
        Log.info(BCSCoreSQLCurrentCount);
        List<Map<String, Object>> BCS_ETLCoreCurrentTableCount = DBManager.getDBResultMap(BCSCoreSQLCurrentCount, Constants.AWS_URL);
        BCSCoreCurrentCount = ((Long) BCS_ETLCoreCurrentTableCount.get(0).get("Target_Count")).intValue();
    }

    @Given("^We know the total count of Inbound tables (.*)$")
    public void getCountInboundTables(String tableName){
        switch (tableName){
            case "etl_accountable_product_current_v":
                Log.info("Getting Inbound Current of Acc Prod Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_ACC_PROD_INBOUND_CURRENT_COUNT;
                break;
            case "etl_manifestation_current_v":
                Log.info("Getting Inbound Current of Manifestation Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_INBOUND_CURRENT_COUNT;
                break;
            case "etl_person_current_v":
                Log.info("Getting Inbound Current of Person Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_PERSON_INBOUND_CURRENT_COUNT;
                break;
            case "etl_product_current_v":
                Log.info("Getting Inbound Current of Product Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_PRODUCT_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_person_role_current_v":
                Log.info("Getting Inbound Current of Work Person Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_WRK_PERSON_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_relationship_current_v":
                Log.info("Getting Inbound Current of Work Relation Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_WRK_RELT_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_current_v":
                Log.info("Getting Inbound Current of Work Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_WRK_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_identifier_current_v":
                Log.info("Getting Inbound Current of Work Identifier Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_WRK_IDENTIF_INBOUND_CURRENT_COUNT;
                break;
            case "etl_manifestation_identifier_current_v":
                Log.info("Getting Inbound Current of Manif Identifier Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_IDENTIF_INBOUND_CURRENT_COUNT;
                break;
            case "etl_accountable_product_previous_v":
                Log.info("Getting Inbound Previous of Acc Prod Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_ACC_PROD_INBOUND_PREVIOUS_COUNT;
                break;
            case "etl_manifestation_previous_v":
                Log.info("Getting Inbound Previous of Manifestation Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_INBOUND_PREVIOUS_COUNT;
                break;
            case "etl_person_previous_v":
                Log.info("Getting Inbound Previous of Person Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_PERSON_INBOUND_PREVIOUS_COUNT;
                break;
            case "etl_product_previous_v":
                Log.info("Getting Inbound Previous of Product Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_PRODUCT_INBOUND_PREVIOUS_COUNT;
                break;
            case "etl_work_person_role_previous_v":
                Log.info("Getting Inbound Previous of Work Person Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_WRK_PERSON_INBOUND_PREVIOUS_COUNT;
                break;
            case "etl_work_relationship_previous_v":
                Log.info("Getting Inbound Previous of Work Relation Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_WRK_RELT_INBOUND_PREVIOUS_COUNT;
                break;
            case "etl_work_previous_v":
                Log.info("Getting Inbound Previous of Work Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_WRK_INBOUND_PREVIOUS_COUNT;
                break;
            case "etl_work_identifier_previous_v":
                Log.info("Getting Inbound Previous of Work Identifier Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_WRK_IDENTIF_INBOUND_PREVIOUS_COUNT;
                break;
            case "etl_manifestation_identifier_previous_v":
                Log.info("Getting Inbound Previous of Manif Identifier Table Count...");
                BCSInboundCurrentSQLCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_IDENTIF_INBOUND_PREVIOUS_COUNT;
                break;

        }
        Log.info(BCSInboundCurrentSQLCount);
        List<Map<String, Object>> BCSInboundCurrentTableCount = DBManager.getDBResultMap(BCSInboundCurrentSQLCount, Constants.AWS_URL);
        BCSInboundCurrentCount = ((Long) BCSInboundCurrentTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of BCS Inbound and BCS Core (.*) tables are identical$")
    public void compareCoreAndInboundCounts(String tableName){
        Log.info("The count for table "+tableName+" => " + BCSCoreCurrentCount + " and in Inbound  => " + BCSInboundCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with "+tableName+" and Inbound ", BCSCoreCurrentCount, BCSInboundCurrentCount);
    }


  /*
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

    }*/

}
