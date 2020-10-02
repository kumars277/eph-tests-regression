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
    private static String BCSCoreSQLDeltaCurrentCount;
    private static String BCSCoreSQLDeltaCurrentHistCount;
    private static String BCSCoreHistSQLCurrentCount;
    private static String BCSCoreFileSQLCurrentCount;
    private static String BCSDiffDeltaAndHistSQLCurrentCount;
    private static int BCSInboundCurrentCount;
    private static int BCSCoreDeltaHistCurrentCount;
    private static int BCSCoreDeltaCurrentCount;
    private static int BCSCoreCurrentHistCount;
    private static int BCSCoreCurrentFileCount;
    private static int BCSDiffDeltaAndHistCurrentCount;
    private static int BCSExcludeCount;
    private static String BCSExclSQLCurrentCount;
    private static String BCSLatestSQLCurrentCount;
    private static int BCSumDeltaExclCount;
    private static String BCSCoreDuplicateLatestSQLCount;
    private static int SDLatestCount;
    private static String SDDeltaCurrHistSQLCount;
    private static int SDDeltaCurrHistCount;
    private static String SDExclSQLCount;
    private static int BCSCoreDuplicateLatestCount;
    private static int BCSLatestCount;
    private static String BCSSumDeltaExclSQLCurrentCount;



    @Given("^Get the total count of BCS Core from Current Tables (.*)$")
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

    @Given("^We know the total count of delta current (.*)$")
    public void getBCSDeltaCurrentCount (String tableName) {
        switch (tableName){
            case "etl_delta_current_accountable_product":
                Log.info("Getting BCS Delta Accountable Product Current Table Count...");
                BCSCoreSQLDeltaCurrentCount = BCS_ETLCoreCountChecksSQL.GET_ACC_PROD_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_manifestation":
                Log.info("Getting BCS Delta Manifestation Current Table Count...");
                BCSCoreSQLDeltaCurrentCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_person":
                Log.info("Getting BCS Delta Person Current Table Count...");
                BCSCoreSQLDeltaCurrentCount = BCS_ETLCoreCountChecksSQL.GET_PERSON_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_product":
                Log.info("Getting BCS Delta Product Current Table Count...");
                BCSCoreSQLDeltaCurrentCount = BCS_ETLCoreCountChecksSQL.GET_PRODUCT_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_work_person_role":
                Log.info("Getting BCS Delta work person Current Table Count...");
                BCSCoreSQLDeltaCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WORK_PERSON_ROLE_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_work_relationship":
                Log.info("Getting BCS Delta work Relation Current Table Count...");
                BCSCoreSQLDeltaCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WORK_RELATION_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_work":
                Log.info("Getting BCS Delta work Current Table Count...");
                BCSCoreSQLDeltaCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WORK_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_work_identifier":
                Log.info("Getting BCS Delta work identifier Current Table Count...");
                BCSCoreSQLDeltaCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WORK_IDENTIF_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_manifestation_identifier":
                Log.info("Getting BCS Delta manif identifier Current Table Count...");
                BCSCoreSQLDeltaCurrentCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_IDENTIF_DELTA_CURR_COUNT;
                break;

        }
        Log.info(BCSCoreSQLDeltaCurrentCount);
        List<Map<String, Object>> BCSCoreDeltaCurrentTableCount = DBManager.getDBResultMap(BCSCoreSQLDeltaCurrentCount, Constants.AWS_URL);
        BCSCoreDeltaCurrentCount = ((Long) BCSCoreDeltaCurrentTableCount.get(0).get("Source_Count")).intValue();
    }

    @Then("^Get the count of delta history of current timestamp from (.*)$")
    public void getBCSDeltaCurrentHistCount (String tableName) {
        switch (tableName){
            case "etl_delta_history_accountable_product_part":
                Log.info("Getting BCS Delta_Hist Accountable Product Current Table Count...");
                BCSCoreSQLDeltaCurrentHistCount = BCS_ETLCoreCountChecksSQL.GET_ACC_PROD_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_manifestation_part":
                Log.info("Getting BCS Delta_Hist Manifestation Current Table Count...");
                BCSCoreSQLDeltaCurrentHistCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_person_part":
                Log.info("Getting BCS Delta_Hist Person Current Table Count...");
                BCSCoreSQLDeltaCurrentHistCount = BCS_ETLCoreCountChecksSQL.GET_PERSON_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_product_part":
                Log.info("Getting BCS Delta_Hist Product Current Table Count...");
                BCSCoreSQLDeltaCurrentHistCount = BCS_ETLCoreCountChecksSQL.GET_PRODUCT_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_work_person_role_part":
                Log.info("Getting BCS Delta_Hist work person Current Table Count...");
                BCSCoreSQLDeltaCurrentHistCount = BCS_ETLCoreCountChecksSQL.GET_WORK_PERSON_ROLE_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_work_relationship_part":
                Log.info("Getting BCS Delta_Hist work Relation Current Table Count...");
                BCSCoreSQLDeltaCurrentHistCount = BCS_ETLCoreCountChecksSQL.GET_WORK_RELATION_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_work_part":
                Log.info("Getting BCS Delta_Hist work Current Table Count...");
                BCSCoreSQLDeltaCurrentHistCount = BCS_ETLCoreCountChecksSQL.GET_WORK_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_work_identifier_part":
                Log.info("Getting BCS Delta_Hist work identifier Current Table Count...");
                BCSCoreSQLDeltaCurrentHistCount = BCS_ETLCoreCountChecksSQL.GET_WORK_IDENTIF_DELTA_HIST_COUNT;
                break;
            case "etl_delta_history_manifestation_identifier_part":
                Log.info("Getting BCS Delta_Hist manif identifier Current Table Count...");
                BCSCoreSQLDeltaCurrentHistCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_IDENTIF_DELTA_HIST_COUNT;
                break;

        }
        Log.info(BCSCoreSQLDeltaCurrentHistCount);
        List<Map<String, Object>> BCSCoreDeltaCurrentHistTableCount = DBManager.getDBResultMap(BCSCoreSQLDeltaCurrentHistCount, Constants.AWS_URL);
        BCSCoreDeltaHistCurrentCount = ((Long) BCSCoreDeltaCurrentHistTableCount.get(0).get("Target_Count")).intValue();
    }
    @And("^Check count of delta current table (.*) and delta history (.*) are identical$")
    public void compareDeltaAndDeltaHistCounts(String srctable, String trgtTable){
        Log.info("The count for Delta table "+srctable+" => " + BCSCoreDeltaCurrentCount + " and in Delta Hist "+trgtTable+"  => " + BCSCoreDeltaHistCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctable+" and "+trgtTable+" ", BCSCoreDeltaHistCurrentCount, BCSCoreDeltaCurrentCount);
    }

    @Given("^Get the count of BCS core history (.*)$")
    public void getBCSCoreHistCount (String tableName) {
        switch (tableName){
            case "etl_transform_history_accountable_product_part":
                Log.info("Getting BCS Core Accountable Product Current Hist Table Count...");
                BCSCoreHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_ACC_PROD_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_manifestation_part":
                Log.info("Getting BCS Core Manifestation Current Hist Table Count...");
                BCSCoreHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_MANIF_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_person_part":
                Log.info("Getting BCS Core Person Current Hist Table Count...");
                BCSCoreHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_PERSON_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_product_part":
                Log.info("Getting BCS Core Product Current Hist Table Count...");
                BCSCoreHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_PRODUCT_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_work_person_role_part":
                Log.info("Getting BCS Core work person Current Hist Table Count...");
                BCSCoreHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_PERS_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_work_relationship_part":
                Log.info("Getting BCS Core work Relation Current Hist Table Count...");
                BCSCoreHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_RELT_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_work_part":
                Log.info("Getting BCS Core work Current Table Hist Count...");
                BCSCoreHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_work_identifier_part":
                Log.info("Getting BCS Core work identifier Current Hist Table Count...");
                BCSCoreHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_part":
                Log.info("Getting BCS Core manif identifier Current Hist Table Count...");
                BCSCoreHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_HIST_COUNT;
                break;

        }
        Log.info(BCSCoreHistSQLCurrentCount);
        List<Map<String, Object>> BCS_ETLCoreCurrentHistTableCount = DBManager.getDBResultMap(BCSCoreHistSQLCurrentCount, Constants.AWS_URL);
        BCSCoreCurrentHistCount = ((Long) BCS_ETLCoreCurrentHistTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of current (.*) and history (.*) are identical$")
    public void compareCurrAndCurrHistCounts(String srctable, String trgtTable){
        Log.info("The count for Curr table "+srctable+" => " + BCSCoreCurrentCount + " and in Curr Hist "+trgtTable+"  => " + BCSCoreCurrentHistCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctable+" and "+trgtTable+" ", BCSCoreCurrentCount, BCSCoreCurrentHistCount);
    }

    @Given("^Get the count of BCS core transform_file (.*)$")
    public void getBCSCoreFileCount (String tableName) {
        switch (tableName){
            case "etl_accountable_product_transform_file_history_part":
                Log.info("Getting BCS Core Accountable Product Current File Table Count...");
                BCSCoreFileSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_ACC_PROD_CURR_FILE_COUNT;
                break;
            case "etl_manifestation_transform_file_history_part":
                Log.info("Getting BCS Core Manifestation Current File Table Count...");
                BCSCoreFileSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_MANIF_CURR_FILE_COUNT;
                break;
            case "etl_person_transform_file_history_part":
                Log.info("Getting BCS Core Person Current File Table Count...");
                BCSCoreFileSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_PERSON_CURR_FILE_COUNT;
                break;
            case "etl_product_transform_file_history_part":
                Log.info("Getting BCS Core Product Current File Table Count...");
                BCSCoreFileSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_PRODUCT_CURR_FILE_COUNT;
                break;
            case "etl_work_person_role_transform_file_history_part":
                Log.info("Getting BCS Core work person Current File Table Count...");
                BCSCoreFileSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_PERS_CURR_FILE_COUNT;
                break;
            case "etl_work_relationship_transform_file_history_part":
                Log.info("Getting BCS Core work Relation Current File Table Count...");
                BCSCoreFileSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_RELT_CURR_FILE_COUNT;
                break;
            case "etl_work_transform_file_history_part":
                Log.info("Getting BCS Core work Current Table File Count...");
                BCSCoreFileSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_CURR_FILE_COUNT;
                break;
            case "etl_work_identifier_transform_file_history_part":
                Log.info("Getting BCS Core work identifier Current File Table Count...");
                BCSCoreFileSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_FILE_COUNT;
                break;
            case "etl_manifestation_identifier_transform_file_history_part":
                Log.info("Getting BCS Core manif identifier Current File Table Count...");
                BCSCoreFileSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_FILE_COUNT;
                break;

        }
        Log.info(BCSCoreFileSQLCurrentCount);
        List<Map<String, Object>> BCS_ETLCoreCurrentFileTableCount = DBManager.getDBResultMap(BCSCoreFileSQLCurrentCount, Constants.AWS_URL);
        BCSCoreCurrentFileCount = ((Long) BCS_ETLCoreCurrentFileTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of current (.*) and tranform_file (.*) are identical$")
    public void compareCurrAndtranformFileCounts(String srctable, String trgtTable){
        Log.info("The count for Curr table "+srctable+" => " + BCSCoreCurrentCount + " and in tranform_file table "+trgtTable+"  => " + BCSCoreCurrentFileCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctable+" and "+trgtTable+" ", BCSCoreCurrentCount, BCSCoreCurrentFileCount);
    }


    @Given("^Get the BCSCore total count difference between delta current and transform current history Table (.*)$")
    public void getBCSCoreDiffDeltaCurrAndHistCount (String tableName) {
        switch (tableName){
            case "etl_transform_history_accountable_product_excl_delta":
                Log.info("Getting Diff of Delta Current and Hist for Accountable Product Current File Table Count...");
                BCSDiffDeltaAndHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_ACC_PROD_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_manifestation_excl_delta":
                Log.info("Getting Diff of Delta Current and Hist for Manifestation Current File Table Count...");
                BCSDiffDeltaAndHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_person_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for Person Current File Table Count...");
                BCSDiffDeltaAndHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_PERSON_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_product_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for Product Current File Table Count...");
                BCSDiffDeltaAndHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_PRODUCT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_work_person_role_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for work person Current File Table Count...");
                BCSDiffDeltaAndHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_PERS_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_work_relationship_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for work Relation Current File Table Count...");
                BCSDiffDeltaAndHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_RELT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_work_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for work Current Table File Count...");
                BCSDiffDeltaAndHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_work_identifier_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for work identifier Current File Table Count...");
                BCSDiffDeltaAndHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_IDENTIF_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for manif identifier Current File Table Count...");
                BCSDiffDeltaAndHistSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_IDENTIF_DIFF_DELTA_AND_HIST_COUNT;
                break;

        }
        Log.info(BCSDiffDeltaAndHistSQLCurrentCount);
        List<Map<String, Object>> BCSDeltaCurrAndHistTableCount = DBManager.getDBResultMap(BCSDiffDeltaAndHistSQLCurrentCount, Constants.AWS_URL);
        BCSDiffDeltaAndHistCurrentCount = ((Long) BCSDeltaCurrAndHistTableCount.get(0).get("Source_Count")).intValue();
    }

    @Then("^Get the BCSCore (.*) exclude data count$")
    public void getBCSCoreExcludCount (String tableName) {
        switch (tableName){
            case "etl_transform_history_accountable_product_excl_delta":
                Log.info("Getting Exclude for Accountable Product Current File Table Count...");
                BCSExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_ACC_PROD_EXCL_COUNT;
                break;
            case "etl_transform_history_manifestation_excl_delta":
                Log.info("Getting Exclude for Manifestation Current File Table Count...");
                BCSExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_EXCL_COUNT;
                break;
            case "etl_transform_history_person_excl_delta":
                Log.info("Getting Exclude for Person Current File Table Count...");
                BCSExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_PERSON_EXCL_COUNT;
                break;
            case "etl_transform_history_product_excl_delta":
                Log.info("Getting Exclude for Product Current File Table Count...");
                BCSExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_PRODUCT_EXCL_COUNT;
                break;
            case "etl_transform_history_work_person_role_excl_delta":
                Log.info("Getting Exclude for work person Current File Table Count...");
                BCSExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_PERS_EXCL_COUNT;
                break;
            case "etl_transform_history_work_relationship_excl_delta":
                Log.info("Getting Exclude for work Relation Current File Table Count...");
                BCSExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_RELT_EXCL_COUNT;
                break;
            case "etl_transform_history_work_excl_delta":
                Log.info("Getting Exclude for work Current Table File Count...");
                BCSExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_EXCL_COUNT;
                break;
            case "etl_transform_history_work_identifier_excl_delta":
                Log.info("Getting Exclude for work identifier Current File Table Count...");
                BCSExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_IDENTIF_EXCL_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_excl_delta":
                Log.info("Getting Exclude for manif identifier Current File Table Count...");
                BCSExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_IDENTIF_EXCL_COUNT;
                break;

        }
        Log.info(BCSExclSQLCurrentCount);
        List<Map<String, Object>> BCSExclTableCount = DBManager.getDBResultMap(BCSExclSQLCurrentCount, Constants.AWS_URL);
        BCSExcludeCount = ((Long) BCSExclTableCount.get(0).get("Target_Count")).intValue();
    }

    @And("^Compare BCScore exclude count of (.*) and (.*) with (.*) are identical$")
    public void compareExclCounts(String srcTable1,String srcTable2, String trgtTable){
        Log.info("The Diff of count for table "+srcTable1+" and "+srcTable2+" => " + BCSDiffDeltaAndHistCurrentCount + " and in "+trgtTable+" => " + BCSExcludeCount);
        Assert.assertEquals("The counts are not equal when compared with Diff of "+srcTable1+" and "+srcTable2+"with "+trgtTable, BCSExcludeCount, BCSDiffDeltaAndHistCurrentCount);
    }

    @Then("^Get the BCSCore (.*) latest data count$")
    public void getBCSCoreLatestCount (String tableName) {
        switch (tableName){
            case "etl_transform_history_accountable_product_latest":
                Log.info("Getting Latest for Accountable Product Current File Table Count...");
                BCSLatestSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_ACC_PROD_LATEST_COUNT;
                break;
            case "etl_transform_history_manifestation_latest":
                Log.info("Getting Latest for Manifestation Current File Table Count...");
                BCSLatestSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_LATEST_COUNT;
                break;
            case "etl_transform_history_person_latest":
                Log.info("Getting Latest for Person Current File Table Count...");
                BCSLatestSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_PERSON_LATEST_COUNT;
                break;
            case "etl_transform_history_product_latest":
                Log.info("Getting Latest for Product Current File Table Count...");
                BCSLatestSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_PRODUCT_LATEST_COUNT;
                break;
            case "etl_transform_history_work_person_role_latest":
                Log.info("Getting Latest for work person Current File Table Count...");
                BCSLatestSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_PERS_LATEST_COUNT;
                break;
            case "etl_transform_history_work_relationship_latest":
                Log.info("Getting Latest for work Relation Current File Table Count...");
                BCSLatestSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_RELT_LATEST_COUNT;
                break;
            case "etl_transform_history_work_latest":
                Log.info("Getting Latest for work Current Table File Count...");
                BCSLatestSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_LATEST_COUNT;
                break;
            case "etl_transform_history_work_identifier_latest":
                Log.info("Getting Latest for work identifier Current File Table Count...");
                BCSLatestSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_IDENTIF_LATEST_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                Log.info("Getting Latest for manif identifier Current File Table Count...");
                BCSLatestSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_IDENTIF_LATEST_COUNT;
                break;

        }
        Log.info(BCSLatestSQLCurrentCount);
        List<Map<String, Object>> BCSLatestTableCount = DBManager.getDBResultMap(BCSLatestSQLCurrentCount, Constants.AWS_URL);
        BCSLatestCount = ((Long) BCSLatestTableCount.get(0).get("Target_Count")).intValue();
    }


    @Given("^Get the sum of total count between BCSCore delta current and and Current_Exclude Table (.*)$")
    public void getSumOfExclAndDeltaCurrCount (String tableName) {
        switch (tableName){
            case "etl_transform_history_accountable_product_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Accountable Product Current File Table Count...");
                BCSSumDeltaExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_ACC_PROD_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_manifestation_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Manifestation Current File Table Count...");
                BCSSumDeltaExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_person_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Person Current File Table Count...");
                BCSSumDeltaExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_PERSON_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_product_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Product Current File Table Count...");
                BCSSumDeltaExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_PRODUCT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_work_person_role_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work person Current File Table Count...");
                BCSSumDeltaExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_PERS_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_work_relationship_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work Relation Current File Table Count...");
                BCSSumDeltaExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_RELT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_work_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work Current Table File Count...");
                BCSSumDeltaExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_work_identifier_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work identifier Current File Table Count...");
                BCSSumDeltaExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_WRK_IDENTIF_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                Log.info("Getting Sum of Delta Curr & Excl for manif identifier Current File Table Count...");
                BCSSumDeltaExclSQLCurrentCount = BCS_ETLCoreCountChecksSQL.GET_MANIF_IDENTIF_SUM_DELTACURR_EXCL_COUNT;
                break;

        }
        Log.info(BCSSumDeltaExclSQLCurrentCount);
        List<Map<String, Object>> BCSSumDeltaExclTableCount = DBManager.getDBResultMap(BCSSumDeltaExclSQLCurrentCount, Constants.AWS_URL);
        BCSumDeltaExclCount = ((Long) BCSSumDeltaExclTableCount.get(0).get("source_count")).intValue();

    }


    @And("^Compare BCSCore latest counts of (.*) and (.*) with (.*) are identical$")
    public void compareLatestCounts(String srcTable1,String srcTable2, String trgtTable){
        Log.info("The Diff of count for table "+srcTable1+" and "+srcTable2+" => " + BCSLatestCount + " and in "+trgtTable+" => " + BCSumDeltaExclCount);
        Assert.assertEquals("The counts are not equal when compared with Diff of "+srcTable1+" and "+srcTable2+"with "+trgtTable, BCSLatestCount, BCSumDeltaExclCount);
    }

    @Given("^Get the BCCore Duplicate count in (.*) table$")
    public void getDuplicateCount(String tableName){
        switch (tableName){
            case "etl_transform_history_accountable_product_latest":
                Log.info("Getting Duplicate Acc Prod Latest Table Count...");
                BCSCoreDuplicateLatestSQLCount = BCS_ETLCoreCountChecksSQL.GET_DUPLICATES_LATEST_ACC_PROD_COUNT;
                break;
            case "etl_transform_history_manifestation_latest":
                Log.info("Getting Duplicate MANIF Latest Table Count...");
                BCSCoreDuplicateLatestSQLCount = BCS_ETLCoreCountChecksSQL.GET_DUPLICATES_LATEST_MANIF_COUNT;
                break;
            case "etl_transform_history_person_latest":
                Log.info("Getting Duplicate Person Latest Table Count...");
                BCSCoreDuplicateLatestSQLCount = BCS_ETLCoreCountChecksSQL.GET_DUPLICATES_LATEST_PERSON_COUNT;
                break;
            case "etl_transform_history_product_latest":
                Log.info("Getting Duplicate Product Latest Table Count...");
                BCSCoreDuplicateLatestSQLCount = BCS_ETLCoreCountChecksSQL.GET_DUPLICATES_LATEST_PROD_COUNT;
                break;
            case "etl_transform_history_work_person_role_latest":
                Log.info("Getting Duplicate Work Person Role Latest Table Count...");
                BCSCoreDuplicateLatestSQLCount = BCS_ETLCoreCountChecksSQL.GET_DUPLICATES_LATEST_WORK_PERS_COUNT;
                break;

            case "etl_transform_history_work_relationship_latest":
                Log.info("Getting Duplicate Work Latest Table Count...");
                BCSCoreDuplicateLatestSQLCount = BCS_ETLCoreCountChecksSQL.GET_DUPLICATES_LATEST_WORK_RELT_COUNT;
                break;

            case "etl_transform_history_work_latest":
                Log.info("Getting Duplicate Work Latest Table Count...");
                BCSCoreDuplicateLatestSQLCount = BCS_ETLCoreCountChecksSQL.GET_DUPLICATES_LATEST_WORK_COUNT;
                break;
            case "etl_transform_history_work_identifier_latest":
                Log.info("Getting Duplicate Work Identifier Latest Table Count...");
                BCSCoreDuplicateLatestSQLCount = BCS_ETLCoreCountChecksSQL.GET_DUPLICATES_WORK_IDENTIFIER_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                Log.info("Getting Duplicate Manif Identifier Table Count...");
                BCSCoreDuplicateLatestSQLCount = BCS_ETLCoreCountChecksSQL.GET_DUPLICATES_MANIF_IDENTIFIER_COUNT;
                break;

        }
        Log.info(BCSCoreDuplicateLatestSQLCount);
        List<Map<String, Object>> BCSCoreDupLatestTableCount = DBManager.getDBResultMap(BCSCoreDuplicateLatestSQLCount, Constants.AWS_URL);
        BCSCoreDuplicateLatestCount = ((Long) BCSCoreDupLatestTableCount.get(0).get("Duplicate_Count")).intValue();
    }

    @Then("^Check the BCSCore count should be equal to Zero (.*)$")
    public void checkDupCountZero(String tableName){
        Log.info("The Duplicate count for "+tableName+" => " + BCSCoreDuplicateLatestCount);
        Assert.assertEquals("There are Duplicate Count of "+tableName,0,BCSCoreDuplicateLatestCount);

    }






}
