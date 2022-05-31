package com.eph.automation.testing.steps.bcs.bcsetlcore;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.bcsetlcoresql.*;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import java.util.List;
import java.util.Map;


public class BcsEtlCoreCountChecksSteps {
    @StaticInjection
    private static String bcsCoreSQLCurrentCount;
    private static int bcsCoreCurrentCount;
    private static String leadIndicatorSQLCount;
    private static String leadIndicatorSQLCountInbound;
    private static int leadIndicatorCountInbound;
    private static int leadIndicatorCountCurr;
    private static String bcsInboundCurrentSQLCount;
    private static String bcsCoreSQLDeltaCurrentCount;
    private static String bcsCoreSQLDeltaCurrentHistCount;
    private static String bcsCoreHistSQLCurrentCount;
    private static String bcsCoreFileSQLCurrentCount;
    private static String bcsDiffDeltaAndHistSQLCurrentCount;
    private static int bcsInboundCurrentCount;
    private static int bcsCoreDeltaHistCurrentCount;
    private static int bcsCoreDeltaCurrentCount;
    private static int bcsCoreCurrentHistCount;
    private static int bcsCoreCurrentFileCount;
    private static int bcsDiffDeltaAndHistCurrentCount;
    private static int bcsExcludeCount;
    private static String bcsExclSQLCurrentCount;
    private static String bcsLatestSQLCurrentCount;
    private static int bcsSumDeltaExclCount;
    private static String bcsCoreDuplicateLatestSQLCount;
    private static int bcsCoreDuplicateLatestCount;
    private static int bcsLatestCount;
    private static String bcsSumDeltaExclSQLCurrentCount;
    private static String bcsDiffTransformFileSQLCount;
    private static int bcsDiffTransformFileCount;

    private static String noTablemsg = "No such tables found";
    @Given("^Get the total count of BCS Core from Current Tables (.*)$")
    public static void getBCSCoreCount(String tableName) throws ReflectiveOperationException {
        switch (tableName) {
            case "etl_accountable_product_current_v":
                bcsCoreSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_ACC_PROD_CURR_COUNT;
                break;
            case "etl_manifestation_current_v":
                bcsCoreSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_MANIF_CURR_COUNT;
                break;
            case "etl_person_current_v":
                bcsCoreSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_PERSON_CURR_COUNT;
                break;
            case "etl_product_current_v":
                bcsCoreSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_PRODUCT_CURR_COUNT;
                break;
            case "etl_work_person_role_current_v":
                bcsCoreSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_PERS_CURR_COUNT;
                break;
            case "etl_work_relationship_current_v":
                bcsCoreSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_RELT_CURR_COUNT;
                break;
            case "etl_work_current_v":
                bcsCoreSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_CURR_COUNT;
                break;
            case "etl_work_identifier_current_v":
                bcsCoreSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_COUNT;
                break;
            case "etl_manifestation_identifier_current_v":
                bcsCoreSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_COUNT;
                break;
            case "all_manifestation_statuses_v":
                bcsCoreSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_MANIF_STATUSES_COUNT;
                break;
            case "all_manifestation_pubdates_v":
                bcsCoreSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_MANIF_PUBDATES_COUNT;
                break;
            default:
                throw new IllegalArgumentException();
        }
        List<Map<String, Object>> bcsETLCoreCurrentTableCount = DBManager.getDBResultMap(bcsCoreSQLCurrentCount, Constants.AWS_URL);
        bcsCoreCurrentCount = ((Long) bcsETLCoreCurrentTableCount.get(0).get("Target_Count")).intValue();
    }

    @Given("^get the total count of Inbound tables (.*)$")
    public static void getCountInboundTables(String tableName) {
        Log.info("Getting Inbound Current View count of: "+tableName);
        switch (tableName) {
            case "etl_accountable_product_current_v":
                bcsInboundCurrentSQLCount = BcsEtlCoreCountChecksSql.GET_ACC_PROD_INBOUND_CURRENT_COUNT;
                break;
            case "etl_manifestation_current_v":
                bcsInboundCurrentSQLCount = BcsEtlCoreCountChecksSql.GET_MANIF_INBOUND_CURRENT_COUNT;
                break;
            case "etl_person_current_v":
                bcsInboundCurrentSQLCount = BcsEtlCoreCountChecksSql.GET_PERSON_INBOUND_CURRENT_COUNT;
                break;
            case "etl_product_current_v":
                bcsInboundCurrentSQLCount = BcsEtlCoreCountChecksSql.GET_PRODUCT_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_person_role_current_v":
                bcsInboundCurrentSQLCount = BcsEtlCoreCountChecksSql.GET_WRK_PERSON_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_relationship_current_v":
                bcsInboundCurrentSQLCount = BcsEtlCoreCountChecksSql.GET_WRK_RELT_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_current_v":
                bcsInboundCurrentSQLCount = BcsEtlCoreCountChecksSql.GET_WRK_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_identifier_current_v":
                bcsInboundCurrentSQLCount = BcsEtlCoreCountChecksSql.GET_WRK_IDENTIF_INBOUND_CURRENT_COUNT;
                break;
            case "etl_manifestation_identifier_current_v":
                bcsInboundCurrentSQLCount = BcsEtlCoreCountChecksSql.GET_MANIF_IDENTIF_INBOUND_CURRENT_COUNT;
                break;
            case "all_manifestation_statuses_v":
                bcsInboundCurrentSQLCount = BcsEtlCoreCountChecksSql.GET_MANIF_STATUSES_INBOUND_COUNT;
                break;
            case "all_manifestation_pubdates_v":
                bcsInboundCurrentSQLCount = BcsEtlCoreCountChecksSql.GET_MANIF_PUBDATES_INBOUND_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsInboundCurrentSQLCount);
        List<Map<String, Object>> bcsInboundCurrentTableCount = DBManager.getDBResultMap(bcsInboundCurrentSQLCount, Constants.AWS_URL);
        bcsInboundCurrentCount = ((Long) bcsInboundCurrentTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of BCS Inbound and BCS Core (.*) tables are identical$")
    public void compareCoreAndInboundCounts(String tableName) {
        Log.info(tableName + ": \nCore current count => " + bcsCoreCurrentCount + " and \nInbound current count  => " + bcsInboundCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with " + tableName + " and Inbound ", bcsCoreCurrentCount, bcsInboundCurrentCount);
    }

    @Given("^Get the total count of the lead indicator from the inbound table$")
    public static void getInboundLeadIndicator() {
        Log.info("Getting Lead indiccator Count from Inbound...");
        leadIndicatorSQLCountInbound = BcsEtlCoreCountChecksSql.GET_LEAD_INDICATOR_INBOUND_CURRENT_COUNT;
        //   Log.info(leadIndicatorSQLCountInbound);
        List<Map<String, Object>> leadIndicatorInboundTableCount = DBManager.getDBResultMap(leadIndicatorSQLCountInbound, Constants.AWS_URL);
        leadIndicatorCountInbound = ((Long) leadIndicatorInboundTableCount.get(0).get("Source_Count")).intValue();

    }

    @Then("^Get the count of the lead indicator from the current table of manifestation identifier$")
    public static void getCurrLeadIndicator() {
        Log.info("Getting Lead indiccator Count from manifestation identifier current...");
        leadIndicatorSQLCount = BcsEtlCoreCountChecksSql.GET_LEAD_INDICATOR_MANIF_IDENTIF_CURR_COUNT;
        //  Log.info(leadIndicatorSQLCount);
        List<Map<String, Object>> leadIndicatorCurrentTableCount = DBManager.getDBResultMap(leadIndicatorSQLCount, Constants.AWS_URL);
        leadIndicatorCountCurr = ((Long) leadIndicatorCurrentTableCount.get(0).get("Target_Count")).intValue();
    }

    @And("^Compare the inbound and current tables of manifestation identifier$")
    public void compareleadIndicatorCounts() {
        Log.info("The count for table manifestationIdentifier_current => " + leadIndicatorCountCurr + " and in Inbound  => " + leadIndicatorCountInbound);
        Assert.assertEquals("The counts are not equal when compared with manifestationIdentifier_current and Inbound ", leadIndicatorCountCurr, leadIndicatorCountInbound);
    }

    @Given("^We know the total count of delta current (.*)$")
    public static void getBCSDeltaCurrentCount(String tableName) {
        switch (tableName) {
            case "etl_delta_current_accountable_product":
                Log.info("Getting bcs Delta Accountable Product Current Table Count...");
                bcsCoreSQLDeltaCurrentCount = BcsEtlCoreCountChecksSql.GET_ACC_PROD_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_manifestation":
                Log.info("Getting bcs Delta Manifestation Current Table Count...");
                bcsCoreSQLDeltaCurrentCount = BcsEtlCoreCountChecksSql.GET_MANIF_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_person":
                Log.info("Getting bcs Delta Person Current Table Count...");
                bcsCoreSQLDeltaCurrentCount = BcsEtlCoreCountChecksSql.GET_PERSON_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_product":
                Log.info("Getting bcs Delta Product Current Table Count...");
                bcsCoreSQLDeltaCurrentCount = BcsEtlCoreCountChecksSql.GET_PRODUCT_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_work_person_role":
                Log.info("Getting bcs Delta work person Current Table Count...");
                bcsCoreSQLDeltaCurrentCount = BcsEtlCoreCountChecksSql.GET_WORK_PERSON_ROLE_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_work_relationship":
                Log.info("Getting bcs Delta work Relation Current Table Count...");
                bcsCoreSQLDeltaCurrentCount = BcsEtlCoreCountChecksSql.GET_WORK_RELATION_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_work":
                Log.info("Getting bcs Delta work Current Table Count...");
                bcsCoreSQLDeltaCurrentCount = BcsEtlCoreCountChecksSql.GET_WORK_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_work_identifier":
                Log.info("Getting bcs Delta work identifier Current Table Count...");
                bcsCoreSQLDeltaCurrentCount = BcsEtlCoreCountChecksSql.GET_WORK_IDENTIF_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_manifestation_identifier":
                Log.info("Getting bcs Delta manif identifier Current Table Count...");
                bcsCoreSQLDeltaCurrentCount = BcsEtlCoreCountChecksSql.GET_MANIF_IDENTIF_DELTA_CURR_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsCoreSQLDeltaCurrentCount);
        List<Map<String, Object>> bcsCoreDeltaCurrentTableCount = DBManager.getDBResultMap(bcsCoreSQLDeltaCurrentCount, Constants.AWS_URL);
        bcsCoreDeltaCurrentCount = ((Long) bcsCoreDeltaCurrentTableCount.get(0).get("Source_Count")).intValue();
    }

    @Then("^Get the count of delta history of current timestamp from (.*)$")
    public static void getBCSDeltaCurrentHistCount(String tableName) {
        switch (tableName) {
            case "etl_delta_history_accountable_product_part":
                Log.info("Getting bcs Delta_Hist Accountable Product Current Table Count...");
                bcsCoreSQLDeltaCurrentHistCount = BcsEtlCoreCountChecksSql.GET_ACC_PROD_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_manifestation_part":
                Log.info("Getting bcs Delta_Hist Manifestation Current Table Count...");
                bcsCoreSQLDeltaCurrentHistCount = BcsEtlCoreCountChecksSql.GET_MANIF_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_person_part":
                Log.info("Getting bcs Delta_Hist Person Current Table Count...");
                bcsCoreSQLDeltaCurrentHistCount = BcsEtlCoreCountChecksSql.GET_PERSON_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_product_part":
                Log.info("Getting bcs Delta_Hist Product Current Table Count...");
                bcsCoreSQLDeltaCurrentHistCount = BcsEtlCoreCountChecksSql.GET_PRODUCT_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_work_person_role_part":
                Log.info("Getting bcs Delta_Hist work person Current Table Count...");
                bcsCoreSQLDeltaCurrentHistCount = BcsEtlCoreCountChecksSql.GET_WORK_PERSON_ROLE_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_work_relationship_part":
                Log.info("Getting bcs Delta_Hist work Relation Current Table Count...");
                bcsCoreSQLDeltaCurrentHistCount = BcsEtlCoreCountChecksSql.GET_WORK_RELATION_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_work_part":
                Log.info("Getting bcs Delta_Hist work Current Table Count...");
                bcsCoreSQLDeltaCurrentHistCount = BcsEtlCoreCountChecksSql.GET_WORK_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_work_identifier_part":
                Log.info("Getting bcs Delta_Hist work identifier Current Table Count...");
                bcsCoreSQLDeltaCurrentHistCount = BcsEtlCoreCountChecksSql.GET_WORK_IDENTIF_DELTA_HIST_COUNT;
                break;
            case "etl_delta_history_manifestation_identifier_part":
                Log.info("Getting bcs Delta_Hist manif identifier Current Table Count...");
                bcsCoreSQLDeltaCurrentHistCount = BcsEtlCoreCountChecksSql.GET_MANIF_IDENTIF_DELTA_HIST_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsCoreSQLDeltaCurrentHistCount);
        List<Map<String, Object>> bcsCoreDeltaCurrentHistTableCount = DBManager.getDBResultMap(bcsCoreSQLDeltaCurrentHistCount, Constants.AWS_URL);
        bcsCoreDeltaHistCurrentCount = ((Long) bcsCoreDeltaCurrentHistTableCount.get(0).get("Target_Count")).intValue();
    }

    @And("^Check count of delta current table (.*) and delta history (.*) are identical$")
    public void compareDeltaAndDeltaHistCounts(String srctable, String trgtTable) {
        Log.info("The count for Delta table " + srctable + " => " + bcsCoreDeltaCurrentCount + " and in Delta Hist " + trgtTable + "  => " + bcsCoreDeltaHistCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with " + srctable + " and " + trgtTable + " ", bcsCoreDeltaHistCurrentCount, bcsCoreDeltaCurrentCount);
    }

    @Given("^Get the count of BCS core history (.*)$")
    public static void getBCSCoreHistCount(String tableName) {
        switch (tableName) {
            case "etl_transform_history_accountable_product_part":
                Log.info("Getting bcs Core Accountable Product Current Hist Table Count...");
                bcsCoreHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_ACC_PROD_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_manifestation_part":
                Log.info("Getting bcs Core Manifestation Current Hist Table Count...");
                bcsCoreHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_MANIF_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_person_part":
                Log.info("Getting bcs Core Person Current Hist Table Count...");
                bcsCoreHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_PERSON_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_product_part":
                Log.info("Getting bcs Core Product Current Hist Table Count...");
                bcsCoreHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_PRODUCT_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_work_person_role_part":
                Log.info("Getting bcs Core work person Current Hist Table Count...");
                bcsCoreHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_PERS_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_work_relationship_part":
                Log.info("Getting bcs Core work Relation Current Hist Table Count...");
                bcsCoreHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_RELT_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_work_part":
                Log.info("Getting bcs Core work Current Table Hist Count...");
                bcsCoreHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_work_identifier_part":
                Log.info("Getting bcs Core work identifier Current Hist Table Count...");
                bcsCoreHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_part":
                Log.info("Getting bcs Core manif identifier Current Hist Table Count...");
                bcsCoreHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_HIST_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsCoreHistSQLCurrentCount);
        List<Map<String, Object>> bcsETLCoreCurrentHistTableCount = DBManager.getDBResultMap(bcsCoreHistSQLCurrentCount, Constants.AWS_URL);
        bcsCoreCurrentHistCount = ((Long) bcsETLCoreCurrentHistTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of current (.*) and history (.*) are identical$")
    public void compareCurrAndCurrHistCounts(String srctable, String trgtTable) {
        Log.info("The count for Curr table " + srctable + " => " + bcsCoreCurrentCount + " and in Curr Hist " + trgtTable + "  => " + bcsCoreCurrentHistCount);
        Assert.assertEquals("The counts are not equal when compared with " + srctable + " and " + trgtTable + " ", bcsCoreCurrentCount, bcsCoreCurrentHistCount);
    }

    @Given("^Get the count of BCS core transform_file (.*)$")
    public static void getBCSCoreFileCount(String tableName) {
        switch (tableName) {
            case "etl_accountable_product_transform_file_history_part":
                Log.info("Getting bcs Core Accountable Product Current File Table Count...");
                bcsCoreFileSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_ACC_PROD_CURR_FILE_COUNT;
                break;
            case "etl_manifestation_transform_file_history_part":
                Log.info("Getting bcs Core Manifestation Current File Table Count...");
                bcsCoreFileSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_MANIF_CURR_FILE_COUNT;
                break;
            case "etl_person_transform_file_history_part":
                Log.info("Getting bcs Core Person Current File Table Count...");
                bcsCoreFileSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_PERSON_CURR_FILE_COUNT;
                break;
            case "etl_product_transform_file_history_part":
                Log.info("Getting bcs Core Product Current File Table Count...");
                bcsCoreFileSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_PRODUCT_CURR_FILE_COUNT;
                break;
            case "etl_work_person_role_transform_file_history_part":
                Log.info("Getting bcs Core work person Current File Table Count...");
                bcsCoreFileSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_PERS_CURR_FILE_COUNT;
                break;
            case "etl_work_relationship_transform_file_history_part":
                Log.info("Getting bcs Core work Relation Current File Table Count...");
                bcsCoreFileSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_RELT_CURR_FILE_COUNT;
                break;
            case "etl_work_transform_file_history_part":
                Log.info("Getting bcs Core work Current Table File Count...");
                bcsCoreFileSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_CURR_FILE_COUNT;
                break;
            case "etl_work_identifier_transform_file_history_part":
                Log.info("Getting bcs Core work identifier Current File Table Count...");
                bcsCoreFileSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_FILE_COUNT;
                break;
            case "etl_manifestation_identifier_transform_file_history_part":
                Log.info("Getting bcs Core manif identifier Current File Table Count...");
                bcsCoreFileSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_FILE_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsCoreFileSQLCurrentCount);
        List<Map<String, Object>> bcsETLCoreCurrentFileTableCount = DBManager.getDBResultMap(bcsCoreFileSQLCurrentCount, Constants.AWS_URL);
        bcsCoreCurrentFileCount = ((Long) bcsETLCoreCurrentFileTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of current (.*) and tranform_file (.*) are identical$")
    public void compareCurrAndtranformFileCounts(String srctable, String trgtTable) {
        Log.info("The count for Curr table " + srctable + " => " + bcsCoreCurrentCount + " and in tranform_file table " + trgtTable + "  => " + bcsCoreCurrentFileCount);
        Assert.assertEquals("The counts are not equal when compared with " + srctable + " and " + trgtTable + " ", bcsCoreCurrentCount, bcsCoreCurrentFileCount);
    }


    @Given("^Get the BCSCore total count difference between delta current and transform current history Table (.*)$")
    public static void getBCSCoreDiffDeltaCurrAndHistCount(String tableName) {
        switch (tableName) {
            case "etl_transform_history_accountable_product_excl_delta":
                Log.info("Getting Diff of Delta Current and Hist for Accountable Product Current File Table Count...");
                bcsDiffDeltaAndHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_ACC_PROD_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_manifestation_excl_delta":
                Log.info("Getting Diff of Delta Current and Hist for Manifestation Current File Table Count...");
                bcsDiffDeltaAndHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_MANIF_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_person_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for Person Current File Table Count...");
                bcsDiffDeltaAndHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_PERSON_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_product_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for Product Current File Table Count...");
                bcsDiffDeltaAndHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_PRODUCT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_work_person_role_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for work person Current File Table Count...");
                bcsDiffDeltaAndHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_PERS_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_work_relationship_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for work Relation Current File Table Count...");
                bcsDiffDeltaAndHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_RELT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_work_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for work Current Table File Count...");
                bcsDiffDeltaAndHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_work_identifier_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for work identifier Current File Table Count...");
                bcsDiffDeltaAndHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_IDENTIF_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for manif identifier Current File Table Count...");
                bcsDiffDeltaAndHistSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_MANIF_IDENTIF_DIFF_DELTA_AND_HIST_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsDiffDeltaAndHistSQLCurrentCount);
        List<Map<String, Object>> bcsDeltaCurrAndHistTableCount = DBManager.getDBResultMap(bcsDiffDeltaAndHistSQLCurrentCount, Constants.AWS_URL);
        bcsDiffDeltaAndHistCurrentCount = ((Long) bcsDeltaCurrAndHistTableCount.get(0).get("Source_Count")).intValue();
    }

    @Then("^Get the BCSCore (.*) exclude data count$")
    public static void getBCSCoreExcludCount(String tableName) {
        switch (tableName) {
            case "etl_transform_history_accountable_product_excl_delta":
                Log.info("Getting Exclude for Accountable Product Current File Table Count...");
                bcsExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_ACC_PROD_EXCL_COUNT;
                break;
            case "etl_transform_history_manifestation_excl_delta":
                Log.info("Getting Exclude for Manifestation Current File Table Count...");
                bcsExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_MANIF_EXCL_COUNT;
                break;
            case "etl_transform_history_person_excl_delta":
                Log.info("Getting Exclude for Person Current File Table Count...");
                bcsExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_PERSON_EXCL_COUNT;
                break;
            case "etl_transform_history_product_excl_delta":
                Log.info("Getting Exclude for Product Current File Table Count...");
                bcsExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_PRODUCT_EXCL_COUNT;
                break;
            case "etl_transform_history_work_person_role_excl_delta":
                Log.info("Getting Exclude for work person Current File Table Count...");
                bcsExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_PERS_EXCL_COUNT;
                break;
            case "etl_transform_history_work_relationship_excl_delta":
                Log.info("Getting Exclude for work Relation Current File Table Count...");
                bcsExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_RELT_EXCL_COUNT;
                break;
            case "etl_transform_history_work_excl_delta":
                Log.info("Getting Exclude for work Current Table File Count...");
                bcsExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_EXCL_COUNT;
                break;
            case "etl_transform_history_work_identifier_excl_delta":
                Log.info("Getting Exclude for work identifier Current File Table Count...");
                bcsExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_IDENTIF_EXCL_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_excl_delta":
                Log.info("Getting Exclude for manif identifier Current File Table Count...");
                bcsExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_MANIF_IDENTIF_EXCL_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsExclSQLCurrentCount);
        List<Map<String, Object>> bcsExclTableCount = DBManager.getDBResultMap(bcsExclSQLCurrentCount, Constants.AWS_URL);
        bcsExcludeCount = ((Long) bcsExclTableCount.get(0).get("Target_Count")).intValue();
    }

    @And("^Compare BCScore exclude count of (.*) and (.*) with (.*) are identical$")
    public void compareExclCounts(String srcTable1, String srcTable2, String trgtTable) {
        Log.info("The Diff of count for table " + srcTable1 + " and " + srcTable2 + " => " + bcsDiffDeltaAndHistCurrentCount + " and in " + trgtTable + " => " + bcsExcludeCount);
        Assert.assertEquals("The counts are not equal when compared with Diff of " + srcTable1 + " and " + srcTable2 + "with " + trgtTable, bcsExcludeCount, bcsDiffDeltaAndHistCurrentCount);
    }

    @Then("^Get the BCSCore (.*) latest data count$")
    public static void getBCSCoreLatestCount(String tableName) {
        switch (tableName) {
            case "etl_transform_history_accountable_product_latest":
                Log.info("Getting Latest for Accountable Product Current File Table Count...");
                bcsLatestSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_ACC_PROD_LATEST_COUNT;
                break;
            case "etl_transform_history_manifestation_latest":
                Log.info("Getting Latest for Manifestation Current File Table Count...");
                bcsLatestSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_MANIF_LATEST_COUNT;
                break;
            case "etl_transform_history_person_latest":
                Log.info("Getting Latest for Person Current File Table Count...");
                bcsLatestSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_PERSON_LATEST_COUNT;
                break;
            case "etl_transform_history_product_latest":
                Log.info("Getting Latest for Product Current File Table Count...");
                bcsLatestSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_PRODUCT_LATEST_COUNT;
                break;
            case "etl_transform_history_work_person_role_latest":
                Log.info("Getting Latest for work person Current File Table Count...");
                bcsLatestSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_PERS_LATEST_COUNT;
                break;
            case "etl_transform_history_work_relationship_latest":
                Log.info("Getting Latest for work Relation Current File Table Count...");
                bcsLatestSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_RELT_LATEST_COUNT;
                break;
            case "etl_transform_history_work_latest":
                Log.info("Getting Latest for work Current Table File Count...");
                bcsLatestSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_LATEST_COUNT;
                break;
            case "etl_transform_history_work_identifier_latest":
                Log.info("Getting Latest for work identifier Current File Table Count...");
                bcsLatestSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_IDENTIF_LATEST_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                Log.info("Getting Latest for manif identifier Current File Table Count...");
                bcsLatestSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_MANIF_IDENTIF_LATEST_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsLatestSQLCurrentCount);
        List<Map<String, Object>> bcsLatestTableCount = DBManager.getDBResultMap(bcsLatestSQLCurrentCount, Constants.AWS_URL);
        bcsLatestCount = ((Long) bcsLatestTableCount.get(0).get("Target_Count")).intValue();
    }


    @Given("^Get the sum of total count between BCSCore delta current and and Current_Exclude Table (.*)$")
    public static void getSumOfExclAndDeltaCurrCount(String tableName) {
        switch (tableName) {
            case "etl_transform_history_accountable_product_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Accountable Product Current File Table Count...");
                bcsSumDeltaExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_ACC_PROD_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_manifestation_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Manifestation Current File Table Count...");
                bcsSumDeltaExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_MANIF_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_person_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Person Current File Table Count...");
                bcsSumDeltaExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_PERSON_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_product_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Product Current File Table Count...");
                bcsSumDeltaExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_PRODUCT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_work_person_role_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work person Current File Table Count...");
                bcsSumDeltaExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_PERS_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_work_relationship_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work Relation Current File Table Count...");
                bcsSumDeltaExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_RELT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_work_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work Current Table File Count...");
                bcsSumDeltaExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_work_identifier_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work identifier Current File Table Count...");
                bcsSumDeltaExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_WRK_IDENTIF_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                Log.info("Getting Sum of Delta Curr & Excl for manif identifier Current File Table Count...");
                bcsSumDeltaExclSQLCurrentCount = BcsEtlCoreCountChecksSql.GET_MANIF_IDENTIF_SUM_DELTACURR_EXCL_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsSumDeltaExclSQLCurrentCount);
        List<Map<String, Object>> bcsSumDeltaExclTableCount = DBManager.getDBResultMap(bcsSumDeltaExclSQLCurrentCount, Constants.AWS_URL);
        bcsSumDeltaExclCount = ((Long) bcsSumDeltaExclTableCount.get(0).get("source_count")).intValue();

    }


    @And("^Compare BCSCore latest counts of (.*) and (.*) with (.*) are identical$")
    public void compareLatestCounts(String srcTable1, String srcTable2, String trgtTable) {
        Log.info("The Diff of count for table " + srcTable1 + " and " + srcTable2 + " => " + bcsLatestCount + " and in " + trgtTable + " => " + bcsSumDeltaExclCount);
        Assert.assertEquals("The counts are not equal when compared with Diff of " + srcTable1 + " and " + srcTable2 + "with " + trgtTable, bcsLatestCount, bcsSumDeltaExclCount);
    }

    @Given("^Get the BCCore Duplicate count in (.*) table$")
    public static void getDuplicateCount(String tableName) {
        switch (tableName) {
            case "etl_transform_history_accountable_product_latest":
                Log.info("Getting Duplicate Acc Prod Latest Table Count...");
                bcsCoreDuplicateLatestSQLCount = BcsEtlCoreCountChecksSql.GET_DUPLICATES_LATEST_ACC_PROD_COUNT;
                break;
            case "etl_transform_history_manifestation_latest":
                Log.info("Getting Duplicate MANIF Latest Table Count...");
                bcsCoreDuplicateLatestSQLCount = BcsEtlCoreCountChecksSql.GET_DUPLICATES_LATEST_MANIF_COUNT;
                break;
            case "etl_transform_history_person_latest":
                Log.info("Getting Duplicate Person Latest Table Count...");
                bcsCoreDuplicateLatestSQLCount = BcsEtlCoreCountChecksSql.GET_DUPLICATES_LATEST_PERSON_COUNT;
                break;
            case "etl_transform_history_product_latest":
                Log.info("Getting Duplicate Product Latest Table Count...");
                bcsCoreDuplicateLatestSQLCount = BcsEtlCoreCountChecksSql.GET_DUPLICATES_LATEST_PROD_COUNT;
                break;
            case "etl_transform_history_work_person_role_latest":
                Log.info("Getting Duplicate Work Person Role Latest Table Count...");
                bcsCoreDuplicateLatestSQLCount = BcsEtlCoreCountChecksSql.GET_DUPLICATES_LATEST_WORK_PERS_COUNT;
                break;

            case "etl_transform_history_work_relationship_latest":
                Log.info("Getting Duplicate Work Latest Table Count...");
                bcsCoreDuplicateLatestSQLCount = BcsEtlCoreCountChecksSql.GET_DUPLICATES_LATEST_WORK_RELT_COUNT;
                break;

            case "etl_transform_history_work_latest":
                Log.info("Getting Duplicate Work Latest Table Count...");
                bcsCoreDuplicateLatestSQLCount = BcsEtlCoreCountChecksSql.GET_DUPLICATES_LATEST_WORK_COUNT;
                break;
            case "etl_transform_history_work_identifier_latest":
                Log.info("Getting Duplicate Work Identifier Latest Table Count...");
                bcsCoreDuplicateLatestSQLCount = BcsEtlCoreCountChecksSql.GET_DUPLICATES_WORK_IDENTIFIER_COUNT;
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                Log.info("Getting Duplicate Manif Identifier Table Count...");
                bcsCoreDuplicateLatestSQLCount = BcsEtlCoreCountChecksSql.GET_DUPLICATES_MANIF_IDENTIFIER_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsCoreDuplicateLatestSQLCount);
        List<Map<String, Object>> bcsCoreDupLatestTableCount = DBManager.getDBResultMap(bcsCoreDuplicateLatestSQLCount, Constants.AWS_URL);
        bcsCoreDuplicateLatestCount = ((Long) bcsCoreDupLatestTableCount.get(0).get("Duplicate_Count")).intValue();
    }

    @Then("^Check the BCSCore count should be equal to Zero (.*)$")
    public void checkDupCountZero(String tableName) {
        Log.info("The Duplicate count for " + tableName + " => " + bcsCoreDuplicateLatestCount);
        Assert.assertEquals("There are Duplicate Count of " + tableName, 0, bcsCoreDuplicateLatestCount);

    }


    @Given("^Get the total count of BCS Core transform_file by diff of current and previous timestamp (.*)$")
    public static void getDiffTransformFileTimeStamp(String tableName) {
        switch (tableName) {
            case "etl_accountable_product_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for Accountable Product Current File Table Count...");
                bcsDiffTransformFileSQLCount = BcsEtlCoreCountChecksSql.GET_ACC_PROD_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_manifestation_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for Manifestation Current File Table Count...");
                bcsDiffTransformFileSQLCount = BcsEtlCoreCountChecksSql.GET_MANIF_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_person_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for Person Current File Table Count...");
                bcsDiffTransformFileSQLCount = BcsEtlCoreCountChecksSql.GET_PERSON_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_product_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for Product Current File Table Count...");
                bcsDiffTransformFileSQLCount = BcsEtlCoreCountChecksSql.GET_PRODUCT_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_work_person_role_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for work person Current File Table Count...");
                bcsDiffTransformFileSQLCount = BcsEtlCoreCountChecksSql.GET_WRK_PERS_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_work_relationship_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for work Relation Current File Table Count...");
                bcsDiffTransformFileSQLCount = BcsEtlCoreCountChecksSql.GET_WRK_RELT_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_work_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for work Current Table File Count...");
                bcsDiffTransformFileSQLCount = BcsEtlCoreCountChecksSql.GET_WRK_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_work_identifier_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for work identifier Current File Table Count...");
                bcsDiffTransformFileSQLCount = BcsEtlCoreCountChecksSql.GET_WRK_IDENTIF_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_manifestation_identifier_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for manif identifier Current File Table Count...");
                bcsDiffTransformFileSQLCount = BcsEtlCoreCountChecksSql.GET_MANIF_IDENTIF_DIFF_TRANSFORM_FILE_COUNT;
                break;
            default:
                Log.info(noTablemsg);


        }
        Log.info(bcsDiffTransformFileSQLCount);
        List<Map<String, Object>> bcsDiffTransformFileTableCount = DBManager.getDBResultMap(bcsDiffTransformFileSQLCount, Constants.AWS_URL);
        bcsDiffTransformFileCount = ((Long) bcsDiffTransformFileTableCount.get(0).get("source_count")).intValue();

    }

    @And("^Compare count of tranform_file (.*) and delta current (.*) are identical$")
    public void compareTransformFileAndDeltaCurrCounts(String srcTable, String trgtTable) {
        Log.info("Thecount for Diff of curr and previous timestamp of table " + srcTable + " => " + bcsDiffTransformFileCount + " and in " + trgtTable + " => " + bcsCoreDeltaCurrentCount);
        Assert.assertEquals("The counts are not equal when compared Diff of curr and previous timestamp of table " + srcTable + " and " + trgtTable, bcsDiffTransformFileCount, bcsCoreDeltaCurrentCount);
    }

}

