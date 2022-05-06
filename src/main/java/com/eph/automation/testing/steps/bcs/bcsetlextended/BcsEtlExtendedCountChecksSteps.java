package com.eph.automation.testing.steps.bcs.bcsetlextended;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.bcsetlextendedsql.BcsEtlExtendedCountChecksSql;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class BcsEtlExtendedCountChecksSteps {

    private static String bcsExtSqlCurrCount;
    private static int bcsExtCurrCount;

    private static String bcsExtSqlDeltaCurrCount;
    private static int bcsExtDeltaCurrCount;

    private static String bcsExtInboundCurrSqlCount;
    private static int bcsExtInboundCurrCount;

    private static String bcsExtSqlDeltaCurrHistCount;
    private static int bcsExtDeltaHistCurrCount;

    private static String bcsExtFileSqlCurrCount;
    private static int bcsExtCurrFileCount;

    private static String bcsExtHistSqlCurrCount;
    private static int bcsExtCurrHistCount;

    private static String bcsExtDiffTransFileSqlCount;
    private static int bcsExtDiffTransFileCount;

    private static String bcsExtDiffDeltaAndHistSqlCurrCount;
    private static int bcsExtDiffDeltaAndHistCurrCount;

    private static String bcsExtExclSqlCurrCount;
    private static int bcsExtExclCount;

    private static String bcsExtSumDeltaExclSqlCurrCount;
    private static int bcsExtSumDeltaExclCount;

    private static String bcsExtLatestSqlCurrCount;
    private static int bcsExtLatestCount;

    private static String bcsExtDupLatestSqlCount;
    private static int bcsExtDupLatestCount;

    private static String noTablemsg = "No tables found";

    @Given("^Get the total count of BCS Extended from Current Tables (.*)$")
    public static void getBCSExtendedCount(String tableName) {
        switch (tableName) {
            case "etl_availability_extended_current_v":
                Log.info("Getting bcs Extended Availability Extended Current Table Count...");
                bcsExtSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_BCS_EXTENDED_AVAILABILITY_EXTENDED_CURR_COUNT;
                break;
            case "etl_manifestation_extended_current_v":
                Log.info("Getting bcs Extended Manifestation Extended Current Table Count...");
                bcsExtSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_BCS_EXTENDED_MANIFESTATION_EXTENDED_CURR_COUNT;
                break;
            case "etl_page_count_extended_current_v":
                Log.info("Getting bcs Extended Page Extended Current Table Count...");
                bcsExtSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_BCS_EXTENDED_PAGE_COUNT_EXTENDED_CURR_COUNT;
                break;
            case "etl_url_extended_current_v":
                Log.info("Getting bcs Extended URL Extended Current Table Count...");
                bcsExtSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_BCS_EXTENDED_URL_EXTENDED_CURR_COUNT;
                break;
            case "etl_work_extended_current_v":
                Log.info("Getting bcs Extended Work Extended Current Table Count...");
                bcsExtSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_BCS_EXTENDED_WORK_EXTENDED_CURR_COUNT;
                break;
            case "etl_work_subject_area_extended_current_v":
                Log.info("Getting bcs Extended Work Subject Area Extended Current Table Count...");
                bcsExtSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_BCS_EXTENDED_WORK_SUBJECT_AREA_EXTENDED_CURR_COUNT;
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                Log.info("Getting bcs Extended Manifestation Restrictions Extended Current Table Count...");
                bcsExtSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_BCS_EXTENDED_MANIFESTATION_RESTRICTIONS_EXTENDED_CURR_COUNT;
                break;
            case "etl_product_prices_extended_current_v":
                Log.info("Getting bcs Extended Product Prices Extended Current Table Count...");
                bcsExtSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_BCS_EXTENDED_PRODUCT_PRICES_EXTENDED_CURR_COUNT;
                break;
            case "etl_work_person_role_extended_current_v":
                Log.info("Getting bcs Extended Work Person Role Current Table Count...");
                bcsExtSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_BCS_EXTENDED_WORK_PERSON_ROLE_EXTENDED_CURR_COUNT;
                break;
             default:
                 Log.info(noTablemsg);

        }
        Log.info(bcsExtSqlCurrCount);
        List<Map<String, Object>> bcsExtCurrTableCount = DBManager.getDBResultMap(bcsExtSqlCurrCount, Constants.AWS_URL);
        bcsExtCurrCount = ((Long) bcsExtCurrTableCount.get(0).get("Target_Count")).intValue();
    }

    @When("^Get the total count of BCS Extended Inbound tables (.*)$")
    public static void getCountInboundTables(String tableName) {
        switch (tableName) {
            case "etl_availability_extended_current_v":
                Log.info("Getting Inbound Current of Availability Extended Table Count...");
                bcsExtInboundCurrSqlCount = BcsEtlExtendedCountChecksSql.GET_AVAILABILITY_INBOUND_CURRENT_COUNT;
                break;
            case "etl_manifestation_extended_current_v":
                Log.info("Getting Inbound Current of Manifestation Extended Table Count...");
                bcsExtInboundCurrSqlCount = BcsEtlExtendedCountChecksSql.GET_MANIFESTATION_INBOUND_CURRENT_COUNT;
                break;
            case "etl_page_count_extended_current_v":
                Log.info("Getting Inbound Current of Page count Extended Table Count...");
                bcsExtInboundCurrSqlCount = BcsEtlExtendedCountChecksSql.GET_PAGE_COUNT_INBOUND_CURRENT_COUNT;
                break;
            case "etl_url_extended_current_v":
                Log.info("Getting Inbound Current of URL Extended Table Count...");
                bcsExtInboundCurrSqlCount = BcsEtlExtendedCountChecksSql.GET_URL_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_extended_current_v":
                Log.info("Getting Inbound Current of Work Extended Table Count...");
                bcsExtInboundCurrSqlCount = BcsEtlExtendedCountChecksSql.GET_WORK_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_subject_area_extended_current_v":
                Log.info("Getting Inbound Current of Work Subject Area Extended Table Count...");
                bcsExtInboundCurrSqlCount = BcsEtlExtendedCountChecksSql.GET_WORK_SUBJECT_AREA_INBOUND_CURRENT_COUNT;
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                Log.info("Getting Inbound Current of Manifestation Restrictions Extended Table Count...");
                bcsExtInboundCurrSqlCount = BcsEtlExtendedCountChecksSql.GET_MANIFESTATION_RESTRICTIONS_INBOUND_CURRENT_COUNT;
                break;
            case "etl_product_prices_extended_current_v":
                Log.info("Getting Inbound Current of Product Prices Extended Table Count...");
                bcsExtInboundCurrSqlCount = BcsEtlExtendedCountChecksSql.GET_PRODUCT_PRICES_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_person_role_extended_current_v":
                Log.info("Getting Inbound Current of Work Person Role Extended Table Count...");
                bcsExtInboundCurrSqlCount = BcsEtlExtendedCountChecksSql.GET_WORK_PERSON_ROLE_INBOUND_CURRENT_COUNT;
                break;
              default:
                  Log.info(noTablemsg);

        }
        Log.info(bcsExtInboundCurrSqlCount);
        List<Map<String, Object>> bcsEtlExtCurrTableCount = DBManager.getDBResultMap(bcsExtInboundCurrSqlCount, Constants.AWS_URL);
        bcsExtInboundCurrCount = ((Long) bcsEtlExtCurrTableCount.get(0).get("Source_Count")).intValue();
    }

    @Then("^Compare count of BCS Inbound and BCS Extended (.*) tables are identical$")
    public void compareExtendedAndInboundCounts(String tableName) {
        Log.info("The count for table " + tableName + " => " + bcsExtCurrCount + " and in Inbound  => " + bcsExtInboundCurrCount);
        Assert.assertEquals("The counts are not equal when compared with " + tableName + " and Inbound ", bcsExtInboundCurrCount,bcsExtCurrCount);

    }

    @Given("^We know the total count of BCS Extended delta current (.*)$")
    public static void getBCSExtDeltaCurrentCount (String tableName) {
        switch (tableName){
            case "etl_delta_current_extended_availability":
                Log.info("Getting bcs etl_delta_current_extended_availability Table Count...");
                bcsExtSqlDeltaCurrCount = BcsEtlExtendedCountChecksSql.GET_AVAILABILITY_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_manifestation":
                Log.info("Getting bcs etl_delta_current_extended_manifestation Table Count...");
                bcsExtSqlDeltaCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_EXT_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_page_count":
                Log.info("Getting bcs etl_delta_current_extended_page_count Table Count...");
                bcsExtSqlDeltaCurrCount = BcsEtlExtendedCountChecksSql.GET_PAGE_COUNT_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_url":
                Log.info("Getting bcs etl_delta_current_extended_url Table Count...");
                bcsExtSqlDeltaCurrCount = BcsEtlExtendedCountChecksSql.GET_URL_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_work":
                Log.info("Getting bcs etl_delta_current_extended_work Table Count...");
                bcsExtSqlDeltaCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_EXT_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_work_subject_area":
                Log.info("Getting bcs etl_delta_current_extended_work_subject_area Table Count...");
                bcsExtSqlDeltaCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_SUBJ_AREA_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_manifestation_restrictions":
                Log.info("Getting bcs etl_delta_current_extended_manifestation_restrictions Table Count...");
                bcsExtSqlDeltaCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_REST_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_product_prices":
                Log.info("Getting bcs etl_delta_current_extended_product_prices Table Count...");
                bcsExtSqlDeltaCurrCount = BcsEtlExtendedCountChecksSql.GET_PROD_PRICE_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_work_person_role":
                Log.info("Getting bcs etl_delta_current_extended_work_person_role Table Count...");
                bcsExtSqlDeltaCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_PERS_ROLE_DELTA_CURR_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsExtSqlDeltaCurrCount);
        List<Map<String, Object>> bcsCoreDeltaCurrTableCount = DBManager.getDBResultMap(bcsExtSqlDeltaCurrCount, Constants.AWS_URL);
        bcsExtDeltaCurrCount = ((Long) bcsCoreDeltaCurrTableCount.get(0).get("Source_Count")).intValue();
    }

    @Then("^Get the count of BCS Extended delta history of current timestamp from (.*)$")
    public static void getBCSExtDeltaCurrentHistCount (String tableName) {
        switch (tableName){
            case "etl_delta_history_extended_availability_part":
                Log.info("Getting bcs etl_delta_history_extended_availability_part Table Count...");
                bcsExtSqlDeltaCurrHistCount = BcsEtlExtendedCountChecksSql.GET_AVAILABILITY_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_manifestation_part":
                Log.info("Getting etl_delta_history_extended_manifestation_part Table Count...");
                bcsExtSqlDeltaCurrHistCount = BcsEtlExtendedCountChecksSql.GET_MANIF_EXT_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_page_count_part":
                Log.info("Getting etl_delta_history_extended_page_count_part Table Count...");
                bcsExtSqlDeltaCurrHistCount = BcsEtlExtendedCountChecksSql.GET_PAGE_COUNT_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_url_part":
                Log.info("Getting etl_delta_history_extended_url_part Table Count...");
                bcsExtSqlDeltaCurrHistCount = BcsEtlExtendedCountChecksSql.GET_URL_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_work_part":
                Log.info("Getting etl_delta_history_extended_work_part Table Count...");
                bcsExtSqlDeltaCurrHistCount = BcsEtlExtendedCountChecksSql.GET_WORK_EXT_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_work_subject_area_part":
                Log.info("Getting etl_delta_history_extended_work_subject_area_part Table Count...");
                bcsExtSqlDeltaCurrHistCount = BcsEtlExtendedCountChecksSql.GET_WORK_SUBJ_AREA_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_manifestation_restrictions_part":
                Log.info("Getting etl_delta_history_extended_manifestation_restrictions_part Table Count...");
                bcsExtSqlDeltaCurrHistCount = BcsEtlExtendedCountChecksSql.GET_MANIF_REST_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_product_prices_part":
                Log.info("Getting etl_delta_history_extended_product_prices_part Table Count...");
                bcsExtSqlDeltaCurrHistCount = BcsEtlExtendedCountChecksSql.GET_PROD_PRICE_DELTA_HIST_COUNT;
                break;
            case "etl_delta_history_extended_work_person_role_part":
                Log.info("Getting etl_delta_history_extended_work_person_role_part Table Count...");
                bcsExtSqlDeltaCurrHistCount = BcsEtlExtendedCountChecksSql.GET_WORK_PERS_ROLE_DELTA_HIST_COUNT;
                break;
             default:
                 Log.info(noTablemsg);

        }
        Log.info(bcsExtSqlDeltaCurrHistCount);
        List<Map<String, Object>> bcsCoreDeltaCurrHisTableCount = DBManager.getDBResultMap(bcsExtSqlDeltaCurrHistCount, Constants.AWS_URL);
        bcsExtDeltaHistCurrCount = ((Long) bcsCoreDeltaCurrHisTableCount.get(0).get("Target_Count")).intValue();
    }
    @And("^Check count of BCS Extended delta current table (.*) and delta history (.*) are identical$")
    public void compareBCSExtDeltaAndDeltaHistCounts(String srctable, String trgtTable){
        Log.info("The count for Delta table "+srctable+" => " + bcsExtDeltaCurrCount + " and in Delta Hist "+trgtTable+"  => " + bcsExtDeltaHistCurrCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctable+" and "+trgtTable+" ", bcsExtDeltaHistCurrCount, bcsExtDeltaCurrCount);
    }

    @Then ("^Get the count of BCS Extended history (.*)$")
    public static void getBCSExtHistCount (String tableName) {
        switch (tableName){
            case "etl_transform_history_extended_availability_part":
                Log.info("Getting etl_transform_history_extended_availability_part Table Count...");
                bcsExtHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_AVAILABILITY_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_part":
                Log.info("Getting etl_transform_history_extended_manifestation_part Table Count...");
                bcsExtHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_EXT_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_page_count_part":
                Log.info("Getting etl_transform_history_extended_page_count_part Table Count...");
                bcsExtHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_PAGE_COUNT_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_url_part":
                Log.info("Getting etl_transform_history_extended_url_part Table Count...");
                bcsExtHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_URL_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_part":
                Log.info("Getting etl_transform_history_extended_work_part Table Count...");
                bcsExtHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_EXT_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_subject_area_part":
                Log.info("Getting etl_transform_history_extended_work_subject_area_part Table Count...");
                bcsExtHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_SUBJ_AREA_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_restrictions_part":
                Log.info("Getting etl_transform_history_extended_manifestation_restrictions_part Table Count...");
                bcsExtHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_REST_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_product_prices_part":
                Log.info("Getting etl_transform_history_extended_product_prices_part Table Count...");
                bcsExtHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_PROD_PRICE_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_person_role_part":
                Log.info("Getting etl_transform_history_extended_work_person_role_part Table Count...");
                bcsExtHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_PERS_ROLE_CURR_HIST_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsExtHistSqlCurrCount);
        List<Map<String, Object>> bcsEtlCoreCurrHistTableCount = DBManager.getDBResultMap(bcsExtHistSqlCurrCount, Constants.AWS_URL);
        bcsExtCurrHistCount = ((Long) bcsEtlCoreCurrHistTableCount.get(0).get("Source_Count")).intValue();
    }
    @And ("^Compare count of BCS Extended current (.*) and history (.*) are identical$")
    public void compareBCSExtCurrAndCurrHistCounts(String srctable, String trgtTable){
        Log.info("The count for Curr table "+srctable+" => " + bcsExtCurrCount + " and in Curr Hist "+trgtTable+"  => " + bcsExtCurrHistCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctable+" and "+trgtTable+" ", bcsExtCurrCount, bcsExtCurrHistCount);
    }

    @Then ("^Get the count of BCS Extended transform_file (.*)$")
    public static void getBCSExtFileCount (String tableName) {
        switch (tableName){
            case "etl_availability_extended_transform_file_history_part":
                Log.info("Getting etl_availability_extended_transform_file_history_part Table Count...");
                bcsExtFileSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_AVAILABILITY_CURR_FILE_COUNT;
                break;
            case "etl_manifestation_extended_transform_file_history_part":
                Log.info("Getting etl_manifestation_extended_transform_file_history_part Table Count...");
                bcsExtFileSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_EXT_CURR_FILE_COUNT;
                break;
            case "etl_page_count_extended_transform_file_history_part":
                Log.info("Getting etl_page_count_extended_transform_file_history_part Table Count...");
                bcsExtFileSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_PAGE_COUNT_CURR_FILE_COUNT;
                break;
            case "etl_url_extended_transform_file_history_part":
                Log.info("Getting etl_url_extended_transform_file_history_part Table Count...");
                bcsExtFileSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_URL_CURR_FILE_COUNT;
                break;
            case "etl_work_extended_transform_file_history_part":
                Log.info("Getting etl_work_extended_transform_file_history_part Table Count...");
                bcsExtFileSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_EXT_CURR_FILE_COUNT;
                break;
            case "etl_work_subject_area_extended_transform_file_history_part":
                Log.info("Getting etl_work_subject_area_extended_transform_file_history_part Table Count...");
                bcsExtFileSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_SUBJ_AREA_CURR_FILE_COUNT;
                break;
            case "etl_manifestation_restrictions_extended_transform_file_history_part":
                Log.info("Getting etl_manifestation_restrictions_extended_transform_file_history_part Table Count...");
                bcsExtFileSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_REST_CURR_FILE_COUNT;
                break;
            case "etl_product_prices_extended_transform_file_history_part":
                Log.info("Getting etl_product_prices_extended_transform_file_history_part Table Count...");
                bcsExtFileSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_PROD_PRICE_CURR_FILE_COUNT;
                break;
            case "etl_work_person_role_extended_transform_file_history_part":
                Log.info("Getting etl_work_person_role_extended_transform_file_history_part Table Count...");
                bcsExtFileSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_PERS_ROLE_CURR_FILE_COUNT;
                break;
             default:
                 Log.info(noTablemsg);

        }
        Log.info(bcsExtFileSqlCurrCount);
        List<Map<String, Object>> bcsEtlExtCurrFileTableCount = DBManager.getDBResultMap(bcsExtFileSqlCurrCount, Constants.AWS_URL);
        bcsExtCurrFileCount = ((Long) bcsEtlExtCurrFileTableCount.get(0).get("Source_Count")).intValue();
    }
    @And ("^Compare BCS Extended count of current (.*) and tranform_file (.*) are identical$")
    public void compareBCSExtCurrAndCurrFileCounts(String srctable, String trgtTable){
        Log.info("The count for Curr table "+srctable+" => " + bcsExtCurrCount + " and in Curr File "+trgtTable+"  => " + bcsExtCurrFileCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctable+" and "+trgtTable+" ", bcsExtCurrCount, bcsExtCurrFileCount);
    }


    @Given("^Get the total count of BCS Extended transform_file by diff of current and previous timestamp (.*)$")
    public static void getDiffPrevCurrTranFileTimeStamp (String tableName) {
        switch (tableName){
            case "etl_availability_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_availability_extended_transform_file_history_part Table Count...");
                bcsExtDiffTransFileSqlCount = BcsEtlExtendedCountChecksSql.GET_AVAILABILTIY_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_manifestation_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_manifestation_extended_transform_file_history_part Table Count...");
                bcsExtDiffTransFileSqlCount = BcsEtlExtendedCountChecksSql.GET_MANIF_EXT_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_page_count_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_page_count_extended_transform_file_history_part Table Count...");
                bcsExtDiffTransFileSqlCount = BcsEtlExtendedCountChecksSql.GET_PAGE_COUNT_DIFF_TRANSFORM_FILE_COUNT;
                break;
           case "etl_url_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_url_extended_transform_file_history_part Table Count...");
                bcsExtDiffTransFileSqlCount = BcsEtlExtendedCountChecksSql.GET_URL_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_work_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_work_extended_transform_file_history_part Table Count...");
                bcsExtDiffTransFileSqlCount = BcsEtlExtendedCountChecksSql.GET_WRK_EXT_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_work_subject_area_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_work_subject_area_extended_transform_file_history_part Table Count...");
                bcsExtDiffTransFileSqlCount = BcsEtlExtendedCountChecksSql.GET_WRK_SUBJ_AREA_DIFF_TRANSFORM_FILE_COUNT;
                break;
         case "etl_manifestation_restrictions_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_manifestation_restrictions_extended_transform_file_history_part Count...");
                bcsExtDiffTransFileSqlCount = BcsEtlExtendedCountChecksSql.GET_MANIF_RESTRICTION_DIFF_TRANSFORM_FILE_COUNT;
                break;
           case "etl_product_prices_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_product_prices_extended_transform_file_history_part Table Count...");
                bcsExtDiffTransFileSqlCount = BcsEtlExtendedCountChecksSql.GET_PROD_PRICE_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_work_person_role_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_work_person_role_extended_transform_file_history_part Table Count...");
                bcsExtDiffTransFileSqlCount = BcsEtlExtendedCountChecksSql.GET_WORK_PERSON_ROLE_DIFF_TRANSFORM_FILE_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsExtDiffTransFileSqlCount);
        List<Map<String, Object>> bcsDiffTransformFileTableCount = DBManager.getDBResultMap(bcsExtDiffTransFileSqlCount, Constants.AWS_URL);
        bcsExtDiffTransFileCount = ((Long) bcsDiffTransformFileTableCount.get(0).get("target_Count")).intValue();
    }

    @And("^Compare count of BCS Ext tranform_file (.*) and delta current (.*) are identical$")
    public void compareTransformFileAndDeltaCurrCounts(String srcTable,String trgtTable){
        Log.info("The count for Diff of curr and previous timestamp of table "+srcTable+" => " + bcsExtDiffTransFileCount + " and in "+trgtTable+" => " + bcsExtDeltaCurrCount);
        Assert.assertEquals("The counts are not equal when compared Diff of curr and previous timestamp of table "+srcTable+" and "+trgtTable, bcsExtDiffTransFileCount, bcsExtDeltaCurrCount);
    }


    @Given ("^Get the BCS Extended Ext total count difference between delta current and transform current history Table (.*)$")
    public static void getBCSExtDiffDeltaCurrAndHistCount (String tableName) {
        switch (tableName){
            case "etl_transform_history_extended_availability_excl_delta":
                Log.info("Getting Diff of Delta Current and Hist for availability Table Count...");
                bcsExtDiffDeltaAndHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_AVAILABILITY_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_excl_delta":
                Log.info("Getting Diff of Delta Current and Hist for Manifestation Extended Table Count...");
                bcsExtDiffDeltaAndHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_EXT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_page_count_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for Page Count Ext Table Count...");
                bcsExtDiffDeltaAndHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_PAGE_COUNT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_url_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for URL Table Count...");
                bcsExtDiffDeltaAndHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_URL_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for work extended Table Count...");
                bcsExtDiffDeltaAndHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_EXT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_subject_area_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for work Subject area Table Count...");
                bcsExtDiffDeltaAndHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WRK_SUBJ_AREA_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for Manif Restrition Table Count...");
                bcsExtDiffDeltaAndHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_RESTRICT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_product_prices_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for Product Price Ext Table Count...");
                bcsExtDiffDeltaAndHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_PROD_PRICE_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_person_role_excl_delta":
                Log.info("Getting bcs Diff of Delta Current and Hist for Work person role Table Count...");
                bcsExtDiffDeltaAndHistSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_PERS_ROLE_DIFF_DELTA_AND_HIST_COUNT;
                break;
             default:
                 Log.info(noTablemsg);

        }
        Log.info(bcsExtDiffDeltaAndHistSqlCurrCount);
        List<Map<String, Object>> bcsDeltaCurrAndHistTableCount = DBManager.getDBResultMap(bcsExtDiffDeltaAndHistSqlCurrCount, Constants.AWS_URL);
        bcsExtDiffDeltaAndHistCurrCount = ((Long) bcsDeltaCurrAndHistTableCount.get(0).get("Source_Count")).intValue();
    }


    @Then ("^Get the BCS Extended (.*) exclude data count$")
    public static void getBCSExtExcludCount (String tableName) {
        switch (tableName){
            case "etl_transform_history_extended_availability_excl_delta":
                Log.info("Getting Exclude for Availability Table Count...");
                bcsExtExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_AVAILABILITY_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_excl_delta":
                Log.info("Getting Exclude for Manifestation Extended Table Count...");
                bcsExtExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_EXT_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_page_count_excl_delta":
                Log.info("Getting Exclude for Page Count Ext Table Count...");
                bcsExtExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_PAGE_COUNT_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_url_excl_delta":
                Log.info("Getting Exclude for URL Table Count...");
                bcsExtExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_URL_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_excl_delta":
                Log.info("Getting Exclude for work extended Table Count...");
                bcsExtExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WRK_EXT_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_subject_area_excl_delta":
                Log.info("Getting Exclude for work Subject area Table Count...");
                bcsExtExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WRK_SUBJ_AREA_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
                Log.info("Getting Exclude for Manif Restrition Count...");
                bcsExtExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_RESTRICT_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_product_prices_excl_delta":
                Log.info("Getting Exclude for Product Price Ext Table Count...");
                bcsExtExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_PROD_PRICE_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_person_role_excl_delta":
                Log.info("Getting Exclude for Work person role Table Count...");
                bcsExtExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_PERS_ROLE_EXCL_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(bcsExtExclSqlCurrCount);
        List<Map<String, Object>> bcsExclTableCount = DBManager.getDBResultMap(bcsExtExclSqlCurrCount, Constants.AWS_URL);
        bcsExtExclCount = ((Long) bcsExclTableCount.get(0).get("Target_Count")).intValue();
    }
    @And ("^Compare BCS Extended exclude count are identical (.*)$")
    public void compareBCSExtExclCounts(String trgtTable){
        Log.info("The diff of count for table delta and currentHistory => " + bcsExtDiffDeltaAndHistCurrCount + " and in "+trgtTable+" => " + bcsExtExclCount);
        Assert.assertEquals("The diff of count for table delta and currentHistory with "+trgtTable, bcsExtExclCount, bcsExtDiffDeltaAndHistCurrCount);
    }

    @Given ("^Get the sum of total count between BCS Extended delta current and Current_Exclude Table (.*)$")
    public static void getBCSExtSumOfExclAndDeltaCurrCount (String tableName) {
        switch (tableName) {
            case "etl_transform_history_extended_availability_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Availability Table Count...");
                bcsExtSumDeltaExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_AVAILABILITY_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Manifestation Extended Table Count...");
                bcsExtSumDeltaExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_EXT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_page_count_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Page Count Ext Table Count...");
                bcsExtSumDeltaExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_PAGE_COUNT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_url_latest":
                Log.info("Getting Sum of Delta Curr & Excl for URL Table Count...");
                bcsExtSumDeltaExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_URL_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work extended Table Count...");
                bcsExtSumDeltaExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WRK_EXT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_subject_area_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work Subject area Table Count...");
                bcsExtSumDeltaExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WRK_SUBJ_AREA_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_restrictions_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Manif Restrition Count...");
                bcsExtSumDeltaExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_RESTRICT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_product_prices_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Product PRice Table Count...");
                bcsExtSumDeltaExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_ROD_PRICE_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_person_role_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work person role Table Count...");
                bcsExtSumDeltaExclSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WORK_PERS_ROLE_SUM_DELTACURR_EXCL_COUNT;
                break;
             default:
                 Log.info(noTablemsg);

        }
        Log.info(bcsExtSumDeltaExclSqlCurrCount);
        List<Map<String, Object>> bcsSumDeltaExclTableCount = DBManager.getDBResultMap(bcsExtSumDeltaExclSqlCurrCount, Constants.AWS_URL);
        bcsExtSumDeltaExclCount = ((Long) bcsSumDeltaExclTableCount.get(0).get("source_count")).intValue();
    }
        @Then("^Get the BCS Extended (.*) latest data count$")
        public static void getbcsExtLatestCount(String tableName) {
            switch (tableName){
                case "etl_transform_history_extended_availability_latest":
                    Log.info("Getting Latest for Availability Table Count...");
                    bcsExtLatestSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_AVAILABILITY_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_manifestation_latest":
                    Log.info("Getting Latest for Manifestation Extended Table Count...");
                    bcsExtLatestSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_EXT_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_page_count_latest":
                    Log.info("Getting Latest for Page count Table Count...");
                    bcsExtLatestSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_PAGE_COUNT_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_url_latest":
                    Log.info("Getting Latest for URL Table Count...");
                    bcsExtLatestSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_URL_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_work_latest":
                    Log.info("Getting Latest for Work extended Table Count...");
                    bcsExtLatestSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WRK_EXT_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_work_subject_area_latest":
                    Log.info("Getting Latest for work subject area Table Count...");
                    bcsExtLatestSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WRK_SUBJ_AREA_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_manifestation_restrictions_latest":
                    Log.info("Getting Latest for manifestation restriction Count...");
                    bcsExtLatestSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_MANIF_RESTRICT_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_product_prices_latest":
                    Log.info("Getting Latest for product price Table Count...");
                    bcsExtLatestSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_PROD_PRICE_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_work_person_role_latest":
                    Log.info("Getting Latest for work person role Table Count...");
                    bcsExtLatestSqlCurrCount = BcsEtlExtendedCountChecksSql.GET_WRK_PERSON_ROLE_LATEST_COUNT;
                    break;
                default:
                    Log.info(noTablemsg);
            }
            Log.info(bcsExtLatestSqlCurrCount);
            List<Map<String, Object>> bcsLatestTableCount = DBManager.getDBResultMap(bcsExtLatestSqlCurrCount, Constants.AWS_URL);
            bcsExtLatestCount = ((Long) bcsLatestTableCount.get(0).get("Target_Count")).intValue();
        }

    @And ("^Compare BCS Extended latest counts of are identical (.*)$")
    public void comparebcsExtLatestCounts( String trgtTable){
        Log.info("The sum of count for table deltaCurrent and ExclCurrent => " + bcsExtSumDeltaExclCount + " and in "+trgtTable+" => " + bcsExtLatestCount);
        Assert.assertEquals("The sum of count for table deltaCurrent and ExclCurrent with "+trgtTable, bcsExtLatestCount, bcsExtSumDeltaExclCount);
    }

    @Given("^Get the BCS Extended duplicate count in (.*) table$")
    public static void getDuplicateCount(String tableName){
        switch (tableName){
            case "etl_transform_history_extended_availability_latest":
                Log.info("Getting Duplicate Availability Latest Table Count...");
                bcsExtDupLatestSqlCount = BcsEtlExtendedCountChecksSql.GET_DUPLICATES_LATEST_AVAILABILITY_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_latest":
                Log.info("Getting Duplicate MANIF Extended Latest Table Count...");
                bcsExtDupLatestSqlCount = BcsEtlExtendedCountChecksSql.GET_DUPLICATES_LATEST_MANIF_EXT_COUNT;
                break;
            case "etl_transform_history_extended_page_count_latest":
                Log.info("Getting Duplicate Page count Latest Table Count...");
                bcsExtDupLatestSqlCount = BcsEtlExtendedCountChecksSql.GET_DUPLICATES_LATEST_PAGE_COUNT_COUNT;
                break;
            case "etl_transform_history_extended_url_latest":
                Log.info("Getting Duplicate URL Latest Table Count...");
                bcsExtDupLatestSqlCount = BcsEtlExtendedCountChecksSql.GET_DUPLICATES_LATEST_URL_COUNT;
                break;
            case "etl_transform_history_extended_work_latest":
                Log.info("Getting Duplicate Work Extended Latest Table Count...");
                bcsExtDupLatestSqlCount = BcsEtlExtendedCountChecksSql.GET_DUPLICATES_LATEST_WORK_EXT_COUNT;
                break;

            case "etl_transform_history_extended_work_subject_area_latest":
                Log.info("Getting Duplicate WorkSubj Area Latest Table Count...");
                bcsExtDupLatestSqlCount = BcsEtlExtendedCountChecksSql.GET_DUPLICATES_LATEST_WORK_SUBJ_AREA_COUNT;
                break;

            case "etl_transform_history_extended_manifestation_restrictions_latest":
                Log.info("Getting Duplicate Manif Restriction Latest Table Count...");
                bcsExtDupLatestSqlCount = BcsEtlExtendedCountChecksSql.GET_DUPLICATES_LATEST_MANIF_RESTRICT_COUNT;
                break;
            case "etl_transform_history_extended_product_prices_latest":
                Log.info("Getting Duplicate Product Price Latest Table Count...");
                bcsExtDupLatestSqlCount = BcsEtlExtendedCountChecksSql.GET_DUPLICATES_LATEST_PROD_PRICE_COUNT;
                break;
            case "etl_transform_history_extended_work_person_role_latest":
                Log.info("Getting Duplicate work person role Table Count...");
                bcsExtDupLatestSqlCount = BcsEtlExtendedCountChecksSql.GET_DUPLICATES_LATEST_WORK_PERS_ROLE_COUNT;
                break;
             default:
                 Log.info(noTablemsg);

        }
        Log.info(bcsExtDupLatestSqlCount);
        List<Map<String, Object>> bcsCoreDupLatestTableCount = DBManager.getDBResultMap(bcsExtDupLatestSqlCount, Constants.AWS_URL);
        bcsExtDupLatestCount = ((Long) bcsCoreDupLatestTableCount.get(0).get("Duplicate_Count")).intValue();
    }

    @Then("^Check the BCS Extended count should be equal to Zero (.*)$")
    public void checkDupCountZero(String tableName){
        Log.info("The Duplicate count for "+tableName+" => " + bcsExtDupLatestCount);
        Assert.assertEquals("There are Duplicate Count of "+tableName,0,bcsExtDupLatestCount);

    }



}
