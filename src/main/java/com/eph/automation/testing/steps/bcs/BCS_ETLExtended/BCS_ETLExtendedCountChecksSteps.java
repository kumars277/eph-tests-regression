package com.eph.automation.testing.steps.bcs.BCS_ETLExtended;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.BCS_ETLExtendedSQL.BCS_ETLExtendedCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class BCS_ETLExtendedCountChecksSteps {

    private String BCSExtendedSQLCurrentCount;
    private static int BCSExtendedCurrentCount;

    private String BCSExtendedSQLDeltaCurrentCount;
    private static int BCSExtendedDeltaCurrentCount;

    private String BCSExtendedInboundCurrentSQLCount;
    private static int BCSExtendedInboundCurrentCount;

    private String BCSExtSQLDeltaCurrentHistCount;
    private static int BCSExtDeltaHistCurrentCount;

    private String BCSExtFileSQLCurrentCount;
    private static int BCSExtCurrentFileCount;

    private String BCSExtHistSQLCurrentCount;
    private static int BCSExtCurrentHistCount;

    private String BCSExtDiffTransformFileSQLCount;
    private static int BCSExtDiffTransformFileCount;

    private String BCSExtDiffDeltaAndHistSQLCurrentCount;
    private static int BCSExtDiffDeltaAndHistCurrentCount;

    private String BCSExtExclSQLCurrentCount;
    private static int BCSExtExcludeCount;

    private String BCSExtSumDeltaExclSQLCurrentCount;
    private static int BCSExtSumDeltaExclCount;

    private String BCSExtLatestSQLCurrentCount;
    private static int BCSExtLatestCount;

    private String BCSExtDuplicateLatestSQLCount;
    private static int BCSExtDuplicateLatestCount;


    @Given("^Get the total count of BCS Extended from Current Tables (.*)$")
    public void getBCSExtendedCount(String tableName) {
        switch (tableName) {
            case "etl_availability_extended_current_v":
                Log.info("Getting BCS Extended Availability Extended Current Table Count...");
                BCSExtendedSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_BCS_EXTENDED_AVAILABILITY_EXTENDED_CURR_COUNT;
                break;
            case "etl_manifestation_extended_current_v":
                Log.info("Getting BCS Extended Manifestation Extended Current Table Count...");
                BCSExtendedSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_BCS_EXTENDED_MANIFESTATION_EXTENDED_CURR_COUNT;
                break;
            case "etl_page_count_extended_current_v":
                Log.info("Getting BCS Extended Page Extended Current Table Count...");
                BCSExtendedSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_BCS_EXTENDED_PAGE_COUNT_EXTENDED_CURR_COUNT;
                break;
            case "etl_url_extended_current_v":
                Log.info("Getting BCS Extended URL Extended Current Table Count...");
                BCSExtendedSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_BCS_EXTENDED_URL_EXTENDED_CURR_COUNT;
                break;
            case "etl_work_extended_current_v":
                Log.info("Getting BCS Extended Work Extended Current Table Count...");
                BCSExtendedSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_BCS_EXTENDED_WORK_EXTENDED_CURR_COUNT;
                break;
            case "etl_work_subject_area_extended_current_v":
                Log.info("Getting BCS Extended Work Subject Area Extended Current Table Count...");
                BCSExtendedSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_BCS_EXTENDED_WORK_SUBJECT_AREA_EXTENDED_CURR_COUNT;
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                Log.info("Getting BCS Extended Manifestation Restrictions Extended Current Table Count...");
                BCSExtendedSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_BCS_EXTENDED_MANIFESTATION_RESTRICTIONS_EXTENDED_CURR_COUNT;
                break;
            case "etl_product_prices_extended_current_v":
                Log.info("Getting BCS Extended Product Prices Extended Current Table Count...");
                BCSExtendedSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_BCS_EXTENDED_PRODUCT_PRICES_EXTENDED_CURR_COUNT;
                break;
            case "etl_work_person_role_extended_current_v":
                Log.info("Getting BCS Extended Work Person Role Current Table Count...");
                BCSExtendedSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_BCS_EXTENDED_WORK_PERSON_ROLE_EXTENDED_CURR_COUNT;
                break;
        }
        Log.info(BCSExtendedSQLCurrentCount);
        List<Map<String, Object>> BCS_ExtendedCurrentTableCount = DBManager.getDBResultMap(BCSExtendedSQLCurrentCount, Constants.AWS_URL);
        BCSExtendedCurrentCount = ((Long) BCS_ExtendedCurrentTableCount.get(0).get("Target_Count")).intValue();
    }


    @When("^We know the total count of BCS Extended Inbound tables (.*)$")
    public void getCountInboundTables(String tableName) {
        switch (tableName) {
            case "etl_availability_extended_current_v":
                Log.info("Getting Inbound Current of Availability Extended Table Count...");
                BCSExtendedInboundCurrentSQLCount = BCS_ETLExtendedCountChecksSQL.GET_AVAILABILITY_INBOUND_CURRENT_COUNT;
                break;
            case "etl_manifestation_extended_current_v":
                Log.info("Getting Inbound Current of Manifestation Extended Table Count...");
                BCSExtendedInboundCurrentSQLCount = BCS_ETLExtendedCountChecksSQL.GET_MANIFESTATION_INBOUND_CURRENT_COUNT;
                break;
            case "etl_page_count_extended_current_v":
                Log.info("Getting Inbound Current of Page count Extended Table Count...");
                BCSExtendedInboundCurrentSQLCount = BCS_ETLExtendedCountChecksSQL.GET_PAGE_COUNT_INBOUND_CURRENT_COUNT;
                break;
            case "etl_url_extended_current_v":
                Log.info("Getting Inbound Current of URL Extended Table Count...");
                BCSExtendedInboundCurrentSQLCount = BCS_ETLExtendedCountChecksSQL.GET_URL_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_extended_current_v":
                Log.info("Getting Inbound Current of Work Extended Table Count...");
                BCSExtendedInboundCurrentSQLCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_subject_area_extended_current_v":
                Log.info("Getting Inbound Current of Work Subject Area Extended Table Count...");
                BCSExtendedInboundCurrentSQLCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_SUBJECT_AREA_INBOUND_CURRENT_COUNT;
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                Log.info("Getting Inbound Current of Manifestation Restrictions Extended Table Count...");
                BCSExtendedInboundCurrentSQLCount = BCS_ETLExtendedCountChecksSQL.GET_MANIFESTATION_RESTRICTIONS_INBOUND_CURRENT_COUNT;
                break;
            case "etl_product_prices_extended_current_v":
                Log.info("Getting Inbound Current of Product Prices Extended Table Count...");
                BCSExtendedInboundCurrentSQLCount = BCS_ETLExtendedCountChecksSQL.GET_PRODUCT_PRICES_INBOUND_CURRENT_COUNT;
                break;
            case "etl_work_person_role_extended_current_v":
                Log.info("Getting Inbound Current of Work Person Role Extended Table Count...");
                BCSExtendedInboundCurrentSQLCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_PERSON_ROLE_INBOUND_CURRENT_COUNT;
                break;

        }
        Log.info(BCSExtendedInboundCurrentSQLCount);
        List<Map<String, Object>> BCS_ETLExtenedCurrentTableCount = DBManager.getDBResultMap(BCSExtendedInboundCurrentSQLCount, Constants.AWS_URL);
        BCSExtendedInboundCurrentCount = ((Long) BCS_ETLExtenedCurrentTableCount.get(0).get("Source_Count")).intValue();
    }

    @Then("^Compare count of BCS Inbound and BCS Extended (.*) tables are identical$")
    public void compareExtendedAndInboundCounts(String tableName) {
        Log.info("The count for table " + tableName + " => " + BCSExtendedCurrentCount + " and in Inbound  => " + BCSExtendedInboundCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with " + tableName + " and Inbound ", BCSExtendedInboundCurrentCount,BCSExtendedCurrentCount);

    }

    @Given("^We know the total count of BCS Extended delta current (.*)$")
    public void getBCSExtDeltaCurrentCount (String tableName) {
        switch (tableName){
            case "etl_delta_current_extended_availability":
                Log.info("Getting BCS etl_delta_current_extended_availability Table Count...");
                BCSExtendedSQLDeltaCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_AVAILABILITY_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_manifestation":
                Log.info("Getting BCS etl_delta_current_extended_manifestation Table Count...");
                BCSExtendedSQLDeltaCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_EXT_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_page_count":
                Log.info("Getting BCS etl_delta_current_extended_page_count Table Count...");
                BCSExtendedSQLDeltaCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PAGE_COUNT_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_url":
                Log.info("Getting BCS etl_delta_current_extended_url Table Count...");
                BCSExtendedSQLDeltaCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_URL_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_work":
                Log.info("Getting BCS etl_delta_current_extended_work Table Count...");
                BCSExtendedSQLDeltaCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_EXT_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_work_subject_area":
                Log.info("Getting BCS etl_delta_current_extended_work_subject_area Table Count...");
                BCSExtendedSQLDeltaCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_SUBJ_AREA_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_manifestation_restrictions":
                Log.info("Getting BCS etl_delta_current_extended_manifestation_restrictions Table Count...");
                BCSExtendedSQLDeltaCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_REST_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_product_prices":
                Log.info("Getting BCS etl_delta_current_extended_product_prices Table Count...");
                BCSExtendedSQLDeltaCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PROD_PRICE_DELTA_CURR_COUNT;
                break;
            case "etl_delta_current_extended_work_person_role":
                Log.info("Getting BCS etl_delta_current_extended_work_person_role Table Count...");
                BCSExtendedSQLDeltaCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_PERS_ROLE_DELTA_CURR_COUNT;
                break;

        }
        Log.info(BCSExtendedSQLDeltaCurrentCount);
        List<Map<String, Object>> BCSCoreDeltaCurrentTableCount = DBManager.getDBResultMap(BCSExtendedSQLDeltaCurrentCount, Constants.AWS_URL);
        BCSExtendedDeltaCurrentCount = ((Long) BCSCoreDeltaCurrentTableCount.get(0).get("Source_Count")).intValue();
    }

    @Then("^Get the count of BCS Extended delta history of current timestamp from (.*)$")
    public void getBCSExtDeltaCurrentHistCount (String tableName) {
        switch (tableName){
            case "etl_delta_history_extended_availability_part":
                Log.info("Getting BCS etl_delta_history_extended_availability_part Table Count...");
                BCSExtSQLDeltaCurrentHistCount = BCS_ETLExtendedCountChecksSQL.GET_AVAILABILITY_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_manifestation_part":
                Log.info("Getting etl_delta_history_extended_manifestation_part Table Count...");
                BCSExtSQLDeltaCurrentHistCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_EXT_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_page_count_part":
                Log.info("Getting etl_delta_history_extended_page_count_part Table Count...");
                BCSExtSQLDeltaCurrentHistCount = BCS_ETLExtendedCountChecksSQL.GET_PAGE_COUNT_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_url_part":
                Log.info("Getting etl_delta_history_extended_url_part Table Count...");
                BCSExtSQLDeltaCurrentHistCount = BCS_ETLExtendedCountChecksSQL.GET_URL_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_work_part":
                Log.info("Getting etl_delta_history_extended_work_part Table Count...");
                BCSExtSQLDeltaCurrentHistCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_EXT_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_work_subject_area_part":
                Log.info("Getting etl_delta_history_extended_work_subject_area_part Table Count...");
                BCSExtSQLDeltaCurrentHistCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_SUBJ_AREA_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_manifestation_restrictions_part":
                Log.info("Getting etl_delta_history_extended_manifestation_restrictions_part Table Count...");
                BCSExtSQLDeltaCurrentHistCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_REST_DELTA_HIST_CURR_COUNT;
                break;
            case "etl_delta_history_extended_product_prices_part":
                Log.info("Getting etl_delta_history_extended_product_prices_part Table Count...");
                BCSExtSQLDeltaCurrentHistCount = BCS_ETLExtendedCountChecksSQL.GET_PROD_PRICE_DELTA_HIST_COUNT;
                break;
            case "etl_delta_history_extended_work_person_role_part":
                Log.info("Getting etl_delta_history_extended_work_person_role_part Table Count...");
                BCSExtSQLDeltaCurrentHistCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_PERS_ROLE_DELTA_HIST_COUNT;
                break;

        }
        Log.info(BCSExtSQLDeltaCurrentHistCount);
        List<Map<String, Object>> BCSCoreDeltaCurrentHistTableCount = DBManager.getDBResultMap(BCSExtSQLDeltaCurrentHistCount, Constants.AWS_URL);
        BCSExtDeltaHistCurrentCount = ((Long) BCSCoreDeltaCurrentHistTableCount.get(0).get("Target_Count")).intValue();
    }
    @And("^Check count of BCS Extended delta current table (.*) and delta history (.*) are identical$")
    public void compareBCSExtDeltaAndDeltaHistCounts(String srctable, String trgtTable){
        Log.info("The count for Delta table "+srctable+" => " + BCSExtendedDeltaCurrentCount + " and in Delta Hist "+trgtTable+"  => " + BCSExtDeltaHistCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctable+" and "+trgtTable+" ", BCSExtDeltaHistCurrentCount, BCSExtendedDeltaCurrentCount);
    }

    @Then ("^Get the count of BCS Extended history (.*)$")
    public void getBCSExtHistCount (String tableName) {
        switch (tableName){
            case "etl_transform_history_extended_availability_part":
                Log.info("Getting etl_transform_history_extended_availability_part Table Count...");
                BCSExtHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_AVAILABILITY_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_part":
                Log.info("Getting etl_transform_history_extended_manifestation_part Table Count...");
                BCSExtHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_EXT_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_page_count_part":
                Log.info("Getting etl_transform_history_extended_page_count_part Table Count...");
                BCSExtHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PAGE_COUNT_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_url_part":
                Log.info("Getting etl_transform_history_extended_url_part Table Count...");
                BCSExtHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_URL_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_part":
                Log.info("Getting etl_transform_history_extended_work_part Table Count...");
                BCSExtHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_EXT_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_subject_area_part":
                Log.info("Getting etl_transform_history_extended_work_subject_area_part Table Count...");
                BCSExtHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_SUBJ_AREA_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_restrictions_part":
                Log.info("Getting etl_transform_history_extended_manifestation_restrictions_part Table Count...");
                BCSExtHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_REST_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_product_prices_part":
                Log.info("Getting etl_transform_history_extended_product_prices_part Table Count...");
                BCSExtHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PROD_PRICE_CURR_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_person_role_part":
                Log.info("Getting etl_transform_history_extended_work_person_role_part Table Count...");
                BCSExtHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_PERS_ROLE_CURR_HIST_COUNT;
                break;

        }
        Log.info(BCSExtHistSQLCurrentCount);
        List<Map<String, Object>> BCS_ETLCoreCurrentHistTableCount = DBManager.getDBResultMap(BCSExtHistSQLCurrentCount, Constants.AWS_URL);
        BCSExtCurrentHistCount = ((Long) BCS_ETLCoreCurrentHistTableCount.get(0).get("Source_Count")).intValue();
    }
    @And ("^Compare count of BCS Extended current (.*) and history (.*) are identical$")
    public void compareBCSExtCurrAndCurrHistCounts(String srctable, String trgtTable){
        Log.info("The count for Curr table "+srctable+" => " + BCSExtendedCurrentCount + " and in Curr Hist "+trgtTable+"  => " + BCSExtCurrentHistCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctable+" and "+trgtTable+" ", BCSExtendedCurrentCount, BCSExtCurrentHistCount);
    }

    @Then ("^Get the count of BCS Extended transform_file (.*)$")
    public void getBCSExtFileCount (String tableName) {
        switch (tableName){
            case "etl_availability_extended_transform_file_history_part":
                Log.info("Getting etl_availability_extended_transform_file_history_part Table Count...");
                BCSExtFileSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_AVAILABILITY_CURR_FILE_COUNT;
                break;
            case "etl_manifestation_extended_transform_file_history_part":
                Log.info("Getting etl_manifestation_extended_transform_file_history_part Table Count...");
                BCSExtFileSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_EXT_CURR_FILE_COUNT;
                break;
            case "etl_page_count_extended_transform_file_history_part":
                Log.info("Getting etl_page_count_extended_transform_file_history_part Table Count...");
                BCSExtFileSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PAGE_COUNT_CURR_FILE_COUNT;
                break;
            case "etl_url_extended_transform_file_history_part":
                Log.info("Getting etl_url_extended_transform_file_history_part Table Count...");
                BCSExtFileSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_URL_CURR_FILE_COUNT;
                break;
            case "etl_work_extended_transform_file_history_part":
                Log.info("Getting etl_work_extended_transform_file_history_part Table Count...");
                BCSExtFileSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_EXT_CURR_FILE_COUNT;
                break;
            case "etl_work_subject_area_extended_transform_file_history_part":
                Log.info("Getting etl_work_subject_area_extended_transform_file_history_part Table Count...");
                BCSExtFileSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_SUBJ_AREA_CURR_FILE_COUNT;
                break;
            case "etl_manifestation_restrictions_extended_transform_file_history_part":
                Log.info("Getting etl_manifestation_restrictions_extended_transform_file_history_part Table Count...");
                BCSExtFileSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_REST_CURR_FILE_COUNT;
                break;
            case "etl_product_prices_extended_transform_file_history_part":
                Log.info("Getting etl_product_prices_extended_transform_file_history_part Table Count...");
                BCSExtFileSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PROD_PRICE_CURR_FILE_COUNT;
                break;
            case "etl_work_person_role_extended_transform_file_history_part":
                Log.info("Getting etl_work_person_role_extended_transform_file_history_part Table Count...");
                BCSExtFileSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_PERS_ROLE_CURR_FILE_COUNT;
                break;

        }
        Log.info(BCSExtFileSQLCurrentCount);
        List<Map<String, Object>> BCS_ETLExtCurrentFileTableCount = DBManager.getDBResultMap(BCSExtFileSQLCurrentCount, Constants.AWS_URL);
        BCSExtCurrentFileCount = ((Long) BCS_ETLExtCurrentFileTableCount.get(0).get("Source_Count")).intValue();
    }
    @And ("^Compare BCS Extended count of current (.*) and tranform_file (.*) are identical$")
    public void compareBCSExtCurrAndCurrFileCounts(String srctable, String trgtTable){
        Log.info("The count for Curr table "+srctable+" => " + BCSExtendedCurrentCount + " and in Curr File "+trgtTable+"  => " + BCSExtCurrentFileCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctable+" and "+trgtTable+" ", BCSExtendedCurrentCount, BCSExtCurrentFileCount);
    }


    @Given("^Get the total count of BCS Extended transform_file by diff of current and previous timestamp (.*)$")
    public void getDiffPrevCurrTranFileTimeStamp (String tableName) {
        switch (tableName){
            case "etl_availability_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_availability_extended_transform_file_history_part Table Count...");
                BCSExtDiffTransformFileSQLCount = BCS_ETLExtendedCountChecksSQL.GET_AVAILABILTIY_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_manifestation_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_manifestation_extended_transform_file_history_part Table Count...");
                BCSExtDiffTransformFileSQLCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_EXT_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_page_count_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_page_count_extended_transform_file_history_part Table Count...");
                BCSExtDiffTransformFileSQLCount = BCS_ETLExtendedCountChecksSQL.GET_PAGE_COUNT_DIFF_TRANSFORM_FILE_COUNT;
                break;
           case "etl_url_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_url_extended_transform_file_history_part Table Count...");
                BCSExtDiffTransformFileSQLCount = BCS_ETLExtendedCountChecksSQL.GET_URL_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_work_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_work_extended_transform_file_history_part Table Count...");
                BCSExtDiffTransformFileSQLCount = BCS_ETLExtendedCountChecksSQL.GET_WRK_EXT_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_work_subject_area_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_work_subject_area_extended_transform_file_history_part Table Count...");
                BCSExtDiffTransformFileSQLCount = BCS_ETLExtendedCountChecksSQL.GET_WRK_SUBJ_AREA_DIFF_TRANSFORM_FILE_COUNT;
                break;
         case "etl_manifestation_restrictions_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_manifestation_restrictions_extended_transform_file_history_part Count...");
                BCSExtDiffTransformFileSQLCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_RESTRICTION_DIFF_TRANSFORM_FILE_COUNT;
                break;
           case "etl_product_prices_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_product_prices_extended_transform_file_history_part Table Count...");
                BCSExtDiffTransformFileSQLCount = BCS_ETLExtendedCountChecksSQL.GET_PROD_PRICE_DIFF_TRANSFORM_FILE_COUNT;
                break;
            case "etl_work_person_role_extended_transform_file_history_part":
                Log.info("Getting Diff Curr and Previous TimeStamp for etl_work_person_role_extended_transform_file_history_part Table Count...");
                BCSExtDiffTransformFileSQLCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_PERSON_ROLE_DIFF_TRANSFORM_FILE_COUNT;
                break;

        }
        Log.info(BCSExtDiffTransformFileSQLCount);
        List<Map<String, Object>> BCSDiffTransformFileTableCount = DBManager.getDBResultMap(BCSExtDiffTransformFileSQLCount, Constants.AWS_URL);
        BCSExtDiffTransformFileCount = ((Long) BCSDiffTransformFileTableCount.get(0).get("target_Count")).intValue();
    }

    @And("^Compare count of BCS Ext tranform_file (.*) and delta current (.*) are identical$")
    public void compareTransformFileAndDeltaCurrCounts(String srcTable,String trgtTable){
        Log.info("The count for Diff of curr and previous timestamp of table "+srcTable+" => " + BCSExtDiffTransformFileCount + " and in "+trgtTable+" => " + BCSExtendedDeltaCurrentCount);
        Assert.assertEquals("The counts are not equal when compared Diff of curr and previous timestamp of table "+srcTable+" and "+trgtTable, BCSExtDiffTransformFileCount, BCSExtendedDeltaCurrentCount);
    }


    @Given ("^Get the BCS Extended Ext total count difference between delta current and transform current history Table (.*)$")
    public void getBCSExtDiffDeltaCurrAndHistCount (String tableName) {
        switch (tableName){
            case "etl_transform_history_extended_availability_excl_delta":
                Log.info("Getting Diff of Delta Current and Hist for availability Table Count...");
                BCSExtDiffDeltaAndHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_AVAILABILITY_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_excl_delta":
                Log.info("Getting Diff of Delta Current and Hist for Manifestation Extended Table Count...");
                BCSExtDiffDeltaAndHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_EXT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_page_count_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for Page Count Ext Table Count...");
                BCSExtDiffDeltaAndHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PAGE_COUNT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_url_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for URL Table Count...");
                BCSExtDiffDeltaAndHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_URL_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for work extended Table Count...");
                BCSExtDiffDeltaAndHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_EXT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_subject_area_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for work Subject area Table Count...");
                BCSExtDiffDeltaAndHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WRK_SUBJ_AREA_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for Manif Restrition Table Count...");
                BCSExtDiffDeltaAndHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_RESTRICT_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_product_prices_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for Product Price Ext Table Count...");
                BCSExtDiffDeltaAndHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PROD_PRICE_DIFF_DELTA_AND_HIST_COUNT;
                break;
            case "etl_transform_history_extended_work_person_role_excl_delta":
                Log.info("Getting BCS Diff of Delta Current and Hist for Work person role Table Count...");
                BCSExtDiffDeltaAndHistSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_PERS_ROLE_DIFF_DELTA_AND_HIST_COUNT;
                break;

        }
        Log.info(BCSExtDiffDeltaAndHistSQLCurrentCount);
        List<Map<String, Object>> BCSDeltaCurrAndHistTableCount = DBManager.getDBResultMap(BCSExtDiffDeltaAndHistSQLCurrentCount, Constants.AWS_URL);
        BCSExtDiffDeltaAndHistCurrentCount = ((Long) BCSDeltaCurrAndHistTableCount.get(0).get("Source_Count")).intValue();
    }


    @Then ("^Get the BCS Extended (.*) exclude data count$")
    public void getBCSExtExcludCount (String tableName) {
        switch (tableName){
            case "etl_transform_history_extended_availability_excl_delta":
                Log.info("Getting Exclude for Availability Table Count...");
                BCSExtExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_AVAILABILITY_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_excl_delta":
                Log.info("Getting Exclude for Manifestation Extended Table Count...");
                BCSExtExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_EXT_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_page_count_excl_delta":
                Log.info("Getting Exclude for Page Count Ext Table Count...");
                BCSExtExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PAGE_COUNT_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_url_excl_delta":
                Log.info("Getting Exclude for URL Table Count...");
                BCSExtExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_URL_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_excl_delta":
                Log.info("Getting Exclude for work extended Table Count...");
                BCSExtExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WRK_EXT_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_subject_area_excl_delta":
                Log.info("Getting Exclude for work Subject area Table Count...");
                BCSExtExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WRK_SUBJ_AREA_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
                Log.info("Getting Exclude for Manif Restrition Count...");
                BCSExtExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_RESTRICT_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_product_prices_excl_delta":
                Log.info("Getting Exclude for Product Price Ext Table Count...");
                BCSExtExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PROD_PRICE_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_person_role_excl_delta":
                Log.info("Getting Exclude for Work person role Table Count...");
                BCSExtExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_PERS_ROLE_EXCL_COUNT;
                break;

        }
        Log.info(BCSExtExclSQLCurrentCount);
        List<Map<String, Object>> BCSExclTableCount = DBManager.getDBResultMap(BCSExtExclSQLCurrentCount, Constants.AWS_URL);
        BCSExtExcludeCount = ((Long) BCSExclTableCount.get(0).get("Target_Count")).intValue();
    }
    @And ("^Compare BCS Extended exclude count are identical (.*)$")
    public void compareBCSExtExclCounts(String trgtTable){
        Log.info("The diff of count for table delta and currentHistory => " + BCSExtDiffDeltaAndHistCurrentCount + " and in "+trgtTable+" => " + BCSExtExcludeCount);
        Assert.assertEquals("The diff of count for table delta and currentHistory with "+trgtTable, BCSExtExcludeCount, BCSExtDiffDeltaAndHistCurrentCount);
    }

    @Given ("^Get the sum of total count between BCS Extended delta current and Current_Exclude Table (.*)$")
    public void getBCSExtSumOfExclAndDeltaCurrCount (String tableName) {
        switch (tableName) {
            case "etl_transform_history_extended_availability_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Availability Table Count...");
                BCSExtSumDeltaExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_AVAILABILITY_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Manifestation Extended Table Count...");
                BCSExtSumDeltaExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_EXT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_page_count_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Page Count Ext Table Count...");
                BCSExtSumDeltaExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PAGE_COUNT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_url_latest":
                Log.info("Getting Sum of Delta Curr & Excl for URL Table Count...");
                BCSExtSumDeltaExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_URL_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work extended Table Count...");
                BCSExtSumDeltaExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WRK_EXT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_subject_area_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work Subject area Table Count...");
                BCSExtSumDeltaExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WRK_SUBJ_AREA_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_restrictions_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Manif Restrition Count...");
                BCSExtSumDeltaExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_RESTRICT_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_product_prices_latest":
                Log.info("Getting Sum of Delta Curr & Excl for Product PRice Table Count...");
                BCSExtSumDeltaExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_ROD_PRICE_SUM_DELTACURR_EXCL_COUNT;
                break;
            case "etl_transform_history_extended_work_person_role_latest":
                Log.info("Getting Sum of Delta Curr & Excl for work person role Table Count...");
                BCSExtSumDeltaExclSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WORK_PERS_ROLE_SUM_DELTACURR_EXCL_COUNT;
                break;

        }
        Log.info(BCSExtSumDeltaExclSQLCurrentCount);
        List<Map<String, Object>> BCSSumDeltaExclTableCount = DBManager.getDBResultMap(BCSExtSumDeltaExclSQLCurrentCount, Constants.AWS_URL);
        BCSExtSumDeltaExclCount = ((Long) BCSSumDeltaExclTableCount.get(0).get("source_count")).intValue();
    }
        @Then("^Get the BCS Extended (.*) latest data count$")
        public void getBCSExtLatestCount(String tableName) {
            switch (tableName){
                case "etl_transform_history_extended_availability_latest":
                    Log.info("Getting Latest for Availability Table Count...");
                    BCSExtLatestSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_AVAILABILITY_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_manifestation_latest":
                    Log.info("Getting Latest for Manifestation Extended Table Count...");
                    BCSExtLatestSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_EXT_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_page_count_latest":
                    Log.info("Getting Latest for Page count Table Count...");
                    BCSExtLatestSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PAGE_COUNT_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_url_latest":
                    Log.info("Getting Latest for URL Table Count...");
                    BCSExtLatestSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_URL_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_work_latest":
                    Log.info("Getting Latest for Work extended Table Count...");
                    BCSExtLatestSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WRK_EXT_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_work_subject_area_latest":
                    Log.info("Getting Latest for work subject area Table Count...");
                    BCSExtLatestSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WRK_SUBJ_AREA_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_manifestation_restrictions_latest":
                    Log.info("Getting Latest for manifestation restriction Count...");
                    BCSExtLatestSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_RESTRICT_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_product_prices_latest":
                    Log.info("Getting Latest for product price Table Count...");
                    BCSExtLatestSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_PROD_PRICE_LATEST_COUNT;
                    break;
                case "etl_transform_history_extended_work_person_role_latest":
                    Log.info("Getting Latest for work person role Table Count...");
                    BCSExtLatestSQLCurrentCount = BCS_ETLExtendedCountChecksSQL.GET_WRK_PERSON_ROLE_LATEST_COUNT;
                    break;
            }
            Log.info(BCSExtLatestSQLCurrentCount);
            List<Map<String, Object>> BCSLatestTableCount = DBManager.getDBResultMap(BCSExtLatestSQLCurrentCount, Constants.AWS_URL);
            BCSExtLatestCount = ((Long) BCSLatestTableCount.get(0).get("Target_Count")).intValue();
        }

    @And ("^Compare BCS Extended latest counts of are identical (.*)$")
    public void compareBCSExtLatestCounts( String trgtTable){
        Log.info("The sum of count for table deltaCurrent and ExclCurrent => " + BCSExtSumDeltaExclCount + " and in "+trgtTable+" => " + BCSExtLatestCount);
        Assert.assertEquals("The sum of count for table deltaCurrent and ExclCurrent with "+trgtTable, BCSExtLatestCount, BCSExtSumDeltaExclCount);
    }

    @Given("^Get the BCS Extended duplicate count in (.*) table$")
    public void getDuplicateCount(String tableName){
        switch (tableName){
            case "etl_transform_history_extended_availability_latest":
                Log.info("Getting Duplicate Availability Latest Table Count...");
                BCSExtDuplicateLatestSQLCount = BCS_ETLExtendedCountChecksSQL.GET_DUPLICATES_LATEST_AVAILABILITY_COUNT;
                break;
            case "etl_transform_history_extended_manifestation_latest":
                Log.info("Getting Duplicate MANIF Extended Latest Table Count...");
                BCSExtDuplicateLatestSQLCount = BCS_ETLExtendedCountChecksSQL.GET_DUPLICATES_LATEST_MANIF_EXT_COUNT;
                break;
            case "etl_transform_history_extended_page_count_latest":
                Log.info("Getting Duplicate Page count Latest Table Count...");
                BCSExtDuplicateLatestSQLCount = BCS_ETLExtendedCountChecksSQL.GET_DUPLICATES_LATEST_PAGE_COUNT_COUNT;
                break;
            case "etl_transform_history_extended_url_latest":
                Log.info("Getting Duplicate URL Latest Table Count...");
                BCSExtDuplicateLatestSQLCount = BCS_ETLExtendedCountChecksSQL.GET_DUPLICATES_LATEST_URL_COUNT;
                break;
            case "etl_transform_history_extended_work_latest":
                Log.info("Getting Duplicate Work Extended Latest Table Count...");
                BCSExtDuplicateLatestSQLCount = BCS_ETLExtendedCountChecksSQL.GET_DUPLICATES_LATEST_WORK_EXT_COUNT;
                break;

            case "etl_transform_history_extended_work_subject_area_latest":
                Log.info("Getting Duplicate WorkSubj Area Latest Table Count...");
                BCSExtDuplicateLatestSQLCount = BCS_ETLExtendedCountChecksSQL.GET_DUPLICATES_LATEST_WORK_SUBJ_AREA_COUNT;
                break;

            case "etl_transform_history_extended_manifestation_restrictions_latest":
                Log.info("Getting Duplicate Manif Restriction Latest Table Count...");
                BCSExtDuplicateLatestSQLCount = BCS_ETLExtendedCountChecksSQL.GET_DUPLICATES_LATEST_MANIF_RESTRICT_COUNT;
                break;
            case "etl_transform_history_extended_product_prices_latest":
                Log.info("Getting Duplicate Product Price Latest Table Count...");
                BCSExtDuplicateLatestSQLCount = BCS_ETLExtendedCountChecksSQL.GET_DUPLICATES_LATEST_PROD_PRICE_COUNT;
                break;
            case "etl_transform_history_extended_work_person_role_latest":
                Log.info("Getting Duplicate work person role Table Count...");
                BCSExtDuplicateLatestSQLCount = BCS_ETLExtendedCountChecksSQL.GET_DUPLICATES_LATEST_WORK_PERS_ROLE_COUNT;
                break;

        }
        Log.info(BCSExtDuplicateLatestSQLCount);
        List<Map<String, Object>> BCSCoreDupLatestTableCount = DBManager.getDBResultMap(BCSExtDuplicateLatestSQLCount, Constants.AWS_URL);
        BCSExtDuplicateLatestCount = ((Long) BCSCoreDupLatestTableCount.get(0).get("Duplicate_Count")).intValue();
    }

    @Then("^Check the BCS Extended count should be equal to Zero (.*)$")
    public void checkDupCountZero(String tableName){
        Log.info("The Duplicate count for "+tableName+" => " + BCSExtDuplicateLatestCount);
        Assert.assertEquals("There are Duplicate Count of "+tableName,0,BCSExtDuplicateLatestCount);

    }



}
