package com.eph.automation.testing.web.steps.BCS_ETLExtended;


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
                BCSExtDiffTransformFileSQLCount = BCS_ETLExtendedCountChecksSQL.GET_MANIF_RESTRICTION_TRANSFORM_FILE_COUNT;
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

}
