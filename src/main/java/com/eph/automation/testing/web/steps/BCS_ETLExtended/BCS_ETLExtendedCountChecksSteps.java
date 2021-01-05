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

    private String BCSExtendedInboundCurrentSQLCount;

    private static int BCSExtendedInboundCurrentCount;


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

}
