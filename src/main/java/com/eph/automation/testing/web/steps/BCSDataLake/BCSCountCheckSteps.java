/* Created by Nishant @ 04 Aug 2020 */

package com.eph.automation.testing.web.steps.BCSDataLake;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.BCSDataLakeSQL.BCSDataLakeCountCheckSQL;
import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIDataLakeCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class BCSCountCheckSteps {

    private static String BCSFullSourceCount_SQL, BCSCurrentCount_SQL,BCSPreviousCount_SQL,BCSHistoryCount_SQL;
    private static int    BCSFullSourceCount   , BCSCurrentCount, BCSPreviousCount,BCSHistoryCount;

    @Given("Get the total count of BCS Data from initial_ingest (.*)")
    public void getTotalCountFromInitialIngest(String tableName) {
        Log.info("getting initial_ingest full count...");
        switch (tableName){
            case "stg_current_classification":    BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_CLASSIFICATION_SOURCE_COUNT; break;
            case "stg_current_content":           BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_CONTENT_SOURCE_COUNT;break;
            case "stg_current_extobject":         BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_EXTOBJECT_SOURCE_COUNT;break;
            case "stg_current_fullversionfamily": BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_FULLVERSIONFAMILY_SOURCE_COUNT; break;
            case "stg_current_originatoraddress": BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_ORIGINATORADDRESS_SOURCE_COUNT; break;
            case "stg_current_originators":       BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_ORIGINATORS_SOURCE_COUNT; break;
            case "stg_current_pricing":           BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_PRICING_SOURCE_COUNT; break;
            case "stg_current_product":           BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_PRODUCT_SOURCE_COUNT; break;
            case "stg_current_production":        BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_PRODUCTION_SOURCE_COUNT; break;
            case "stg_current_relations":         BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_RELATIONS_SOURCE_COUNT; break;
            case "stg_current_responsibilities":  BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_RESPONSIBILITIES_SOURCE_COUNT; break;
            case "stg_current_sublocation":       BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_SUBLOCATION_SOURCE_COUNT; break;
            case "stg_current_text":              BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_TEXT_SOURCE_COUNT; break;
            case "stg_current_versionfamily":     BCSFullSourceCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_VERSIONFAMILY_SOURCE_COUNT; break;
        }
        List<Map<String, Object>> BCSFullSourceTableCount = DBManager.getDLResultMap(BCSFullSourceCount_SQL, Constants.AWS_URL);
        BCSFullSourceCount = ((Long) BCSFullSourceTableCount.get(0).get("Source_Count")).intValue();
        Log.info(tableName+" source count :" +BCSFullSourceCount);
    }

    @Then("^Get total count of BCS Current table (.*)$")
    public void getTotalCountOfBCSCurrentTable(String tableName) {
        Log.info("getting BCS current table count...");
        switch (tableName){
            case "stg_current_classification":    BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_CLASSIFICATION_CURRENT_COUNT;     break;
            case "stg_current_content":           BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_CONTENT_CURRENT_COUNT;break;
            case "stg_current_extobject":         BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_EXTOBJECT_CURRENT_COUNT;break;
            case "stg_current_fullversionfamily": BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_FULLVERSIONFAMILY_CURRENT_COUNT; break;
            case "stg_current_originatoraddress": BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_ORIGINATORADDRESS_CURRENT_COUNT; break;
            case "stg_current_originators":       BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_ORIGINATORS_CURRENT_COUNT; break;
            case "stg_current_pricing":           BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_PRICING_CURRENT_COUNT; break;
            case "stg_current_product":           BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_PRODUCT_CURRENT_COUNT; break;
            case "stg_current_production":        BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_PRODUCTION_CURRENT_COUNT; break;
            case "stg_current_relations":         BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_RELATIONS_CURRENT_COUNT; break;
            case "stg_current_responsibilities":  BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_RESPONSIBILITIES_CURRENT_COUNT; break;
            case "stg_current_sublocation":       BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_SUBLOCATION_CURRENT_COUNT; break;
            case "stg_current_text":              BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_TEXT_CURRENT_COUNT; break;
            case "stg_current_versionfamily":     BCSCurrentCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_VERSIONFAMILY_CURRENT_COUNT; break;
        }
        List<Map<String, Object>> BCSCurrentTableCount = DBManager.getDBResultMap(BCSCurrentCount_SQL, Constants.AWS_URL);
        BCSCurrentCount = ((Long) BCSCurrentTableCount.get(0).get("Current_Count")).intValue();
        Log.info(tableName+" current count :" +BCSCurrentCount);
    }

    @And("^Compare count of initial ingest with current table (.*)$")
    public void compareCountOfInitialIngestWithCurrentTable(String tableName) {
        Log.info(tableName+ " : initial_ingest count = " + BCSFullSourceCount + " Vs BCS current table count = " + BCSCurrentCount);
        Assert.assertEquals("counts are not equal for initial_ingest and "+tableName, BCSFullSourceCount, BCSCurrentCount);
    }




    @Given("^We know the total count of stg_previous BCS data from (.*)$")
    public void getCountfromPreviousTables(String tableName){
        Log.info("Getting Previous Table Count...");
        switch (tableName){
            case "stg_previous_classification":    BCSPreviousCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_CLASSIFICATION_PREVIOUS_COUNT; break;
        }

        List<Map<String, Object>> BCSPreviousTableCount = DBManager.getDBResultMap(BCSPreviousCount_SQL, Constants.AWS_URL);
        BCSPreviousCount = ((Long) BCSPreviousTableCount.get(0).get("{Previous_Count")).intValue();
        Log.info(tableName+" previous count :" +BCSPreviousCount);
    }

    @Then("^Get the count of BCS stg_history (.*)$")
    public void getCountFromCurrentHistory(String tableName){
        Calendar cal = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String currentDate= dateFormat.format(cal.getTime());
        Log.info("Getting History Table Count...");
        Log.info("Current Date=> "+currentDate);
        switch (tableName){
            case "stg_history_classification_part":
                BCSHistoryCount_SQL = BCSDataLakeCountCheckSQL.GET_BCS_CLASSIFICATION_HISTORY_COUNT;
                break;
        }

        List<Map<String, Object>> JRBICurrentHistoryTableCount = DBManager.getDBResultMap(BCSHistoryCount_SQL, Constants.AWS_URL);
        BCSHistoryCount = ((Long)JRBICurrentHistoryTableCount.get(0).get("History_Count")).intValue();
    }

    @And("^Check count of previous table (.*) and history (.*) are identical$")
    public void compareCurrentandCurrentHistoryCount(String SrctableName,String trgttableName){
        Log.info( SrctableName + " = " + BCSPreviousCount + " Vs "+trgttableName+" => " + BCSHistoryCount);
        Assert.assertEquals("counts are not equal for "+SrctableName+" and "+trgttableName, BCSPreviousCount, BCSHistoryCount);
    }


}
