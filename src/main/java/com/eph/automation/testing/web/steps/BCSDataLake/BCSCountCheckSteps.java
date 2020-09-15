/* Created by Nishant @ 04 Aug 2020 */

package com.eph.automation.testing.web.steps.BCSDataLake;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.BCSDataLakeSQL.BCSDataLakeCountCheckSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import java.util.List;
import java.util.Map;

public class BCSCountCheckSteps {

    private static String BCSFullSourceCountSQL, BCSCurrentCountSQL;
    private static int    BCSFullSourceCount   , BCSCurrentCount;

    @Given("Get the total count of BCS Data from initial_ingest (.*)")
    public void getTotalCountFromInitialIngest(String tableName) {
        Log.info("getting initial_ingest full count...");
        switch (tableName){
            case "stg_current_classification":    BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_CLASSIFICATION_SOURCE_COUNT; break;
            case "stg_current_content":           BCSFullSourceCountSQL =BCSDataLakeCountCheckSQL.GET_BCS_CONTENT_SOURCE_COUNT;break;
            case "stg_current_extobject":         BCSFullSourceCountSQL =BCSDataLakeCountCheckSQL.GET_BCS_EXTOBJECT_SOURCE_COUNT;break;
            case "stg_current_fullversionfamily": BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_FULLVERSIONFAMILY_SOURCE_COUNT; break;
            case "stg_current_originatoraddress": BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_ORIGINATORADDRESS_SOURCE_COUNT; break;
            case "stg_current_originators":       BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_ORIGINATORS_SOURCE_COUNT; break;
            case "stg_current_pricing":           BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_PRICING_SOURCE_COUNT; break;
            case "stg_current_product":           BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_PRODUCT_SOURCE_COUNT; break;
            case "stg_current_production":        BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_PRODUCTION_SOURCE_COUNT; break;
            case "stg_current_relations":         BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_RELATIONS_SOURCE_COUNT; break;
            case "stg_current_responsibilities":  BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_RESPONSIBILITIES_SOURCE_COUNT; break;
            case "stg_current_sublocation":       BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_SUBLOCATION_SOURCE_COUNT; break;
            case "stg_current_text":              BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_TEXT_SOURCE_COUNT; break;
            case "stg_current_versionfamily":     BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_VERSIONFAMILY_SOURCE_COUNT; break;
        }
        List<Map<String, Object>> BCSFullSourceTableCount = DBManager.getDLResultMap(BCSFullSourceCountSQL, Constants.AWS_URL);
        BCSFullSourceCount = ((Long) BCSFullSourceTableCount.get(0).get("Source_Count")).intValue();
        Log.info(tableName+" source count :" +BCSFullSourceCount);
    }

    @Then("^Get total count of BCS Current table (.*)$")
    public void getTotalCountOfBCSCurrentTable(String tableName) {
        Log.info("getting BCS current table count...");
        switch (tableName){
            case "stg_current_classification":    BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_CLASSIFICATION_CURRENT_COUNT;     break;
            case "stg_current_content":           BCSCurrentCountSQL=BCSDataLakeCountCheckSQL.GET_BCS_CONTENT_CURRENT_COUNT;break;
            case "stg_current_extobject":         BCSCurrentCountSQL=BCSDataLakeCountCheckSQL.GET_BCS_EXTOBJECT_CURRENT_COUNT;break;
            case "stg_current_fullversionfamily": BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_FULLVERSIONFAMILY_CURRENT_COUNT; break;
            case "stg_current_originatoraddress": BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_ORIGINATORADDRESS_CURRENT_COUNT; break;
            case "stg_current_originators":       BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_ORIGINATORS_CURRENT_COUNT; break;
            case "stg_current_pricing":           BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_PRICING_CURRENT_COUNT; break;
            case "stg_current_product":           BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_PRODUCT_CURRENT_COUNT; break;
            case "stg_current_production":        BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_PRODUCTION_CURRENT_COUNT; break;
            case "stg_current_relations":         BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_RELATIONS_CURRENT_COUNT; break;
            case "stg_current_responsibilities":  BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_RESPONSIBILITIES_CURRENT_COUNT; break;
            case "stg_current_sublocation":       BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_SUBLOCATION_CURRENT_COUNT; break;
            case "stg_current_text":              BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_TEXT_CURRENT_COUNT; break;
            case "stg_current_versionfamily":     BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_VERSIONFAMILY_CURRENT_COUNT; break;
        }
        List<Map<String, Object>> BCSCurrentTableCount = DBManager.getDBResultMap(BCSCurrentCountSQL, Constants.AWS_URL);
        BCSCurrentCount = ((Long) BCSCurrentTableCount.get(0).get("Current_Count")).intValue();
        Log.info(tableName+" current count :" +BCSCurrentCount);
    }

    @And("^Compare count of initial ingest with current table (.*)$")
    public void compareCountOfInitialIngestWithCurrentTable(String tableName) {
        Log.info(tableName+ " : initial_ingest count = " + BCSFullSourceCount + " Vs BCS current table count = " + BCSCurrentCount);
        Assert.assertEquals("counts are not equal for initial_ingest and "+tableName, BCSFullSourceCount, BCSCurrentCount);
    }
}
