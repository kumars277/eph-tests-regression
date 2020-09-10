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

    private String BCSFullSourceCountSQL, BCSCurrentCountSQL;
    private int    BCSFullSourceCount   , BCSCurrentCount;

    @Given("Get the total count of BCS Data from initial_ingest (.*)")
    public void getTotalCountFromInitialIngest(String tableName) {
        Log.info("getting initial_ingest full count...");
        switch (tableName){
            case "stg_current_classification":
                BCSFullSourceCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_CLASSIFICATION_SOURCE_COUNT; break;
        }

        List<Map<String, Object>> BCSFullSourceTableCount = DBManager.getDLResultMap(BCSFullSourceCountSQL, Constants.AWS_URL);
        BCSFullSourceCount = ((Long) BCSFullSourceTableCount.get(0).get("Source_Count")).intValue();
        Log.info(tableName+" source count :" +BCSFullSourceCount);
    }


    @Then("^Get total count of BCS Current table (.*)$")
    public void getTotalCountOfBCSCurrentTable(String tableName) {
        Log.info("getting BCS current table count...");
        switch (tableName){
            case "stg_current_classification":
                BCSCurrentCountSQL = BCSDataLakeCountCheckSQL.GET_BCS_CURRENT_CLASSIFICATION_COUNT;     break;
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
