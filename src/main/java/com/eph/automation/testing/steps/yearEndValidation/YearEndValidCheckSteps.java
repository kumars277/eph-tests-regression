package com.eph.automation.testing.steps.yearEndValidation;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.YearEndValidSQL.YearEndValidChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class YearEndValidCheckSteps {

    private static String yearEndSQLCount;
    private static int yearEndSourceCount;
    private static String pmcAndOpcChangesTableCount;
    private static int pmcAndOpcChangesCount;




    @Given("^Get the total count from year_end_full table for (.*)$")
    public static void getYearEndTableCount (String table) {
        Log.info("Getting Year End Table count...");
        switch (table){
            case "pmc_changes_full_v":
                yearEndSQLCount = YearEndValidChecksSQL.YEAR_END_TABLE_PMC_COUNT;
                break;
            case "opco_rc_changes_full_v":
                yearEndSQLCount = YearEndValidChecksSQL.YEAR_END_TABLE_OPCO_RC_COUNT;
                break;
            default:
                throw new IllegalArgumentException();
        }
        Log.info(yearEndSQLCount);
        List<Map<String, Object>> yearEndSrcTableCount = DBManager.getDLResultMap(yearEndSQLCount, Constants.AWS_URL);
        yearEndSourceCount = ((Long) yearEndSrcTableCount.get(0).get("sourceCount")).intValue();

    }

    @Given("^We get the total count from (.*)$")
    public static void getpmcAndOpcoRCCount (String table) {
        Log.info("Getting PMC and OPCO RC Changes count...");
        switch (table){
            case "pmc_changes_full_v":
                pmcAndOpcChangesTableCount = YearEndValidChecksSQL.PMC_CHANGES_FULL_COUNT;
                break;
            case "opco_rc_changes_full_v":
                pmcAndOpcChangesTableCount = YearEndValidChecksSQL.OPCO_RC_CHANGES_FULL_COUNT;
                break;
            default:
                throw new IllegalArgumentException();
        }
        Log.info(pmcAndOpcChangesTableCount);
        List<Map<String, Object>> pmcAndOpc0ChangesTableCount = DBManager.getDLResultMap(pmcAndOpcChangesTableCount, Constants.AWS_URL);
        pmcAndOpcChangesCount = ((Long) pmcAndOpc0ChangesTableCount.get(0).get("targetCount")).intValue();

    }

    @And("^Compare count of year_end_full and (.*)$")
    public void compareCounts(String tables){
        Log.info("The count for YearEndTable => " + yearEndSourceCount + " and in "+tables+" => " + pmcAndOpcChangesCount);
        Assert.assertEquals("The counts are not equal when compared with YearEndTable and "+tables+"", pmcAndOpcChangesCount, yearEndSourceCount);
    }



}
