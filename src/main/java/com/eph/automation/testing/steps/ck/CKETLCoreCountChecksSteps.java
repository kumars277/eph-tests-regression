package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKETLCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class CKETLCoreCountChecksSteps {
    private static String CKSQL;
    private static int CK_Delta_CurrentCount;
    private static int CK_Delta_HistoryCount;

    //Delta History to Delta Current count checks
    @Given("We know the number of CK (.*) data in Delta History")
    public void getCKDeltaHistoryCount(String DeltaHistorytablename) {
        CKSQL = String.format(CKETLCountChecksSQL.GET_CK_Delta_History, DeltaHistorytablename, DeltaHistorytablename);
        Log.info(CKSQL);
        List<Map<String,Object>> CKDeltaHistoryTableCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_Delta_HistoryCount = ((Long) CKDeltaHistoryTableCount.get(0).get("Count")).intValue();
        Log.info(DeltaHistorytablename + " table in CK Delta History has the Count: " + CK_Delta_HistoryCount);
    }

    @Then("^Get the count for CK (.*) Delta Current")
    public void getCKDeltaCurrentCount(String DeltaCurrenttablename) {
        CKSQL= String.format(CKETLCountChecksSQL.GET_CK_Delta_Current, DeltaCurrenttablename);
        Log.info(CKSQL);
        List<Map<String,Object>> CKCurrentCount = DBManager.getDLResultMap(CKSQL,Constants.AWS_URL);
        CK_Delta_CurrentCount = ((Long) CKCurrentCount.get(0).get("Count")).intValue();
        Log.info(DeltaCurrenttablename + " table in CK Delta Current has the Count: " + CK_Delta_CurrentCount);
    }

    @And("^Compare the CK count for (.*) table between History and Delta Current$")
    public void CKcompareHistorytoDeltaCurrent(String DeltaHistorytablename) {
        Log.info("The count for table" + DeltaHistorytablename + " in CK Delta History: " + CK_Delta_HistoryCount + " and Delta Current: " + CK_Delta_CurrentCount);
        Assert.assertEquals("The counts for table " + DeltaHistorytablename + " is not equal", CK_Delta_HistoryCount, CK_Delta_CurrentCount);
    }
}
