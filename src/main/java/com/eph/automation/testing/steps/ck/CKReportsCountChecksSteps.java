package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKReportsCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class CKReportsCountChecksSteps {
    private static String CKSQL;
    private static int CK_Reports_ViewCount;
    private static int CK_Reports_TableCount;
    private static String sql;
    private static List<String> ids;

    //LongList View to Table count checks
    @Given("We know the number of (.*) data in Reports View")
    public void getCKReportsViewCount(String DPPReportsView) {
        CKSQL = String.format(CKReportsCountChecksSQL.GET_Reports_VIEW_COUNT, DPPReportsView);
        Log.info(CKSQL);
        List<Map<String, Object>> CK_ReportsViewCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_Reports_ViewCount = ((Long) CK_ReportsViewCount.get(0).get("Count")).intValue();
        Log.info(DPPReportsView + " table in CK Reports View has the Count: " + CK_Reports_ViewCount);
    }

    @Then("^Get the count for (.*) Reports Table")
    public void getCKReportsTableCount(String DPPReportsTable) {
        CKSQL = String.format(CKReportsCountChecksSQL.GET_Reports_Table_COUNT, DPPReportsTable);
        Log.info(CKSQL);
        List<Map<String, Object>> CKLongListTableCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_Reports_TableCount = ((Long) CKLongListTableCount.get(0).get("Count")).intValue();
        Log.info(DPPReportsTable + " table in CK Reports Table has the Count: " + CK_Reports_TableCount);
    }

    @And("^Compare the count for (.*) table between Reports View and Reports Table$")
    public void CKcompareReportsViewtoTable(String DPPReportsTable) {
        if (CK_Reports_TableCount == 0) {
            Log.info("Reports table empty Count is " + CK_Reports_TableCount);
        } else {
            Log.info("The count for table" + DPPReportsTable + " in CK Reports View: " + CK_Reports_ViewCount + " and CK Reports Table: " + CK_Reports_TableCount);
            Assert.assertEquals("The counts for table " + DPPReportsTable + " is not equal", CK_Reports_ViewCount, CK_Reports_TableCount);
        }
    }
}
