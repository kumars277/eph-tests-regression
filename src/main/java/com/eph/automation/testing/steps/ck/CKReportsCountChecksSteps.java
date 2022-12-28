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
    private static int CK_Reports_InboundCount;
    private static int CK_Reports_TableCount;
    private static String sql;
    private static List<String> ids;

    //LongList View to Table count checks
    @Given("We know the count from the inbound tables (.*)")
    public void getCKReportsViewCount(String table) {
        switch (table){
            case "ck_workflow_tableau":
                CKSQL = String.format(CKReportsCountChecksSQL.GET_WorkFlow_Tableu_INBOUND_COUNT);
                break;
            case "ck_workflow_control_p1":
                CKSQL = String.format(CKReportsCountChecksSQL.GET_Workflow_p1_INBOUND_COUNT);
                break;
            case "ck_workflow_control_p2":
                CKSQL = String.format(CKReportsCountChecksSQL.GET_Workflow_p2_INBOUND_COUNT);
                break;
            case "ck_workflow_tableau_package_works":
                CKSQL = String.format(CKReportsCountChecksSQL.GET_WorkFlow_tableu_pkg_Inbound_work);
                break;
            case "ck_transaction_workflow":
                CKSQL = String.format(CKReportsCountChecksSQL.GET_Transaction_workflow_Inbound_Count);
                break;
        }

        Log.info(CKSQL);
        List<Map<String, Object>> CK_ReportsInboundViewCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_Reports_InboundCount = ((Long) CK_ReportsInboundViewCount.get(0).get("sourceCount")).intValue();
        Log.info("Inbound count for the "+table+" has the Count: " + CK_Reports_InboundCount);
    }

    @Then("^Get the count for (.*) Reports Table")
    public void getCKReportsTableCount(String DPPReportsTable) {
        CKSQL = String.format(CKReportsCountChecksSQL.GET_Reports_Table_COUNT, DPPReportsTable);
        Log.info(CKSQL);
        List<Map<String, Object>> CKReportTableCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_Reports_TableCount = ((Long) CKReportTableCount.get(0).get("targetCount")).intValue();
        Log.info(DPPReportsTable + " table in CK Reports Table has the Count: " + CK_Reports_TableCount);
    }

    @And("^Compare the count between Inbound and Reports Table (.*)$")
    public void CKcompareReportsViewtoTable(String DPPReportsTable) {
        if (CK_Reports_TableCount == 0) {
            Log.info("Reports table empty Count is " + CK_Reports_TableCount);
        } else {
            Log.info("The count in inbounds are "+CK_Reports_InboundCount+" and in the ReportTables are "+CK_Reports_TableCount);
            Assert.assertEquals("The counts between inbound and "+DPPReportsTable+" are not matching", CK_Reports_TableCount, CK_Reports_InboundCount);
        }
    }
}
