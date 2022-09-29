package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKCMMSInboundCountChecksSQL;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKReportsCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class CKCMMSInboundCountCheckSteps {
    private static String CKSQL;
    private static int CK_CMMSINBOUND_ViewCount;
    private static int CK_CMMSINBOUND_QueryCount;
    private static String sql;
    private static List<String> ids;

    @Given("We know the number of (.*) data in CMMS View")
    public void getCKCMMSInboundViewCount(String CMMSInboundView) {
        CKSQL = String.format(CKCMMSInboundCountChecksSQL.GET_CMMSINBOUND_VIEW_COUNT, CMMSInboundView);
        Log.info(CKSQL);
        List<Map<String, Object>> CK_CMMSInboundViewCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_CMMSINBOUND_ViewCount = ((Long) CK_CMMSInboundViewCount.get(0).get("Count")).intValue();
        Log.info(CMMSInboundView + "  CK CMMSInbound View has the Count: " + CK_CMMSINBOUND_ViewCount);
    }
    @Then("Get the Query count for (.*) CMMSInbound")
    public void getCKCMMSInboundQuery(String CMMSInboundView) {
        switch("CMMSInboundView") {
            case "cmms_durable_url1_form_v":
                CKSQL = String.format(CKCMMSInboundCountChecksSQL.GET_CMMSINBOUND_Durable_url1_form_Query_COUNT);
                break;
            case"cmms_durable_url2_form_v":
                CKSQL = String.format(CKCMMSInboundCountChecksSQL.GET_CMMSINBOUND_Durable_url2_form_Query_COUNT);
                break;
            case "cmms_durable_url3_form_v":
                CKSQL = String.format(CKCMMSInboundCountChecksSQL.GET_CMMSINBOUND_Durable_url3_form_Query_COUNT);
                break;
            case "cmms_durable_url_transform_v":
                CKSQL = String.format(CKCMMSInboundCountChecksSQL.GET_CMMSINBOUND_Durable_url_Transform_Query_COUNT);
                break;
        }
        Log.info(CKSQL);
        List<Map<String, Object>> CK_CMMSInboundQueryCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_CMMSINBOUND_QueryCount = ((Long) CK_CMMSInboundQueryCount.get(0).get("Count")).intValue();
        Log.info(CMMSInboundView + "  CK Query has the Count: " + CK_CMMSINBOUND_QueryCount);
    }

    @And("Compare the count for (.*) table between CMMS Inbound View and CMMSInbound query")
    public void CKcompareViewtoQuery(String CMMSInboundView) {
        if (CK_CMMSINBOUND_QueryCount == 0) {
            Log.info("CMMSInbound Query empty Count is " + CK_CMMSINBOUND_QueryCount);
        } else {
            Log.info("The count for View " + CMMSInboundView + " in CKCMMS Inbound View: " + CK_CMMSINBOUND_ViewCount + " and CKCMMS Query count: " + CK_CMMSINBOUND_QueryCount);
            Assert.assertEquals("The counts for View " + CMMSInboundView + " is not equal", CK_CMMSINBOUND_ViewCount, CK_CMMSINBOUND_QueryCount);
        }
    }


}
