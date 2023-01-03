package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKCMMSExportCountCheckSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class CKCMMSExportCountChecksSteps {
    private static String CKInboundSQL;
    private static String CKSQLView;
    private static int CK_CMMS_ViewCount;
    private static int CK_CMMS_TableCount;

    //CMMS View to Table count checks
    @Given("Get the count from the inbound for (.*)")
    public void getCKCMMSViewCount(String DPPCMMSView) {

        switch (DPPCMMSView){
            case "cmms_workflow_v":
                CKInboundSQL = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_WORKFLOW_VIEW_INBOUND_COUNT);
                break;
            case "cmms_work1_v":
                CKInboundSQL = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_WORK1_INBOUND_COUNT);
                break;
            case "cmms_work2_identifiers_v":
                CKInboundSQL = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_WORK2_IDENTIFIERS_INBOUND_COUNT);
                break;
            case "cmms_work3_subject_areas_v":
                CKInboundSQL = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_WORK3_SUBJ_AREA_INBOUND_COUNT);
                break;
            case "cmms_package1_v":
                CKInboundSQL = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_PKG1_INBOUND_COUNT);
                break;
            case "cmms_package2_works_v":
                CKInboundSQL = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_PKG2_WORKS_INBOUND_COUNT);
                break;
        }
        Log.info(CKInboundSQL);
        List<Map<String, Object>> CK_CMMSViewCount = DBManager.getDLResultMap(CKInboundSQL, Constants.AWS_URL);
        CK_CMMS_ViewCount = ((Long) CK_CMMSViewCount.get(0).get("COUNT")).intValue();
    }

    @Then("^Get the count for (.*) views")
    public void getCKCMMSTableCount(String DPPCMMSView) {
        switch (DPPCMMSView){
            case "cmms_workflow_v":
                CKSQLView = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_WORKFLOW_VIEW_COUNT);
                break;
            case "cmms_work1_v":
                CKSQLView = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_WORK1_VIEW_COUNT);
                break;
            case "cmms_work2_identifiers_v":
                CKSQLView = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_WORK2_IDENTIFIERS_VIEW_COUNT);
                break;
            case "cmms_work3_subject_areas_v":
                CKSQLView = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_WORK3_SUBJ_AREA_VIEW_COUNT);
                break;
            case "cmms_package1_v":
                CKSQLView = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_PKG1_VIEW_COUNT);
                break;
            case "cmms_package2_works_v":
                CKSQLView = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_PKG2_WORKS_VIEW_COUNT);
                break;
        }
        Log.info(CKSQLView);
        List<Map<String, Object>> CKCMMSTableCount = DBManager.getDLResultMap(CKSQLView, Constants.AWS_URL);
        CK_CMMS_TableCount = ((Long) CKCMMSTableCount.get(0).get("COUNT")).intValue();
    }

    @And("^Compare the count between the inbounds and (.*) views$")
    public void CKcompareCMMSViewtoTable(String DPPCMMSTable) {
        if (CK_CMMS_TableCount == 0) {
            Log.info("Delta Current table empty Count is " + CK_CMMS_TableCount);
        } else {
            Log.info("The Inbound count for " + DPPCMMSTable + ": "+CK_CMMS_ViewCount+" and in the views of "+DPPCMMSTable+": "+CK_CMMS_TableCount);
            Assert.assertEquals("The Inbound counts and " + DPPCMMSTable + " is not equal", CK_CMMS_ViewCount, CK_CMMS_TableCount);
        }
    }
}
