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
    private static String CKSQL;
    private static int CK_CMMS_ViewCount;
    private static int CK_CMMS_TableCount;

    //CMMS View to Table count checks
    @Given("We know the number of CK (.*) data in CMMS Outbound View")
    public void getCKCMMSViewCount(String DPPCMMSView) {
        CKSQL = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_VIEW_COUNT, DPPCMMSView);
        Log.info(CKSQL);
        List<Map<String, Object>> CK_CMMSViewCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_CMMS_ViewCount = ((Long) CK_CMMSViewCount.get(0).get("count")).intValue();
        Log.info(DPPCMMSView + " table in CK CMMS View has the Count: " + CK_CMMS_ViewCount);
    }

    @Then("^Get the count for CK (.*) CMMS Table")
    public void getCKCMMSTableCount(String DPPCMMSTable) {
        CKSQL = String.format(CKCMMSExportCountCheckSQL.GET_CMMS_Table_COUNT, DPPCMMSTable);
        Log.info(CKSQL);
        List<Map<String, Object>> CKCMMSTableCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_CMMS_TableCount = ((Long) CKCMMSTableCount.get(0).get("Count")).intValue();
        Log.info(DPPCMMSTable + " table in CK Delta Current has the Count: " + CK_CMMS_TableCount);
    }

    @And("^Compare the CK count for (.*) table between CMMS View and CMMS Table$")
    public void CKcompareCMMSViewtoTable(String DPPCMMSTable) {
        if (CK_CMMS_TableCount == 0) {
            Log.info("Delta Current table empty Count is " + CK_CMMS_TableCount);
        } else {
            Log.info("The count for table" + DPPCMMSTable + " in CK CMMS View: " + CK_CMMS_ViewCount + " and CK CMMS Table: " + CK_CMMS_TableCount);
            Assert.assertEquals("The counts for table " + DPPCMMSTable + " is not equal", CK_CMMS_ViewCount, CK_CMMS_TableCount);
        }
    }
}
