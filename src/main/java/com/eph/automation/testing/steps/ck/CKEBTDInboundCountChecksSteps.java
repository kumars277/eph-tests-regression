package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKEBTDInboundCountCheckSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class CKEBTDInboundCountChecksSteps {
    private static String CKSQL;
    private static int CK_EBTD_Inbound_Count;
    private static int CK_EBTD_Inbound_ViewCount;

    //EBTD View to Table count checks
    @Given("We know the number of CK Inbound data in EBTD View")
    public void getCKCMMSViewCount() {
        CKSQL = String.format(CKEBTDInboundCountCheckSQL.GET_EBTD_Inbound_COUNT);
        Log.info(CKSQL);
        List<Map<String, Object>> CK_EBTDInboundViewCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_EBTD_Inbound_Count = ((Long) CK_EBTDInboundViewCount.get(0).get("sourceCount")).intValue();
        Log.info("Inbound table in CK EBTD Inbound has the Count: " + CK_EBTD_Inbound_Count);
    }

    @Then("^Get the count for CK (.*) EBTD Table")
    public void getCKCMMSTableCount(String DPPEBTDTable) {
        CKSQL = String.format(CKEBTDInboundCountCheckSQL.GET_EBTD_Inbound_View_COUNT, DPPEBTDTable);
        Log.info(CKSQL);
        List<Map<String, Object>> CKCMMSTableCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_EBTD_Inbound_ViewCount = ((Long) CKCMMSTableCount.get(0).get("targetCount")).intValue();
        Log.info(DPPEBTDTable + " table in CK EBTD Inbound View has the Count: " + CK_EBTD_Inbound_ViewCount);
    }

    @And("^Compare the CK count for (.*) and Inbound$")
    public void CKcompareCMMSViewtoTable(String DPPEBTDTable) {
        if (CK_EBTD_Inbound_ViewCount == 0) {
            Log.info("EBTD Inbound table empty Count is " + CK_EBTD_Inbound_ViewCount);
        } else {
            Log.info("The count for table" + DPPEBTDTable + " in Inbound: " + CK_EBTD_Inbound_Count + " and EBTD View: " + CK_EBTD_Inbound_ViewCount);
            Assert.assertEquals("The counts for inbound and EBTD Views", CK_EBTD_Inbound_Count, CK_EBTD_Inbound_ViewCount);
        }
    }
}
