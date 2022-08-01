package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.CK.CKAccessDLContext;
import com.eph.automation.testing.models.dao.CK.CKCurrentTablesDataObject;
import com.eph.automation.testing.models.dao.CK.CKInboundSourceTableDataObject;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKEBTDInboundCountCheckSQL;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKEBTDInboundDataChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CKEBTDInboundCountChecksSteps {
    private static String CKSQL;
    private static int CK_EBTD_Inbound_ViewCount;
    private static int CK_EBTD_Inbound_TableCount;
    private static String sql;
    private static List<String> ids;

    //EBTD View to Table count checks
    @Given("We know the number of CK (.*) data in EBTD View")
    public void getCKCMMSViewCount(String DPPEBTDView) {
        CKSQL = String.format(CKEBTDInboundCountCheckSQL.GET_EBTD_Inbound_VIEW_COUNT, DPPEBTDView);
        Log.info(CKSQL);
        List<Map<String, Object>> CK_EBTDInboundViewCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_EBTD_Inbound_ViewCount = ((Long) CK_EBTDInboundViewCount.get(0).get("Count")).intValue();
        Log.info(DPPEBTDView + " table in CK EBTD Inbound View has the Count: " + CK_EBTD_Inbound_ViewCount);
    }

    @Then("^Get the count for CK (.*) EBTD Table")
    public void getCKCMMSTableCount(String DPPEBTDTable) {
        CKSQL = String.format(CKEBTDInboundCountCheckSQL.GET_EBTD_Inbound_Table_COUNT, DPPEBTDTable);
        Log.info(CKSQL);
        List<Map<String, Object>> CKCMMSTableCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_EBTD_Inbound_TableCount = ((Long) CKCMMSTableCount.get(0).get("Count")).intValue();
        Log.info(DPPEBTDTable + " table in CK EBTD Inbound Table has the Count: " + CK_EBTD_Inbound_TableCount);
    }

    @And("^Compare the CK count for (.*) table between EBTD View and EBTD Table$")
    public void CKcompareCMMSViewtoTable(String DPPEBTDTable) {
        if (CK_EBTD_Inbound_TableCount == 0) {
            Log.info("EBTD Inbound table empty Count is " + CK_EBTD_Inbound_TableCount);
        } else {
            Log.info("The count for table" + DPPEBTDTable + " in CK CMMS View: " + CK_EBTD_Inbound_ViewCount + " and CK CMMS Table: " + CK_EBTD_Inbound_TableCount);
            Assert.assertEquals("The counts for table " + DPPEBTDTable + " is not equal", CK_EBTD_Inbound_ViewCount, CK_EBTD_Inbound_TableCount);
        }
    }
}
