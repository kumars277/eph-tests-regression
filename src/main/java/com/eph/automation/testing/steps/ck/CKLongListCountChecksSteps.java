package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKLongListCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class CKLongListCountChecksSteps {
    private static String CKSQL;
    private static int CK_LongList_ViewCount;
    private static int CK_LongList_TableCount;
    private static String sql;
    private static List<String> ids;

    //LongList View to Table count checks
    @Given("We know the number of (.*) data in LongList View")
    public void getCKLongListViewCount(String DPPLongListView) {
        CKSQL = String.format(CKLongListCountChecksSQL.GET_LongList_VIEW_COUNT, DPPLongListView);
        Log.info(CKSQL);
        List<Map<String, Object>> CK_LongListViewCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_LongList_ViewCount = ((Long) CK_LongListViewCount.get(0).get("Count")).intValue();
        Log.info(DPPLongListView + " table in CK LongList View has the Count: " + CK_LongList_ViewCount);
    }

    @Then("^Get the count for (.*) LongList Table")
    public void getCKLongListTableCount(String DPPLongListTable) {
        CKSQL = String.format(CKLongListCountChecksSQL.GET_LongList_Table_COUNT, DPPLongListTable);
        Log.info(CKSQL);
        List<Map<String, Object>> CKLongListTableCount = DBManager.getDLResultMap(CKSQL, Constants.AWS_URL);
        CK_LongList_TableCount = ((Long) CKLongListTableCount.get(0).get("Count")).intValue();
        Log.info(DPPLongListTable + " table in CK EBTD Inbound Table has the Count: " + CK_LongList_TableCount);
    }

    @And("^Compare the count for (.*) table between LongList View and LongList Table$")
    public void CKcompareLongListViewtoTable(String DPPLongListTable) {
        if (CK_LongList_TableCount == 0) {
            Log.info("Long List table empty Count is " + CK_LongList_TableCount);
        } else {
            Log.info("The count for table" + DPPLongListTable + " in CK LongList View: " + CK_LongList_ViewCount + " and CK CMMS Table: " + CK_LongList_TableCount);
            Assert.assertEquals("The counts for table " + DPPLongListTable + " is not equal", CK_LongList_ViewCount, CK_LongList_TableCount);
        }
    }
}
