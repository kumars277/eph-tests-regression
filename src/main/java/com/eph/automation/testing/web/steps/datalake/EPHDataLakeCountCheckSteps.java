package com.eph.automation.testing.web.steps.datalake;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.DataLakeSql.EPHDataLakeCountSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import java.util.List;
import java.util.Map;



public class EPHDataLakeCountCheckSteps {

    private static String sqlDLSTG;
    private static String sqlEPH;
    private static int DLSTGWork;
    private static int ephWork;
    private static String sql;
    private static List<String> workIds;


    @Given("^We know the number of Works in EPH$")
    public void getEphWorksCount() {
        sqlEPH = EPHDataLakeCountSQL.EPH_WORK_COUNT;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEph = DBManager.getDBResultMap(sqlEPH,Constants.EPH_URL);
        ephWork = ((Long) workCountEph.get(0).get("EPH_Work_Count")).intValue();
        Log.info("Works in EPH are => " + ephWork);
    }


    @When("^The works are in DL Outbound$")
    public void getDLWorksCount() {
        // Run the Talend job
        sqlDLSTG = EPHDataLakeCountSQL.DL_WORK_COUNT;
        Log.info(sqlDLSTG);
        List<Map<String, Object>> workCountDLSTG = DBManager.getDLResultMap(sqlDLSTG,
                Constants.AWS_URL);
        DLSTGWork = ((Long) workCountDLSTG.get(0).get("DL_Work_Count")).intValue();
        Log.info("Works in DL staging are => " + DLSTGWork);

    }

    @Then("^The work count between EPH and DL are identical$")
    public void compareWorkCountEPHtoDL() {
        Assert.assertEquals("Work Counts in the EPH and DL is not equal",ephWork,DLSTGWork);
    }


}
