package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import com.eph.automation.testing.helper.Log;

import java.util.List;

public class WorksCountCheckSteps {
    private static List<WorkDataObject> workCountPmx;
    private static List<WorkDataObject> workCountPMXSTG;
    private static List<WorkDataObject> workCountPMXSTGDistinct;
    private static List<WorkDataObject> workCountSTGDQ;
    private static List<WorkDataObject> workCountSTGDQNOERROR;
    private static List<WorkDataObject> workCountEPH;
    private static List<WorkDataObject> workCountEPHGD;
    private static String sqlPMX;
    private static String sqlPMXSTG;
    private static String sqlPMXSTGDistinct;
    private static String sqlEPH;
    private static String sql;
    private static int pmxWork;
    private static int pmxSTGWork;
    private static int pmxSTGWorkDistinct;
    private static int ephWork;
    private static int ephWorkGD;
    private static int dqWorks;
    private static int dqNoErrorWorks;

    @Given("^We know the number of Works in PMX$")
    public void getPmxWorks() {
        sqlPMX = WorkCountSQL.PMX_WORKS_COUNT;
        Log.info(sqlPMX);
        workCountPmx = DBManager.getDBResultAsBeanList(sqlPMX, WorkDataObject.class,
                Constants.PMX_URL);
        pmxWork = workCountPmx.get(0).workCountPmx;
        Log.info("Works in PMX are: " + pmxWork);
    }


    @When("^The works are in PMX Staging$")
    public void getPMXStagingWorks(){

        // Run the Talend job

        sqlPMXSTG = WorkCountSQL.PMX_STG_WORKS_COUNT;
        workCountPMXSTG =DBManager.getDBResultAsBeanList(sqlPMXSTG, WorkDataObject.class,
                Constants.EPH_URL);
        pmxSTGWork = workCountPMXSTG.get(0).workCountPMXSTG;
        Log.info("\nWorks in PMX staging are: " + pmxSTGWork);

    }

    @Then("^The work number between (.*) and (.*) is identical$")
    public void comparePMXtoPMXStaging(String source, String target){
        if (source.contentEquals("PMX")) {
            Assert.assertEquals("The number of works in PMX and PMX Staging is not equal!", pmxWork, pmxSTGWork);
            Log.info("The count between PMX and Staging is equal");
        }else if (source.contentEquals("PMX STG")){
            Assert.assertEquals("The number of works in PMX Staging and DQ is not equal!", pmxSTGWorkDistinct,
                    dqWorks);
            Log.info("The count between Staging and SA is equal");
        } else if (target.contentEquals("SA")){
            Assert.assertEquals("The number of works in DQ and SA is not equal!", dqNoErrorWorks, ephWork);
            Log.info("The count between SA and GD is equal");
        } else{
            Assert.assertEquals("The number of works in SA and GD is not equal!", ephWork, ephWorkGD);
            Log.info("The count between SA and GD is equal");
        }
    }

    @When("^The works are transferred to EPH")
    public void getEPHWorks(){
        // Run the Talend Job

        sqlEPH = WorkCountSQL.EPH_SA_WORKS_COUNT;
        workCountEPH =DBManager.getDBResultAsBeanList(sqlEPH, WorkDataObject.class,
                Constants.EPH_URL);
        ephWork = workCountEPH.get(0).workCountEPH;
        Log.info("\nWorks in EPH SA are: " + ephWork);

        sqlEPH = WorkCountSQL.EPH_GD_WORKS_COUNT;
        workCountEPHGD =DBManager.getDBResultAsBeanList(sqlEPH, WorkDataObject.class,
                Constants.EPH_URL);
        ephWorkGD = workCountEPHGD.get(0).workCountEPHGD;
        Log.info("\nWorks in EPH GD are: " + ephWorkGD);

        sqlPMXSTGDistinct = WorkCountSQL.PMX_STG_WORKS_COUNT_Distinct;
        workCountPMXSTGDistinct =DBManager.getDBResultAsBeanList(sqlPMXSTGDistinct, WorkDataObject.class,
                Constants.EPH_URL);
        pmxSTGWorkDistinct = workCountPMXSTGDistinct.get(0).workCountPMXSTG;
        Log.info("\nDistinct works in PMX staging are: " + pmxSTGWorkDistinct);

        sql = WorkCountSQL.PMX_STG_DQ_WORKS_COUNT;
        workCountSTGDQ =DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                Constants.EPH_URL);
        dqWorks = workCountSTGDQ.get(0).workCountDQSTG;
        Log.info("\nThe Works in DQ table are : " + workCountSTGDQ.get(0).workCountDQSTG);

        sql = WorkCountSQL.PMX_STG_DQ_WORKS_COUNT_NoErr;
        workCountSTGDQNOERROR =DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                Constants.EPH_URL);
        dqNoErrorWorks = workCountSTGDQNOERROR.get(0).workCountDQSTGnoError;
        Log.info("\nThe Works in DQ table without error are : " + workCountSTGDQNOERROR.get(0).workCountDQSTGnoError);

    }


}
