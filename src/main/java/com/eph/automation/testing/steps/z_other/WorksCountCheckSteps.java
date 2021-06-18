package com.eph.automation.testing.steps.z_other;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import com.eph.automation.testing.helper.Log;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

//import static sun.security.krb5.Confounder.intValue;

public class WorksCountCheckSteps {

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
    private static int aeCount;
    private static String refreshDate;

    @Given("^We know the number of Works in PMX$")
    public void getPmxWorks() {
        sqlPMX = WorkCountSQL.PMX_WORKS_COUNT;
        Log.info(sqlPMX);
        List<Map<String, Object>> workCountPmx = DBManager.getDBResultMap(sqlPMX,
                Constants.PMX_URL);
        pmxWork = ((BigDecimal) workCountPmx.get(0).get("workCountPmx")).intValue();
        Log.info("Works in PMX are: " + pmxWork);
    }


    @When("^The works are in PMX Staging$")
    public void getPMXStagingWorks() {

        // Run the Talend job
        sqlPMXSTG = WorkCountSQL.PMX_STG_WORKS_COUNT;
        List<Map<String, Object>> workCountPMXSTG = DBManager.getDBResultMap(sqlPMXSTG,
                Constants.EPH_URL);
        pmxSTGWork = ((Long) workCountPMXSTG.get(0).get("workCountPMXSTG")).intValue();
        Log.info("Works in PMX staging are: " + pmxSTGWork);

    }

    @Then("^The work number between (.*) and (.*) is identical$")
    public void comparePMXtoPMXStaging(String source, String target) {
        if (source.contentEquals("PMX")) {
            Assert.assertEquals("The number of works in PMX and PMX Staging is not equal!", pmxWork, pmxSTGWork);
            Log.info("The count between PMX and Staging is equal");
        } else if (source.contentEquals("EPH STG")) {
            Assert.assertEquals("The number of works in PMX Staging and DQ is not equal!", pmxSTGWorkDistinct,
                    dqWorks);
            Log.info("The count between Staging and SA is equal");
        } else if (target.contentEquals("EPH SA")) {
            Assert.assertEquals("The number of works in DQ and SA is not equal!", dqNoErrorWorks, ephWork);
            Log.info("The count between SA and GD is equal");
        } else if (target.contentEquals("EPH GD")) {
            Assert.assertEquals("The number of works in SA and GD is not equal!", ephWork, ephWorkGD);
            Log.info("The count between SA and GD is equal");
        } else {
            Assert.assertEquals("The number of works in SA and GD with AE is not equal!", ephWork, ephWorkGD + aeCount);
            Log.info("The count between SA and GD is equal");
        }
    }

    @When("^The works are transferred to EPH")
    public void getEPHWorks() {
        // Run the Talend Job

        sqlEPH = WorkCountSQL.EPH_SA_WORKS_COUNT;
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH, Constants.EPH_URL);
        ephWork = ((Long) workCountEPH.get(0).get("workCountEPH")).intValue();
        Log.info("Works in EPH SA are: " + ephWork);

        sqlEPH = WorkCountSQL.EPH_GD_WORKS_COUNT;
        List<Map<String, Object>> workCountEPHGD = DBManager.getDBResultMap(sqlEPH, Constants.EPH_URL);
        ephWorkGD = ((Long) workCountEPHGD.get(0).get("workCountEPHGD")).intValue();
        Log.info("Works in EPH GD are: " + ephWorkGD);


        if (System.getProperty("LOAD") == null || System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
            sqlPMXSTGDistinct = WorkCountSQL.PMX_STG_WORKS_COUNT_Distinct;
            Log.info(sql);
        } else {
            sql = WorkCountSQL.GET_REFRESH_DATE;
            Log.info(sql);
            List<Map<String, Object>> refreshDateNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            refreshDate = (String) refreshDateNumber.get(1).get("refresh_timestamp");
            Log.info("refresh date: " + refreshDate);
            sqlPMXSTGDistinct = WorkCountSQL.PMX_STG_WORKS_COUNT_DELTA.replace("PARAM1", refreshDate);
            Log.info(sqlPMXSTGDistinct);
        }

        List<Map<String, Object>> workCountPMXSTGDistinct = DBManager.getDBResultMap(sqlPMXSTGDistinct, Constants.EPH_URL);
        pmxSTGWorkDistinct = ((Long) workCountPMXSTGDistinct.get(0).get("workCountPMXSTG")).intValue();
        Log.info("Distinct works in PMX staging are: " + pmxSTGWorkDistinct);



        sql = WorkCountSQL.PMX_STG_DQ_WORKS_COUNT;
        List<Map<String, Object>> workCountSTGDQ = DBManager.getDBResultMap(sql,
                Constants.EPH_URL);
        dqWorks = ((Long) workCountSTGDQ.get(0).get("workCountDQSTG")).intValue();
        Log.info("The Works in DQ table are : " + dqWorks);

        sql = WorkCountSQL.PMX_STG_DQ_WORKS_COUNT_NoErr;
        List<Map<String, Object>> workCountSTGDQNOERROR = DBManager.getDBResultMapWithSetSchema(sql,
                Constants.EPH_URL);
        dqNoErrorWorks = ((Long) workCountSTGDQNOERROR.get(0).get("count")).intValue();
        Log.info("The Works in DQ table without error are : " + dqNoErrorWorks);

        sql = WorkCountSQL.EPH_AE_WORKS_COUNT;
        List<Map<String, Object>> errorsCount = DBManager.getDBResultMap(sql,
                Constants.EPH_URL);
        aeCount = ((Long) errorsCount.get(0).get("errorsCountEPH")).intValue();
        Log.info("The Works in AE table are : " + aeCount);

    }


}
