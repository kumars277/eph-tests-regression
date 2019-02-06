package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.List;

public class WorksCountCheckSteps {
    private static List<ProductDataObject> workCountPmx;
    private static List<ProductDataObject> workCountPMXSTG;
    private static List<ProductDataObject> workCountPMXSTGDistinct;
    private static List<ProductDataObject> workCountEPH;
    private static List<ProductDataObject> workCountEPHGD;
    private static String sqlPMX;
    private static String sqlPMXSTG;
    private static String sqlPMXSTGDistinct;
    private static String sqlEPH;
    private static int pmxWork;
    private static int pmxSTGWork;
    private static int pmxSTGWorkDistinct;
    private static int ephWork;
    private static int ephWorkGD;

    @Given("^We know the number of Works in PMX$")
    public void getPmxWorks() {
        sqlPMX = WorkCountSQL.PMX_WORKS_COUNT;
        System.out.println(sqlPMX);
        workCountPmx = DBManager.getDBResultAsBeanList(sqlPMX, ProductDataObject.class,
                Constants.PMX_SIT_URL);
        pmxWork = workCountPmx.get(0).workCountPmx;
        System.out.println("Works in PMX are: " + pmxWork);
    }


    @When("^The works are in PMX Staging$")
    public void getPMXStagingWorks(){

        // Run the Talend job

        sqlPMXSTG = WorkCountSQL.PMX_STG_WORKS_COUNT;
        workCountPMXSTG =DBManager.getDBResultAsBeanList(sqlPMXSTG, ProductDataObject.class,
                Constants.EPH_SIT_URL);
        pmxSTGWork = workCountPMXSTG.get(0).workCountPMXSTG;
        System.out.println("\nWorks in PMX staging are: " + pmxSTGWork);

    }

    @Then("^The work number between (.*) and (.*) is identical$")
    public void comparePMXtoPMXStaging(String source, String target){
        if (source.contentEquals("PMX")) {
            Assert.assertEquals("The number of works in PMX and PMX Staging is not equal!", pmxWork, pmxSTGWork);
        }else if (source.contentEquals("PMX STG")){
            Assert.assertEquals("The number of works in PMX Staging and EPH SA is not equal!", pmxSTGWorkDistinct, ephWork);
        } else {
            Assert.assertEquals("The number of works in SA and GD is not equal!", ephWork, ephWorkGD);
        }
    }

    @When("^The works are transferred to EPH")
    public void getEPHWorks(){
        // Run the Talend Job

        sqlEPH = WorkCountSQL.EPH_SA_WORKS_COUNT;
        workCountEPH =DBManager.getDBResultAsBeanList(sqlEPH, ProductDataObject.class,
                Constants.EPH_SIT_URL);
        ephWork = workCountEPH.get(0).workCountEPH;
        System.out.println("\nWorks in EPH SA are: " + ephWork);

        sqlEPH = WorkCountSQL.EPH_GD_WORKS_COUNT;
        workCountEPHGD =DBManager.getDBResultAsBeanList(sqlEPH, ProductDataObject.class,
                Constants.EPH_SIT_URL);
        ephWorkGD = workCountEPH.get(0).workCountEPH;
        System.out.println("\nWorks in EPH GD are: " + ephWork);

        sqlPMXSTGDistinct = WorkCountSQL.PMX_STG_WORKS_COUNT_Distinct;
        workCountPMXSTGDistinct =DBManager.getDBResultAsBeanList(sqlPMXSTGDistinct, ProductDataObject.class,
                Constants.EPH_SIT_URL);
        pmxSTGWorkDistinct = workCountPMXSTGDistinct.get(0).workCountPMXSTG;
        System.out.println("\nDistinct works in PMX staging are: " + pmxSTGWorkDistinct);

    }


}
