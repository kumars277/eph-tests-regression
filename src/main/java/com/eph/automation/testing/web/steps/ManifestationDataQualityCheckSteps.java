package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.ProductExtractSQL;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.List;

import static database.DataAccessUnitChecks.dataQualityContext;
import static org.junit.Assert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

public class ManifestationDataQualityCheckSteps {
    private String sql;
    private static int countManifestationsEPH;
    private static int countManifestationsPMX;
    private static int countManifestationsSTGPMX;




    @Given("We get the count of the manifestations records in PMX$")
    public void getCountManifestationsPmxGdProductManifestation() {
        sql = ProductExtractSQL.COUNT_MANIFESTATIONS_IN_PMX_GD_PRODUCT_MANIFESTATION_TABLE;
        List manifestationsNumber = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class,
                Constants.PMX_SIT_URL);
        countManifestationsPMX = Integer.parseInt((String) manifestationsNumber.get(0));
        System.out.println("Count of manifestations in PMX.GD_PRODUCT_MANIFESTATION table is: " + countManifestationsPMX);
    }

    @When("We get the count of the manifestations records in PMX STG$")
    public void getCountManifestationsPmxStg() {
        sql = ProductExtractSQL.COUNT_MANIFESTATIONS_IN_EPH_STG_PMX_MANIFESTATION_TABLE;
        List manifestationsNumber = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class,
                Constants.EPH_SIT_URL);
        countManifestationsSTGPMX = Integer.parseInt((String) manifestationsNumber.get(0));
        System.out.println("Count of manifestations in STG_PMX_MANIFESTATION table is: " + countManifestationsSTGPMX);

    }

    @Then("^The number of the records in PMX and EPH staging table is equal$")
    public void verifyCountOfManifestationDataInPMXAndEPHIsEqual() {
        Assert.assertEquals("The number of manifestations in PMX GD_PRODUCT_MANIFESTATION and STG_PMX_MANIFESTATION is equal", countManifestationsPMX, countManifestationsPMX);
    }

    @When("^The manifestations are transferred to EPH$")
    public void getCountSAManifestation() {
        sql = ProductExtractSQL.COUNT_MANIFESTATIONS_IN_SA_MANIFESTATION_TABLE;
        List manifestationsNumber = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class,
                Constants.EPH_SIT_URL);
        countManifestationsEPH = Integer.parseInt((String)manifestationsNumber.get(0));
        System.out.println("Count of manifestations in STG_PMX_MANIFESTATION table is: " + countManifestationsEPH);

    }

    @Then("^The number of the records in EPH staging table and SA_MANIFESTATION is equal$")
    public void verifyCountOfManifestationDataInSTGEPHandSAManifestationIsEqual() {
        Assert.assertEquals("The number of manifestations in PMX_STG and SA_MANIFESTATION is equal", countManifestationsSTGPMX, countManifestationsEPH);
    }

    @Given("^We have manifestations in PMX$")
    public void getPMXManifestationData() {
        sql = ProductExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX;
        dataQualityContext.productDataObjectsFromSource = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class,Constants.PMX_SIT_URL);


    }

    @When("^We have manifestations in PMX STG$")
    public void getEPHManifestationData() {
        sql = ProductExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX.replace("PARAM1", DataQualityContext.productIdentifierID);
        DataQualityContext.productDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class,
                Constants.PMX_SIT_URL);
    }

    @Then("^The data for manifestations in PMX and PMX STG is equal$")
    public void compareManifestationDataBetweenPMXAndEPH() {
     assertThat("Data for manifestations in PMX and EPH is equal without order", DataQualityContext.productDataObjectsFromEPH, containsInAnyOrder(DataQualityContext.productDataObjectsFromSource));
    }
}
