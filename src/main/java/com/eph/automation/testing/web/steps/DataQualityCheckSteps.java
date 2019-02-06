/*
package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.ProductExtractSQL;
import cucumber.api.PendingException;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import static org.junit.Assert.assertTrue;

*/
/**
 * Created by RAVIVARMANS on 25/11/2018.
 *//*

public class DataQualityCheckSteps {

    @StaticInjection
    public DataQualityContext dataQualityContext;

    private static String sql;

    @Given("^the data is loaded from PMX to EPH$")
    public void the_data_is_loaded_from_PMX_to_EPH() throws Throwable {
        //Fabricate the test data that should fail for the each rule

        //Store the Data In the Context

        //Load the Data by Invoking the Semarchy Store Procedure
        //Framework to invoke Stored Function
//        DataLoadServiceImpl.createProductByStoreProcedure();
    }

    @When("^the product from PMX Source fails a (.*) Tier 1 DQ rule$")
    public void the_product_from_PMX_Source_fails_a_rule_Tier_DQ_rule(String dqRule) throws Throwable {
        // Express the Regexp above with the code you wish you had
        throw new PendingException();
    }

    @Then("^the product is not loaded into the golden layer in EPH$")
    public void the_product_is_not_loaded_into_the_golden_layer_in_EPH() throws Throwable {
        // Express the Regexp above with the code you wish you had
        throw new PendingException();
    }

    @And("^the product is written to the Data Quality output table with all attribute$")
    public void the_product_is_written_to_the_Data_Quality_output_table_with_all_attribute() throws Throwable {
        // Express the Regexp above with the code you wish you had
        throw new PendingException();
    }

    @And("^the product is listed as Tier 1$")
    public void the_product_is_listed_as_Tier() throws Throwable {
        // Express the Regexp above with the code you wish you had
        throw new PendingException();
    }

    @Given("^A product loaded into EPH from PMX$")
    public void A_product_loaded_into_EPH_from_PMX() throws Throwable {
        dataQualityContext.productIdentifierID = "9781416049722";
    }

    @When("^checking the loaded product in EPH$")
    public void checking_the_loaded_product_in_EPH() throws Throwable {
        sql = ProductExtractSQL.PMX_WORK_EXTRACT.replace("PARAM1",dataQualityContext.productIdentifierID);
        dataQualityContext.productDataObjectsFromSource = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class,
                Constants.PMX_UAT_URL);

        //Invoke Talend Job

        sql = ProductExtractSQL.PRODUCT_MANIFESTATION_FROM_EPH_SA.replace("PARAM1", dataQualityContext.productIdentifierID);
        dataQualityContext.productDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
    }

    @Then("^the product details are consistent between EPH and PMX$")
    public void the_product_details_are_consistent_between_EPH_and_PMX() throws Throwable {
        System.out.println(dataQualityContext.productDataObjectsFromSource.toString());
        System.out.println(dataQualityContext.productDataObjectsFromEPH.toString());

        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource
                        .equals(dataQualityContext.productDataObjectsFromEPH));
    }
}
*/
