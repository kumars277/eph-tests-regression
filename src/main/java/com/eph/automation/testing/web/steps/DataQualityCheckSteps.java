package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.GenericDataObject;
import com.eph.automation.testing.services.db.DataAccessServiceImpl;
import com.eph.automation.testing.services.db.sql.DataQualityChecksSQL;
import cucumber.api.PendingException;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

/**
 * Created by RAVIVARMANS on 25/11/2018.
 */
public class DataQualityCheckSteps {

    @StaticInjection
    public DataQualityContext dataQualityContext;

    private static String sql;


    @Given("^I am EPH System User$")
    public void I_am_System_User() throws Throwable {
        sql = DataQualityChecksSQL.GET_TOTAL_COUNT_BY_TABLE_NAME
                .replace("SCHEMA_PARAM", Constants.PPM_PROD_SCHEMA)
                .replace("TABLE_PARAM", "AP_LOCATION");

        dataQualityContext.genericDataObjectsFromSource = DataAccessServiceImpl.getPPMTableRows(sql, GenericDataObject.class);

        System.out.println(dataQualityContext.genericDataObjectsFromSource.get(0).TOTAL_COUNT);

    }

    @Given("^the data is loaded from PMX to EPH$")
    public void the_data_is_loaded_from_PMX_to_EPH() throws Throwable {
        //Fabricate the test data that should fail for the each rule

        //Store the Data In the Context

        //Load the Data by Invoking the Semarchy Store Procedure
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
}
