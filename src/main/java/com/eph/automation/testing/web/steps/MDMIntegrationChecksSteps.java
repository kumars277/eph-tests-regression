package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.Product;
import com.eph.automation.testing.models.contexts.LoadBatchContext;
import com.eph.automation.testing.services.db.DataLoadServiceImpl;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

/**
 * Created by RAVIVARMANS on 28/11/2018.
 */
public class MDMIntegrationChecksSteps {

    @StaticInjection
    public static LoadBatchContext loadBatchContext;

    @Given("^A work is moved from PMX to EPH$")
    public void aWorkNeedToCreateInEPH() throws Throwable {
        //Set the Context to create New Work
        loadBatchContext.productDetailsToCreate = Product.getNewProduct();
    }

    @When("^A new work is created that has not existed previously$")
    public void A_new_work_is_created_that_has_not_existed_previously() throws Throwable {
        //Check the Product ID not exist in Semarchy - SQL Checks

        //Now Create the New Product BY  Calling the Semarchy Certification
        DataLoadServiceImpl.createProductByStoreProcedure();

        //Wait till the batch

    }

    @Then("^A unique reference is assigned that is clearly identifiable as a work identifier$")
    public void A_unique_reference_is_assigned_that_is_clearly_identifiable_as_a_work_identifier() throws Throwable {
        //Build the SQL to get the details from GD Layer and Assert
    }
}
