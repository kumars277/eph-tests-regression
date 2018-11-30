package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.Product;
import com.eph.automation.testing.models.contexts.LoadBatchContext;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

/**
 * Created by RAVIVARMANS on 29/11/2018.
 */
public class TalendInBoundIntegrationSteps {

    @StaticInjection
    public static LoadBatchContext loadBatchContext;

    @Given("^I have details of PMX product details to load$")
    public void loadDetails() throws Throwable {
        //Set the Context to create New Work
        loadBatchContext.productDetailsToCreate = Product.getNewProduct();
    }

    @When("^I load complete a full reload of PMX data for Books and Journals$")
    public void I_load_complete_a_full_reload_of_PMX_data_for_Books_and_Journals() throws Throwable {
        //Call the Talend Job
        //RunTalend Job is the Framework as soon as the TALEND EPH JOB developed this services can be used
        // to run the Talend Job
//        TACServices.runTalendJob("EPH Talend Job");
        //Wait for few minutes to JOb finish - Implicit Wait
    }

    @Then("^The staging tables for PMX is populated successfully$")
    public void The_staging_tables_for_PMX_is_populated_successfully() throws Throwable {
        //Now Built the SQL to get the details from SA Tables using the Framework
    }

    @And("^I wait till PMX batch is completed$")
    public void I_wait_till_PMX_batch_is_completed() throws Throwable {
        //When the Talend Finished, it would call the Semarchy Cerfication JOb

        //Wait till the certification job to be finished. The logic very much same as CMX - Refer Ringgold Job


    }

    @And("^all the necessary entities are loaded in golden layer for Work$")
    public void all_the_necessary_entities_are_loaded_in_golden_layer_for_Work() throws Throwable {

        //Now built the SQL to talk to GD Layer

    }

}
