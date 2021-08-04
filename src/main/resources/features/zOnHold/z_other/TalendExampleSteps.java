/*
package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.helper.JobUtils;
import features.zOnHold.znotused.context.DataLoadContext;
import features.zOnHold.znotused.context.LoadBatchContext;
import features.zOnHold.talend.TACServices;
import features.zOnHold.talend.TalendServerVariables;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;

*/
/**
 * Created by RAVIVARMANS on 30/11/2018.
 *//*

public class TalendExampleSteps {

    @StaticInjection
    public static LoadBatchContext loadBatchContext;

    @StaticInjection
    public static DataLoadContext dataLoadContext;


    @Given("^I have existing but no change in ringgold customer details to load into ECH$")
    public void setTheFileToLoadInTheContext() throws Throwable {
        dataLoadContext.incrementalFile  = "src/main/resources/xml/example/institution_base.xml";
        dataLoadContext.sftpLandingLocation = TalendServerVariables.inputDirPrj114Job200;
    }

    @When("^I run the weekly Ringgold Job$")
    public void I_run_the_Talend_Job() throws Throwable {
        TACServices.uploadSourceFileAndRunTalendJob(dataLoadContext);
        //Wait for few minutes
        JobUtils.waitForTheJob(2);
    }

    @And("^I wait till Ringgold batch is completed$")
    public void I_wait_till_Ringgold_batch_is_completed() throws Throwable {
        //Need to find the batch ID for this Talend Job by looking at in DL_BATCH with the Load ID
        String jobSQL = "";

        //Once knwoing the batch ID then wait till the batch to complete
        //Set the Batch ID in the Load Batch Context

        //The Wait
        JobUtils.waitTillTheBatchComplete();
    }
}
*/
