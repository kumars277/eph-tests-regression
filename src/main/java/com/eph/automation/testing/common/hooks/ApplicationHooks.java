package com.eph.automation.testing.common.hooks;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.ui.SpecificTasks;
import com.google.inject.Inject;
import com.google.inject.Provider;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import org.openqa.selenium.WebDriver;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 */
public class ApplicationHooks {
    @Inject
    private Provider<WebDriver> driverProvider;


    @Before
    public void setUp() {
        //Set up the test pre-requisite
        Log.info("Test is starting ...");
        SpecificTasks specificTasks = new SpecificTasks();
        DataQualityContext.DateAndTime = specificTasks.getDateNTime();

    }
    @After("@UI")
    public void closeDriver() {
        Log.info("Test is ending hook ...");
        if (TestContext.getValues().gridRun && null != driverProvider) {
            driverProvider.get().quit();
        }
    }


    @After("@UI")
    public void closeBrowser()
    {
        Log.info("UI test ending...");
    }

}
