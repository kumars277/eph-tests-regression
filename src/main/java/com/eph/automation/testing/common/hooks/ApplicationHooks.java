package com.eph.automation.testing.common.hooks;


import com.eph.automation.testing.models.TestContext;
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
    }
    @After(order = 99)
    public void closeDriver() {
        if (TestContext.getValues().gridRun && null != driverProvider) {
            driverProvider.get().quit();
        }
    }

}
