package com.eph.automation.testing.models;

import com.eph.automation.testing.annotations.StaticInjection;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 * updated by Nishant @ 18 Mar 2021 for JM UI automation
 */
@StaticInjection
public class TestContext {

    public static TestProperties getValues() {
        TestProperties testProperties = new TestProperties();
        // System.setProperty("ENV","UAT");
        System.setProperty("browser","firefox");

        String browserName = System.getProperty("browser");
        String gridRun = System.getProperty("gridRun");
        String targetDB = System.getProperty("targetDB");
        String environment = System.getProperty("ENV");
        testProperties.browserType = browserName != null ? browserName : "chromeHeadless";
        testProperties.gridRun = gridRun != null ? Boolean.TRUE : Boolean.FALSE;
        testProperties.environment = environment !=null ? environment : EnumConstants.ENVIRONMENTS.PRODUCTION.name();
        testProperties.targetDB = targetDB != null ? Boolean.TRUE : Boolean.FALSE;
        return testProperties;
    }
}
