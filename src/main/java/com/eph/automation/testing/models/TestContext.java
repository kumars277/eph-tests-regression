package com.eph.automation.testing.models;


import com.eph.automation.testing.annotations.StaticInjection;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 */
@StaticInjection
public class TestContext {

    public static TestProperties getValues() {
        TestProperties testProperties = new TestProperties();
        String browserName = System.getProperty("browser");
        String gridRun = System.getProperty("gridRun");
        String targetDB = System.getProperty("targetDB");
        String environment = System.getProperty("environment");
        testProperties.browserType = browserName != null ? browserName : "chrome";
        testProperties.gridRun = gridRun != null ? Boolean.TRUE : Boolean.FALSE;
        testProperties.environment = environment !=null ? environment : EnumConstants.ENVIRONMENTS.SIT.name();
        testProperties.targetDB = targetDB != null ? Boolean.TRUE : Boolean.FALSE;
        return testProperties;
    }
}
