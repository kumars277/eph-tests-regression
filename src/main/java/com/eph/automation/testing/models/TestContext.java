package com.eph.automation.testing.models;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.helper.Log;

@StaticInjection
public class TestContext {

    public static TestProperties getValues() {
        TestProperties testProperties = new TestProperties();

         // System.setProperty("ENV","UAT");
         //   System.setProperty("browser","chrome");


        String browserName = System.getProperty("browser");
        String gridRun = System.getProperty("gridRun");
        String targetDB = System.getProperty("targetDB");
        String environment = System.getProperty("ENV");
        String s3Key  = System.getProperty("S3file");
        String rowFrom = System.getProperty("rowFrom");
        String rowTill = System.getProperty("rowTill");

        testProperties.browserType = browserName != null ? browserName : "chrome";
        testProperties.gridRun = gridRun != null ? Boolean.TRUE : Boolean.FALSE;
        testProperties.environment = environment !=null ? environment : EnumConstants.ENVIRONMENTS.SIT.name();
        testProperties.targetDB = targetDB != null ? Boolean.TRUE : Boolean.FALSE;
        testProperties.s3Key = s3Key !=null ? s3Key: "Image URLs for parties.csv";
        testProperties.rowFrom =rowFrom !=null ?rowFrom:"";
        testProperties.rowTill =rowTill !=null ?rowTill:"";


        return testProperties;
    }
}
