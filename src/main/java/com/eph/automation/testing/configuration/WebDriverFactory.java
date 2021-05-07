package com.eph.automation.testing.configuration;


import com.eph.automation.testing.models.TestContext;
import com.google.inject.Provider;
import io.github.bonigarcia.wdm.ChromeDriverManager;
import io.github.bonigarcia.wdm.InternetExplorerDriverManager;
import org.openqa.selenium.Platform;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxBinary;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.firefox.FirefoxProfile;
import org.openqa.selenium.ie.InternetExplorerDriver;
import org.openqa.selenium.remote.CapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 * updated by Nishant @20 April 2021
 */

public class WebDriverFactory implements Provider<WebDriver> {


    @Override
    public WebDriver get() {

        if (TestContext.getValues().gridRun) {
            try {
                DesiredCapabilities capabilities = DesiredCapabilities.chrome();
                capabilities.setPlatform(Platform.ANY);
                URL hubUrl = new URL("http://10.153.95.253:4444/wd/hub");


                return new RemoteWebDriver(hubUrl, capabilities);


            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        } else if (true) {
            WebDriver driver;
            String labdataHub = "https://n.chitre:SFsaM0JzcIrCPa2V29NgVzFsHPaLawZ8D0gKp287B7lwUqH7j0@hub.lambdatest.com/wd/hub";

            DesiredCapabilities capability = new DesiredCapabilities();

            capability.setCapability(CapabilityType.BROWSER_NAME, TestContext.getValues().browserType);
               capability.setCapability(CapabilityType.VERSION, "88.0");
               capability.setCapability(CapabilityType.PLATFORM, "win10");
            capability.setCapability("build", "LambadaTest-SIT Test");
            capability.setCapability("name", "lambada remote browser Test");
       //     capability.setCapability("network", true);
        //    capability.setCapability("video", true);
         //   capability.setCapability("console", true);
          //  capability.setCapability("visual", true);

            //    String gridURL = "https://" + username + ":" + accesskey + "@hub.lambdatest.com/wd/hub";
            try {
                driver = new RemoteWebDriver(new URL(labdataHub), capability);
                driver.get("https://lambdatest.github.io/sample-todo-app/");
                return driver;
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
            return null;

        } else {
            switch (TestContext.getValues().browserType.toLowerCase()) {
                case "firefox":
                    return new MarionetteDriver().getFirefoxDriver();

                case "chrome":
                    return new MarionetteDriver().getChromeDriver();

                case "ie":
                    //   InternetExplorerDriverManager.getInstance().setup();
                    DesiredCapabilities capabilities = DesiredCapabilities.internetExplorer();
                    capabilities.setCapability(InternetExplorerDriver.INTRODUCE_FLAKINESS_BY_IGNORING_SECURITY_DOMAINS, true);
                    return new InternetExplorerDriver(capabilities);

                default:
                    //ChromeDriverManager.chromedriver().setup();
                    //ChromeDriverManager.chromedriver().properties("--headless");
                    return new ChromeDriver();
            }
        }
    }

}
