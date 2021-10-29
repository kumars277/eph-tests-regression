package com.eph.automation.testing.configuration;


import com.eph.automation.testing.models.TestContext;
import com.google.inject.Provider;
import org.openqa.selenium.Platform;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.ie.InternetExplorerDriver;
import org.openqa.selenium.remote.CapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 * updated by Nishant @20 April 2021
 */

public class WebDriverFactory implements Provider<WebDriver> {


    @Override
    public WebDriver get() {

        //if (TestContext.getValues().gridRun) {
            if (true) {
            try {
                DesiredCapabilities capabilities = DesiredCapabilities.chrome();
                capabilities.setPlatform(Platform.ANY);
                URL hubUrl = new URL("http://10.183.88.203:4444/wd/hub");

                RemoteWebDriver rwd =new RemoteWebDriver(hubUrl, capabilities);
                rwd.get("https://google.com");
                System.out.println(rwd.getTitle());

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        else if (false) {
            WebDriver driver;
            String username = "n.chitre";
            String accesskey = "";

            DesiredCapabilities capability = new DesiredCapabilities();
            capability.setCapability(CapabilityType.BROWSER_NAME, "chrome");
            capability.setCapability(CapabilityType.VERSION, "latest");
            capability.setCapability("platform", "win10");
            capability.setCapability("build", "EPH-QA");
            capability.setCapability("name", "lambada remote browser Test");
            capability.acceptInsecureCerts();
         //   System.setProperty("javax.net.ssl.trustStore","clientTrustStore.key");
        //    System.setProperty("javax.net.ssl.trustStorePassword","changeit");
            String gridURL = "http://" + username + ":" + accesskey + "@hub.lambdatest.com/wd/hub";
            try {
                return new RemoteWebDriver(new URL(gridURL), capability);
            } catch (MalformedURLException e) {
                e.printStackTrace();
                return null;
            }

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
