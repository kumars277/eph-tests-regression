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
import org.openqa.selenium.chrome.ChromeOptions;
import java.net.MalformedURLException;
import java.net.URL;
import com.eph.automation.testing.configuration.SecretsManagerHandler;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 * updated by Nishant @20 April 2021
 * updated by Nishant @12 April 2022 for jenkin job implementation EPHD-4161
 */

public class WebDriverFactory implements Provider<WebDriver> {

    @Override
    public WebDriver get() {
        WebDriver driver = null;
        //if (TestContext.getValues().gridRun) {
        if (false) {driver = gridRun();}
        else if (true) {driver =lambdaTestRun();}
        else {driver = selectBrowser();}
        return driver;
    }

    public WebDriver gridRun()
    {
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

    public WebDriver lambdaTestRun()
    {
        WebDriver driver;
//        String username = "t.kruck";
//        String accesskey = "";

        DesiredCapabilities capability = new DesiredCapabilities();
        capability.setCapability(CapabilityType.BROWSER_NAME, "chrome");
        capability.setCapability(CapabilityType.VERSION, "latest");
        capability.setCapability("platform", "win10");
        capability.setCapability("build", "EPH-QA");
        capability.setCapability("name", "lambada remote browser Test");
        capability.setCapability("tunnel", true);
        capability.acceptInsecureCerts();
        //   System.setProperty("javax.net.ssl.trustStore","clientTrustStore.key");
        //    System.setProperty("javax.net.ssl.trustStorePassword","changeit");

        String Lambda_URL= null;
        Lambda_URL= SecretsManagerHandler.getDBConnection("Lambda");
        System.out.println(Lambda_URL);
        String gridURL = Lambda_URL;

//        String gridURL = "http://" + username + ":" + accesskey + "@hub.lambdatest.com/wd/hub";
        try {
            return new RemoteWebDriver(new URL(gridURL), capability);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        }

    }

    public WebDriver selectBrowser()
    {
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

            case "lambda":
                WebDriver driver;
                String username = "t.kruck";
                String accesskey = "pLTBfxDVkx8LtiC1QLfk0d5kuctvfbVSeobc1RJwIZAlizf3BS";
                String gridURL = "@hub.lambdatest.com/wd/hub";
                boolean status = false;
                DesiredCapabilities capability = new DesiredCapabilities();
                capability.setCapability("browserName", "chrome");
                capability.setCapability("version", "95.0");
                capability.setCapability("platform", "win10"); // If this cap isn't specified, it will just get any available one.
                capability.setCapability("build", "ProductFinderApp");
                capability.setCapability("name", "ProductFinderAppTest");
                capability.setCapability("tunnel", true);
                try {
                 return new RemoteWebDriver(new URL("https://" + username + ":" + accesskey + gridURL), capability);
                } catch (MalformedURLException e) {
                    System.out.println("Invalid grid URL");
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }

            default:
                //ChromeDriverManager.chromedriver().setup();
                //ChromeDriverManager.chromedriver().properties("--headless");
                return new ChromeDriver();
        }
    }
}
