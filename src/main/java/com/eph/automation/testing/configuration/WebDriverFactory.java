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
import org.openqa.selenium.firefox.FirefoxProfile;
import org.openqa.selenium.ie.InternetExplorerDriver;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import java.io.File;
import java.net.URL;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 */

public class WebDriverFactory implements Provider<WebDriver> {


    @Override
    public WebDriver get() {

        if (TestContext.getValues().gridRun) {

            try {
                DesiredCapabilities capabilities = DesiredCapabilities.chrome();
                capabilities.setPlatform(Platform.ANY);
                URL hubUrl = new URL("http://10.153.95.253:4444/wd/hub");
                return new RemoteWebDriver(hubUrl,capabilities);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        } else {
            switch (TestContext.getValues().browserType.toLowerCase()) {
               /* case "firefox":
                    // set driver properties
                    File pathBinary = new File("C:\\Program Files\\Mozilla Firefox\\firefox.exe");
                    FirefoxBinary firefoxBinary = new FirefoxBinary(pathBinary);
                    FirefoxProfile profile = new FirefoxProfile();
                    profile.setPreference("javascript.enabled", true);
                    profile.setEnableNativeEvents(true);
                    profile.setPreference("webdriver.load.strategy", "fast");


                    //Allow geo location :
                    profile.setPreference("geo.prompt.testing", true);
                    profile.setPreference("geo.prompt.testing.allow", true);
                    return new FirefoxDriver(firefoxBinary,profile);*/
               /* case "ie":
                    InternetExplorerDriverManager.getInstance().setup();
                    DesiredCapabilities capabilities = DesiredCapabilities.internetExplorer();
                    capabilities.setCapability(InternetExplorerDriver.INTRODUCE_FLAKINESS_BY_IGNORING_SECURITY_DOMAINS, true);
                    return new InternetExplorerDriver(capabilities);*/
                case "chrome":
                    ChromeDriverManager.getInstance().setup();
                    return new ChromeDriver();
                default:
                    ChromeDriverManager.getInstance().setup();
                    return new ChromeDriver();
            }
        }

    }
}
