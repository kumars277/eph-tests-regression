package com.eph.automation.testing.configuration;

import com.google.inject.Provider;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxBinary;
import org.openqa.selenium.firefox.FirefoxDriver;
//import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.firefox.FirefoxProfile;
import org.openqa.selenium.remote.DesiredCapabilities;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Created by Bistra Drazheva on 03/10/2019
 */
public class MarionetteDriver implements Provider<WebDriver>{

    public WebDriver getChromeDriver(){
        String pathToDriver = null;
        String directoryPath = null;
        final String osName = System.getProperty("os.name").toLowerCase();
        if (osName.toLowerCase().contains("windows")) {
            directoryPath = System.getProperty("user.dir");
            pathToDriver = "C://Users//sureshkumard//Downloads//Selenium//chromedriver.exe";
        }
        final File file = new File(pathToDriver);
        System.setProperty("webdriver.chrome.driver",file.getAbsolutePath());
        //System.setProperty(ChromeDriver.SystemProperty.BROWSER_LOGFILE, "/dev/null");
        final WebDriver driver = new ChromeDriver();
        driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
        driver.manage().window().maximize();
        return driver;
    }

       /* public WebDriver getFirefoxDriver() {
            final FirefoxOptions firefoxOptions = new FirefoxOptions();
            FirefoxProfile profile = new FirefoxProfile();

            profile.setPreference("pdfjs.disabled", true);
            profile.setPreference("security.insecure_password.ui.enabled", false);
            profile.setPreference("security.insecure_field_warning.contextual.enabled", false);

            final FirefoxBinary firefoxBinary = new FirefoxBinary();
//        firefoxBinary.addCommandLineOptions("--headless");
            firefoxOptions.setBinary(firefoxBinary);

            final DesiredCapabilities capabilities = DesiredCapabilities.firefox();
            // This tells the driver is will use marionette (geckodriver)
            capabilities.setCapability("marionette", true);
            setGeckoDriver();
            final WebDriver driver = new FirefoxDriver(firefoxOptions);
            driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
            driver.manage().window().maximize();
            return driver;
        }

        private void setGeckoDriver() {
            String pathToDriver = null;

            final String osName = System.getProperty("os.name").toLowerCase();

            if (osName.toLowerCase().contains("windows")) {
                pathToDriver = "D://geckodriver.exe";
            }
            final File file = new File(pathToDriver);
            System.setProperty("webdriver.gecko.driver", file.getAbsolutePath());
            // This option hides all the annoying Selenium loging. It can be turned off but I've never
            // found anything useful in the extra logging so I suggest leaving it this way.
            System.setProperty(FirefoxDriver.SystemProperty.BROWSER_LOGFILE, "/dev/null");
        }*/

    @Override
    public WebDriver get() {
        return null;
    }
}


