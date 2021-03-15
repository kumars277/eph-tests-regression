package com.eph.automation.testing.configuration;

import com.google.inject.Provider;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import io.github.bonigarcia.wdm.*;
import org.openqa.selenium.firefox.FirefoxBinary;
import org.openqa.selenium.firefox.FirefoxDriver;
//import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.firefox.FirefoxProfile;
import org.openqa.selenium.remote.DesiredCapabilities;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Created by Bistra Drazheva on 03/10/2019
 */
public class MarionetteDriver implements Provider<WebDriver>{

    public WebDriver getChromeDriver(){
        String chromeDriverVersion="latest";

        switch(chromeDriverVersion) //created by Nishant @ 12 June 2020
        {
            case "84":
                String _driver = "\\chromedriver_84.exe";
                String directoryPath = null;
                final String osName = System.getProperty("os.name").toLowerCase();
                if (osName.contains("windows")) {directoryPath = System.getProperty("user.dir");}
                System.setProperty("webdriver.chrome.driver",directoryPath+_driver);
                break;

            case "latest":
                //Added by Nishant @11 Feb 2020, to handle webdrivers dynamically
                DriverManagerType chrome = DriverManagerType.CHROME;
                WebDriverManager.getInstance(chrome).setup();
                break;
        }

        final WebDriver driver = new ChromeDriver();
        driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);
        driver.manage().window().maximize();
        return driver;
    }

    public WebDriver getFirefoxDriver() {
        final FirefoxOptions firefoxOptions = new FirefoxOptions();
        FirefoxProfile profile = new FirefoxProfile();

        profile.setPreference("pdfjs.disabled", true);
        profile.setPreference("security.insecure_password.ui.enabled", false);
        profile.setPreference("security.insecure_field_warning.contextual.enabled", false);

        final FirefoxBinary firefoxBinary = new FirefoxBinary();
        //firefoxBinary.addCommandLineOptions("--headless");
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

            pathToDriver = System.getProperty("user.dir")+"\\geckodriver.exe";

        }
        final File file = new File(pathToDriver);
        System.setProperty("webdriver.gecko.driver", file.getAbsolutePath());
        // This option hides all the annoying Selenium loging. It can be turned off but I've never
        // found anything useful in the extra logging so I suggest leaving it this way.
        System.setProperty(FirefoxDriver.SystemProperty.BROWSER_LOGFILE, "/dev/null");
    }

    @Override
    public WebDriver get() {
        return null;
    }
}


