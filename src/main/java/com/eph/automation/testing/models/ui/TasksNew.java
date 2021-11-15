/**
 * Copyright (C) Estafet Ltd updatec by Nihsant @ 18 May 2020 for latest product finder UI changes
 */
package com.eph.automation.testing.models.ui;
/** Created by GVLAYKOV */
import com.eph.automation.testing.configuration.MarionetteDriver;
import com.eph.automation.testing.configuration.WebDriverFactory;
import com.eph.automation.testing.helper.Log;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.openqa.selenium.*;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Singleton
public class TasksNew {

  public WebDriver driver;
  public long pageLoadTimeout;
  WebDriverWait wait;

  @Inject
  public TasksNew() {
    //  this.driver = new MarionetteDriver().getFirefoxDriver();
    //   this.driver = new MarionetteDriver().getChromeDriver();
    this.driver = new WebDriverFactory().get();
    this.wait = new WebDriverWait(driver, 10);
    this.pageLoadTimeout = 30000;
    loginWithScience();
  }

  public void loginWithScience() {
    String id = "svc-sciproductfinder";
    String pwd = "";
    String url = "https://sit.productfinder.elsevier.net";

    if (!pwd.equalsIgnoreCase(""))
      url = "https://" + id + ":" + pwd + "@" + "productfinder.elsevier.net";
    else {
      openPage(url);

      try {
        sendKeys(
            "NAME",
            ProductFinderConstants.loginByEmail,
            System.getenv("username") + ProductFinderConstants.SCIENCE_ID);
        click("ID", ProductFinderConstants.nextButton);
        Thread.sleep(1000);
        waitUntilPageLoad();
        Log.info("signed in to "+url);
      } catch (Exception e) {
        Log.error(e.getMessage());
      }
    }
  }

  public WebElement findElementByText(final String text) {
    // updated by Nishant @ 19 May 2020
    WebElement element = null;
    try {
      element = driver.findElement(By.xpath("//*[contains(text(), \"" + text + "\")]"));
      ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", element);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return element;
  }

  public void elementScrolltoView(WebElement element) {
    // created by Nishant @ 20 May 2020
    try {
      ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", element);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public String authenticateUri(String uri) {
    String id = "";
    String pwd = "";
    return "https://" + id + ":" + pwd + "@" + uri;
  }

  public void openPage(final String url) {
    driver.get(url);
  }

  public void waitUntilPageLoad() throws InterruptedException { // created by Nishant @ 15 May 2020
    Thread.sleep(3000);
    new WebDriverWait(driver, pageLoadTimeout)
        .until(
            webDriver ->
                ((JavascriptExecutor) webDriver)
                    .executeScript("return document.readyState")
                    .equals("complete"));
  }

  public WebElement findElement(String locatorType, String locatorValue) {
    // created by Nishant @ 18 May 2020
    WebElement element = null;
    switch (locatorType) {
      case "XPATH":
        element = driver.findElement(By.xpath(locatorValue));
        break;
      case "NAME":
        element = driver.findElement(By.name(locatorValue));
        break;
      case "ID":
        element = driver.findElement(By.id(locatorValue));
        break;
      case "TAG":
        element = driver.findElement(By.tagName(locatorValue));
        break;
      case "CSS":
        element = driver.findElement(By.cssSelector(locatorValue));
        break;
    }
    return element;
  }

  public List<WebElement> findmultipleElements(String locatorType, String locatorValue) {
    // updated by Nishant @ 18 may 2020
    List<WebElement> elements = null;
    try {
      switch (locatorType) {
        case "XPATH":
          elements = driver.findElements(By.xpath(locatorValue));
          break;
        case "CLASS":
          elements = driver.findElements(By.className(locatorValue));
          break;
        case "TAG":
          elements = driver.findElements(By.tagName(locatorValue));
          break;
      }
      wait.until(ExpectedConditions.visibilityOfAllElements(elements));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return elements;
  }

  public List<WebElement> findAllLinks() { // created by Nishant @ 30 Apr 2021
    List<WebElement> links = null;
    links = driver.findElements(By.xpath("//a"));
    return links;
  }

  public boolean isObjectpresent(String locatorType, String locatorValue) {
    // created by Nishant @ 18 May 2020
    try {
      WebElement element = findElement(locatorType, locatorValue);
      if (element.isDisplayed() && element.isEnabled()) return true;
      else return false;
    } catch (NoSuchElementException e) {
      return false;
    }
  }

  public void clearText(String locatorType, final String locatorValue) {
    // updated by Nishant @ 18 May 2020
    try {
      WebElement element = findElement(locatorType, locatorValue);
      wait.until(ExpectedConditions.visibilityOf(element)); //  element.click();
      element.clear();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void click(String locatorType, final String locatorValue) {
    // updated by Nishant @ 18 May 2020
    try {
      WebElement element = findElement(locatorType, locatorValue);
      element.isDisplayed();
      elementScrolltoView(element);
      wait.until(ExpectedConditions.elementToBeClickable(element));
      element.click();
      Thread.sleep(3000);
      //waitUntilPageLoad();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void sendKeys(String locatorType, final String locatorValue, String text) {
    // updated by Nishant @ 18 May 2020
    try {
      WebElement element = findElement(locatorType, locatorValue);
      wait.until(ExpectedConditions.visibilityOf(element));
      element.sendKeys(text);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void javaScriptExecutor(WebElement element) {
    ((JavascriptExecutor) driver).executeScript("arguments[0].click();", element);
  }

  public boolean isSelected(String locatorType, String locatorValue) {
    try {
      WebElement element = findElement(locatorType, locatorValue);
      element.isSelected();
      return true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  public boolean verifyElementisDisplayed(String locatorType, String locatorValue)
      throws InterruptedException {
    // updated by Nishant @ 18 May 2020
    try {
      WebElement element = findElement(locatorType, locatorValue);
      wait.until(ExpectedConditions.visibilityOf(element));
      return element.isDisplayed();
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  public boolean verifyElementisClickable(String locatorType, String locatorValue) {
    try {
      WebElement element = findElement(locatorType, locatorValue);
      wait.until(ExpectedConditions.elementToBeClickable(element));
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean verifyElementTextisDisplayed(String text) {
    boolean requiredText = Boolean.parseBoolean(null);
    try {
      requiredText = driver.getPageSource().contains(text);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return requiredText;
  }

  public boolean doesPageContainsText(String text) {
    String bodyText = driver.findElement(By.tagName("body")).getText();
    return bodyText.contains(text);
  }

  public String getTextofElement(String locatorType, String locatorValue) {
    // updated by Nishant @ 18 May 2020
    WebDriverWait wait = new WebDriverWait(driver, 10);
    String getTextVal = null;
    try {
      WebElement element = findElement(locatorType, locatorValue);
      wait.until(ExpectedConditions.visibilityOf(element));
      getTextVal = element.getText();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return getTextVal;
  }

  public String getTextofElement(WebElement element) {
    // created by Nishant @ 18 May 2020
    wait.until(ExpectedConditions.visibilityOf(element));
    return element.getText();
  }

  public void waitTime(int seconds) throws InterruptedException {
    TimeUnit.SECONDS.sleep(seconds);
  }

  public String getCurrentPageUrl() {
    return driver.getCurrentUrl();
  }

  public void acceptAlert() {
    driver.switchTo().alert().accept();
  }

  public void keyboardEvents(String locatorType, String locatorValue, String keyName) {
    try {
      WebElement element = findElement(locatorType, locatorValue);

      switch (keyName) {
        case "RETURN":
          element.sendKeys(Keys.RETURN);
          break;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void closeBrowser() {
    driver.close();
  }
}
