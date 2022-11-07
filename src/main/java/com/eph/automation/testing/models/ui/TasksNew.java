/**
 * Copyright (C) Estafet Ltd updatec by Nihsant @ 18 May 2020 for latest product finder UI changes
 */
package com.eph.automation.testing.models.ui;
/** Created by GVLAYKOV */
import com.eph.automation.testing.configuration.MarionetteDriver;
import com.eph.automation.testing.configuration.SecretsManagerHandler;
import com.eph.automation.testing.configuration.WebDriverFactory;
import com.eph.automation.testing.helper.Log;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import net.minidev.json.JSONObject;
import org.openqa.selenium.*;
import org.openqa.selenium.interactions.Action;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.nio.file.WatchEvent;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Singleton
public class TasksNew {

  public WebDriver driver;
  public long pageLoadTimeout;
  WebDriverWait wait;

  @Inject
  public TasksNew() {
    this.driver = new WebDriverFactory().get();
    this.wait = new WebDriverWait(driver, 10);
    this.pageLoadTimeout = 30000;

   // driver.get("https://productfinder.elsevier.net");
    //loginWithCredential();
    //driver.get("https://uat.productfinder.elsevier.net");
    driver.get("https://productfinder.elsevier.net");
    loginWithCredential();
    //loginWithScience(); //local execution

   }

  public void loginWithCredential() {
    //created by Nishant in Apr 2022
    JSONObject svc = SecretsManagerHandler.getSMKeys("eph_svcUsers");
    String loginId = svc.getAsString("svc4");
    String pwd =svc.getAsString("svc4pwd");

    try {
      sendKeys(
              "NAME",
              ProductFinderConstants.loginByEmail,
              loginId + ProductFinderConstants.SCIENCE_ID);
      click("ID", ProductFinderConstants.nextButton);
      Thread.sleep(5000);

    //  driver.get("https://"+loginId+":"+pwd+"@"+driver.getCurrentUrl().split("//")[1]);
     // driver.get("https://"+loginId+":"+pwd+"@"+"<Url>");
      waitUntilPageLoad();
    //  Thread.sleep(5000);
      Log.info("url after science id url");
      Log.info(driver.getCurrentUrl());
      sendCredential(loginId,pwd);

      if(!driver.getCurrentUrl().contains("productfinder.elsevier.net/"))
      {
        signIntoYourOrganisation(loginId,pwd);
      }

      Thread.sleep(3000);

      if(driver.getCurrentUrl().contains("productfinder.elsevier.net/"))
      {
        Log.info("signed in successful ");
      }
      else {
        Log.info("sign in issue for below link");
        Log.info(driver.getCurrentUrl());
      }
    } catch (Exception e) {
      Log.error(e.getMessage());
    }

  }

  public void loginWithScience() {
      try {
        sendKeys(
            "NAME",
            ProductFinderConstants.loginByEmail,
            System.getenv("username") + ProductFinderConstants.SCIENCE_ID);
        click("ID", ProductFinderConstants.nextButton);
        Thread.sleep(1000);
        waitUntilPageLoad();

      } catch (Exception e) {
        Log.error(e.getMessage());
      }
  }

  public void sendCredential(String user, String pwd)
  {
//created by Nishant @ 19 Apr 2022 for PF jenkins login

    //  String text = tasks.driver.findElement(By.className("product-header")).getText();
    //  Assert.assertTrue("Basic Authentication failed",text.contains("Product Finder"));

    //JavascriptExecutor jse = (JavascriptExecutor)tasks.driver;
    //jse.executeScript("browserstack_executor: {\"action\": \"dismissBasicAuth\",\"arguments\": {\"timeout\": \"5000\"}}");
    //jse.executeScript("browserstack_executor: {\"action\": \"sendBasicAuth\", \"arguments\": {\"username\":\""+loginId+"\", \"password\": \""+pwd+"\", \"timeout\": \"5000\"}}");

    //parent window handle id
    String window_before = driver.getWindowHandle();
    Log.info(window_before);
    Set<String> w = driver.getWindowHandles();

// iterate handles
    Iterator<String> i = w.iterator();
    //child window handle id
    String child = i.next();
    Log.info(child);

    // child window switch
    driver.switchTo().window(child);

    Actions actionProvider  = new Actions(driver);
    actionProvider .sendKeys(user).build().perform();
    actionProvider.sendKeys(Keys.TAB).build().perform();
    actionProvider .sendKeys(pwd).perform();
    actionProvider .sendKeys(Keys.ENTER).perform();
   // builder.keyDown(Keys.TAB).perform();

  }

  public void signIntoYourOrganisation(String id, String pwd) throws InterruptedException {
    //created by Nishant @ 19 Apr 2022
    WebElement userAccount = driver.findElement(By.xpath("//input[@id='userNameInput']"));
    WebElement password = driver.findElement(By.xpath("//input[@id = 'passwordInput']"));
    WebElement btn_signIn = driver.findElement(By.xpath("//*[@id = 'submitButton']"));
    password.sendKeys(pwd);
    btn_signIn.click();
    waitUntilPageLoad();
    Log.info("sign in to your organisation funtion called");
    Thread.sleep(3000);
    Log.info(driver.getCurrentUrl());
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

  public void mouseHoverClick(WebElement element){
    try{
      Actions actions = new Actions(driver);
      actions.moveToElement(element).click();
    }catch (Exception e){
      e.printStackTrace();
    }
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
     // wait.until(ExpectedConditions.visibilityOfAllElements(elements));
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
//      Thread.sleep(3000);
      waitUntilPageLoad();

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

  public List<String> getTextofAllElements(String locatorType, String locatorValue){
    WebDriverWait wait = new WebDriverWait(driver, 10);
    List<String> getTextVal = new ArrayList<>();
    try{
      List<WebElement> elements = findmultipleElements(locatorType, locatorValue);
      wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.xpath(locatorValue)));
      for(int i=0;i<elements.size();i++){
        getTextVal.add(elements.get(i).getText());
      }
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

  public int getSize(String locatorType, String locatorValue) {
    int size = 0;
    try {
      switch (locatorType) {
        case "XPATH":
          size = driver.findElements(By.xpath(locatorValue)).size();
          break;
        case "CLASS":
          size = driver.findElements(By.className(locatorValue)).size();
          break;
        case "TAG":
          size = driver.findElements(By.tagName(locatorValue)).size();
          break;
      }
      // wait.until(ExpectedConditions.visibilityOfAllElements(elements));
    } catch (Exception e) {
      e.printStackTrace();
    }
      return size;
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
