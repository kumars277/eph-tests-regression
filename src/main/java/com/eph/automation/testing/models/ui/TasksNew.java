/**
 * Copyright (C) Estafet Ltd
 */
package com.eph.automation.testing.models.ui;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.configuration.MarionetteDriver;
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

    @Inject
    public TasksNew() {
        // this.driver = new MarionetteDriver().getFirefoxDriver();
        this.driver = new MarionetteDriver().getChromeDriver();
    }

    public WebElement findElementByText(final String text) {
        WebElement element = null;
        try {
            element = driver.findElement(By.xpath("//*[contains(text(), \'" + text + "\')]"));
            ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", element);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return element;
    }

    // Opens a page
    public void openPage(final String url) {
        driver.get(url);
    }

    public void javaScriptExecutor(){
        WebElement element=null;
        //element = driver.findElement(By.xpath(AddJournalConstants.INCLUDE_JOURNAL));
        ((JavascriptExecutor) driver).executeScript("arguments[0].click();",element);
    }
    public boolean isSelected(String locatorType, String locatorValue){
        WebElement element = null;
        try {
            switch (locatorType) {
                case "XPATH":
                    element = driver.findElement(By.xpath(locatorValue));
                    element.isSelected();
                    break;
            }
            return true;
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public boolean verifyElementisDisplayed(String locatorType, String locatorValue) throws InterruptedException {
        WebElement element = null;
        WebDriverWait wait = new WebDriverWait(driver,10);
        try{
            switch (locatorType) {
                case "XPATH":
                    element =  driver.findElement(By.xpath(locatorValue));
                    wait.until(ExpectedConditions.visibilityOf(element));
                    break;
                case "NAME":
                    element = driver.findElement(By.name(locatorValue));
                    wait.until(ExpectedConditions.visibilityOf(element));
                    break;
                case "ID":
                    element = driver.findElement(By.id(locatorValue));
                    wait.until(ExpectedConditions.visibilityOf(element));
                    break;
                case "TAG":
                    element = driver.findElement(By.tagName(locatorValue));
                    wait.until(ExpectedConditions.visibilityOf(element));
                    break;
            }
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return element.isDisplayed();
    }
public boolean verifyElementisClickable(String locatorType, String locatorValue){
        WebElement element = null;
        WebDriverWait wait = new WebDriverWait(driver,10);
        try{
        switch (locatorType) {
        case "XPATH":
        element = driver.findElement(By.xpath(locatorValue));
        wait.until(ExpectedConditions.elementToBeClickable(element));
        break;
        }
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
        }

public boolean verifyElementTextisDisplayed(String text){
        boolean requiredText = Boolean.parseBoolean(null);
        try{
        requiredText = driver.getPageSource().contains(text);

        }catch (Exception e){
        e.printStackTrace();
        }
        return requiredText;
        }

public boolean doesPageContainsText(String text){
        String bodyText = driver.findElement(By.tagName("body")).getText();
        return bodyText.contains(text);
        }

public String getTextofElement(String locatorType, String locatorValue){
        WebDriverWait wait = new WebDriverWait(driver,10);
        WebElement element = null;
        String getTextVal = null;
        try{
        switch (locatorType){
        case "XPATH":
        element = driver.findElement(By.xpath(locatorValue));
        wait.until(ExpectedConditions.visibilityOf(element));
        getTextVal = element.getText();
        break;
        }
        return getTextVal;

        }catch (Exception e){
        e.printStackTrace();
        }
        return  getTextVal;
        }


public List<WebElement> multipleElements(String locatorType, String locatorValue){
        List<WebElement> elements = null;
        WebDriverWait wait = new WebDriverWait(driver,10);
        try{
        switch (locatorType){
        case "XPATH":
        elements = driver.findElements(By.xpath(locatorValue));
        wait.until(ExpectedConditions.visibilityOfAllElements(elements));
        break;
        case "CLASS":
        elements = driver.findElements(By.className(locatorValue));
        wait.until(ExpectedConditions.visibilityOfAllElements(elements));
        break;
        case "TAG":
        elements = driver.findElements(By.tagName(locatorValue));
        wait.until(ExpectedConditions.visibilityOfAllElements(elements));
        break;
        }
        }catch (Exception e){
        e.printStackTrace();
        }
        return elements;
        }

public void waitTime(int seconds) throws InterruptedException {
        TimeUnit.SECONDS.sleep(seconds);
        }

// Returns the current page URL
public String getCurrentPage() {
        return driver.getCurrentUrl();
        }

// Closes the WebDriver instance
public void closeBrowser() {
        driver.close();
        }

public WebElement clearText(String locatorType, final String locatorValue){
        WebDriverWait wait = new WebDriverWait(driver,10);
        WebElement element = null;
        try{
        switch (locatorType){
        case "XPATH":
        element = driver.findElement(By.xpath(locatorValue));
        wait.until(ExpectedConditions.visibilityOf(element));
        element.clear();
        break;
        case "ID":
        element = driver.findElement(By.id(locatorValue));
        wait.until(ExpectedConditions.visibilityOf(element));
        element.clear();
        break;
        }
        }catch (Exception e){
        e.printStackTrace();
        }
        return element;
        }

public void acceptAlert(){
        driver.switchTo().alert().accept();
        }

public WebElement click(String locatorType, final String locatorValue ){
        WebElement element = null;
        try{
        WebDriverWait wait = new WebDriverWait(driver,10);
        switch (locatorType) {
        case "ID":
        element =  driver.findElement(By.id(locatorValue));
        wait.until(ExpectedConditions.elementToBeClickable(element));
        element.click();
        break;
        case "NAME":
        element = driver.findElement(By.name(locatorValue));
        wait.until(ExpectedConditions.elementToBeClickable(element));
        element.click();
        break;
        case "CSS":
        element = driver.findElement(By.cssSelector(locatorValue));
        wait.until(ExpectedConditions.elementToBeClickable(element));
        element.click();
        break;
        case "XPATH":
        element = driver.findElement(By.xpath(locatorValue));
        wait.until(ExpectedConditions.elementToBeClickable(element));
        element.click();
        break;
        case "TAG":
        element = driver.findElement(By.tagName(locatorValue));
        wait.until(ExpectedConditions.elementToBeClickable(element));
        element.click();
        break;
        }
        }catch (Exception e){
        e.printStackTrace();
        }
        return element;
        }

public WebElement sendKeys(String locatorType, final String locatorValue, String text ){
        WebElement element = null;
        try{
        WebDriverWait wait = new WebDriverWait(driver,10);
        switch (locatorType) {
        case "ID":
        element =  driver.findElement(By.id(locatorValue));
        wait.until(ExpectedConditions.visibilityOf(element));
        element.sendKeys(text);
        break;
        case "NAME":
        element = driver.findElement(By.name(locatorValue));
        wait.until(ExpectedConditions.visibilityOf(element));
        element.sendKeys(text);
        break;
        case "CSS":
        element = driver.findElement(By.cssSelector(locatorValue));
        wait.until(ExpectedConditions.visibilityOf(element));
        element.sendKeys(text);
        break;
        case "XPATH":
        element = driver.findElement(By.xpath(locatorValue));
        wait.until(ExpectedConditions.visibilityOf(element));
        element.sendKeys(text);
        break;
        case "TAG":
        element = driver.findElement(By.tagName(locatorValue));
        wait.until(ExpectedConditions.visibilityOf(element));
        element.sendKeys(text);
        break;
        }
        }catch (Exception e){
        e.printStackTrace();
        }
        return element;

        }
public void keyboardEvents(WebElement element, String keyName){
        try{
        switch (keyName){
        case "RETURN":
        element.sendKeys(Keys.RETURN);
        break;
        }
        }catch (Exception e){
        e.printStackTrace();
        }

        }



        }


