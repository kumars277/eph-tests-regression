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
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.pagefactory.ByChained;

import java.util.List;

@Singleton
public class Tasks {

    public WebDriver driver;

    @Inject
   /* public Tasks() {
        this.driver = new MarionetteDriver().getFirefoxDriver();

    }*/

    // Opens a page
    public void openPage(final String url) {
        driver.get(url);
    }

    // This is a thing of beauty- my patent:
    // this method finds an element by a CSS selector, waits up to webDriverWaitInSeconds for it to be clickable
    // uses JS to scroll to it (to avoid those pesky bottom of the page elements) and handles exceptions.
    public WebElement findElement(final String identifier) {
        WebElement element = driver.findElement(By.cssSelector(identifier));
        
        ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", element);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return element;
    }

    public WebElement findElementById(final String identifier) {
        WebElement element = driver.findElement(By.id(identifier));
        
        ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", element);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return element;
    }

    public WebElement findElementByXpath(final String identifier) {
        WebElement element = driver.findElement(By.xpath(identifier));
        
        ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", element);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return element;
    }

    public WebElement findChainedElementByClassnameAndText(final String parentIdentifier, final String childIdentifier){
        WebElement element = driver.findElement(new ByChained(By.className(parentIdentifier), By.className(childIdentifier)));

        ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", element);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return element;
    }

    public WebElement findElementByText(final String text){
        WebElement element = driver.findElement(By.xpath("//*[contains(text(), \'"+text+"\')]"));

        ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", element);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return element;
    }

    public WebElement findElementByClassName(final String classname){
        WebElement element = driver.findElement(By.className(classname));

        ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", element);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return element;
    }

    public void verifyTextDisplayed(String text){
        String bodyText = driver.findElement(By.tagName("body")).getText();
        Assert.assertTrue("Not Found:" + text, bodyText.contains(text));
    }

    public void verifyElementContainsText(String element, String text){
        String bodyText = driver.findElement(By.className(element)).getText();
        Assert.assertTrue("Not Found:" + text, bodyText.contains(text));
    }

    public boolean doesPageContainsText(String text){
        String bodyText = driver.findElement(By.tagName("body")).getText();
        return bodyText.contains(text);
    }

    // Finds multiple elements.
    public List<WebElement> findElementsByClass(final String classname) {
        List<WebElement> elements = driver.findElements(By.className(classname));
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return elements;
    }

    public List<WebElement> findElementsByTagName(final String tagName) {
        List<WebElement> elements = driver.findElements(By.tagName(tagName));
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return elements;
    }

    // Explicit wait. Use this method if you are too lazy for handling ElementNotFound exception
    public void explicitWait(final int secondsToWait) {
        try {
            Thread.sleep(secondsToWait * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Returns the current page URL
    public String getCurrentPage() {
        return driver.getCurrentUrl();
    }

    // Closes the WebDriver instance
    public void closeBrowser() {
        driver.close();
    }


}
