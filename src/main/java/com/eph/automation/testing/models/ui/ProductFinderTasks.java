package com.eph.automation.testing.models.ui;
//updated by Nishant @ 03032021 for UAT2 environment
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.SecretsManagerHandler;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.Product;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.services.api.AuthorizationService;
import com.eph.automation.testing.services.api.AzureOauthTokenFetchingException;
import com.google.inject.Inject;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import cucumber.api.java.en.When;
import gherkin.lexer.Da;
import net.minidev.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.html5.LocalStorage;
import org.openqa.selenium.html5.WebStorage;
import org.openqa.selenium.interactions.Actions;

import javax.net.ssl.SSLHandshakeException;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

public class ProductFinderTasks {

    private TasksNew tasks;
    private SpecificTasks specificTasks;



    @Inject
    public ProductFinderTasks(final TasksNew tasks, SpecificTasks specificTasks) {
        this.tasks = tasks;
        this.specificTasks = new SpecificTasks();
    }

    public static String searchResultId;
    public Properties prop_info = new Properties();
    public Properties prop_subArea = new Properties();
    public Properties prop_identifier = new Properties();
    public Properties prop_specialties = new Properties();

    public Properties prop_comCode = new Properties();
    public Properties prop_AccProducts = new Properties();
    public Properties prop_editorial1 = new Properties();
    public Properties prop_editorial2 = new Properties();
    public List<Properties> list_people = new ArrayList<>();
    public Properties prop_links = new Properties();

    public void authentication_browser() throws AzureOauthTokenFetchingException, ExecutionException, InterruptedException {
        LocalStorage local = ((WebStorage) tasks.driver).getLocalStorage();
        String accessToken = AuthorizationService.getCookies();
        local.setItem("adal.idtoken", accessToken);
        local.clear();
    }

    public void openHomePage() throws InterruptedException {//updated by Nishant @ 18 May 2020
        String HomePageAddress = "";
        switch (TestContext.getValues().environment) {
            case "SIT":
                HomePageAddress = Constants.PRODUCT_FINDER_EPH_SIT_UI;
                break;
            case "UAT":
                HomePageAddress = Constants.PRODUCT_FINDER_EPH_UAT_UI;
                break;
            case "UAT2":
                HomePageAddress = Constants.PRODUCT_FINDER_EPH_UAT2_UI;
                break;
            case "PROD":
            default:
                HomePageAddress = Constants.PRODUCT_FINDER_EPH_PROD_UI;
                break;
        }

        if (DataQualityContext.uiUnderTest.equalsIgnoreCase("JF")) HomePageAddress += "journals";
        tasks.openPage(HomePageAddress);
        Thread.sleep(1000);
        Log.info("\nhome page accessed...");
        Log.info(HomePageAddress);
    }


    public void loginWithCredential() {
       JSONObject svc = SecretsManagerHandler.getSMKeys("eph_svcUsers");
        String loginId = svc.getAsString("svc4");
        String pwd = svc.getAsString("svc4pwd");

            try {
               tasks.sendKeys(
                        "NAME",
                        ProductFinderConstants.loginByEmail,
                        loginId + ProductFinderConstants.SCIENCE_ID);
                tasks.click("ID", ProductFinderConstants.nextButton);
                Thread.sleep(3000);

              tasks.driver.get("https://"+loginId+":"+pwd+"@"+tasks.driver.getCurrentUrl().split("//")[1]);
                tasks.waitUntilPageLoad();
                Thread.sleep(3000);
                if(!tasks.driver.getCurrentUrl().contains("productfinder.elsevier.net/"))
                {
                    tasks.signIntoYourOrganisation(loginId,pwd);
                }

                Thread.sleep(3000);

                if(tasks.driver.getCurrentUrl().contains("productfinder.elsevier.net/"))
                {
                Log.info("signed in successful ");
                }
                else {
                    Log.info("sign in issue for below link");
                    tasks.driver.getCurrentUrl();
                }
            } catch (Exception e) {
                Log.error(e.getMessage());
            }

    }

    public void loginByScienceAccount(String scienceEmailId) throws InterruptedException {
        //updated by Nishant @ 13 Feb 2020
        try {
            tasks.sendKeys("NAME", ProductFinderConstants.loginByEmail, System.getenv("username") + scienceEmailId);
            tasks.click("ID", ProductFinderConstants.nextButton);
            Log.info("signed in with science id...");
        } catch (Exception e) {
            Log.info("User already signed in...");
            Log.info(e.getMessage());
        }

    }

    public void selectSearchType(String searchType) { //created by Nishant @ 10 Jul 2020
        switch (searchType) {
            case "All":
                break;
            case "PMG":
                tasks.click("XPATH", ProductFinderConstants.searchDropdownPmg);
                break;
            case "PMC":
                tasks.click("XPATH", ProductFinderConstants.searchDropdownPmc);
                break;
            case "Person":
                tasks.click("XPATH", ProductFinderConstants.searchDropdownPerson);
                break;
        }
    }

    public void searchFor(final String searchID) throws InterruptedException {
        //updated by Nishant @ 14 May 2020
        //updated by Nishant @ 03 Jul 2020 for Journal Finder

        while (!tasks.isObjectpresent("XPATH", ProductFinderConstants.searchBar)) {
            tasks.driver.navigate().refresh();
            Thread.sleep(30000);
            Log.info("page refreshed as search bar not available...");
            Log.info("after 30 sec from page refreshed");
            Log.info("\n");
          //  Log.info(tasks.driver.getPageSource());
            Log.info("\n");
        }

        Log.info("Searching " + searchID + " on " + DataQualityContext.uiUnderTest);
        tasks.clearText("XPATH", ProductFinderConstants.searchBar);
        tasks.sendKeys("XPATH", ProductFinderConstants.searchBar, searchID);
        tasks.click("XPATH", ProductFinderConstants.searchButton);
        Thread.sleep(3000);
        tasks.driver.findElement(By.xpath("(//div[@class='ng-star-inserted'])[3]")).click();
        Thread.sleep(3000);
    }

    public void searchForManifestation(final String searchID) throws InterruptedException {

        while (!tasks.isObjectpresent("XPATH", ProductFinderConstants.searchBar)) {
            tasks.driver.navigate().refresh();
            Thread.sleep(30000);
            Log.info("page refreshed as search bar not available...");
            Log.info("after 30 sec from page refreshed");
            Log.info("\n");
            //  Log.info(tasks.driver.getPageSource());
            Log.info("\n");
        }

        Log.info("Searching " + searchID + " on " + DataQualityContext.uiUnderTest);
        tasks.clearText("XPATH", ProductFinderConstants.searchBar);
        tasks.sendKeys("XPATH", ProductFinderConstants.searchBar, searchID);
        tasks.click("XPATH", ProductFinderConstants.searchButton);
        Thread.sleep(3000);
        tasks.driver.findElement(By.xpath(ProductFinderConstants.manifestation_page_click)).click();
        Thread.sleep(3000);


    }



        public void searchForJournal(final String searchID) throws InterruptedException {
         //updated by Sarath @ 12 Oct 2022 for Journal Finder

        while (!tasks.isObjectpresent("XPATH", ProductFinderConstants.journalSearchbar)) {
            tasks.driver.navigate().refresh();
            Thread.sleep(30000);
            Log.info("page refreshed as search bar not available...");
            Log.info("after 30 sec from page refreshed");
            Log.info("\n");
            //  Log.info(tasks.driver.getPageSource());
            Log.info("\n");
        }

        Log.info("Searching " + searchID + " on " + DataQualityContext.uiUnderTest);
        tasks.clearText("XPATH", ProductFinderConstants.journalSearchbar);
        tasks.sendKeys("XPATH", ProductFinderConstants.journalSearchbar, searchID);
        tasks.click("XPATH", ProductFinderConstants.journalsearchButton);
        Thread.sleep(3000);
    }

    public boolean isPageContainingString(String text) {
        return tasks.doesPageContainsText(text);
    }


    public void clickProductAndPackages_tab() throws InterruptedException {
        //created by Nishant @ 19 May 2020
        tasks.verifyElementisClickable("XPATH", ProductFinderConstants.tab_product_andPackages);
        tasks.click("XPATH", ProductFinderConstants.tab_product_andPackages);
        Thread.sleep(3000);
    }



    public void clickWork(String workId) throws InterruptedException {
        //created by Nishant @ 22 May 2020
        String IdLocator = String.format(ProductFinderConstants.buildIdLocator, workId);
        tasks.click("XPATH", IdLocator);
        tasks.waitUntilPageLoad();
        Thread.sleep(2000);
    }

    public void clickLogo() throws InterruptedException {
        //created by Nishant @ 23 Oct 2020
        tasks.click("XPATH", ProductFinderConstants.elsevierLogo);
        Thread.sleep(3000);
    }

    public void clickManifestation_tab() throws InterruptedException {
        //created by Nishant @ 19 May 2020
        tasks.verifyElementisClickable("XPATH", ProductFinderConstants.tab_manifestation);
        tasks.click("XPATH", ProductFinderConstants.tab_manifestation);
        Thread.sleep(3000);
    }

    public void clickMaximumResultPerPage() throws InterruptedException {
        //created by Nishant @ 18 May 2020
        List<WebElement> elements = null;
        if (tasks.isObjectpresent("XPATH", ProductFinderConstants.resultPerPage_100))
            elements = tasks.findmultipleElements("XPATH", ProductFinderConstants.resultPerPage_100);
        else elements = tasks.findmultipleElements("XPATH", ProductFinderConstants.resultPerPage_50);

        elements.get(0).click();
        Thread.sleep(3000);
    }

    public String[] getPageNofMCount() {
        List<WebElement> elements = tasks.findmultipleElements("XPATH", ProductFinderConstants.SearchResultPageCount);
        return elements.get(0).getText().split(" ");
    }

    public void goToNextPage() {
        tasks.click("XPATH", ProductFinderConstants.nextPageButton);
    }

    public boolean searchOnResultPages(String SearchingTerm) throws InterruptedException {
        //created by Nishant @ 21 May 2020
        boolean searchFound = false;
        clickMaximumResultPerPage();
        tasks.waitUntilPageLoad();
        String[] pageNofM = getPageNofMCount();
        int looper = Integer.valueOf(pageNofM[pageNofM.length - 1]);
        for (int i = 0; i < looper; i++) {
            Log.info("Finding workid on search result page ..." + (i + 1) + " - " + SearchingTerm);
            if (!isPageContainingString(SearchingTerm)) {
                goToNextPage();
                Thread.sleep(5000);
            } else {
                searchFound = true;
                break;
            }
        }
        if (!searchFound) Log.info("search complete, could not find " + SearchingTerm + " on all pages");
        return searchFound;
    }

    public WebElement getElementByTitle(String title) {
        return tasks.findElementByText(title);
    }

    public boolean isUserOnWorkPage(String workID) {
        //updated by Nishant @ 18 May 2020
        Log.info("verifying if user is on the work page overview...");
        String targetURL = "";
        String referenceUrl = "work/" + workID + "/overview";

        if (DataQualityContext.uiUnderTest.equalsIgnoreCase("JF")) referenceUrl = "journals/" + referenceUrl;
        JSONObject svc = SecretsManagerHandler.getSMKeys("eph_svcUsers");
        String loginId = svc.getAsString("svc4");
        String pwd = svc.getAsString("svc4pwd");
        switch (TestContext.getValues().environment) {
            case "SIT":targetURL = Constants.PRODUCT_FINDER_EPH_SIT_UI + referenceUrl;break;
//            targetURL = "https://"+ loginId + ":" + pwd + "@sit.productfinder.elsevier.net/" + referenceUrl;
            case "UAT":targetURL = Constants.PRODUCT_FINDER_EPH_UAT_UI + referenceUrl;break;
            case "UAT2":targetURL = Constants.PRODUCT_FINDER_EPH_UAT2_UI + referenceUrl;break;
            case "PROD":
            case "PRODUCTION":targetURL = Constants.PRODUCT_FINDER_EPH_PROD_UI + referenceUrl;break;
        }

        Log.info("Expected Target URL " + targetURL);
        if (targetURL.equalsIgnoreCase(tasks.getCurrentPageUrl())) return true;
        else {
            Log.info("Actual URL = " + tasks.getCurrentPageUrl());
            return false;
        }
    }
    public boolean isUserOnProductPage(String productId) {
        //created by Nishant @ 23 Apr 2020
        Log.info("verifying if user is on the product page overview...");
        String targetURL = "";
        String referenceUrl = "product/" + productId + "/overview";

        if (DataQualityContext.uiUnderTest.equalsIgnoreCase("JF")) referenceUrl = "journals/" + referenceUrl;

        switch (TestContext.getValues().environment) {
            case "SIT":
                targetURL = Constants.PRODUCT_FINDER_EPH_SIT_UI + referenceUrl;
                break;
            case "UAT":
                targetURL = Constants.PRODUCT_FINDER_EPH_UAT_UI + referenceUrl;
                break;
            case "UAT2":
                targetURL = Constants.PRODUCT_FINDER_EPH_UAT2_UI + referenceUrl;
                break;
            case "PROD":
            case "PRODUCTION":
                targetURL = Constants.PRODUCT_FINDER_EPH_PROD_UI + referenceUrl;
                break;
        }

        Log.info("Target URL " + targetURL);
        Log.info("currentPageUrl "+tasks.getCurrentPageUrl());
        if (targetURL.equalsIgnoreCase(tasks.getCurrentPageUrl())) return true;
        else return false;
    }

    public boolean isUserOnManifestationPage(String manifestationId) {
        //created by Nishant @ 19 May 2020
        String targetURL = "";
        String mani_work = DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getF_WWORK();
        if (TestContext.getValues().environment.equalsIgnoreCase("SIT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_SIT_UI + "manifestation/" + mani_work + "/" + manifestationId + "/overview";
        else if (TestContext.getValues().environment.equalsIgnoreCase("UAT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_UAT_UI + "manifestation/" + mani_work + "/" + manifestationId + "/overview";

        Log.info("Target URL " + targetURL);
        if (targetURL.equalsIgnoreCase(tasks.getCurrentPageUrl())) return true;
        else return false;
    }

    public void getUI_WorkOverview_Information() {
        //created by Nishant @5 Jun 2020
        //updated UI changes and locator by Nishant @ 15 Oct 2020 for EPHD-2241
        //updated UI changes and locator by Nishant @ 12 Oct 2021 for regression testing
        Log.info("getUI_WorkOverview_Information - Start");
        if (DataQualityContext.uiUnderTest.equalsIgnoreCase("PF"))  //for Product Finder UI
        {
            //capture Information values
            List<WebElement> rows_info = tasks.findmultipleElements("XPATH", ProductFinderConstants.sectionDetail + "/table/tbody/tr");
            for (int i = 0; i < rows_info.size(); i++) {
                String key = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
                String value = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
                prop_info.setProperty(key, value);
            }

            //capture identifiers values
            List<WebElement> rows_identifiers = tasks.findmultipleElements("XPATH", ProductFinderConstants.identifierRow + "/table/tbody/tr");
            for (int i = 1; i <= rows_identifiers.size(); i++) {
                String key = tasks.findElement("XPATH", ProductFinderConstants.identifierRow + "/table/tbody/tr[" + (i) + "]/td[1]").getText();
                String value = tasks.findElement("XPATH", ProductFinderConstants.identifierRow + "/table/tbody/tr[" + (i) + "]/td[2]").getText();
                prop_identifier.setProperty(key, value);
            }

            //capture subject area values
            List<WebElement> rows_subArea = tasks.findmultipleElements("XPATH", ProductFinderConstants.subAreaRow + "/table/tbody/tr");
            for (int i = 0; i < rows_subArea.size(); i++) {
                String key = tasks.findElement("XPATH", ProductFinderConstants.subAreaRow + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
                String value = tasks.findElement("XPATH", ProductFinderConstants.subAreaRow + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
                prop_subArea.setProperty(key, value);
            }

            //capture specialties
            List<WebElement> rows_specialties = tasks.findmultipleElements("XPATH", ProductFinderConstants.subAreaRow + "/table/tbody/tr");
            for (int i = 0; i < rows_subArea.size(); i++) {
                String key = tasks.findElement("XPATH", ProductFinderConstants.subAreaRow + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
                String value = tasks.findElement("XPATH", ProductFinderConstants.subAreaRow + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
                prop_subArea.setProperty(key, value);
            }
        }
        else //JF
        {
            //capture Information values
            List<WebElement> rows_info = tasks.findmultipleElements("XPATH", ProductFinderConstants.sectionDetailJF + "/table/tbody/tr");
            for (int i = 0; i < rows_info.size(); i++) {
                String key = tasks.findElement("XPATH", ProductFinderConstants.sectionDetailJF + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
                String value = tasks.findElement("XPATH", ProductFinderConstants.sectionDetailJF + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
                prop_info.setProperty(key, value);}


        //capture identifiers values
        List<WebElement> rows_identifiers = tasks.findmultipleElements("XPATH", ProductFinderConstants.identifierRowJF + "/table/tbody/tr");
        for (int i = 1; i <= rows_identifiers.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.identifierRowJF + "/table/tbody/tr[" + (i) + "]/td[1]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.identifierRowJF + "/table/tbody/tr[" + (i) + "]/td[2]").getText();
                prop_identifier.setProperty(key, value);
        }
        //capture subject area values
        List<WebElement> rows_subArea = tasks.findmultipleElements("XPATH", ProductFinderConstants.subAreaRowJF + "/table/tbody/tr");
        for (int i = 0; i < rows_subArea.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.subAreaRowJF + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.subAreaRowJF + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            prop_subArea.setProperty(key, value);
        }

        //capture specialties
        List<WebElement> rows_specialties = tasks.findmultipleElements("XPATH", ProductFinderConstants.specialtiesRowJF + "/li");
        for (int i = 1; i <= rows_specialties.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.specialtiesRowJF + "/li[" + i + "]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.specialtiesRowJF + "/li[" + i  + "]").getText();
            prop_specialties.setProperty(key, value);
        }
        }

        Log.info("getUI_WorkOverview_Information - End");

    }

    public void getUI_WorkOverview_Financial() {
        //created by Nishant @15 Jun 2020

        //click on Financial tab
        tasks.click("XPATH", ProductFinderConstants.financialTab);

        //capture company code values
        List<WebElement> rows_info = tasks.findmultipleElements("XPATH", ProductFinderConstants.DetailInformation1 + "/table/tbody/tr");
        for (int i = 0; i < rows_info.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation1 + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation1 + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            prop_comCode.setProperty(key, value);
        }


        //capture accountable product values
        List<WebElement> rows_subArea = tasks.findmultipleElements("XPATH", ProductFinderConstants.DetailInformation2 + "/table/tbody/tr");
        for (int i = 0; i < rows_subArea.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation2 + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation2 + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            prop_AccProducts.setProperty(key, value);
        }
    }

    public void getUI_Editorial() {
        //created by Nishant @07 Jul 2020

        //click on Editorial tab
        tasks.click("XPATH", ProductFinderConstants.editorialTab);

        //capture left editorial info values
        List<WebElement> rows_info = tasks.findmultipleElements("XPATH", ProductFinderConstants.DetailInformation1 + "/table/tbody/tr");
        for (int i = 0; i < rows_info.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation1 + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation1 + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            prop_editorial1.setProperty(key, value);
        }

        //capture right editorial info values
        List<WebElement> rows_subArea = tasks.findmultipleElements("XPATH", ProductFinderConstants.DetailInformation2 + "/table/tbody/tr");
        for (int i = 0; i < rows_subArea.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation2 + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation2 + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            prop_editorial2.setProperty(key, value);
        }
    }

    public void getUI_People() {
        //created by Nishant @10 Jul 2020
        //click on People tab
        tasks.click("XPATH", ProductFinderConstants.peopleTab);

        //capture web elements
        List<WebElement> rows_info = tasks.findmultipleElements("XPATH", ProductFinderConstants.sectionDetail + "//table/tbody/tr");
        for (int i = 0; i < rows_info.size(); i++) {
            Properties prop_people = new Properties();

            String PersonName = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "//table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String Role = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "//table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            String Email = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "//table/tbody/tr[" + (i + 1) + "]/td[3]").getText();
            prop_people.setProperty("PersonName", PersonName);
            prop_people.setProperty("Role", Role);
            prop_people.setProperty("Email", Email);
            list_people.add(prop_people);
        }

    }

    public void getUI_Links() {
        //created by Nishant @15 Jul 2020

        //click on Links tab
        tasks.click("XPATH", ProductFinderConstants.linkTab);

        //capture links values
        List<WebElement> rows_info = tasks.findmultipleElements("XPATH", ProductFinderConstants.sectionDetail + "/table/tbody/tr");
        for (int i = 0; i < rows_info.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            prop_links.setProperty(key, value);
        }
    }

    public void filter_Search_Result_Randomly(String filterType, String filterValue) throws InterruptedException {
//created by Nishant @ 23 Apr 2021

        //find out how many types of filter available on search result page
        List<WebElement> filterTypes = tasks.findmultipleElements("XPATH", ProductFinderConstants.FilterLocator);

        //identify and select intended filter type for the scenarios
        for (int f = 0; f < filterTypes.size(); f++) {
            if (filterTypes.get(f).getText().contains(filterType)) {
                Log.info("filter type to be chosen -" + filterType);

                if (!filterValue.isEmpty()) {
                    tasks.click("XPATH", "//span[contains(text(),'" + filterValue + "')]");
                    Thread.sleep(1000);
                    DataQualityContext.prop_filters.setProperty(filterType, filterValue);
                    break;
                } else {
                    //find out how many options are available under intended filter type
                    String filterOptionIdentifier = ProductFinderConstants.FilterLocator + "[" + (f + 1) + "]" + "/child::div";
                    List<WebElement> filterOptions = tasks.findmultipleElements("XPATH", filterOptionIdentifier);

                    //choose a randomly from available options
                    int cnt = getRandomCount(filterOptions.size()) + 1;
                    WebElement filterChosen = tasks.findElement("XPATH", filterOptionIdentifier + "[" + cnt + "]//label/span");

                    //capture filter name chosen randomly and click it

                    DataQualityContext.prop_filters.setProperty(filterType, filterChosen.getText());
                    filterChosen.click();
                    Thread.sleep(1000);

                    break;
                }
            }
        }
        Log.info("Filtered by " + filterType + " - " + DataQualityContext.prop_filters.getProperty(filterType));
    }


    public List<WebElement> getLinks() {
        List<WebElement> links = tasks.findmultipleElements("XPATH", "//a");
        return links;
    }

    public void closeBrowser() {
        tasks.closeBrowser();
    }

    public int getRandomCount(int limit) {//created by Nishant @ 22 Apr 2021
        Random random = new Random();
        return random.nextInt(limit);
    }

    public void verifyUserIsOnOverviewPage() throws InterruptedException {//created by Nishant @ 23 Apr 2021
       // tasks.waitUntilPageLoad();
       // Thread.sleep(3000);
        if (TestContext.getValues().environment.equalsIgnoreCase("UAT") |
                TestContext.getValues().environment.equalsIgnoreCase("SIT")) {
            if (DataQualityContext.productDataObjectsFromEPHGD != null)
                ProductFinderTasks.searchResultId = DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID();
        }
        assertTrue(isUserOnProductPage(ProductFinderTasks.searchResultId));
    }

    public void verifyWorkTab() throws InterruptedException {
        //created by Nishant @ 28 Apr 2021
        Assert.assertTrue("Works tab displayed",tasks.verifyElementisDisplayed("XPATH", ProductFinderConstants.tab_Works));
        Assert.assertTrue("Works tab clickable",tasks.verifyElementisClickable("XPATH", ProductFinderConstants.tab_Works));
        System.out.println("'Works' tab displayed and clickable");
    }
    public void verifyProductTab() throws InterruptedException {
        //created by Nishant @ 28 Apr 2021
        Assert.assertTrue("Product & Packages tab displayed",tasks.verifyElementisDisplayed("XPATH", ProductFinderConstants.tab_product_andPackages));
        Assert.assertTrue("Product & Packages tab clickable", tasks.verifyElementisClickable("XPATH", ProductFinderConstants.tab_product_andPackages));
        System.out.println("'Product & Packages' tab displayed and clickable");
    }
    public void verifyManifestationTab() throws InterruptedException {
        //created by Nishant @ 28 Apr 2021
        Assert.assertTrue("Manifestation tab displayed",tasks.verifyElementisDisplayed("XPATH", ProductFinderConstants.tab_manifestation));
        Assert.assertTrue("Manifestation tab clickable",tasks.verifyElementisClickable("XPATH", ProductFinderConstants.tab_manifestation));
        System.out.println("'Manifestation' tab displayed and clickable");
    }

    public void verifyLeftCoreTab() throws InterruptedException {
        //created by Nishant @ 28 Apr 2021
        Assert.assertTrue("Core tab displayed",tasks.verifyElementisDisplayed("XPATH", ProductFinderConstants.coreTab));
        Assert.assertTrue("Core tab clickable",tasks.verifyElementisClickable("XPATH", ProductFinderConstants.coreTab));
        System.out.println("'Core' tab displayed and clickable");
    }

    public void verifyLeftPeopleTab() throws InterruptedException {
        //created by Nishant @ 28 Apr 2021
        Assert.assertTrue("People tab displayed",tasks.verifyElementisDisplayed("XPATH", ProductFinderConstants.peopleTab));
        Assert.assertTrue("People tab clickable",tasks.verifyElementisClickable("XPATH", ProductFinderConstants.peopleTab));
        System.out.println("'People' tab displayed and clickable");
    }
    public void verifyLeftFinancialTab() throws InterruptedException {
        //created by Nishant @ 28 Apr 2021
        Assert.assertTrue("Financial tab displayed",tasks.verifyElementisDisplayed("XPATH", ProductFinderConstants.financialTab));
        Assert.assertTrue("Financial tab clickable",tasks.verifyElementisClickable("XPATH", ProductFinderConstants.financialTab));
        System.out.println("'Financial' tab displayed and clickable");
    }
    public void verifyLeftEditorialTab() throws InterruptedException {
        //created by Nishant @ 28 Apr 2021
        Assert.assertTrue("Editorial tab displayed",tasks.verifyElementisDisplayed("XPATH", ProductFinderConstants.editorialTab));
        Assert.assertTrue("Editorial tab clickable",tasks.verifyElementisClickable("XPATH", ProductFinderConstants.editorialTab));
        System.out.println("'Editorial' tab displayed and clickable");
    }
    public void verifyLeftLinkTab() throws InterruptedException {
        //created by Nishant @ 28 Apr 2021
        Assert.assertTrue("Links tab displayed",tasks.verifyElementisDisplayed("XPATH", ProductFinderConstants.linkTab));
        Assert.assertTrue("Links tab clickable",tasks.verifyElementisClickable("XPATH", ProductFinderConstants.linkTab));
        System.out.println("'Links' tab displayed and clickable");
    }

    public void verifyPreviousSearch(){//created by Nishant @ 30 Apr 2021
        try {
            Assert.assertTrue("Previous Search",tasks.verifyElementisDisplayed("XPATH",ProductFinderConstants.previousSearchParent));
            List<WebElement> previousSearch = tasks.findmultipleElements("XPATH",ProductFinderConstants.previousSearchChild);
            System.out.println(previousSearch.size()+" previous searches available");
            sysoutln("verified previous search");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void verifyHelpAndSupport(){//created by Nishant @ 30 Apr 2021

        try {
            Assert.assertTrue("Help and Support displayed",tasks.verifyElementisDisplayed("XPATH",ProductFinderConstants.helpSupportParent));
            Log.info("verifying 'Help and Support'");
            List<WebElement> helpSupport = tasks.findmultipleElements("XPATH",ProductFinderConstants.helpSupportChild);

            Assert.assertEquals(helpSupport.get(0).getText(),"About Product Finder");
            sysoutln("Displayed - About Product Finder");

            if(DataQualityContext.uiUnderTest.equalsIgnoreCase("JF")) {
                Assert.assertEquals(helpSupport.get(1).getText(), "Journal Portfolio Dashboard");
                sysoutln("Displayed - Journal Portfolio Dashboard");
                Assert.assertEquals(helpSupport.get(2).getText(), "How to use Product Finder");
                sysoutln("Displayed - How to use Product Finder");
                Assert.assertEquals(helpSupport.get(3).getText(), "Contact us");
                sysoutln("Displayed - Contact us");
            }
            else
            if(DataQualityContext.uiUnderTest.equalsIgnoreCase("PF")) {
                Assert.assertEquals(helpSupport.get(1).getText(), "How to use Product Finder");
                sysoutln("Displayed - How to use Product Finder");
                Assert.assertEquals(helpSupport.get(2).getText(), "Contact us");
                sysoutln("Displayed - Contact us");
            }
            sysoutln("verified Help and Support");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void verifyLatestWork() {//created by Nishant @ 30 Apr 2021

        try {
            Assert.assertTrue("Latest work displayed",tasks.verifyElementisDisplayed("XPATH",ProductFinderConstants.latestWorkParent));
            Log.info("verifying latest work");
            List<WebElement> latestWorks = tasks.findmultipleElements("XPATH",ProductFinderConstants.latestWorkChild);
            sysoutln("before ViewAllClick - Latest work displayed "+latestWorks.size());
            Assert.assertTrue("Latest work View More displayed",tasks.verifyElementisDisplayed("XPATH",ProductFinderConstants.latestWorkViewMore));
            Assert.assertTrue("Latest work View More clickable",tasks.verifyElementisClickable("XPATH",ProductFinderConstants.latestWorkViewMore));
            // sysoutln("View More");
            tasks.click("XPATH",ProductFinderConstants.latestWorkViewMore);
            List<WebElement>worksDisplayed = tasks.findmultipleElements("XPATH", ProductFinderConstants.itemDetail);
            sysoutln("after ViewAllClick - Latest work displayed "+worksDisplayed.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void captureLinksFromPageAndVerify(String page)
    {//created by Nishant @ 30 Apr 2021

        List<WebElement> availableLinks = tasks.findAllLinks();
        System.out.println("\navailable links - "+availableLinks.size());

        String resultFile = "C:/Users/Chitren/Office Work/Project doc/EPH sprint testing/Result files/LinksResult_"+DataQualityContext.uiUnderTest+" "+DataQualityContext.DateAndTime+".csv";

        ArrayList<String> resultHeaders = new ArrayList<>(Arrays. asList("Page","Text", "Link", "Status code","comment"));
        if(!new File(resultFile).exists())
        {
            specificTasks.writeCsv(resultFile,resultHeaders);
        }

        for(WebElement link:availableLinks)
        {
            try {
                DataQualityContext.DataToWrite.add(page);
                DataQualityContext.DataToWrite.add("\""+link.getText()+"\"");
                DataQualityContext.DataToWrite.add("\""+link.getAttribute("href")+"\"");
                sysoutln(link.getAttribute("href"));
                specificTasks.verifySingleLink(link.getAttribute("href"));
             //   if(DataQualityContext.DataToWrite.size()>2)
             //   Assert.assertTrue(Integer.parseInt(DataQualityContext.DataToWrite.get(1))<400);
            } catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                specificTasks.writeCsv(resultFile,DataQualityContext.DataToWrite);
                DataQualityContext.DataToWrite.clear();
            }
        }


    }

    public void sysoutln(String str)
    {System.out.println(str);}


}