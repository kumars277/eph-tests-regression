package com.eph.automation.testing.models.ui;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.Product;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.google.inject.Inject;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ProductFinderTasks {

    private TasksNew tasks;

    @Inject
    public ProductFinderTasks(final TasksNew tasks) {
        this.tasks = tasks;
    }

    public static String searchResultWorkId;
    public Properties prop_info = new Properties();
    public Properties prop_subArea = new Properties();
    public Properties prop_identifier = new Properties();
    public Properties prop_comCode = new Properties();
    public Properties prop_AccProducts = new Properties();
    public Properties prop_editorial1 = new Properties();
    public Properties prop_editorial2 = new Properties();
    public List<Properties> list_people=new ArrayList();
    public Properties prop_links = new Properties();
    public void openHomePage() throws InterruptedException {//updated by Nishant @ 18 May 2020
        String HomePageAddress="";
        if (TestContext.getValues().environment.equalsIgnoreCase("SIT"))
            HomePageAddress=Constants.PRODUCT_FINDER_EPH_SIT_UI;
        else if (TestContext.getValues().environment.equalsIgnoreCase("UAT"))
            HomePageAddress=Constants.PRODUCT_FINDER_EPH_UAT_UI;
        if(DataQualityContext.uiUnderTest=="JF")HomePageAddress+="journals";
        tasks.openPage(HomePageAddress);
        Thread.sleep(1000);
        tasks.waitUntilPageLoad();
        Log.info("\nhome page loaded...");
    }

    public void loginByScienceAccount(String scienceEmailId) throws InterruptedException {
        //updated by Nishant @ 13 Feb 2020
        //  if(!tasks.isObjectpresent("XPATH",ProductFinderConstants.userDetail))
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
        switch(searchType)
        {
            case "All":break;
            case "PMG":   tasks.click("XPATH",ProductFinderConstants.searchDropdownPmg);break;
            case "PMC":   tasks.click("XPATH",ProductFinderConstants.searchDropdownPmc);break;
            case "Person":tasks.click("XPATH",ProductFinderConstants.searchDropdownPerson);break;
        }
    }

    public void searchFor(final String searchID) throws InterruptedException {
        //updated by Nishant @ 14 May 2020
        //updated by Nishant @ 03 Jul 2020 for Journal Finder
        while (!tasks.isObjectpresent("XPATH", ProductFinderConstants.searchBar)) {
            tasks.driver.navigate().refresh();
            Thread.sleep(3000);
        }
        Log.info("Searching " + searchID + " on Product Finder/Journal Finder");
        tasks.clearText("XPATH", ProductFinderConstants.searchBar);
        tasks.sendKeys("XPATH", ProductFinderConstants.searchBar, searchID);
        tasks.click("XPATH", ProductFinderConstants.searchButton);
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
        String buildWorkIdLocator = String.format(ProductFinderConstants.buildWorkIdLocator, workId);
        tasks.click("XPATH", buildWorkIdLocator);
        Thread.sleep(5000);
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
        String referenceUrl="work/" + workID + "/overview";

        if(DataQualityContext.uiUnderTest=="JF")referenceUrl="journals/"+referenceUrl;
        if (TestContext.getValues().environment.equalsIgnoreCase("SIT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_SIT_UI + referenceUrl;
        else if (TestContext.getValues().environment.equalsIgnoreCase("UAT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_UAT_UI + referenceUrl;

        Log.info("Expected Target URL " + targetURL);
        if (targetURL.equalsIgnoreCase(tasks.getCurrentPageUrl())) return true;
        else return false;
    }

    public boolean isUserOnProductPage(String productId) {
        //created by Nishant @ 19 May 2020
        String targetURL = "";
        if (TestContext.getValues().environment.equalsIgnoreCase("SIT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_SIT_UI + "product/" + productId + "/overview";
        else if (TestContext.getValues().environment.equalsIgnoreCase("UAT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_UAT_UI + "product/" + productId + "/overview";

        Log.info("Target URL " + targetURL);
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

        //capture Information values
        List<WebElement> rows_info = tasks.findmultipleElements("XPATH", ProductFinderConstants.DetailInformation1 + "/table/tbody/tr");
        for (int i = 0; i < rows_info.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation1 + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation1 + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            prop_info.setProperty(key, value);
        }

        //capture subject area values
        List<WebElement> rows_subArea = tasks.findmultipleElements("XPATH", ProductFinderConstants.DetailInformation2 + "/table/tbody/tr");
        for (int i = 0; i < rows_subArea.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation2 + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.DetailInformation2 + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            prop_subArea.setProperty(key, value);
        }

        //capture identifiers values
        List<WebElement> rows_identifiers = tasks.findmultipleElements("XPATH", ProductFinderConstants.DetailIdentifiers + "/table/tbody/tr");
        for (int i = 0; i < rows_identifiers.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.DetailIdentifiers + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.DetailIdentifiers + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            prop_identifier.setProperty(key, value);
        }
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
            Properties prop_people=new Properties();

            String PersonName = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "//table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String Role = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "//table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            String Email = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "//table/tbody/tr[" + (i + 1) + "]/td[3]").getText();
            prop_people.setProperty("PersonName", PersonName);
            prop_people.setProperty("Role", Role);
            prop_people.setProperty("Email", Email);
            list_people.add(prop_people);
        }

    }

    public void getUI_Links() {//created by Nishant @15 Jul 2020

        //click on Editorial tab
        tasks.click("XPATH", ProductFinderConstants.linkTab);

        //capture links values
        List<WebElement> rows_info = tasks.findmultipleElements("XPATH", ProductFinderConstants.sectionDetail + "/table/tbody/tr");
        for (int i = 0; i < rows_info.size(); i++) {
            String key = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "/table/tbody/tr[" + (i + 1) + "]/td[1]").getText();
            String value = tasks.findElement("XPATH", ProductFinderConstants.sectionDetail + "/table/tbody/tr[" + (i + 1) + "]/td[2]").getText();
            prop_links.setProperty(key, value);
        }
    }


    public List<WebElement> getLinks()
{
    List<WebElement> links= tasks.findmultipleElements("XPATH","//a");
    return links;
}
}
