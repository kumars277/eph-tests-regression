package com.eph.automation.testing.steps.specific;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.api.WorkApiObject;
import com.eph.automation.testing.models.api.WorkExtendedPersons;
import com.eph.automation.testing.models.api.WorkManifestationApiObject;
import com.eph.automation.testing.models.api.WorksMatchedApiObject;
import com.eph.automation.testing.models.api.PersonsApiObject;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import static com.eph.automation.testing.models.contexts.DataQualityContext.*;
import com.eph.automation.testing.models.contexts.FinancialAttribsContext;
import com.eph.automation.testing.models.dao.*;
import com.eph.automation.testing.models.ui.ProductFinderConstants;
import com.eph.automation.testing.models.ui.ProductFinderTasks;
import com.eph.automation.testing.models.ui.TasksNew;
import com.eph.automation.testing.services.api.AzureOauthTokenFetchingException;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.eph.automation.testing.services.db.sql.PersonWorkRoleDataSQL;
import com.eph.automation.testing.services.db.sql.ProductFinderSQL;
import com.eph.automation.testing.steps.search_api.ApiWorksSearchSteps;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import cucumber.api.java.en.*;


import static org.apache.commons.lang.math.RandomUtils.nextInt;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import net.bytebuddy.implementation.bytecode.Throw;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.openqa.selenium.WebElement;
import javax.net.ssl.SSLHandshakeException;


public class ProductFinderUISteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    private ProductFinderTasks productFinderTasks;
    private TasksNew tasks;
    private String sql;
   // private String randomWorkStatus;

    private static List<String> productIdList;
    private static List<String> workIdList = new ArrayList<>();
    private static List<String> workTypeCode;

    private static String[] Book_Types = {"Book Set","Books Series","Major Ref work", "Non-Elsevier Book", "Other Book", "Reference Book", "Serial", "Text Book"};
    private static String[] Journal_Types = {"Abstracts Journal", "B2B Journal", "Journal", "Newsletter","Non-Elsevier Journal"};
    private static String[] Other_Types = {"Drug Monograph", "Medical Procedure"};
    private static String[] ck_Types = {"Flex Package", "Flex AG Package","Specialty Package"};
    String[] allWorkTypesCode = {"BKS", "MRW", "OTH", "SER", "TBK", "RBK", "ABS", "JNL", "JBB", "NWL", "DMG", "MPR"};

    private List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD;
    private FinancialAttribsContext financialAttribs = new FinancialAttribsContext();
    private WorkApiObject workApiObject = new WorkApiObject();
    private WorkManifestationApiObject workManifestationApiObject = new WorkManifestationApiObject();
    private ApiWorksSearchSteps apiWorksSearchSteps = new ApiWorksSearchSteps();
    private PersonsApiObject personsApiObject = new PersonsApiObject();

    @Inject
    public ProductFinderUISteps(ProductFinderTasks productFinderTasks, TasksNew tasks) {
        this.productFinderTasks = productFinderTasks;
        this.tasks = tasks;
    }

    //###########################################################################
    //work steps

    @Given("^get (.*) random work id from DB")
    public void getRandomWorkIds(String numberOfRecords) {
        //created by Nishant @ 15 May 2020
        sql = String.format(APIDataSQL.SELECT_GD_RANDOM_WORK_ID, numberOfRecords);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
       // ids.clear(); ids.add("EPR-W-106TTG");      Log.info("hard coded work ids are : " + ids);
        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }

    @And("We get the work search data from EPH GD for (.*)")
    public void getSpecificWorksDataFromEPHGD(String WorkId) {
        sql = String.format(ProductFinderSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, WorkId);
        DataQualityContext.workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        DataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));
    }

    @Given("^user is on Product Finder search page$")
    public void userOpensPFHomePage() throws InterruptedException {
        //updated by Nishant @ 15 May 2020
        DataQualityContext.uiUnderTest = "PF";
        productFinderTasks.openHomePage();
      //  productFinderTasks.loginWithCredential();
        tasks.waitUntilPageLoad();
    }

    @Given("^user is on Journal Finder search page$")
    public void userOpensJFHomePage() throws InterruptedException {
        //Created by Nishant @ 03 Jul 2020
        DataQualityContext.uiUnderTest = "JF";
        productFinderTasks.openHomePage();

      //  tasks.waitUntilPageLoad();

    }

    @Given("^user is on Product/Journal Finder search page (.*)$")
    public void userOpensHomePage(String ui) throws InterruptedException {
        //created by Nishant @ 20 Apr 2021
        DataQualityContext.uiUnderTest = ui;
        productFinderTasks.openHomePage();
      // productFinderTasks.loginWithCredential();
    }

    @Then("^Search works by (.*)$")
    public void search_works_by_options(String option) throws InterruptedException {
        //created by Nishant @ 15 May 2020
        Log.info("searching work by " + option);
        switch (option) {
            case "Id":
                productFinderTasks.searchFor(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                break;
            case "Title":
                productFinderTasks.searchFor(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
                break;
            case "Keyword":
                String[] titleSplit = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().replaceAll("[^a-zA-Z0-9]", " ").split(" ");
                String keyword = "";
                for(int i=0;i<=(titleSplit.length)/2;i++)
                {
                    keyword = keyword+titleSplit[i]+" ";
                }
                productFinderTasks.searchFor(keyword);
                break;
        }

        //added by Nishant @ 18 May 2020
        Log.info("title to be searched..." + DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        boolean workSearched = productFinderTasks.searchOnResultPages(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        Assert.assertTrue("searched key is on search result", workSearched);
    }

    @Then("^The searched work is listed and clicked$")
    public void checkSearchResultsAndClickWork() throws InterruptedException {
        Log.info("found entry ..." + DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        productFinderTasks.clickWork(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());

    }

    @And("^User is forwarded to the searched works page from DB$")
    public void verifyUserIsForwardedToWorksPageFromDatabase() {
        assertTrue(productFinderTasks.isUserOnWorkPage(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID()));
    }

    //for specific workid
    @And("^Verify user is forwarded to the searched work page from Search Result$")
    public void verifyUserIsForwardedToSearchedWorkPageFromSearchResult() throws InterruptedException {
        tasks.waitUntilPageLoad();
        Thread.sleep(5000);
        assertTrue(productFinderTasks.isUserOnWorkPage(ProductFinderTasks.searchResultId));
    }

    @And("^Verify user is forwarded to the searched work page of (.*)$")
    public void verifyUserIsForwardedToSearchedWorkPageOf(String workId) {
        assertTrue(productFinderTasks.isUserOnWorkPage(workId));
    }

    @And("^Verify user is forwarded to the searched work page$")
    public void verifyUserIsForwardedToSearchedWorkPage() {

        assertTrue(productFinderTasks.isUserOnWorkPage(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID()));
    }

    @Given("^Get the available Work Types from the DB \"([^\"]*)\"$")
    public void get_the_available_Work_Types_from_the_DB(String chooseWorkType) {
        //updated by Nishant @ 21 May 2020
        switch (chooseWorkType) {
            case "Book":
                sql = ProductFinderSQL.SELECT_AVAILABLE_WORK_TYPES_FOR_BOOK;
                break;
            case "Journal":
                sql = ProductFinderSQL.SELECT_AVAILABLE_WORK_TYPES_FOR_JOURNAL;
                break;
            case "Other":
                sql = ProductFinderSQL.SELECT_AVAILABLE_WORK_TYPES_FOR_OTHER;
                break;
                default: throw new IllegalArgumentException("unexpected value");
        }

        List<Map<?, ?>> availableWorkTypeCode = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        if (availableWorkTypeCode.size() > 0) {
            workTypeCode = availableWorkTypeCode.stream().map(m -> (String) m.get("WORK_TYPE")).map(String::valueOf).collect(Collectors.toList());
            Log.info("\nAvailable Work Types for " + chooseWorkType + " in DB: " + workTypeCode);
        } else {
            Log.info("Records for the work Type => " + chooseWorkType + " not available in DB.");
        }
    }

    @Then("^Get a Work Id for each Work Types available in the DB$")
    public void get_Work_Id_for_each_Work_Types_available_in_DB() {
        //updated by Nishant @ 21 May 2020
        List<String> workforWorkType;
        for (String workTypeCodes : workTypeCode) {
            sql = String.format(ProductFinderSQL.SELECT_WORKID_FOR_WORK_TYPE, workTypeCodes);
            List<Map<?, ?>> workIdsAvailableforWorkTypes = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            workforWorkType = workIdsAvailableforWorkTypes.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
            workIdList.add(workforWorkType.get(0));
        }
        Log.info("\nWork Ids Used: " + workIdList);
    }

    @Given("^Search for the Work by Work Ids Filter workType and verify the work Type is \"([^\"]*)\"$")
    public void verify_the_result(String chooseWorkType) {
        try {
            int i=0;
            for (String workId : workIdList) {
                productFinderTasks.searchFor(workId);
                filter_Search_Result_for_workType(chooseWorkType);

                productFinderTasks.searchOnResultPages(workId);
                assertTrue(tasks.verifyElementTextisDisplayed(workId));

                productFinderTasks.clickWork(workId);
                assertTrue(productFinderTasks.isUserOnWorkPage(workId));

                boolean isWorkTypeCorrect = verifyWorkTypeForWorkId(workId, chooseWorkType);
                assertTrue(i+ "out of "+workIdList.size()+" Work Id " + workId + " Successfully filtered by Work Type: " + chooseWorkType, isWorkTypeCorrect);

                productFinderTasks.openHomePage();
                i++;
            }
            workIdList.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @And("^verify latest work,previous search and help$")
    public void verify_latest_work()
    {//created by Nishant @ 30 Apr 2021
        productFinderTasks.verifyPreviousSearch();
        productFinderTasks.verifyHelpAndSupport();
        productFinderTasks.verifyLatestWork();
    }

    @And("^verify links on (.*)$")
    public void verifyLinksOnLandingPage(String landingPage)
    {//created by Nishant @ 30 Apr 2021
        Log.info("verifying links on "+landingPage);
        productFinderTasks.captureLinksFromPageAndVerify(landingPage);
    }

    @And ("^validate links on work overview page$")
    public void verifyLinksOnWorkOrverview()
    { //created by Nishant @ 3 May 2021
        Log.info("verifying overview links...");
        //1.1 Core data validation
        verifyLinksOnLandingPage("Core data");

        //1.2 click on People tab
        tasks.click("XPATH", ProductFinderConstants.peopleTab);
        verifyLinksOnLandingPage("People data");

        //1.3. click on Financial tab
        tasks.click("XPATH", ProductFinderConstants.financialTab);
        verifyLinksOnLandingPage("Financial data");

        if(uiUnderTest.equalsIgnoreCase("JF"))
        {
            //1.4. click on Editorial tab
            tasks.click("XPATH", ProductFinderConstants.editorialTab);
            verifyLinksOnLandingPage("Editorial data");

            //1.5. click on Links tab
            tasks.click("XPATH", ProductFinderConstants.linkTab);
            verifyLinksOnLandingPage("Links data");
        }

        //2. click on Manifestations tab
        tasks.click("XPATH", ProductFinderConstants.tab_manifestation);
        verifyLinksOnLandingPage("manifestation data");

        //3. click on Products tab
        tasks.click("XPATH", ProductFinderConstants.tab_product_andPackages);
        verifyLinksOnLandingPage("Products data");

        //4. click on Packages tab
        tasks.click("XPATH", ProductFinderConstants.tab_Packages);
        verifyLinksOnLandingPage("Packages data");

    }

    @And ("^validate links on manifestation overview page$")
    public void verifyLinksOnManifestationOrverview()
    { //created by Nishant @ 3 May 2021
        Log.info("verifying overview links...");
        //1.1 Core data validation
        verifyLinksOnLandingPage("Core data");

        //1.2 click on People tab
        tasks.click("XPATH", ProductFinderConstants.peopleTab);
        verifyLinksOnLandingPage("People data");

        //1.3. click on Financial tab
        tasks.click("XPATH", ProductFinderConstants.financialTab);
        verifyLinksOnLandingPage("Financial data");

        if(uiUnderTest.equalsIgnoreCase("JF"))
        {
            //1.4. click on Editorial tab
            tasks.click("XPATH", ProductFinderConstants.editorialTab);
            verifyLinksOnLandingPage("Editorial data");

            //1.5. click on Links tab
            tasks.click("XPATH", ProductFinderConstants.linkTab);
            verifyLinksOnLandingPage("Links data");
        }

        //2. click on Work tab
        tasks.click("XPATH", ProductFinderConstants.tab_Works);
        verifyLinksOnLandingPage("work data");

        //3. click on Products tab
        tasks.click("XPATH", ProductFinderConstants.tab_product_andPackages);
        verifyLinksOnLandingPage("Products data");

        //4. click on Packages tab
        tasks.click("XPATH", ProductFinderConstants.tab_Packages);
        verifyLinksOnLandingPage("Packages data");

        //5. click on Relataed manifestation tab
        tasks.click("XPATH", ProductFinderConstants.tab_RelatedManifestations);
        verifyLinksOnLandingPage("Related Manifestation data");
    }

    @And ("^validate links on product overview page$")
    public void verifyLinksOnProductOrverview()
    { //created by Nishant @ 3 May 2021
        Log.info("verifying overview links...");
        //1.1 Core data validation
        verifyLinksOnLandingPage("Core data");

        //1.2 click on People tab
        tasks.click("XPATH", ProductFinderConstants.peopleTab);
        verifyLinksOnLandingPage("People data");

        //1.3. click on Financial tab
        tasks.click("XPATH", ProductFinderConstants.financialTab);
        verifyLinksOnLandingPage("Financial data");

        if(uiUnderTest.equalsIgnoreCase("JF"))
        {
            //1.4. click on Editorial tab
            tasks.click("XPATH", ProductFinderConstants.editorialTab);
            verifyLinksOnLandingPage("Editorial data");

            //1.5. click on Links tab
            tasks.click("XPATH", ProductFinderConstants.linkTab);
            verifyLinksOnLandingPage("Links data");
        }

        //2. click on Work tab
        tasks.click("XPATH", ProductFinderConstants.tab_Works);
        verifyLinksOnLandingPage("work data");

        //3. click on Manifestation tab
        tasks.click("XPATH", ProductFinderConstants.tab_manifestation);
        verifyLinksOnLandingPage("Manifestation data");
    }


    @Given("^Searches for given ([^\"]*)$")
    public void searches_for_works_by_given(String searchKeyword) throws InterruptedException {
       // Log.info("searching keyword..." + searchKeyword);
        productFinderTasks.searchFor(searchKeyword);
    }

    @Given("^Searches work by id$")
    public void searches_works_by_id() throws InterruptedException {
        productFinderTasks.searchFor(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
    }

    @Then ("^verify Works, Products&Packages, Manifestation tab$")
    public void verifyWorksProductsManifestationTab() throws InterruptedException {
      productFinderTasks.verifyWorkTab();
      productFinderTasks.verifyProductTab();
      productFinderTasks.verifyManifestationTab();
    }

    @Given("^Filter Search Result with workType \"([^\"]*)\" and workStatus \"([^\"]*)\"$")
    public void filter_Search_Result_for_workStatus_AndType(String workType,String keyword) throws InterruptedException {
        Random rand =  new Random();
        int i=0;
        List<String> ignoreStatus = new ArrayList<>();
        try{
            do{
                if(i!=0){productFinderTasks.searchFor(keyword);}i++;
                //filter with work type
                String buildWorkTypeFilterLocator = "//span[contains(text(),\'"+workType+"\')]";
                tasks.click("XPATH",buildWorkTypeFilterLocator);
                Thread.sleep(1000);

                //filter with work status
                List<WebElement> availableWorkStatus =tasks.findmultipleElements("XPATH","//div[@formarrayname='workStatus']") ;
                do{
                    randomWorkStatus = availableWorkStatus.get(rand.nextInt(availableWorkStatus.size())).getText();
                }
                while(ignoreStatus.contains(randomWorkStatus));
                ignoreStatus.add(randomWorkStatus);
                String buildWorkFilterLocator = "//span[contains(text(),\'" + randomWorkStatus + "\')]";
                tasks.click("XPATH", buildWorkFilterLocator);
                Log.info("chosen work status - "+randomWorkStatus);
                Thread.sleep(1000);
            }
            while(tasks.findElement("XPATH",
                    "//div[contains(text(),' There are no results that match your search')]").isDisplayed());
        }
        catch (org.openqa.selenium.NoSuchElementException e)
        {Log.info("result filtered by work status..."+randomWorkStatus);}

    }


    @Given("^Filter the Search Result for workStatus \"([^\"]*)\"$")
    public void filter_Search_Result_for_workStatus(String keyword) throws InterruptedException {
        Random rand =  new Random();
        int i=0;
        List<String> ignoreStatus = new ArrayList<>();
        try{
        do{
            if(i!=0){productFinderTasks.searchFor(keyword);}i++;
            List<WebElement> availableWorkStatus =tasks.findmultipleElements("XPATH","//div[@formarrayname='workStatus']") ;
            do{
            randomWorkStatus = availableWorkStatus.get(rand.nextInt(availableWorkStatus.size())).getText();
            }
            while(ignoreStatus.contains(randomWorkStatus));
            ignoreStatus.add(randomWorkStatus);
            String buildWorkFilterLocator = "//span[contains(text(),\'" + randomWorkStatus + "\')]";
        tasks.click("XPATH", buildWorkFilterLocator);
        Log.info("chosen work status - "+randomWorkStatus);
        Thread.sleep(1000);}
        while(tasks.findElement("XPATH",
                "//div[contains(text(),' There are no results that match your search')]").isDisplayed());
        }
        catch (org.openqa.selenium.NoSuchElementException e)
        {Log.info("result filtered by work status..."+randomWorkStatus);}

    }

    @Given("^Filter Search Result for workType \"([^\"]*)\"$")
    public void filter_Search_Result_for_workType(String workType) throws InterruptedException {
        String buildWorkTypeFilterLocator = "//span[contains(text(),\'"+workType+"\')]";
        tasks.click("XPATH",buildWorkTypeFilterLocator);
        Thread.sleep(1000);
    }

    @Given("^Filter the Search Result by filter \"([^\"]*)\" and value \"(.*)\"$")
    public void filter_Search_Result_Randomly(String FilterType, String filterValue) throws InterruptedException {
        //created by Nishant @ 22 Apr 2021
        productFinderTasks.filter_Search_Result_Randomly(FilterType, filterValue);
        Thread.sleep(1000);
    }

    @Then("^Search items are listed and click an id from the result")
    public void search_items_are_listed_and_click_aWorkId() throws InterruptedException {
        //created by Nishant @ 22 May 2020
        getFirstIdOnPage();
        productFinderTasks.clickWork(ProductFinderTasks.searchResultId);
        tasks.waitUntilPageLoad();
    }



    @Given("^Filter the Search Result by filter \"([^\"]*)\" and value \"(.*)\" and click first id$")
    public void filter_Search_Result_and_clickFirstId(String FilterType, String filterValue) throws InterruptedException {
        //created by Nishant @ 22 Apr 2021
        productFinderTasks.filter_Search_Result_Randomly(FilterType, filterValue);

        getFirstIdOnPage();
        productFinderTasks.clickWork(ProductFinderTasks.searchResultId);
        Thread.sleep(1000);
    }

    @Then("^Search items are listed and click specific work (.*) from the result")
    public void search_items_are_listed_and_click_workId(String workId) throws InterruptedException {
        //created by Nishant @ 4 JUne 2020
        Log.info("verifying " + workId + " is available on search result page");
        boolean workSearched = productFinderTasks.searchOnResultPages(workId);
        Assert.assertTrue("searched key is on search result", workSearched);

        productFinderTasks.clickWork(workId);
        Log.info("clicked " + workId + " id from search result");
    }

    @Then("^Search items are listed and click the workid from result")
    public void search_items_are_listed_and_click_workId() throws InterruptedException {
        //created by Nishant @ 4 JUne 2020
        String workId = DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID();
        boolean workSearched = productFinderTasks.searchOnResultPages(workId);
        Assert.assertTrue("searched key is on search result", workSearched);
        productFinderTasks.clickWork(workId);
        Log.info("clicked " + workId + " id from search result");
    }

    @Then("^Verify the Work id Type is \"([^\"]*)\"$")
    public boolean verifyWorkType(String chooseWorkType) {
        //created by Nishant @ 22 May 2020
        String booksworkId = ProductFinderTasks.searchResultId;
        boolean isWorkTypeCorrect = verifyWorkTypeForWorkId(booksworkId, chooseWorkType);
        assertTrue("The given work Id is successfully filtered by the Work Type: " + chooseWorkType, isWorkTypeCorrect);
        return isWorkTypeCorrect;
    }


    @Then("^Verify the Work Status$")
    public void verify_the_Work_Status() throws InterruptedException {
        //updated by Nishant @ 22 May 2020
        //updated by Nishant @ 22 Apr 2021
        List<String> workStatusCode;
        String[] workStatusCodesofLaunched = {"WDA", "WLA", "WPU", "WSC"};
        String[] workStatusCodesofNoLongerPub = {"WDI", "WDV", "WTA"};
        String[] workStatusCodesofPlanned = {"WAM", "WAP", "WCO", "WIP", "WPL", "WSP"};
        String[] workStatusCodesofApproved = {"WAP"};
        boolean flag = false;
       // tasks.waitUntilPageLoad();
        workStatusUIValidation(randomWorkStatus);

        if (!TestContext.getValues().environment.equalsIgnoreCase("PROD") &&
                !TestContext.getValues().environment.equalsIgnoreCase("PRODUCTION")&&
                !TestContext.getValues().environment.equalsIgnoreCase("UAT2")) {

            sql = String.format(APIDataSQL.SELECT_GD_WORK_TYPE_STATUS_BY_WORKID, ProductFinderTasks.searchResultId);
            List<Map<?, ?>> workTypeStatusCode = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            workStatusCode = workTypeStatusCode.stream().map(m -> (String) m.get("WORK_STATUS")).map(String::valueOf).collect(Collectors.toList());

            switch (randomWorkStatus) {
                case "Approved":
                    if (Arrays.stream(workStatusCodesofApproved).anyMatch(workStatusCode::contains)) flag = true;
                    break;
                case "Launched":
                    if (Arrays.stream(workStatusCodesofLaunched).anyMatch(workStatusCode::contains)) flag = true;
                    break;
                case "Planned":
                    if (Arrays.stream(workStatusCodesofPlanned).anyMatch(workStatusCode::contains)) flag = true;
                    break;
                case "No Longer Published":
                case "Divested":
                case "Discontinued":
                    if (Arrays.stream(workStatusCodesofNoLongerPub).anyMatch(workStatusCode::contains)) flag = true;
                    break;
            }
            assertTrue("The given work Id is successfully filtered by the Work Status and verified by DB: " + randomWorkStatus, flag);
        }
    }


    @Then("^Verify the Product filter is \"([^\"]*)\"$")
    public void verify_the_Product_Status_is(String filterType) throws InterruptedException {
        //created by Nishant @ 21 April 2021 EPHD-3053
        productFinderTasks.verifyUserIsOnOverviewPage();
        productStatusUIValidation(filterType);
    }


private void workStatusUIValidation(String workStatus) {//created by Nishant @23 Oct 2020
    boolean flag = false;
    productFinderTasks.getUI_WorkOverview_Information();
    String overviewWorkStatus = productFinderTasks.prop_info.getProperty("Work Status");
    switch (workStatus) {
        case "Launched":if (overviewWorkStatus.contains("Launched") ||
                    overviewWorkStatus.contains("Published")) flag = true;break;
        case "Approved":    if (overviewWorkStatus.contains("Approved"))flag = true;break;
        case "Planned":if (overviewWorkStatus.contains("Planned"))flag = true;break;
        case "Discontinue Approved": if (overviewWorkStatus.contains("Discontinue Approved"))flag = true;break;
        case "Transfer Approved":if (overviewWorkStatus.contains("Transfer Approved"))flag = true;break;
        case "Divestment Approved":if (overviewWorkStatus.contains("Divestment Approved"))flag = true;break;
        case "Transferred":if (overviewWorkStatus.contains("Transferred"))flag = true;break;
        case "Withdrawn":
        case "Never Published":if (overviewWorkStatus.contains("Withdrawn"))flag = true;break;
        case "Divested":if (overviewWorkStatus.contains("Divested"))flag = true;break;
        case "Discontinued":if (overviewWorkStatus.contains("Discontinued"))flag = true;break;
        case "No Longer Published":
            if (overviewWorkStatus.contains("Discontinued")||
                    overviewWorkStatus.contains("Divested"))flag = true;break;
                    default:throw new IllegalArgumentException();
    }
    assertTrue("The given work Id is successfully filtered by the Work Status and verified in UI: " + workStatus, flag);
}

    private void productStatusUIValidation(String filterType) {//created by Nishant @21 Apr 2021
        boolean flag = false;
        productFinderTasks.getUI_WorkOverview_Information();

        //for Product status
        switch (DataQualityContext.prop_filters.getProperty(filterType)) {
            case "No Longer Published":
                if (productFinderTasks.prop_info.getProperty(filterType).contains("Discontinued")||
                        productFinderTasks.prop_info.getProperty(filterType).contains("Divested")||
                        productFinderTasks.prop_info.getProperty(filterType).contains("Stopped"))
                    flag = true;break;
            case "Never Published":
                if (productFinderTasks.prop_info.getProperty(filterType).contains("Withdrawn"))
                    flag = true;break;

            default:
                if(productFinderTasks.prop_info.getProperty(filterType).contains(DataQualityContext.prop_filters.getProperty(filterType)))
                    flag = true;break;
        }
        assertTrue("The search is successfully filtered and verified in UI: " + filterType, flag);
        Log.info("productStatusUIValidation complete");

    }

    private boolean verifyWorkTypeForWorkId(String workId, String chooseWorkType) {//by Nishant @ 02 Jun 2020
        Log.info("verifying work type for the work id...");
        boolean isWorkTypeCorrect = false;
        switch (chooseWorkType) {
            case "Book":
                for (String book_type : Book_Types) {
                    isWorkTypeCorrect = productFinderTasks.isPageContainingString(book_type);
                    if (isWorkTypeCorrect) {
                        Log.info("Work Id=> " + workId + " and work Type: " + book_type);
                        break;
                    }
                }
                break;

            case "Journal":
                for (String journal_type : Journal_Types) {
                    isWorkTypeCorrect = productFinderTasks.isPageContainingString(journal_type);
                    if (isWorkTypeCorrect) {
                        Log.info("Work Id=> " + workId + " and work Type: " + journal_type);
                        break;
                    }
                }
                break;

            case "Other":
                for (String other_type : Other_Types) {
                    isWorkTypeCorrect = productFinderTasks.isPageContainingString(other_type);
                    if (isWorkTypeCorrect) {
                        Log.info("Work Id=> " + workId + " and work Type: " + other_type);
                        break;
                    }
                }
                break;
        }
        return isWorkTypeCorrect;
    }


    //###########################################################################
    //product steps

    @Given("^get (\\d+) random product id from DB$")
    public void getRandomProductIds(String numberOfRecords) {
        //created by Nishant @ 19 May 2020
        sql = String.format(APIDataSQL.SELECT_GD_RANDOM_PRODUCT_ID, numberOfRecords);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product ids are : " + ids);
        //added by Nishant for debugging failures
        //  ids.clear(); ids.add("EPR-10H6KK"); Log.info("hard coded product ids are : " + ids);
        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }

    @And("^We get the product search data from DB$")
    public void getProductsDataFromEPHGD() {
        //created by Nishant @ 19 May 2020
        Log.info("get products data from EPH GD ...");
        sql = String.format(APIDataSQL.GET_GD_DATA_PRODUCT, Joiner.on("','").join(ids));
        DataQualityContext.productDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
        DataQualityContext.productDataObjectsFromEPHGD.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
        Assert.assertFalse("Verify That product objects list from DB is not empty.", DataQualityContext.productDataObjectsFromEPHGD.isEmpty());
    }

    @Then("^Search product by (.*)$")
    public void search_product_by_options(String option) throws InterruptedException {
        //created by Nishant @ 19 May 2020
        Log.info("searching product by " + option);
        switch (option) {
            case "Id":
                productFinderTasks.searchFor(dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID());
                break;
            case "Title":
                productFinderTasks.searchFor(dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
                break;
            case "Keyword":
                String[] keyword = dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME().replaceAll("[^a-zA-Z0-9]", " ").split(" ");
                productFinderTasks.searchFor(keyword[0] + " " + keyword[1]);
                break;
                default:throw new IllegalArgumentException(option);
        }

        productFinderTasks.clickProductAndPackages_tab();
        Log.info("title to be searched..." + DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
        boolean productSearched = productFinderTasks.searchOnResultPages(DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID());
        Assert.assertTrue("searched key is on search result", productSearched);
    }

    @When("^Search for given (.*) and switch to Products and Packages tab$")
    public void searchAndswitch_to_ProductsandPackages_tab(String searchKeyword) throws InterruptedException {
        //created by Nishant @ 23 Apr 2021
        Log.info("searching keyword..." + searchKeyword);
        productFinderTasks.searchFor(searchKeyword);
        productFinderTasks.clickProductAndPackages_tab();
    }

    @When("^switch to Products and Packages tab$")
    public void switch_to_ProductsandPackages_tab() throws InterruptedException {
        //created by Nishant @ 3 May 2021
        productFinderTasks.clickProductAndPackages_tab();
    }

    @When("^switch to Manifestations tab$")
    public void switch_to_Manifestations_tab() throws InterruptedException {
        //created by Nishant @ 3 May 2021
        productFinderTasks.clickManifestation_tab();
    }

    @Then("^The searched product is listed and clicked$")
    public void checkSearchResultsAndClickproduct() throws InterruptedException {
        //created by Nishant @ 19 May 2020
        Log.info("found entry ..." + DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
        String productIdLocator = String.format(ProductFinderConstants.buildProductIdLocator, DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID());
        tasks.click("XPATH", productIdLocator);
        Thread.sleep(5000);
    }

    @And("^User is forwarded to the searched product page from DB$")
    public void verifyUserIsForwardedToProductPageFromDB() throws InterruptedException {
        productFinderTasks.verifyUserIsOnOverviewPage();
    }


    //###########################################################################
    //manifestation steps

    @Given("^get (\\d+) random manifestation id$")
    public void getRandomManifestationIdFromDB(int arg0) {
        getRandomProductIds("1");
        getProductsDataFromEPHGD();
        Log.info("selected random manifestation id..." + dataQualityContext.productDataObjectsFromEPHGD.get(0).getF_PRODUCT_MANIFESTATION_TYP());
    }

    @And("^We get the manifestation data from DB$")
    public void getManifestationDetailByID() {
        Log.info("get manifestation data from EPH DB...");
        List<String> manIds = new ArrayList<>();
        manIds.add(dataQualityContext.productDataObjectsFromEPHGD.get(0).getF_PRODUCT_MANIFESTATION_TYP());

        Log.info("And get the manifestations in EPH GD ..");
        sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID, Joiner.on("','").join(manIds));
        dataQualityContext.manifestationDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    }

    @And("^Search manifestation by (.*)$")
    public void searchManifestationByOptions(String option) throws InterruptedException {
        Log.info("searching manifestation by " + option);
        switch (option) {
            case "Id":
                productFinderTasks.searchFor(dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_ID());
                break;
            case "Title":
                productFinderTasks.searchFor(dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE());
                break;
            case "Keyword":
                String[] keyword = dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE().replaceAll("[^a-zA-Z0-9]", " ").split(" ");
                productFinderTasks.searchFor(keyword[0] + " " + keyword[1]);
                break;
                default:throw new IllegalArgumentException(option);
        }

        productFinderTasks.clickManifestation_tab();
        Log.info("title to be searched..." + DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE());
        boolean manifestationSearched = productFinderTasks.searchOnResultPages(DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_ID());
        Assert.assertTrue("searched key is on search result", manifestationSearched);
    }

    @Then("^The searched manifestation is listed and clicked$")
    public void checkSearchResultsAndClickManifestation() throws InterruptedException {
        //created by Nishant @ 19 May 2020
        Log.info("found entry ..." + DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE());
        String linkManifestationIdLocator = String.format(ProductFinderConstants.linkManifestationIdLocator, DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_ID());
        tasks.click("XPATH", linkManifestationIdLocator);
        Thread.sleep(5000);
    }

    @And("^User is forwarded to the searched manifestation page from DB$")
    public void verifyUserIsForwardedToManifestationPageFromDB() {
        assertTrue(productFinderTasks.isUserOnManifestationPage(DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_ID()));
    }


    //###########################################################################
    //generic steps

    @Then("^user is searching for \"([^\"]*)\"$")
    public void search_for_string(String searchString) throws InterruptedException {
        Log.info("searching on page... ");
        productFinderTasks.searchFor(searchString);
        Thread.sleep(1000);
    }

    @Then("^No results message is displayed for \"([^\"]*)\"$")
    public void no_results_message_is_displayed_for(String searchText) throws InterruptedException {
        Assert.assertTrue("No Records found for the keyword " + searchText, tasks.verifyElementisDisplayed("XPATH", ProductFinderConstants.searchNoResults));
    }

    @Then("^capture search suggestion for \"([^\"]*)\" and validate with \"([^\"]*)\"$")
    public void searchSuggestion_is_displayed_for(String keyword, String ExpSuggestion) throws InterruptedException {
        //created by Nishant @ 12 Jan 2021 for EPHD-2660
        if (!keyword.equals(ExpSuggestion)) {
            String searchSuggestion = tasks.getTextofElement("XPATH", ProductFinderConstants.searchSuggesion);
            Log.info("Expected searchSuggestion=> Did you mean: \n" + ExpSuggestion + "\n actual searchSuggestion=>" + searchSuggestion);
            Assert.assertEquals("Did you mean: \n" + ExpSuggestion, searchSuggestion);
        } else {
            Log.info("search result of " + keyword + " showing " + tasks.getTextofElement("XPATH", ProductFinderConstants.SearchResultPageCount));
            Assert.assertTrue("search result not available for " + keyword, tasks.verifyElementisDisplayed("XPATH", ProductFinderConstants.SearchResultPageCount));
        }
    }


    @Given("^We get the id for work search (.*)$")
    public void getWorkDataFromEPHGD(String workID) {
        sql = String.format(ProductFinderSQL.SELECT_WORK_BY_ID_FOR_SEARCH, workID);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
    }

    @Then("^Searches for works by ID$")
    public void search_for_ID() throws InterruptedException {
        productFinderTasks.searchFor(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        Thread.sleep(2000);

        while (!productFinderTasks.isPageContainingString(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())) {
            productFinderTasks.goToNextPage();
            Thread.sleep(2000);
        }
    }

    @Then("^Searches for works by title$")
    public void search_for_Title() throws InterruptedException {
        productFinderTasks.searchFor(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        Thread.sleep(2000);

        while (!productFinderTasks.isPageContainingString(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE())) {
            productFinderTasks.goToNextPage();
            Thread.sleep(2000);
        }
    }

    @Then("^Search works by keyword \"([^\"]*)\"$")
    public void search_for_Keyword(String keyword) throws InterruptedException {
        Log.info("keyword is" + keyword);
        productFinderTasks.searchFor(keyword);

        while (!productFinderTasks.isPageContainingString(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE())) {
            productFinderTasks.goToNextPage();
            Thread.sleep(1000);
        }
    }


    @And("^Searches for Product by id$")
    public void searches_for_Product_by_id() throws InterruptedException {
        String productId;
        productId = productIdList.toString();
        productId = productId.replace("\\[", "").replace("]", "");
        productFinderTasks.searchFor(productId);
    }

    @Given("^We get the id and title for product search from DB$")
    public void getProductId() {
        List<String> productTitleList;
        List<Map<?, ?>> availableProduct;
        sql = ProductFinderSQL.SELECT_PRODUCTID_TITLE_FOR_SEARCH;

        availableProduct = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        if (!availableProduct.isEmpty()) {
            productIdList = availableProduct.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
            productTitleList = availableProduct.stream().map(m -> (String) m.get("PRODUCT_TITLE")).map(String::valueOf).collect(Collectors.toList());
            Log.info("Product ID => " + productIdList + "\n");
            Log.info("product Title =>" + productTitleList);
        }
    }

    private void getFirstIdOnPage() throws InterruptedException {//by Nishant @ 2 Jun 2020
        tasks.waitUntilPageLoad();
        List<WebElement> itemInfo = tasks.findmultipleElements("XPATH", ProductFinderConstants.itemDetail);
        ArrayList<String> idFound = new ArrayList<>();
        for (int i = 0; i < itemInfo.size(); i++) {

            List<WebElement> itemInfo_in = tasks.findmultipleElements("XPATH", ProductFinderConstants.itemDetail + "[" + (i + 1) + "]" + ProductFinderConstants.itemidentifier);
            for (WebElement webElement : itemInfo_in) {
                String text = webElement.getText();
                if (text.contains("ID")) {idFound.add(text);break;}
            }
        }
        Assert.assertTrue("zero id present on page ",!idFound.isEmpty());

        Log.info("first id on search result page is : " + idFound.get(0));
        ProductFinderTasks.searchResultId = idFound.get(0).split(" ")[2];
    }


    @And("get Extended Data from DB")
    public void getExtendedData() {
        Log.info("getting Extended work and Manifestation data from DB...");
        workApiObject.getJsonToObject_extendedWork(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());


    }

    @Then("^Verify overview tabs$")
    public void verifyOverviewTabs() throws InterruptedException {
        productFinderTasks.verifyLeftCoreTab();
        productFinderTasks.verifyLeftPeopleTab();
        productFinderTasks.verifyLeftFinancialTab();
        if(uiUnderTest.equalsIgnoreCase("JF")) {
            productFinderTasks.verifyLeftEditorialTab();
            productFinderTasks.verifyLeftLinkTab();
        }
    }

    @Then("^Verify PF/JF UI work overview values$")
    public void verifyPFJFUIWorkOverviewValues() throws ParseException, IOException {
        verifyWorkOverviewInformationUI();
        verifyWorkFinancialInformation();
        verifyPeople();
        if (uiUnderTest.equals("JF")) {
            verifyEditorialInfo();
            verifyLinksOnLinkTab(); //defect EPHD-2254
        }
    }

    @Then("^search work and verify links")
    public void verifyPFJFUIWorkOverviewLinks() throws InterruptedException, IOException {//Created by Nishant @ 04 Aug 2020
        userOpensJFHomePage();
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0)
                productFinderTasks.clickLogo();
            String workId = ids.get(i);
            productFinderTasks.searchFor(workId);
            boolean workSearched = productFinderTasks.searchOnResultPages(workId);
            Assert.assertTrue("searched key is on search result", workSearched);
            productFinderTasks.clickWork(workId);
            Log.info("clicked " + workId + " id from search result");
            assertTrue(productFinderTasks.isUserOnWorkPage(workId));
            verifyLinksOnLinkTab();
        }
    }


    private void verifyWorkOverviewInformationUI() throws ParseException {//created by Nishant @ 4 Jun 2020
        Log.info("\nVerifying Work Overview - Core tab...");
        Log.info("...................................\n");
        productFinderTasks.getUI_WorkOverview_Information();
        validate_workOverview_info();
        validateIdentifiers();
        validateSubjectArea();

    }

    private void verifyWorkFinancialInformation() {//created by Nishant @ 17 Jun 2020
        Log.info("\nverifiying Work Overview - Financial tab");
        Log.info("...................................\n");
        productFinderTasks.getUI_WorkOverview_Financial();
        validateAccountableProduct();
        validateCompanyCodes();
    }

    private void verifyEditorialInfo() {
        //created by Nishant @ 07 Jul 2020 for jrbi data validation on JF UI
        Log.info("\nverifying Work Overview - Editorial tab");
        Log.info("...................................\n");

        if (DataQualityContext.workExtendedTestClass != null) {
            productFinderTasks.getUI_Editorial();

            Assert.assertEquals(productFinderTasks.prop_editorial1.getProperty("Editorial Submission Site"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSystem());
            printLog("UI:Editorial Submission Site with jrbi:primarySiteSystem");

            Assert.assertEquals(productFinderTasks.prop_editorial1.getProperty("Editorial Submission Site Acronym"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteAcronym());
            printLog("UI:Editorial Submission Site Acronym with jrbi: primarySiteAcronym");

            Assert.assertEquals(productFinderTasks.prop_editorial1.getProperty("Editorial Support Level"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSupportLevel());
            printLog("UI:Editorial Support Level with jrbi: primarySiteSupportLevel");

            String dbProductionSite = "";
            if (DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalProdSiteCode() != null)
                dbProductionSite = DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalProdSiteCode();
            Assert.assertEquals(productFinderTasks.prop_editorial1.getProperty("Production Site"), dbProductionSite);
            printLog("UI:Production Site with jrbi: JournalProdSiteCode");


            Assert.assertEquals(productFinderTasks.prop_editorial1.getProperty("Production Type"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getIssueProdTypeCode());
            printLog("UI:Production Type with jrbi: issueProdTypeCode");

            Assert.assertEquals(productFinderTasks.prop_editorial2.getProperty("First Volume"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumeFrom());
            printLog("UI:First Volume with jrbi: catalogueVolumeFrom");

            Assert.assertEquals(productFinderTasks.prop_editorial2.getProperty("Last Volume"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumeTo());
            printLog("UI:Last Volume with jrbi: catalogueVolumeTo");

            Assert.assertEquals(productFinderTasks.prop_editorial2.getProperty("Volumes in Catalogue"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumesQty());
            printLog("UI:Volumes in Catalogue with jrbi: catalogueVolumesQty");

            Assert.assertEquals(productFinderTasks.prop_editorial2.getProperty("Total issues"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueIssuesQty());
            printLog("UI:Total issues with jrbi: catalogueIssuesQty");

            Assert.assertEquals(productFinderTasks.prop_editorial2.getProperty("No Issues (Budget)"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getRfIssuesQty());
            printLog("UI:No Issues (Budget) with jrbi: rfIssuesQty");

            Assert.assertEquals(productFinderTasks.prop_editorial2.getProperty("First Volume/Issue (Budget)"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getRfFvi());
            printLog("UI:First Volume/Issue (Budget) with jrbi: rfFvi");

            Assert.assertEquals(productFinderTasks.prop_editorial2.getProperty("Last Volume/Issue (Budget)"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getRfLvi());
            printLog("UI:Last Volume/Issue (Budget) with jrbi: rfLvi");

            Assert.assertEquals(productFinderTasks.prop_editorial2.getProperty("Total pages (Budget)"),
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getRfTotalPagesQty());
            printLog("UI:Total pages (Budget) with jrbi: rfTotalPagesQty");

        }

        //get extended record for manifestation
        List<String> manifestationId = apiWorksSearchSteps.getManifestationIdsForWorkID(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        DataQualityContext.manifestationExtendedTestClass = null;
        for (String s : manifestationId) {
            try {
                workManifestationApiObject.getJsonToObject_extendedManifestation(s);
                break;
            } catch (Exception e) {
                Log.info(e.getMessage());
            }
        }

        if (DataQualityContext.manifestationExtendedTestClass != null) {
            //coming from manifestation Extended
            //EPR-W-11560F

            //getJournalProdSiteCode moved from manifeationExtended to workExtended level
            //  Assert.assertEquals(productFinderTasks.prop_editorial1.getProperty("Production Site"),DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getJournalProdSiteCode());      printLog("UI:Production Site with jrbi: journalProdSiteCode");

            //coming from manifestation Extended
            String DBWarReference = "";
            if (DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getWarReference() != null)
                DBWarReference = DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getWarReference();
            Assert.assertEquals(productFinderTasks.prop_editorial1.getProperty("Despatch Location"), DBWarReference.replaceAll("  "," "));
            printLog("UI:Despatch Location with jrbi: warReference");

            //coming from manifestation Extended
            Assert.assertEquals(productFinderTasks.prop_editorial1.getProperty("Trim Size"),
                    DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getJournalIssueTrimSize());
            printLog("UI:Trim Size with jrbi: journalIssueTrimSize");

        }


        //Assert.assertEquals(productFinderTasks.prop_editorial2.getProperty("Launch Year"), DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumesQty());
        //printLog("Launch Year");

    }

    private void verifyPeople() { //created by Nishant @ 10 Jul 2020 for jrbi data validation on JF UI
        Log.info("\nverifying Work Overview - People tab");
        Log.info("...................................\n");
        productFinderTasks.getUI_People();
        verifyPeopleInfo();

        if (uiUnderTest.equals("JF")) {
            verifyDuplicatePeopleRoles();
            verifyValidPersonRole();
        }
    }

    private void verifyLinksOnLinkTab() throws IOException {//created by Nishant @ 15 Jul 2020
        Log.info("\nverifying Work Overview - Links tab");
        Log.info("...................................\n");
        productFinderTasks.getUI_Links();

        String url;
        HttpURLConnection huc;
        int respCode;

        //List<WebElement> links = productFinderTasks.getLinks(); //capture links from entire page
        Collection<Object> links = productFinderTasks.prop_links.values(); //capture links only within the links tab
        System.out.println("number of links found on page :" + links.size());
        boolean brokenLink;
        Iterator<Object> it = links.iterator();
        int l = 0;
        while (it.hasNext()) {
            l++;

            url = it.next().toString();

            if (url == null || url.isEmpty()) {
                System.out.println(l + ": URL is not configured for anchor tag or it is empty");
                continue;
            }
            System.out.println(l + ": " + url);
            try {
                huc = (HttpURLConnection) (new URL(url).openConnection());
                //    String cookie = huc.getHeaderField( "Set-Cookie").split(";")[0];
                //    huc.setRequestProperty("Accept","*/*");
                huc.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0");
                //    huc.setRequestProperty("Cookie", cookie );
                huc.setRequestMethod("GET");
                huc.connect();
                respCode = huc.getResponseCode();
                String statusDescription = "";
                String comment = "";
                System.out.println("status code :" + respCode);
                switch (respCode) {
                    case 200:
                        statusDescription = "valid link";
                        break;
                    case 301:
                        System.out.println("moved permanently to " + huc.getHeaderField("Location"));
                        statusDescription = "moved permanently";
                        comment = huc.getHeaderField("Location");
                        break;
                    case 401:
                        System.out.println("401 Unauthorized");
                        statusDescription = "401 Unauthorized";
                        break;
                    case 403:
                        System.out.println("403 Forbidden - The server understood the request but is refusing to fulfill it");
                        statusDescription = "403 Forbidden";
                        break;
                    case 404:
                        System.out.println("page not found");
                        statusDescription = "Page not Found";
                        break;
                    default:	System.out.println("less frequent error code: "+respCode);statusDescription="less frequent error code: "+respCode;break;
                }

                if (respCode >= 400) {
                    brokenLink = true;//EPR-W-102TM0
                    Log.info("-------------------------------------->");
                    System.out.println("its a broken link");
                }
                //Verify.verify(!brokenLink,"found broken link \n"+url+"\n status code: "+respCode);

            } catch (MalformedURLException e) {
                Log.info(e.getMessage());
                Log.info("not a valid url format");
            } catch (SSLHandshakeException e) {
                Log.info(e.getMessage());
            } catch (IOException e) {
                Log.info(e.getMessage());
            } catch (ClassCastException e) {
                Log.info(e.getMessage());
                Log.info("unable to establish HttpURLConnection");
            }

        }

    }


    private void verifyDuplicatePeopleRoles() {//created by Nishant @ 10 Jul 2020
        Log.info("Test : verifying duplicate People Role...");
        Set<String> uniqueRole = new HashSet();
        List<String> duplicate = new ArrayList();

        for (int c = 0; c < productFinderTasks.list_people.size(); c++) {
            if (!uniqueRole.add(productFinderTasks.list_people.get(c).getProperty("Role")))
                duplicate.add(productFinderTasks.list_people.get(c).getProperty("Role"));
        }

        Assert.assertEquals(
                "Should be only one person per role" + "\n" +
                        "Total Roles: " + productFinderTasks.list_people.size() + "\n" +
                        "Unique Roles: " + uniqueRole.size() + "\n" +
                        "Duplicate Roles: " + duplicate.size() + "\n",
                productFinderTasks.list_people.size(), uniqueRole.size());

    }

    private void verifyPeopleInfo() {
        //created by Nishant @ 14 Jul 2020
        compareCorePersonWithDB();
        //commented by Nishant @24 Feb 2021, need clarification from mark why Extended work person not reflecting in PF
        if (DataQualityContext.workExtendedTestClass != null) {
            compareExtendedPersonWithDB();
        }

    }

    private void verifyValidPersonRole() {//created by Nishant @ 15 Oct 2020 as part of EPHD-2241
        Log.info("...................................................");
        Log.info("Test: verify if there is any role other than expeted");
        String[] validRoles = {
                "Advertising Production Manager",
                "Business Controller",
                "Journal Manager",
                "Local Supplier Manager",
                "Marketing Communications Manager",
                "Publisher",
                "Publishing Assistant ECP",
                "Publishing Content Specialist",
                "Publishing Director",
                "Publishing Support Manager",
                "Senior Vice President"
        };

        List<String> invalidRoles = new ArrayList<>();
        for (int c = 0; c < productFinderTasks.list_people.size(); c++) {
            if (!Arrays.asList(validRoles).contains(productFinderTasks.list_people.get(c).getProperty("Role")))
                invalidRoles.add(productFinderTasks.list_people.get(c).getProperty("Role"));
        }

        Assert.assertTrue("Person roles present other than expected\n" + invalidRoles.toString(), invalidRoles.isEmpty());

    }

    private List<Map<String, String>> getPersonsByWorkId(String workId) {//created by Nishant @ 14 Jul 2020
        sql = String.format(PersonWorkRoleDataSQL.getPersonsByWorkId, workId);
        return (List<Map<String, String>>) DBManager.getDBResultMap(sql, Constants.EPH_URL);
    }

    private void compareCorePersonWithDB() {//created by Nishant @ 15 Jul 2020
        Log.info("verifying workCorePerson...");
        List<Map<String, String>> workPerson = getPersonsByWorkId(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        List<Integer> ignore = new ArrayList<>();
        for (Map<String, String> person : workPerson) {
            if (dataQualityContext.personDataObjectsFromEPHGD != null) {
                dataQualityContext.personDataObjectsFromEPHGD.clear();
            }
            if (dataQualityContext.personWorkRoleDataObjectsFromEPHGD != null) {
                dataQualityContext.personWorkRoleDataObjectsFromEPHGD.clear();
            }

            String DB_workPersonRole = lov_personRole(person.get("f_role"));

          //  Log.info("verifying Core work person... " + DB_workPersonRole);

            for (int cnt = 0; cnt < productFinderTasks.list_people.size(); cnt++) {
                if (ignore.contains(cnt)) continue;
                Object temp = person.get("f_person");
                String personId = temp.toString();
                personsApiObject.getPersonDataFromEPHGD(personId);
                String DB_workPersonName = dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " +
                        dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME();
                if (productFinderTasks.list_people.get(cnt).getProperty("Role").contentEquals(DB_workPersonRole)
                        && productFinderTasks.list_people.get(cnt).getProperty("PersonName").trim().contentEquals(DB_workPersonName)) {
                    //Assert.assertEquals(DB_workPersonRole, productFinderTasks.list_people.get(cnt).getProperty("Role")); printLog("person role ");
                    //Assert.assertEquals((productFinderTasks.list_people.get(cnt).getProperty("PersonName")).trim(),DB_workPersonName);printLog("PersonName");
                    printLog(DB_workPersonRole+": "+DB_workPersonName);

                    if (!(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_EMAIL_ID() == null &&
                            productFinderTasks.list_people.get(cnt).getProperty("Email").equalsIgnoreCase(""))) {
                        assertTrue(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_EMAIL_ID().equalsIgnoreCase(productFinderTasks.list_people.get(cnt).getProperty("Email")));
                        printLog("email");
                    }

                    ignore.add(cnt);
                    break;
                }
            }
        }
    }

    private void compareExtendedPersonWithDB() {//created by Nishant @ 14 Jul 2020
        if (DataQualityContext.workExtendedTestClass.getWorkExtended().getWorkExtendedPersons() != null) {
            WorkExtendedPersons[] workExtendedPersons = DataQualityContext.workExtendedTestClass.getWorkExtended().getWorkExtendedPersons().clone();

            for (WorkExtendedPersons workExtendedPerson : workExtendedPersons) {
                String extRoleName = workExtendedPerson.getExtendedRole().get("name").toString();
               String  extPersonFullName = workExtendedPerson.getExtendedPerson().get("firstName") + " " +
                        workExtendedPerson.getExtendedPerson().get("lastName");

                extPersonFullName = extPersonFullName.replace("null","");
                String extEmailId = "";
                if (workExtendedPerson.getExtendedPerson().get("email") != null)
                    extEmailId = workExtendedPerson.getExtendedPerson().get("email").toString();

                boolean extPersonFound = false;
                List<Integer> ignore = new ArrayList<>();
                for (int uiperson = 0; uiperson < productFinderTasks.list_people.size(); uiperson++) {
                    if (ignore.contains(uiperson)) continue;
                    if (productFinderTasks.list_people.get(uiperson).getProperty("Role").equalsIgnoreCase(extRoleName) &&
                            productFinderTasks.list_people.get(uiperson).getProperty("PersonName").trim().equalsIgnoreCase(extPersonFullName.trim())) {
                        Log.info("verifying work extended person..." + extRoleName + "-" + extPersonFullName);
                        extPersonFound = true;
                        printLog("person role");
                        printLog("PersonName");
                        assertEquals(productFinderTasks.list_people.get(uiperson).getProperty("Email"), extEmailId);
                        printLog("Email");
                        ignore.add(uiperson);
                        break;
                    }
                }
                assertTrue("Extended person not found " + extRoleName, extPersonFound);
            }
        }
    }

    private String lov_personRole(String roleCode) {//created by Nishant @ 15 Jul 2020
        String value_Role = "";
        switch (roleCode) {
            case "PAECP" : value_Role = "Publishing Assistant ECP" ; break;
            case "PCS"   : value_Role = "Publishing Content Specialist" ; break;
            case "PO"    : value_Role = "Product Owner" ; break;
            case "AU"    : value_Role = "Author" ; break;
            case "ED"    : value_Role = "Editor" ; break;
            case "PD"    : value_Role = "Publishing Director" ; break;
            case "PU"    : value_Role = "Publisher" ; break;
            case "AE"    : value_Role = "Acquisition Editor" ; break;
            case "BC"    : value_Role = "Business Controller" ; break;
            case "SVP"   : value_Role = "Senior Vice President" ; break;
            case "CO"    : value_Role = "Contributor" ; break;
            case "EC"    : value_Role = "Editor in Chief" ; break;
            case "SERE"  : value_Role = "Serial Editor" ; break;
            case "SESE"  : value_Role = "Series Editor" ; break;
            case "SESV"  : value_Role = "Series Volume Editor" ; break;
            case "SVE"   : value_Role = "Serial Volume Editor" ; break;
            case "PSM"   : value_Role = "Publishing Support Manager" ; break;
            case "MCM"   : value_Role = "Marketing Communications Manager" ; break;
            default: throw new IllegalArgumentException(roleCode);
        }
        return value_Role;
    }

    private void validateCompanyCodes() {//created by Nishant @ 17 Jun 2020

        getFinancialData(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());

        if (financialAttribs.financialDataFromGD != null && financialAttribs.financialDataFromGD.size() != 0) {
            sql = "select l_description FROM semarchy_eph_mdm.gd_x_lov_gl_company WHERE code='" + financialAttribs.financialDataFromGD.get(0).getGl_company() + "'";
            List<Map<String, String>> glCompnyName = DBManager.getDBResultMap(sql, Constants.EPH_URL);

            String DBvalue_GL_Company = financialAttribs.financialDataFromGD.get(0).getGl_company() + " - " + glCompnyName.get(0).get("l_description");
            //  Assert.assertEquals(productFinderTasks.prop_comCode.getProperty("GL Company"), DBvalue_GL_Company);
            Assert.assertEquals(productFinderTasks.prop_comCode.getProperty("Operating Company"), DBvalue_GL_Company);
            printLog("Operating Company");

            sql = "select l_description FROM semarchy_eph_mdm.gd_x_lov_gl_resp_centre WHERE code='" + financialAttribs.financialDataFromGD.get(0).getCost_resp_centre() + "'";
            List<Map<String, String>> costResCentre = DBManager.getDBResultMap(sql, Constants.EPH_URL);

            //EPR-W-102SDV, updated by Nishant @ 16 Oct 2020
            String DBvalue_costResCentre = financialAttribs.financialDataFromGD.get(0).getCost_resp_centre() + " - " + costResCentre.get(0).get("l_description").trim();
            Assert.assertEquals(productFinderTasks.prop_comCode.getProperty("Cost Responsibility Centre"), DBvalue_costResCentre);
            printLog("Cost Responsibility Centre");

            sql = "select l_description FROM semarchy_eph_mdm.gd_x_lov_gl_resp_centre WHERE code='" + financialAttribs.financialDataFromGD.get(0).getRevenue_resp_centre() + "'";
            List<Map<String, String>> revenueResCentre = DBManager.getDBResultMap(sql, Constants.EPH_URL);

            String DBvalue_revenueResCentre = financialAttribs.financialDataFromGD.get(0).getRevenue_resp_centre() + " - " + revenueResCentre.get(0).get("l_description").replace("  ", " ").trim();

            Assert.assertEquals(productFinderTasks.prop_comCode.getProperty("Revenue Responsibility Centre"), DBvalue_revenueResCentre);
            printLog("Revenue Responsibility Centre");
        }
    }

    private void getFinancialData(String workid) {
        String sql = String.format(APIDataSQL.GET_GD_DATA_FINN_ATTR, workid);
        financialAttribs.financialDataFromGD = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
    }

    private void printLog(String verified) {
        Log.info("verified..." + verified);
    }

    private void getAccountableProductFromEPHGD(String accountable_product_id) {
        String sql = String.format(APIDataSQL.GET_GD_DATA_ACCOUNTABLEPRODUCT_BY_ID, accountable_product_id);
        accountableProductDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);
    }

    private void validateAccountableProduct() {//created by Nishant @ 16 Jun 2020

        String DBValue_accProdSegment = "-";
        if (DataQualityContext.workDataObjectsFromEPHGD.get(0).getF_accountable_product() != null) {
            getAccountableProductFromEPHGD(DataQualityContext.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
            DBValue_accProdSegment = accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_CODE() + " - " + accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_NAME();

            sql = "select l_description from semarchy_eph_mdm.gd_x_lov_gl_prod_seg_parent where code='" + accountableProductDataObjectsFromEPHGD.get(0).getF_GL_PRODUCT_SEGMENT_PARENT() + "'";
            List<Map<String, String>> DB_accProdParent = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            String value_accProdParent = accountableProductDataObjectsFromEPHGD.get(0).getF_GL_PRODUCT_SEGMENT_PARENT() + " - " + DB_accProdParent.get(0).get("l_description");

            Assert.assertEquals((productFinderTasks.prop_AccProducts.getProperty("Accountable Product Parent")), value_accProdParent);
            Log.info("verified...Accountable Product Parent");

        }
        Assert.assertEquals(productFinderTasks.prop_AccProducts.getProperty("Accountable Product Segment"), DBValue_accProdSegment);
        Log.info("verified...Accountable Product Segment");
    }

    private void validateSubjectArea() {
        //created by Nishant @ 15 Jun 2020
        //need to be updated when there are CK specialities  EPR-W-102NR7
        Log.info("verifing......Subject Area");
        List<Map<String, Object>> subArea = getSubjectArea();
/*
    //below logic fails when there is no parent assigned to subject area and primary and secondary subject area become same.
        HashSet hs_uniqueSubParents = new HashSet();//finding unique parents
        for (Map<String, Object> stringObjectMap : subArea) {hs_uniqueSubParents.add(stringObjectMap.get("f_parent_subject_area"));}
        Assert.assertEquals(productFinderTasks.prop_subArea.size(), hs_uniqueSubParents.size());
        Log.info("verified...number of sub area");
*/
        for (Map<String, Object> stringObjectMap : subArea) {
            //get primary sub area name
            String ValuePrimarySubArea;
            if (stringObjectMap.get("f_parent_subject_area") == null)
                ValuePrimarySubArea = stringObjectMap.get("name").toString();
            else {
                sql = "select name from semarchy_eph_mdm.gd_subject_area where subject_area_id='" + stringObjectMap.get("f_parent_subject_area") + "'";
                List<Map<String, String>> PrimSubArea = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                ValuePrimarySubArea = PrimSubArea.get(0).get("name");
            }

            //get area type name
            sql = "select l_description from semarchy_eph_mdm.gd_x_lov_subject_area_type where code ='" + stringObjectMap.get("f_type") + "'";
            List<Map<String, Object>> subAreaType = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            boolean subAreaMatched = false;

            //specialties validation by Nishant @ 12 Oct 2021
            if(stringObjectMap.get("f_type").toString().equalsIgnoreCase("CK"))
            {
                if (productFinderTasks.prop_specialties.getProperty(ValuePrimarySubArea).contains(stringObjectMap.get("name").toString())) {
                    subAreaMatched = true;
                }
                assertTrue(subAreaMatched);
                Log.info("verified specialties..." + stringObjectMap.get("name").toString());
            }
           else
               { //get secondary sub area
            String secondaryArea = subAreaType.get(0).get("l_description").toString() + " / " + stringObjectMap.get("name");

            if (productFinderTasks.prop_subArea.getProperty(ValuePrimarySubArea).contains(secondaryArea)) {
                subAreaMatched = true;
            }
            assertTrue(subAreaMatched);
            Log.info("verified..." + secondaryArea);
            }
        }
    }



    private List<Map<String, Object>> getSubjectArea() {//created by Nishant @ 16 Jun 2020
        sql = String.format(ProductFinderSQL.SELECT_SUBJECT_AREA, DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        return (List<Map<String, Object>>) DBManager.getDBResultMap(sql, Constants.EPH_URL);
    }

    private void validateIdentifiers() {//created by Nishant @ 15 Jun 2020
        Log.info("verifying......Identifiers");
        List<Map<String, Object>> identifierDetail = getIdentifier();

        Assert.assertEquals(productFinderTasks.prop_identifier.size(), identifierDetail.size());
        printLog("number of identifiers");

        for (int i = 0; i < productFinderTasks.prop_identifier.size(); i++) {
            sql = "select l_description from semarchy_eph_mdm.gd_x_lov_identifier_type where code='" + identifierDetail.get(i).get("f_type") + "'";
            List<Map<String, String>> identifierType = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            Assert.assertEquals(productFinderTasks.prop_identifier.getProperty(identifierType.get(0).get("l_description")), identifierDetail.get(i).get("identifier"));
            printLog(identifierType.get(0).get("l_description"));
        }
    }

    public List<Map<String, Object>> getIdentifier() {//created by Nishant @ 15 jun 2020
        sql = String.format(ProductFinderSQL.SELECT_IDENTIFIER_OF_WORK, DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        return (List<Map<String, Object>>) DBManager.getDBResultMap(sql, Constants.EPH_URL);
    }

    private void validate_workOverview_info() throws ParseException {
        //created by Nishant @08 Jun 2020
        //updated by Nishant @09 Jul 2020
        coreDataValidation();
        dataModelValidation();

        //Extended data, discovered during EPH-1952 testing on 22 Jun 2020
        //Business Unit:	STM Health && Medical Sciences         //EPR-W-108RXC
        if (DataQualityContext.workExtendedTestClass != null) {
            if (productFinderTasks.prop_info.getProperty("Business Unit") != null &&
                    !DataQualityContext.workExtendedTestClass.getWorkExtended().getPtsBusinessUnitDesc().equalsIgnoreCase("")) {
                Assert.assertEquals(productFinderTasks.prop_info.getProperty("Business Unit"),
                        DataQualityContext.workExtendedTestClass.getWorkExtended().getPtsBusinessUnitDesc());
                printLog("Business Unit with jrbi ptsBusinessUnitDesc");
            }
        }
    }

    private void coreDataValidation() {
        Log.info("Core data validation...");
        if (productFinderTasks.prop_info.containsKey("Sub Title") && !getValue_subTitle().equalsIgnoreCase("")) {
            Assert.assertEquals(productFinderTasks.prop_info.getProperty("Sub Title"), getValue_subTitle());
            Log.info("verified...Sub Title");
        }
        if (productFinderTasks.prop_info.containsKey("Work Type")) {
            String DBWorkType = getDBWorkType();
            Assert.assertEquals(productFinderTasks.prop_info.getProperty("Work Type"), DBWorkType);
            Log.info("verified...Work Type");
        }

        String DBWorkStatus = getDBWorkStatus();
        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Work Status"), DBWorkStatus);
        Log.info("verified...Work Status");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Imprint").toUpperCase(), getValue_ImprintFromEPHGD().toUpperCase());
        Log.info("verified...Imprint");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Language"), getValue_language());
        Log.info("verified...Language");

        String[] PMCDetail = getValue_PMC();
        Assert.assertEquals(productFinderTasks.prop_info.getProperty("PMG"), PMCDetail[1]);
        Log.info("verified...PMG");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("PMC").trim(), PMCDetail[0].trim());
        Log.info("verified...PMC");

    }

    private void dataModelValidation() throws ParseException {
        Log.info("Data Model changes validation...");

        if (productFinderTasks.prop_info.containsKey("Planned Launch Date")) {
            Assert.assertEquals(productFinderTasks.prop_info.getProperty("Planned Launch Date"), getFormat_PlannedLaunchDate());
            Log.info("verified...Planned Launch Date");
        }

        if (productFinderTasks.prop_info.containsKey("Legal Ownership")) {
            Assert.assertEquals(productFinderTasks.prop_info.getProperty("Legal Ownership"), getValue_LegalOwnership());
            Log.info("verified...Legal Ownership");
        }

        if (productFinderTasks.prop_info.containsKey("Owner")) {
            String[] OwnershipDetail = getValue_OwnershipDescription();
            Assert.assertEquals(productFinderTasks.prop_info.getProperty("Owner"), OwnershipDetail[1]);
            Log.info("verified...Owner");

            if (productFinderTasks.prop_info.containsKey("Ownership Description")) {
                Assert.assertEquals(productFinderTasks.prop_info.getProperty("Ownership Description"), OwnershipDetail[0]);
                Log.info("verified...Ownership Description");
            }
        }
        // commented untill defect EPH-1936 get fixed

        if (productFinderTasks.prop_info.containsKey("Access Model")) {
            ArrayList<String> accessModel = getValue_AccessModelFromEPHGD();
            Assert.assertTrue(accessModel.contains(productFinderTasks.prop_info.getProperty("Access Model")));
            Log.info("verified...Access Model");
        }

        if (productFinderTasks.prop_info.containsKey("Business Model")) {
            ArrayList<String> businessModel = getValue_BusinessModelFromEPHGD();
            boolean bModelFound = businessModel.contains(productFinderTasks.prop_info.getProperty("Business Model"));
            Assert.assertTrue(bModelFound);
            Log.info("verified...Business Model");
        }

        if (productFinderTasks.prop_info.containsKey("Copyright Year")) {
            String dbCopyrightYear = "";
            if (DataQualityContext.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR() != null) {
                dbCopyrightYear = DataQualityContext.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR();
            }
            Assert.assertEquals(productFinderTasks.prop_info.getProperty("Copyright Year"), dbCopyrightYear);
            Log.info("verified...Copyright Year");
        }

        if (productFinderTasks.prop_info.containsKey("Edition Number")) {
                String editionDB = DataQualityContext.workDataObjectsFromEPHGD.get(0).getEDITION_NUMBER();
                if(editionDB==null) editionDB="";

            Assert.assertEquals(productFinderTasks.prop_info.getProperty("Edition Number"), editionDB);
            Log.info("verified...Edition Number");
        }

        if (productFinderTasks.prop_info.containsKey("Volume")) {
            Assert.assertEquals(productFinderTasks.prop_info.getProperty("Volume"), getValue_volume());
            Log.info("verified...Volume");
        }

    }

    private String getValue_subTitle() {
        String valueSubTitle = "";
        if (DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE() != null)
            valueSubTitle = DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE();
        return valueSubTitle;
    }

    private String getDBWorkType() {
        String DBWorkType = "";
        switch (DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE()) {
            case "ABS": DBWorkType = "Abstracts Journal";break;
            case "BKS": DBWorkType = "Books Series";break;
            case "BST": DBWorkType = "Book Set";break;
            case "CKFLEX": DBWorkType = "Flex Package";break;
            case "CKFLEXAG": DBWorkType = "Flex AG Package";break;
            case "CKSPECPKG": DBWorkType = "Specialty Package";break;
            case "DMG": DBWorkType = "Drug Monograph";break;
            case "FSPKG": DBWorkType = "Full Set Package";break;
            case "JBB": DBWorkType = "B2B Journal";break;
            case "JNL": DBWorkType = "Journal";break;
            case "MPR": DBWorkType = "Medical Procedure";break;
            case "MRW": DBWorkType = "Major Ref work";break;
            case "NEB": DBWorkType = "Non-Elsevier Book";break;
            case "NEJ": DBWorkType = "Non-Elsevier Journal";break;
            case "NWL": DBWorkType = "Newsletter";break;
            case "OTH": DBWorkType = "Other Book";break;
            case "RBK": DBWorkType = "Reference Book";break;
            case "SER": DBWorkType = "Serial";break;
            case "TBK": DBWorkType = "Text Book";break;
            case "UNK": DBWorkType = "Unknown";break;
default: throw new IllegalArgumentException(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
        }
        return DBWorkType;
    }

    private String getDBWorkStatus() {
        //updated by Nishant @ 05 apr 2021
        sql = String.format("select l_description from semarchy_eph_mdm.gd_x_lov_work_status gxlws where code ='%s'", DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
        List<Map<String, Object>> workStatusDespription = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        return workStatusDespription.get(0).get("l_description").toString();
    }

    private String getFormat_PlannedLaunchDate() throws ParseException {//created by Nishant @ 11 Jun 2020
        String f1date = DataQualityContext.workDataObjectsFromEPHGD.get(0).getPLANNED_LAUNCH_DATE();
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse(f1date);
        return new SimpleDateFormat("d MMM yyyy").format(date);
    }

    private String getValue_LegalOwnership() {//created by Nishant @ 11 Jun 2020
        String legalOwnership = "";
        switch (DataQualityContext.workDataObjectsFromEPHGD.get(0).getLEGAL_OWNERSHIP()) {
            case "ELS":legalOwnership = "Elsevier";break;
            case "SOC":legalOwnership = "Society";break;
            case "COM":legalOwnership = "Company";break;
            case "UNI":legalOwnership = "University";break;
            case "JVE":legalOwnership = "Joint Venture";//legalOwnership = "Third Party";
                break;
                default:throw new IllegalArgumentException(DataQualityContext.workDataObjectsFromEPHGD.get(0).getLEGAL_OWNERSHIP());
        }
        return legalOwnership;
    }

    private String[] getValue_OwnershipDescription() {//created by Nishant @ 12 Jun 2020
        String[] OwnerShip = new String[2];
        sql = String.format(ProductFinderSQL.SELECT_OWNERSHIP_DESCRIPTION, DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        List<Map<String, Object>> OwnershipDescription = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        OwnerShip[0] = OwnershipDescription.get(0).get("l_description").toString();


        String legalOwner = OwnershipDescription.get(0).get("f_legal_owner").toString();

        sql = String.format(ProductFinderSQL.SELECT_LEGAL_OWNER, legalOwner);
        List<Map<String, Object>> LegalOwner = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        OwnerShip[1] = LegalOwner.get(0).get("name").toString();
        return OwnerShip;
    }

    private ArrayList<String> getValue_BusinessModelFromEPHGD() {//created by Nishant @ 9 Jun 2020
        sql = String.format(ProductFinderSQL.SELECT_BUSINESS_MODEL, DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        List<Map<String, Object>> BusinessModelDB = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ArrayList<String> businessModelValue = new ArrayList<>();

        for (Map<String, Object> stringObjectMap : BusinessModelDB) {
            switch (stringObjectMap.get("business_model").toString()) {
                case "SBD":
                    businessModelValue.add("Subsidized");
                    break;
                case "SBS":
                    businessModelValue.add("Subscription");
                    break;
                case "APC":
                    businessModelValue.add("Article Publishing Charge");
                    break;
                    default:throw new IllegalArgumentException(stringObjectMap.get("business_model").toString());
            }
        }
        return businessModelValue;
    }

    private ArrayList<String> getValue_AccessModelFromEPHGD() {//created by Nishant @ 9 Jun 2020
        sql = String.format(ProductFinderSQL.SELECT_ACCESS_MODEL, DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        List<Map<String, Object>> AccessModel = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ArrayList<String> accessModelValue = new ArrayList<>();
        for (Map<String, Object> stringObjectMap : AccessModel) {
            switch (stringObjectMap.get("access_model").toString()) {
                case "OP":
                    accessModelValue.add("Open");
                    break;
                case "FR":
                    accessModelValue.add("Free");
                    break;
                case "PD":
                    accessModelValue.add("Paid");
                    break;
                    default:throw new IllegalArgumentException(stringObjectMap.get("access_model").toString());
            }
        }
        return accessModelValue;
    }

    private String getValue_ImprintFromEPHGD() {//created by Nishant @ 11 Jun 2020
        String Value_imprint = "";
        if (DataQualityContext.workDataObjectsFromEPHGD.get(0).getIMPRINT() != null) {
            sql = String.format(ProductFinderSQL.SELECT_IMPRINT_INFO, DataQualityContext.workDataObjectsFromEPHGD.get(0).getIMPRINT());
            List<Map<String, String>> imprintInfo = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            Value_imprint = imprintInfo.get(0).get("l_description");
        }
        return Value_imprint;
    }

    private String getValue_volume() {//updated by Nishant @ 15 Oct 2020 for EPHD-2241
        String valueVolume = "";
        if (DataQualityContext.workDataObjectsFromEPHGD.get(0).getVOLUME() != null)
            //!DataQualityContext.workDataObjectsFromEPHGD.get(0).getVOLUME().equalsIgnoreCase("0"))
            valueVolume = DataQualityContext.workDataObjectsFromEPHGD.get(0).getVOLUME();

        return valueVolume;
    }

    private String getValue_language() {//created by Nishant @ 11 Jun 2020
        String value_language = "";
        if (DataQualityContext.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE() != null) {
            sql = String.format(ProductFinderSQL.SELECT_LANGUAGE_INFO, DataQualityContext.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE());
            List<Map<String, String>> languageInfo = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            value_language = languageInfo.get(0).get("l_description");
        }
        return value_language;
    }

    private String[] getValue_PMC() {//created by Nishant @15 Jun 2020
        String[] pmcInfo = new String[2];
        String pmcCode = DataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC();
        sql = String.format(ProductFinderSQL.SELECT_PMC_INFO, pmcCode);
        List<Map<String, Object>> pmcDetail = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        pmcInfo[0] = pmcCode + " - " + pmcDetail.get(0).get("l_description").toString();

        sql = String.format(ProductFinderSQL.SELECT_PMG_INFO, pmcDetail.get(0).get("f_pmg").toString());
        List<Map<String, Object>> pmgDetail = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        pmcInfo[1] = pmcDetail.get(0).get("f_pmg").toString() + " - " + pmgDetail.get(0).get("l_description").toString();

        return pmcInfo;
    }


    @And("^Searches journal work by person (.*)")
    public void searchesJournalByfullName(String personSearchOption) throws AzureOauthTokenFetchingException, InterruptedException {
        //created by Nishant @ 10 Jul 2020
        WorksMatchedApiObject returnedWorks;

        while (!tasks.isObjectpresent("XPATH", ProductFinderConstants.searchBar)) {
            tasks.driver.navigate().refresh();
            Thread.sleep(3000);
        }

        for (int i = 0; i < dataQualityContext.personDataObjectsFromEPHGD.size(); i++) {
            String queryValue;

            productFinderTasks.selectSearchType("Person");
            queryValue = dataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FIRST_NAME() +
                    " " + dataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FAMILY_NAME();

            returnedWorks = apiWorksSearchSteps.callAPI_workByOption(personSearchOption, queryValue + "&workType=ABS,JBB,JNL,NWL&workStatus=WLA");
            returnedWorks.verifyEnddatedPerson(queryValue);

            //   Log.info("searching keyword..." + queryValue);
            productFinderTasks.searchFor(queryValue);

            int totalProductFound = 0;
            if (!tasks.isObjectpresent("XPATH", ProductFinderConstants.zeroResultFound)) {
                String ProductFound = tasks.getTextofElement("XPATH", ProductFinderConstants.productFoundOf);
                String[] showingProducts = ProductFound.split(" ");
                totalProductFound = Integer.valueOf(showingProducts[showingProducts.length - 1]);
            }
            Assert.assertEquals("match UI count with API count", returnedWorks.getTotalMatchCount(), totalProductFound);

        }


    }

    @And("^Searches journal by pmc (.*)")
    public void searchesJournalByPmc(String journalSearchOption) throws AzureOauthTokenFetchingException, InterruptedException {
        //created by Nishant @ 14 Jul 2020
        WorksMatchedApiObject returnedWorks = null;

        while (!tasks.isObjectpresent("XPATH", ProductFinderConstants.searchBar)) {
            tasks.driver.navigate().refresh();
            Thread.sleep(3000);
        }

        for (int i = 0; i < dataQualityContext.workDataObjectsFromEPHGD.size(); i++) {
            String queryValue = "";
            switch (journalSearchOption) {
                case "pmcCode":
                    productFinderTasks.selectSearchType("PMC");
                    queryValue = dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC();
                    returnedWorks = apiWorksSearchSteps.callAPI_workByOption(journalSearchOption, queryValue + "&workType=ABS,JBB,JNL,NWL&workStatus=WLA,WDA,WTA,WVA");
                    break;
                case "pmgCode":
                    productFinderTasks.selectSearchType("PMG");
                    queryValue = apiWorksSearchSteps.getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
                    returnedWorks = apiWorksSearchSteps.callAPI_workByOption(journalSearchOption, queryValue + "&workType=ABS,JBB,JNL,NWL&workStatus=WLA,WDA,WTA,WVA");
                    break;
                    default:throw new IllegalArgumentException(journalSearchOption);
            }

            Log.info("searching journals by..." + journalSearchOption + " " + queryValue);
            productFinderTasks.searchFor(queryValue);

            int totalProductFound = 0;
            if (!tasks.isObjectpresent("XPATH", ProductFinderConstants.zeroResultFound)) {
                String ProductFound = tasks.getTextofElement("XPATH", ProductFinderConstants.productFoundOf);
                String[] showingProducts = ProductFound.split(" ");
                totalProductFound = Integer.valueOf(showingProducts[showingProducts.length - 1]);
            }
            assert returnedWorks != null;
            Assert.assertEquals(returnedWorks.getTotalMatchCount(), totalProductFound);
            Log.info(journalSearchOption + " matched for UI and API");
            Log.info("....................................");
        }


    }

    @And("^closer the browser$")
    public void closeBrowser() {
        tasks.closeBrowser();
    }






}
