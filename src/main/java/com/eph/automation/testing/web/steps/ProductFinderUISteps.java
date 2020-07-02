package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.contexts.FinancialAttribsContext;
import com.eph.automation.testing.models.dao.*;
import com.eph.automation.testing.models.ui.ProductFinderConstants;
import com.eph.automation.testing.models.ui.ProductFinderTasks;
import com.eph.automation.testing.models.ui.TasksNew;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.eph.automation.testing.services.db.sql.ProductFinderSQL;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
//import com.sun.java.util.jar.pack.Instruction;
import cucumber.api.PendingException;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
//import java.util.Date;
//import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;


public class ProductFinderUISteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;

    /**
     * Created by GVLAYKOV
     * update by Nishant @ 15 May 2020
     */

    private ProductFinderTasks productFinderTasks;
    private TasksNew tasks;

    private String sql;
    private String workIds;
    private static String productId;
    private String searchingID;
    private static List<String> ids;
    private static List<String> workTypeCode;
    private static List<String> productIdList;
    private static List<String> productTitleList;
    private static List<String> workIdList;
    private static List<String> booksworkIdList = new ArrayList<String>();
    private static List<Map<?, ?>> workTypesCodeAvailable;
    private static List<Map<?, ?>> availableProduct;

    private static String[] Book_Types = {"Books Series", "Major Ref Work", "Other Book", "Reference Book", "Serial", "Text Book"};
    private static String[] Journal_Types = {"Abstracts Journal", "B2B Journal", "Journal", "Newsletter"};
    private static String[] Other_Types = {"Drug Monograph", "Medical Procedure"};
    String allWorkTypesCode[] = {"BKS", "MRW", "OTH", "SER", "TBK", "RBK", "ABS", "JNL", "JBB", "NWL", "DMG", "MPR"};


    //   private static List<String> allWorkTypesCodeArrayToList;

    private static List<ManifestationIdentifierObject> manifestationIdentifiers;
    private static List<String> workStatusCode;
    private static List<String> manifestation_Ids;
    private static List<String> manifestationIdentifiers_Ids;
    private static List<String> workStatusCodesofLaunchedToList, workStatusCodesofPlannedToList, workStatusCodesofNoLongerPubList;
    private String finalworkTypeCode;
    private String finalworkStatusCode;
    private List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD;
    private FinancialAttribsContext financialAttribs;

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
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_IDS_FOR_SEARCH, numberOfRecords);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
        // ids.clear(); ids.add("EPR-W-104Y61");  Log.info("hard coded work ids are : " + ids);
        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }

    @And("^We get the work search data from the EPH GD$")
    public void getWorksDataFromEPHGD() {
        sql = String.format(ProductFinderSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        DataQualityContext.workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        DataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));
    }


    @Given("^user is on search page$")
    public void userOpensHomePage() throws InterruptedException, ParseException {
        //updated by Nishant @ 15 May 2020
        productFinderTasks.openHomePage();
        productFinderTasks.loginByScienceAccount(ProductFinderConstants.SCIENCE_ID);
        tasks.waitUntilPageLoad();
        Log.info("signed in with science id...");
    }

    @Then("^Search works by (.*)$")
    public void search_works_by_options(String option) throws Throwable {
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
                productFinderTasks.searchFor(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0]);
                break;
        }
        // TODO: add page counter
        //added by Nishant @ 18 May 2020
        Log.info("title to be searched..." + DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        boolean workSearched = productFinderTasks.searchOnResultPages(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        Assert.assertTrue("searched key is on search result", workSearched);
    }

    @Then("^The searched work is listed and clicked$")
    public void checkSearchResultsAndClickWork() throws Throwable {
        Log.info("found entry ..." + DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        productFinderTasks.clickWork(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());

    }

    @And("^User is forwarded to the searched works page from DB$")
    public void verifyUserIsForwardedToWorksPageFromDatabase() {
        assertTrue(productFinderTasks.isUserOnWorkPage(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID()));
    }

    //for specific workid
    @And("^Verify user is forwarded to the searched work page from Search Result$")
    public void verifyUserIsForwardedToSearchedWorkPageFromSearchResult() {
        assertTrue(productFinderTasks.isUserOnWorkPage(ProductFinderTasks.searchResultWorkId));
    }

    @And("^Verify user is forwarded to the searched work page of (.*)$")
    public void verifyUserIsForwardedToSearchedWorkPageOf(String workId) {
        assertTrue(productFinderTasks.isUserOnWorkPage(workId));
    }

    @Given("^Get the available Work Types from the DB \"([^\"]*)\"$")
    public void get_the_available_Work_Types_from_the_DB(String chooseWorkType) {
        //updated by Nishant @ 21 May 2020
        try {
            switch (chooseWorkType) {
                case "Book":
                    sql = String.format(ProductFinderSQL.SELECT_AVAILABLE_WORK_TYPES_FOR_BOOK);
                    break;
                case "Journal":
                    sql = String.format(ProductFinderSQL.SELECT_AVAILABLE_WORK_TYPES_FOR_JOURNAL);
                    break;
                case "Other":
                    sql = String.format(ProductFinderSQL.SELECT_AVAILABLE_WORK_TYPES_FOR_OTHER);
                    break;
            }
            workTypesCodeAvailable = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            if (workTypesCodeAvailable.size() > 0) {
                workTypeCode = workTypesCodeAvailable.stream().map(m -> (String) m.get("WORK_TYPE")).map(String::valueOf).collect(Collectors.toList());
                Log.info("\nAvailable Work Types for " + chooseWorkType + " in DB: " + workTypeCode);
            } else {
                Log.info("Records for the work Type => " + chooseWorkType + " not available in DB.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Then("^Get a Work Id for each Work Types available in the DB$")
    public void get_Work_Id_for_each_Work_Types_available_in_DB() {
        //updated by Nishant @ 21 May 2020
        try {
            if (workTypesCodeAvailable.size() > 0) {
                for (String workTypeCodes : workTypeCode) {
                    sql = String.format(ProductFinderSQL.SELECT_WORKID_FOR_WORK_TYPE, workTypeCodes);
                    List<Map<?, ?>> workIdsAvailableforWorkTypes = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                    workIdList = workIdsAvailableforWorkTypes.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
                    booksworkIdList.add(workIdList.get(0));
                }
                Log.info("\nWork Ids Used: " + booksworkIdList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Given("^Search for the Work by Work Ids Filter workType and verify the work Type is \"([^\"]*)\"$")
    public void verify_the_result(String chooseWorkType) throws InterruptedException {
        try {
            if (workTypesCodeAvailable.size() > 0) {
                for (String booksworkId : booksworkIdList) {
                    productFinderTasks.searchFor(booksworkId);
                    filter_Search_Result_by_workType(chooseWorkType);

                    productFinderTasks.searchOnResultPages(booksworkId);
                    assertTrue(tasks.verifyElementTextisDisplayed(booksworkId));

                    productFinderTasks.clickWork(booksworkId);
                    assertTrue(productFinderTasks.isUserOnWorkPage(booksworkId));

                    boolean isWorkTypeCorrect = verifyWorkTypeForWorkId(booksworkId, chooseWorkType);
                    assertTrue("Work Id " + booksworkId + " Successfully filtered by Work Type: " + chooseWorkType, isWorkTypeCorrect);

                    productFinderTasks.openHomePage();
                }
            }
            booksworkIdList.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Given("^Searches for works by given ([^\"]*)$")
    public void searches_for_works_by_given(String searchKeyword) throws InterruptedException {
        Log.info("searching keyword..." + searchKeyword);
        productFinderTasks.searchFor(searchKeyword);
    }

    @Given("^Filter the Search Result by \"([^\"]*)\"$")
    public void filter_Search_Result_by_workType(String workFilterType) throws InterruptedException {
        String buildWorkFilterLocator = "//span[contains(text(),\'" + workFilterType + "\')]";
        tasks.click("XPATH", buildWorkFilterLocator);
        Thread.sleep(1000);
    }

    @Then("^Search items are listed and click a work id from the result")
    public void search_items_are_listed_and_click_aWorkId() throws InterruptedException {
        //created by Nishant @ 22 May 2020
        getFirstIdOnPage();
        productFinderTasks.clickWork(ProductFinderTasks.searchResultWorkId);
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

    @Then("^Verify the Work id Type is \"([^\"]*)\"$")
    public boolean verifyWorkType(String chooseWorkType) {
        //created by Nishant @ 22 May 2020
        String booksworkId = ProductFinderTasks.searchResultWorkId;
        boolean isWorkTypeCorrect = verifyWorkTypeForWorkId(booksworkId, chooseWorkType);
        assertTrue("The given work Id is successfully filtered by the Work Type: " + chooseWorkType, isWorkTypeCorrect);
        return isWorkTypeCorrect;
    }


    @Then("^Verify the Work Status is \"([^\"]*)\"$")
    public void verify_the_Work_Status_is(String workStatus) throws Throwable {
        //updated by Nishant @ 22 May 2020
        String workStatusCodesofLaunched[] = {"WDA", "WLA", "WPU", "WSC"};
        String workStatusCodesofNoLongerPub[] = {"WDI", "WDV", "WTA"};
        String workStatusCodesofPlanned[] = {"WAM", "WAP", "WCO", "WIP", "WPL", "WSP"};
        boolean flag = false;
        sql = String.format(APIDataSQL.SELECT_GD_WWORK_TYPE_STATUS, ProductFinderTasks.searchResultWorkId);
        List<Map<?, ?>> workTypeStatusCode = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        workStatusCode = workTypeStatusCode.stream().map(m -> (String) m.get("WORK_STATUS")).map(String::valueOf).collect(Collectors.toList());

        if (workStatus.equalsIgnoreCase("Launched")) {
            workStatusCodesofLaunchedToList = Arrays.asList(workStatusCodesofLaunched);
            for (int i = 0; i < workStatusCodesofLaunchedToList.size(); i++) {
                if (workStatusCode.get(0).contains(workStatusCodesofLaunchedToList.get(i))) {
                    flag = true;
                    break;
                }
            }
        } else if (workStatus.equalsIgnoreCase("Planned")) {
            workStatusCodesofPlannedToList = Arrays.asList(workStatusCodesofPlanned);
            for (int i = 0; i < workStatusCodesofPlannedToList.size(); i++) {
                if (workStatusCode.get(0).contains(workStatusCodesofPlannedToList.get(i))) {
                    flag = true;
                    break;
                }
            }
        } else if (workStatus.equalsIgnoreCase("No Longer Published")) {
            workStatusCodesofNoLongerPubList = Arrays.asList(workStatusCodesofNoLongerPub);
            for (int i = 0; i < workStatusCodesofNoLongerPubList.size(); i++) {
                if (workStatusCode.get(0).contains(workStatusCodesofNoLongerPubList.get(i))) {
                    flag = true;
                    break;
                }
            }
        }
        assertTrue("The given work Id is successfully filtered by the Work Status: " + workStatus, flag);
    }

    public boolean verifyWorkTypeForWorkId(String workId, String chooseWorkType) {//by Nishant @ 02 Jun 2020
        boolean isWorkTypeCorrect = false;
        switch (chooseWorkType) {
            case "Book":
                for (int i = 0; i < Book_Types.length; i++) {
                    isWorkTypeCorrect = productFinderTasks.isPageContainingString(Book_Types[i]);
                    if (isWorkTypeCorrect) {
                        Log.info("Work Id=> " + workId + " and work Type: " + Book_Types[i]);
                        break;
                    }
                }
                break;

            case "Journal":
                for (int i = 0; i < Journal_Types.length; i++) {
                    isWorkTypeCorrect = productFinderTasks.isPageContainingString(Journal_Types[i]);
                    if (isWorkTypeCorrect) {
                        Log.info("Work Id=> " + workId + " and work Type: " + Journal_Types[i]);
                        break;
                    }
                }
                break;

            case "Other":
                for (int i = 0; i < Other_Types.length; i++) {
                    isWorkTypeCorrect = productFinderTasks.isPageContainingString(Other_Types[i]);
                    if (isWorkTypeCorrect) {
                        Log.info("Work Id=> " + workId + " and work Type: " + Other_Types[i]);
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
        sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_IDS_FOR_SEARCH, numberOfRecords);
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
        sql = String.format(APIDataSQL.EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        DataQualityContext.productDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
        DataQualityContext.productDataObjectsFromEPHGD.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
        Assert.assertFalse("Verify That product objects list from DB is not empty.", DataQualityContext.productDataObjectsFromEPHGD.isEmpty());
    }

    @Then("^Search product by (.*)$")
    public void search_product_by_options(String option) throws Throwable {
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
                String[] keyword = dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME().split(" ");
                productFinderTasks.searchFor(keyword[0] + " " + keyword[1]);
                break;
        }

        productFinderTasks.clickProductAndPackages_tab();
        Log.info("title to be searched..." + DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
        boolean productSearched = productFinderTasks.searchOnResultPages(DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID());
        Assert.assertTrue("searched key is on search result", productSearched);
    }

    @Then("^The searched product is listed and clicked$")
    public void checkSearchResultsAndClickproduct() throws Throwable {
        //created by Nishant @ 19 May 2020
        Log.info("found entry ..." + DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
        String productIdLocator = String.format(ProductFinderConstants.buildProductIdLocator, DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID());
        tasks.click("XPATH", productIdLocator);
        Thread.sleep(5000);
    }

    @And("^User is forwarded to the searched product page from DB$")
    public void verifyUserIsForwardedToProductPageFromDB() {
        assertTrue(productFinderTasks.isUserOnProductPage(DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID()));
    }


    //###########################################################################
    //manifestation steps

    @Given("^get (\\d+) random manifestation id$")
    public void getRandomManifestationIdFromDB(int arg0) throws Throwable {
        getRandomProductIds("1");
        getProductsDataFromEPHGD();
        Log.info("selected random manifestation id..." + dataQualityContext.productDataObjectsFromEPHGD.get(0).getF_PRODUCT_MANIFESTATION_TYP());
    }

    @And("^We get the manifestation data from DB$")
    public void getManifestationByID() {
        Log.info("get manifestation data from EPH DB...");
        List<String> manIds = new ArrayList<>();
        manIds.add(dataQualityContext.productDataObjectsFromEPHGD.get(0).getF_PRODUCT_MANIFESTATION_TYP());
        //manIds.clear();manIds.add();

        Log.info("And get the manifestations in EPH GD ..");
        sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID, Joiner.on("','").join(manIds));
        dataQualityContext.manifestationDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    }

    @And("^Search manifestation by (.*)$")
    public void searchManifestationByOptions(String option) throws Throwable {
        Log.info("searching manifestation by " + option);
        switch (option) {
            case "Id":
                productFinderTasks.searchFor(dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_ID());
                break;
            case "Title":
                productFinderTasks.searchFor(dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE());
                break;
            case "Keyword":
                String[] keyword = dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE().split(" ");
                productFinderTasks.searchFor(keyword[0] + " " + keyword[1]);
                break;
        }

        productFinderTasks.clickManifestation_tab();
        Log.info("title to be searched..." + DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE());
        boolean manifestationSearched = productFinderTasks.searchOnResultPages(DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_ID());
        Assert.assertTrue("searched key is on search result", manifestationSearched);
    }

    @Then("^The searched manifestation is listed and clicked$")
    public void checkSearchResultsAndClickManifestation() throws Throwable {
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
    public void search_for_string(String searchString) throws Throwable {
        productFinderTasks.searchFor(searchString);
        Thread.sleep(1000);
    }

    @Then("^No results message is displayed for \"([^\"]*)\"$")
    public void no_results_message_is_displayed_for(String searchText) throws Throwable {
        Assert.assertTrue("No Records found for the keyword " + searchText, tasks.verifyElementisDisplayed("XPATH", ProductFinderConstants.searchNoResults));
        //  Assert.assertTrue("No Records found for the keyword "+searchText,true);


    }


    @Given("^We get the id for work search (.*)$")
    public void getWorkDataFromEPHGD(String workID) {
        sql = String.format(ProductFinderSQL.SELECT_WORK_BY_ID_FOR_SEARCH, workID);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
    }

    @Then("^Searches for works by ID$")
    public void search_for_ID() throws Throwable {
        productFinderTasks.searchFor(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        Thread.sleep(2000);
        // TODO: add page counter
        while (!productFinderTasks.isPageContainingString(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())) {
            productFinderTasks.goToNextPage();
            Thread.sleep(2000);
        }
    }

    @Then("^Searches for works by title$")
    public void search_for_Title() throws Throwable {
        productFinderTasks.searchFor(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        Thread.sleep(2000);
        // TODO: add page counter
        while (!productFinderTasks.isPageContainingString(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE())) {
            productFinderTasks.goToNextPage();
            Thread.sleep(2000);
        }
    }

    @Then("^Search works by keyword \"([^\"]*)\"$")
    public void search_for_Keyword(String keyword) throws Throwable {
        Log.info("keyword is" + keyword);
        productFinderTasks.searchFor(keyword);
        // TODO: add page counter
        while (!productFinderTasks.isPageContainingString(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE())) {
            productFinderTasks.goToNextPage();
            Thread.sleep(1000);
        }
    }


    @And("^Searches for Product by id$")
    public void searches_for_Product_by_id() throws Throwable {
        productId = productIdList.toString();
        productId = productId.replaceAll("\\[", "").replaceAll("\\]", "");
        productFinderTasks.searchFor(productId);
    }

    @Given("^We get the id and title for product search from DB$")
    public void getProductId() throws Throwable {
        sql = String.format(ProductFinderSQL.SELECT_PRODUCTID_TITLE_FOR_SEARCH);
        //  Log.info(sql);
        availableProduct = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        if (availableProduct.size() > 0) {
            productIdList = availableProduct.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
            productTitleList = availableProduct.stream().map(m -> (String) m.get("PRODUCT_TITLE")).map(String::valueOf).collect(Collectors.toList());
            Log.info("Product ID => " + productIdList + "\n");
            Log.info("product Title =>" + productTitleList);
        }
    }

    public void getFirstIdOnPage() {//by Nishant @ 2 Jun 2020
        List<WebElement> itemInfo = tasks.findmultipleElements("XPATH", ProductFinderConstants.itemDetail);
        ArrayList<String> idFound = new ArrayList<>();
        for (int i = 0; i < itemInfo.size(); i++) {
            List<WebElement> itemInfo_in = tasks.findmultipleElements("XPATH", ProductFinderConstants.itemDetail + "[" + (i + 1) + "]" + ProductFinderConstants.itemDetailInner);
            for (int i2 = 0; i2 < itemInfo_in.size(); i2++) {
                String text = itemInfo_in.get(i2).getText();
                if (text.contains("ID")) {
                    idFound.add(text);
                    break;
                }
            }
        }
        Log.info("first work id on search result page is : " + idFound.get(0));
        ProductFinderTasks.searchResultWorkId = idFound.get(0).split(" ")[2];
    }

    @And("We get the work search data from EPH GD for (.*)")
    public void getSpecificWorksDataFromEPHGD(String WorkId) {
        sql = String.format(ProductFinderSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, WorkId);
        DataQualityContext.workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        DataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));
    }

    @Then("^Verify work Overview Information for (.*)$")
    public void verifyWorkOverviewInformationUI(String workId) throws ParseException {//created by Nishant @ 4 Jun 2020

        productFinderTasks.getUI_WorkOverview_Information();
        validate_workOverview_info();
        validateIdentifiers();
        validateSubjectArea();
    }

    @And ("Verify work Finanfial records")
    public void verifyWorkFinancialInformation(){//created by Nishant @ 17 Jun 2020
        Log.info("verifiying......work Financial info");

        productFinderTasks.getUI_WorkOverview_Financial();
        validateAccountableProduct();
        validateCompanyCodes();

    }


    public void validateCompanyCodes() {//created by Nishant @ 17 Jun 2020

        getFinancialData(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());

        sql="select l_description FROM semarchy_eph_mdm.gd_x_lov_gl_company WHERE code='"+financialAttribs.financialDataFromGD.get(0).getGl_company()+"'";
        List<Map<String,String>> glCompnyName=DBManager.getDBResultMap(sql,Constants.EPH_URL);

        String DBvalue_GL_Company=financialAttribs.financialDataFromGD.get(0).getGl_company()+" - "+glCompnyName.get(0).get("l_description");
        Assert.assertEquals(productFinderTasks.prop_comCode.getProperty("GL Company"),DBvalue_GL_Company);
        printLog("GL Company");

        sql="select l_description FROM semarchy_eph_mdm.gd_x_lov_gl_resp_centre WHERE code='"+financialAttribs.financialDataFromGD.get(0).getCost_resp_centre()+"'";
        List<Map<String,String>> costResCentre=DBManager.getDBResultMap(sql,Constants.EPH_URL);

        String DBvalue_costResCentre=financialAttribs.financialDataFromGD.get(0).getCost_resp_centre()+" - "+costResCentre.get(0).get("l_description");
        Assert.assertEquals(productFinderTasks.prop_comCode.getProperty("Cost Responsibility Centre"),DBvalue_costResCentre);
        printLog("Cost Responsibility Centre");

        sql="select l_description FROM semarchy_eph_mdm.gd_x_lov_gl_resp_centre WHERE code='"+financialAttribs.financialDataFromGD.get(0).getRevenue_resp_centre()+"'";
        List<Map<String,String>> revenueResCentre=DBManager.getDBResultMap(sql,Constants.EPH_URL);

        String DBvalue_revenueResCentre=financialAttribs.financialDataFromGD.get(0).getRevenue_resp_centre()+" - "+revenueResCentre.get(0).get("l_description").replace("  "," ");;
        Assert.assertEquals(productFinderTasks.prop_comCode.getProperty("Revenue Responsibility Centre"),DBvalue_revenueResCentre);
        printLog("Revenue Responsibility Centre");

    }

    private void getFinancialData(String workid){
        String sql = String.format(APIDataSQL.GET_GD_FinnAttr_DATA, workid);
        financialAttribs.financialDataFromGD = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
    }
    private void printLog(String verified){Log.info("verified..."+verified);}

    private void getAccountableProductFromEPHGD(String accountable_product_id){
        String sql=String.format(APIDataSQL.SELECT_ACCOUNTABLE_PRODUCT_BY_ACCOUNTABLEID,accountable_product_id);
        accountableProductDataObjectsFromEPHGD=DBManager.getDBResultAsBeanList(sql, AccountableProductDataObject.class,Constants.EPH_URL);
    }

    public void validateAccountableProduct() {//created by Nishant @ 16 Jun 2020
        getAccountableProductFromEPHGD(DataQualityContext.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
        String DBValue_accProdSegment=accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_CODE()+" - "+accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_NAME();
        Assert.assertEquals(productFinderTasks.prop_AccProducts.getProperty("Accountable Product Segment"),DBValue_accProdSegment);
        Log.info("verified...Accountable Product Segment");

        sql="select l_description from semarchy_eph_mdm.gd_x_lov_gl_prod_seg_parent where code='"+accountableProductDataObjectsFromEPHGD.get(0).getF_GL_PRODUCT_SEGMENT_PARENT()+"'";
        List<Map<String,String>> DB_accProdParent=DBManager.getDBResultMap(sql,Constants.EPH_URL);
        String value_accProdParent=accountableProductDataObjectsFromEPHGD.get(0).getF_GL_PRODUCT_SEGMENT_PARENT()+" - "+DB_accProdParent.get(0).get("l_description");

        Assert.assertEquals((productFinderTasks.prop_AccProducts.getProperty("Accountable Product Parent")),value_accProdParent);
        Log.info("verified...Accountable Product Parent");
    }

    public void validateSubjectArea() {//created by Nishant @ 15 Jun 2020
        Log.info("verifing......Subject Area");
        List<Map<String, Object>> subArea = getSubjectArea();

        Assert.assertEquals(productFinderTasks.prop_subArea.size(), subArea.size());
        Log.info("verified...number of sub area");

        for (int i = 0; i < productFinderTasks.prop_subArea.size(); i++) {
            //get primary sub area name
            String ValuePrimarySubArea = "";
            if (subArea.get(i).get("f_parent_subject_area") == null)
                ValuePrimarySubArea = subArea.get(i).get("name").toString();
            else {
                sql = "select name from semarchy_eph_mdm.gd_subject_area where subject_area_id='" + subArea.get(i).get("f_parent_subject_area") + "'";
                List<Map<String, String>> PrimSubArea = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                ValuePrimarySubArea = PrimSubArea.get(0).get("name");
            }

            //get area type name
            sql = "select l_description from semarchy_eph_mdm.gd_x_lov_subject_area_type where code ='" + subArea.get(i).get("f_type") + "'";
            List<Map<String, Object>> subAreaType = DBManager.getDBResultMap(sql, Constants.EPH_URL);

            //get secondary sub area
            String secondaryArea = subAreaType.get(0).get("l_description").toString() + " / " + subArea.get(i).get("name");

            Assert.assertEquals(productFinderTasks.prop_subArea.getProperty(ValuePrimarySubArea), secondaryArea);
            Log.info("verified..." + ValuePrimarySubArea);
        }
    }

    public List<Map<String, Object>> getSubjectArea() {//created by Nishant @ 16 Jun 2020
        sql = String.format(ProductFinderSQL.SELECT_SUBJECT_AREA, DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        List<Map<String, Object>> subArea = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        return subArea;
    }

    public void validateIdentifiers() {//created by Nishant @ 15 Jun 2020
        Log.info("verifying......Identifiers");
        List<Map<String, Object>> identifierDetail = getIdentifier();

        Assert.assertEquals(productFinderTasks.prop_identifier.size(), identifierDetail.size());
        Log.info("verified... number of identifiers");

        for (int i = 0; i < productFinderTasks.prop_identifier.size(); i++) {
            sql = "select l_description from semarchy_eph_mdm.gd_x_lov_identifier_type where code='" + identifierDetail.get(i).get("f_type") + "'";
            List<Map<String, String>> identifierType = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            Assert.assertEquals(productFinderTasks.prop_identifier.getProperty(identifierType.get(0).get("l_description")), identifierDetail.get(i).get("identifier"));
            Log.info("verified..." + identifierType.get(0).get("l_description"));
        }
    }

    public List<Map<String, Object>> getIdentifier() {//created by Nishant @ 15 jun 2020
        sql = String.format(ProductFinderSQL.SELECT_IDENTIFIER_OF_WORK, DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        List<Map<String, Object>> identifierDetail = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        return identifierDetail;
    }


    public void validate_workOverview_info() throws ParseException {
        //created by Nishant @ 8 Jun 2020

        Log.info("verifying......work Overview Information tab");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Sub Title"), getValue_subTitle());
        Log.info("verified...Sub Title");

        String DBWorkType = getDBWorkType();
        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Work Type"), DBWorkType);
        Log.info("verified...Work Type");

        String DBWorkStatus = getDBWorkStatus();
        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Work Status"), DBWorkStatus);
        Log.info("verified...Work Status");

        //extended data
        //discovered during EPH-1952 testing on 22 Jun 2020
        //EPR-W-108RXC
        //Business Unit:	STM Health & Medical Sciences



        //data model changes
        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Planned Launch Date"), getFormat_PlannedLaunchDate());
        Log.info("verified...Planned Launch Date");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Legal Ownership"), getValue_LegalOwnership());
        Log.info("verified...Legal Ownership");

        String[] OwnershipDetail = getValue_OwnershipDescription();

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Owner"), OwnershipDetail[1]);
        Log.info("verified...Owner");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Ownership Description"), OwnershipDetail[0]);
        Log.info("verified...Ownership Description");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Business Model"), getValue_BusinessModelFromEPHGD());
        Log.info("verified...Business Model");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Access Model"), getValue_AccessModelFromEPHGD());
        Log.info("verified...Access Model");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Imprint"), getValue_ImprintFromEPHGD());
        Log.info("verified...Imprint");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Copyright Year"), DataQualityContext.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR());
        Log.info("verified...Copyright Year");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Edition Number"), DataQualityContext.workDataObjectsFromEPHGD.get(0).getEDITION_NUMBER());
        Log.info("verified...Edition Number");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Volume"), getValue_volume());
        Log.info("verified...Volume");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("Language"), getValue_language());
        Log.info("verified...Language");

        String[] PMCDetail = getValue_PMC();

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("PMC"), PMCDetail[0]);
        Log.info("verified...PMC");

        Assert.assertEquals(productFinderTasks.prop_info.getProperty("PMG"), PMCDetail[1]);
        Log.info("verified...PMG");
    }


    public String getValue_subTitle() {
        String valueSubTitle = "";
        if (DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE() != null)
            valueSubTitle = DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE();
        return valueSubTitle;
    }

    public String getDBWorkType() {
        String DBWorkType = "";
        switch (DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE()) {
            case "BKS":
                DBWorkType = "Books Series";
                break;
            case "MRW":
                DBWorkType = "Major Ref work";
                break;
            case "OTH":
                DBWorkType = "Other Book";
                break;
            case "RBK":
                DBWorkType = "Reference Book";
                break;
            case "SER":
                DBWorkType = "Serial";
                break;
            case "TBK":
                DBWorkType = "Text Book";
                break;
            case "ABS":
                DBWorkType = "Abstracts Journal";
                break;
            case "JBB":
                DBWorkType = "B2B Journal";
                break;
            case "JNL":
                DBWorkType = "Journal";
                break;
            case "NWL":
                DBWorkType = "Newsletter";
                break;
        }
        return DBWorkType;
    }

    public String getDBWorkStatus() {
        String DBWorkStatus = "";
        switch (DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_STATUS()) {
            case "WID":
                DBWorkStatus = "In Development";
                break;
            case "WDA":
                DBWorkStatus = "Discontinue Approved";
                break;
            case "WLA":
                DBWorkStatus = "Launched";
                break;
            case "WPU":
                DBWorkStatus = "Published";
                break;
            case "WSC":
                DBWorkStatus = "Submissions Closed";
                break;
            case "WWI":
                DBWorkStatus = "Withdrawn";
                break;
            case "WDI":
                DBWorkStatus = "Discontinued";
                break;
            case "WDV":
                DBWorkStatus = "Divested";
                break;
            case "WTA":
                DBWorkStatus = "Transferred";
                break;
            case "WAM":
                DBWorkStatus = "Announced to Market";
                break;
            case "WAP":
                DBWorkStatus = "Approved";
                break;
            case "WCO":
                DBWorkStatus = "Contracted";
                break;
            case "WIP":
                DBWorkStatus = "Idea";
                break;
            case "WPL":
                DBWorkStatus = "Planned";
                break;
            case "WSP":
                DBWorkStatus = "Submitted and Transmitted to Production";
                break;
            case "WPA":
                DBWorkStatus = "Proposal Agreed";
                break;
            case "WCD":
                DBWorkStatus = "Content Delivered";
                break;
            case "WWA":
                DBWorkStatus = "Withdrawn/Abandoned";
                break;
        }
        return DBWorkStatus;
    }

    public String getFormat_PlannedLaunchDate() throws ParseException {//created by Nishant @ 11 Jun 2020
        String f1date = DataQualityContext.workDataObjectsFromEPHGD.get(0).getPLANNED_LAUNCH_DATE();
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse(f1date);
        String formatedDate = new SimpleDateFormat("d MMM yyyy").format(date);
        return formatedDate;
    }

    public String getValue_LegalOwnership() {//created by Nishant @ 11 Jun 2020
        String legalOwnership = "";
        switch (DataQualityContext.workDataObjectsFromEPHGD.get(0).getLEGAL_OWNERSHIP()) {
            case "ELS":
                legalOwnership = "Elsevier";
                break;
            case "SOC":
                legalOwnership = "Society";
                break;
            case "COM":
                legalOwnership = "Company";
                break;
            case "UNI":
                legalOwnership = "University";
                break;
            case "JVE":
                legalOwnership = "Joint Venture";
                legalOwnership = "Third Party";
                break;
        }
        return legalOwnership;
    }

    public String[] getValue_OwnershipDescription() {//created by Nishant @ 12 Jun 2020
        String[] OwnerShip = new String[2];
        sql = String.format(ProductFinderSQL.SELECT_OWNERSHIP_DESCRIPTION, DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        List<Map<String, Object>> OwnershipDescription = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        OwnerShip[0] = OwnershipDescription.get(0).get("l_description").toString();
        ;

        String legalOwner = OwnershipDescription.get(0).get("f_legal_owner").toString();

        sql = String.format(ProductFinderSQL.SELECT_LEGAL_OWNER, legalOwner);
        List<Map<String, Object>> LegalOwner = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        OwnerShip[1] = LegalOwner.get(0).get("name").toString();
        return OwnerShip;
    }

    public String getValue_BusinessModelFromEPHGD() {//created by Nishant @ 9 Jun 2020
        sql = String.format(ProductFinderSQL.SELECT_BUSINESS_MODEL, DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        List<Map<String, Object>> BusinessModel = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String businessModel = "";
        switch (BusinessModel.get(0).get("business_model").toString()) {
            case "SBD":
                businessModel = "Subsidized";
                break;
            case "SBS":
                businessModel = "Subscription";
                break;
            case "APC":
                businessModel = "Article Publishing Charge";
                break;
        }
        return businessModel;
    }

    public String getValue_AccessModelFromEPHGD() {//created by Nishant @ 9 Jun 2020
        sql = String.format(ProductFinderSQL.SELECT_ACCESS_MODEL, DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        List<Map<String, Object>> AccessModel = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String accessModel = "";
        switch (AccessModel.get(0).get("access_model").toString()) {
            case "OP":
                accessModel = "Open";
                break;
            case "FR":
                accessModel = "Free";
                break;
            case "PD":
                accessModel = "Paid";
                break;
        }
        return accessModel;
    }

    public String getValue_ImprintFromEPHGD() {//created by Nishant @ 11 Jun 2020
        String Value_imprint = "";
        switch (DataQualityContext.workDataObjectsFromEPHGD.get(0).getIMPRINT()) {
            case "ABU":
                Value_imprint = "Ahmadu Bello University";
                break;
            case "SHUS     ":
                Value_imprint = "";
                break;
            case "CHANPUB  ":
                Value_imprint = "";
                break;
            case "HBNRC    ":
                Value_imprint = "";
                break;
            case "ANPEC    ":
                Value_imprint = "";
                break;
            case "VE       ":
                Value_imprint = "";
                break;
            case "MRU      ":
                Value_imprint = "";
                break;
            case "NEWNES   ":
                Value_imprint = "";
                break;
            case "ELSUP    ":
                Value_imprint = "";
                break;
            case "AU       ":
                Value_imprint = "";
                break;
            case "WILLAN   ":
                Value_imprint = "";
                break;
            case "CMU      ":
                Value_imprint = "";
                break;
            case "MORKAUF  ":
                Value_imprint = "";
                break;
            case "ESA      ":
                Value_imprint = "";
                break;
            case "ASU      ":
                Value_imprint = "";
                break;
            case "CABS     ":
                Value_imprint = "";
                break;
            case "CSJK     ":
                Value_imprint = "";
                break;
            case "BAS      ":
                Value_imprint = "";
                break;
            case "TU       ":
                Value_imprint = "";
                break;
            case "BAFS     ":
                Value_imprint = "";
                break;
            case "KA       ":
                Value_imprint = "";
                break;
            case "CHLIV    ":
                Value_imprint = "";
                break;
            case "UJ       ":
                Value_imprint = "";
                break;
            case "ELSTAIW  ":
                Value_imprint = "";
                break;
            case "CELL     ":
                Value_imprint = "";
                break;
            case "KICIS    ":
                Value_imprint = "";
                break;
            case "HEPLC    ":
                Value_imprint = "";
                break;
            case "ERI      ":
                Value_imprint = "";
                break;
            case "ELSSING  ":
                Value_imprint = "";
                break;
            case "IHRS     ":
                Value_imprint = "";
                break;
            case "PARADPR  ":
                Value_imprint = "";
                break;
            case "SHBTRI   ":
                Value_imprint = "";
                break;
            case "CUP      ":
                Value_imprint = "";
                break;
            case "GULF     ":
                Value_imprint = "";
                break;
            case "CHLIVAU  ":
                Value_imprint = "";
                break;
            case "KFSHRC   ":
                Value_imprint = "";
                break;
            case "CMP      ":
                Value_imprint = "";
                break;
            case "MOSBJE   ":
                Value_imprint = "";
                break;
            case "CSM      ":
                Value_imprint = "";
                break;
            case "TKSO     ":
                Value_imprint = "";
                break;
            case "ELSPOL   ":
                Value_imprint = "";
                break;
            case "CBAFU    ":
                Value_imprint = "";
                break;
            case "ELD      ":
                Value_imprint = "";
                break;
            case "SAUNDCA  ":
                Value_imprint = "";
                break;
            case "UCSV     ":
                Value_imprint = "";
                break;
            case "JEMS     ":
                Value_imprint = "";
                break;
            case "JGS      ":
                Value_imprint = "";
                break;
            case "MANSOU   ":
                Value_imprint = "";
                break;
            case "EMB      ":
                Value_imprint = "";
                break;
            case "BCHEMIN  ":
                Value_imprint = "";
                break;
            case "ABLEX    ":
                Value_imprint = "";
                break;
            case "ELSEDI   ":
                Value_imprint = "";
                break;
            case "SPPU     ":
                Value_imprint = "";
                break;
            case "MADESIMP ":
                Value_imprint = "";
                break;
            case "KARABU   ":
                Value_imprint = "";
                break;
            case "CBSK     ":
                Value_imprint = "";
                break;
            case "AN       ":
                Value_imprint = "";
                break;
            case "JPPS     ":
                Value_imprint = "";
                break;
            case "MPC      ":
                Value_imprint = "";
                break;
            case "MERCKMAN ":
                Value_imprint = "";
                break;
            case "ELSJPN   ":
                Value_imprint = "";
                break;
            case "FPCU     ":
                Value_imprint = "";
                break;
            case "ASPEN    ":
                Value_imprint = "";
                break;
            case "APC      ":
                Value_imprint = "";
                break;
            case "EGSZ     ":
                Value_imprint = "";
                break;
            case "BMIDW    ":
                Value_imprint = "";
                break;
            case "IMIC     ":
                Value_imprint = "";
                break;
            case "TCCS     ":
                Value_imprint = "";
                break;
            case "NIN      ":
                Value_imprint = "";
                break;
            case "ESTGAZ   ":
                Value_imprint = "";
                break;
            case "AAU      ":
                Value_imprint = "";
                break;
            case "BIOPUB   ":
                Value_imprint = "";
                break;
            case "BSP      ":
                Value_imprint = "";
                break;
            case "PERG     ":
                Value_imprint = "";
                break;
            case "DIGPRESS ":
                Value_imprint = "";
                break;
            case "LANCET   ":
                Value_imprint = "";
                break;
            case "CUG      ":
                Value_imprint = "";
                break;
            case "AZTI     ":
                Value_imprint = "";
                break;
            case "CAS      ":
                Value_imprint = "";
                break;
            case "ISTEPELS ":
                Value_imprint = "";
                break;
            case "BUCM     ":
                Value_imprint = "";
                break;
            case "NWRC     ":
                Value_imprint = "";
                break;
            case "CMRS     ":
                Value_imprint = "";
                break;
            case "NIFST    ":
                Value_imprint = "";
                break;
            case "NON      ":
                Value_imprint = "";
                break;
            case "JBADHS   ":
                Value_imprint = "";
                break;
            case "EGSC     ":
                Value_imprint = "";
                break;
            case "FU       ":
                Value_imprint = "";
                break;
            case "WIESE    ":
                Value_imprint = "";
                break;
            case "ELSMAS   ":
                Value_imprint = "";
                break;
            case "WB12     ":
                Value_imprint = "";
                break;
            case "QU       ":
                Value_imprint = "";
                break;
            case "IPB      ":
                Value_imprint = "";
                break;
            case "CL13     ":
                Value_imprint = "";
                break;
            case "PAUSA    ":
                Value_imprint = "";
                break;
            case "LENPET   ":
                Value_imprint = "";
                break;
            case "BSU      ":
                Value_imprint = "";
                break;
            case "EGSRSA   ":
                Value_imprint = "";
                break;
            case "CSSC     ":
                Value_imprint = "";
                break;
            case "SCA      ":
                Value_imprint = "";
                break;
            case "SYNGR    ":
                Value_imprint = "";
                break;
            case "NAR      ":
                Value_imprint = "";
                break;
            case "LEXNEX   ":
                Value_imprint = "";
                break;
            case "BALTINLTD":
                Value_imprint = "";
                break;
            case "PLAGH    ":
                Value_imprint = "";
                break;
            case "DHRIS    ":
                Value_imprint = "";
                break;
            case "CSAA     ":
                Value_imprint = "";
                break;
            case "XJU      ":
                Value_imprint = "";
                break;
            case "CFBNTU   ":
                Value_imprint = "";
                break;
            case "BALTIN   ":
                Value_imprint = "";
                break;
            case "FIEB     ":
                Value_imprint = "";
                break;
            case "BIAS     ":
                Value_imprint = "";
                break;
            case "EMS      ":
                Value_imprint = "";
                break;
            case "ELSSRL   ":
                Value_imprint = "";
                break;
            case "ELSADVT  ":
                Value_imprint = "";
                break;
            case "EPS      ":
                Value_imprint = "";
                break;
            case "NMS      ":
                Value_imprint = "";
                break;
            case "PUCV     ":
                Value_imprint = "";
                break;
            case "WPI      ":
                Value_imprint = "";
                break;
            case "FMA      ":
                Value_imprint = "";
                break;
            case "ESRNM    ":
                Value_imprint = "";
                break;
            case "IMM      ":
                Value_imprint = "";
                break;
            case "PDXMD    ":
                Value_imprint = "";
                break;
            case "JAI      ":
                Value_imprint = "";
                break;
            case "HANBEL   ":
                Value_imprint = "";
                break;
            case "BYHCMU   ":
                Value_imprint = "";
                break;
            case "AFREX    ":
                Value_imprint = "";
                break;
            case "EPA      ":
                Value_imprint = "";
                break;
            case "JEM      ":
                Value_imprint = "";
                break;
            case "ARNOLD   ":
                Value_imprint = "";
                break;
            case "KLFU     ":
                Value_imprint = "";
                break;
            case "SIE      ":
                Value_imprint = "";
                break;
            case "FADADHS  ":
                Value_imprint = "";
                break;
            case "NRIAG    ":
                Value_imprint = "";
                break;
            case "CURMED   ":
                Value_imprint = "";
                break;
            case "JPSALL   ":
                Value_imprint = "";
                break;
            case "ELSCI    ":
                Value_imprint = "";
                break;
            case "DONICA   ":
                Value_imprint = "";
                break;
            case "SHIN     ":
                Value_imprint = "";
                break;
            case "UKERB    ":
                Value_imprint = "";
                break;
            case "HYU      ":
                Value_imprint = "";
                break;
            case "BCDECK   ":
                Value_imprint = "";
                break;
            case "MOSBLTD  ":
                Value_imprint = "";
                break;
            case "ESENT    ":
                Value_imprint = "";
                break;
            case "MAS      ":
                Value_imprint = "";
                break;
            case "NH       ":
                Value_imprint = "";
                break;
            case "ABU      ":
                Value_imprint = "";
                break;
            case "APA      ":
                Value_imprint = "";
                break;
            case "NSMK     ":
                Value_imprint = "";
                break;
            case "ESF      ":
                Value_imprint = "";
                break;
            case "BHSEC    ":
                Value_imprint = "";
                break;
            case "CAUM     ":
                Value_imprint = "";
                break;
            case "COS      ":
                Value_imprint = "";
                break;
            case "ECCCP    ":
                Value_imprint = "";
                break;
            case "SP       ":
                Value_imprint = "";
                break;
            case "CMNCKU   ":
                Value_imprint = "";
                break;
            case "LAXTON   ":
                Value_imprint = "";
                break;
            case "SMPP     ":
                Value_imprint = "";
                break;
            case "ACACL    ":
                Value_imprint = "";
                break;
            case "QUALMED  ":
                Value_imprint = "";
                break;
            case "VNCNTZ   ":
                Value_imprint = "";
                break;
            case "AMIRSYS  ":
                Value_imprint = "";
                break;
            case "ELSEVIER ":
                Value_imprint = "";
                break;
            case "BH       ":
                Value_imprint = "";
                break;
            case "CU       ":
                Value_imprint = "";
                break;
            case "KLU      ":
                Value_imprint = "";
                break;
            case "LEGINT   ":
                Value_imprint = "";
                break;
            case "SYSU     ":
                Value_imprint = "";
                break;
            case "MOSBY    ":
                Value_imprint = "";
                break;
            case "Harcourt ":
                Value_imprint = "";
                break;
            case "CAMPUS   ":
                Value_imprint = "";
                break;
            case "GORD     ":
                Value_imprint = "";
                break;
            case "CO       ":
                Value_imprint = "";
                break;
            case "AFEM     ":
                Value_imprint = "";
                break;
            case "IIM      ":
                Value_imprint = "";
                break;
            case "CMI      ":
                Value_imprint = "";
                break;
            case "PASCHMP  ":
                Value_imprint = "";
                break;
            case "CHMTEC   ":
                Value_imprint = "";
                break;
            case "SMMU     ":
                Value_imprint = "";
                break;
            case "ASRT     ":
                Value_imprint = "";
                break;
            case "AUFM     ":
                Value_imprint = "";
                break;
            case "NCICU    ":
                Value_imprint = "";
                break;
            case "CNA      ":
                Value_imprint = "";
                break;
            case "NIOF     ":
                Value_imprint = "";
                break;
            case "FEFU     ":
                Value_imprint = "";
                break;
            case "HOHAI    ":
                Value_imprint = "";
                break;
            case "KSU      ":
                Value_imprint = "";
                break;
            case "FDTU     ":
                Value_imprint = "";
                break;
            case "URBFI    ":
                Value_imprint = "";
                break;
            case "MOSBCAN  ":
                Value_imprint = "";
                break;
            case "SCADE    ":
                Value_imprint = "";
                break;
            case "MDUNITZ  ":
                Value_imprint = "";
                break;
            case "IAT      ":
                Value_imprint = "";
                break;
            case "EI       ":
                Value_imprint = "";
                break;
            case "BHBCLA   ":
                Value_imprint = "";
                break;
            case "UF       ":
                Value_imprint = "";
                break;
            case "ELSIND   ":
                Value_imprint = "";
                break;
            case "AGI      ":
                Value_imprint = "";
                break;
            case "OIPRS    ":
                Value_imprint = "";
                break;
            case "FDC      ":
                Value_imprint = "";
                break;
            case "IMN      ":
                Value_imprint = "";
                break;
            case "TAIBAH   ":
                Value_imprint = "";
                break;
            case "FVMCU    ":
                Value_imprint = "";
                break;
            case "TRENDS   ":
                Value_imprint = "";
                break;
            case "GPC      ":
                Value_imprint = "";
                break;
            case "SPU      ":
                Value_imprint = "";
                break;
            case "SPEKTRUM ":
                Value_imprint = "";
                break;
            case "AOCS     ":
                Value_imprint = "";
                break;
            case "SAUNDAU  ":
                Value_imprint = "";
                break;
            case "FOCPRESS ":
                Value_imprint = "";
                break;
            case "WRIGHT   ":
                Value_imprint = "";
                break;
            case "PERGFL   ":
                Value_imprint = "";
                break;
            case "KAIMST   ":
                Value_imprint = "";
                break;
            case "EXCMED   ":
                Value_imprint = "";
                break;
            case "HSUK     ":
                Value_imprint = "";
                break;
            case "ARCHPRESS":
                Value_imprint = "";
                break;
            case "ABPB     ":
                Value_imprint = "";
                break;
            case "INTMED   ":
                Value_imprint = "";
                break;
            case "TPU      ":
                Value_imprint = "";
                break;
            case "HARINDIA ":
                Value_imprint = "";
                break;
            case "GWMEDPUB ":
                Value_imprint = "";
                break;
            case "SAUND    ":
                Value_imprint = "";
                break;
            case "GULENG   ":
                Value_imprint = "";
                break;
            case "CIMA     ":
                Value_imprint = "";
                break;
            case "JSRM     ":
                Value_imprint = "";
                break;
            case "SAUNDLTD ":
                Value_imprint = "";
                break;
            case "MOSBAU   ":
                Value_imprint = "";
                break;
            case "MEFS     ":
                Value_imprint = "";
                break;
            case "KASLI    ":
                Value_imprint = "";
                break;
            case "ELSESP   ":
                Value_imprint = "";
                break;
            case "UNIBAH   ":
                Value_imprint = "";
                break;
            case "ESCDT    ":
                Value_imprint = "";
                break;
            case "ELSSOKO  ":
                Value_imprint = "";
                break;
            case "ACADPR   ":
                Value_imprint = "";
                break;
            case "SJU      ":
                Value_imprint = "";
                break;
            case "HMU      ":
                Value_imprint = "";
                break;
            case "EPRI     ":
                Value_imprint = "";
                break;
            case "ESJDA    ":
                Value_imprint = "";
                break;
            case "HANS     ":
                Value_imprint = "";
                break;
            case "WOPUB    ":
                Value_imprint = "";
                break;

        }
        return Value_imprint;
    }

    public String getValue_volume() {
        String valueVolume = "";
        if (DataQualityContext.workDataObjectsFromEPHGD.get(0).getVOLUME() != null)
            valueVolume = DataQualityContext.workDataObjectsFromEPHGD.get(0).getVOLUME();
        return valueVolume;
    }

    public String getValue_language() {//created by Nishant @ 11 Jun 2020
        String value_language = "";
        switch (DataQualityContext.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE()) {
            case "EE":
                value_language = "Ewe";
                break;
            case "EL":
                value_language = "Greek Modern";
                break;
            case "EN":
                value_language = "English";
                break;
            case "Code":
                value_language = "Description";
                break;
            case "AA":
                value_language = "Afar";
                break;
            case "AB":
                value_language = "Abkhazian";
                break;
            case "AE":
                value_language = "Avestan";
                break;
            case "AF":
                value_language = "Afrikaans";
                break;
            case "AK":
                value_language = "Akan";
                break;
            case "AM":
                value_language = "Amharic";
                break;
            case "AN":
                value_language = "Aragonese";
                break;
            case "AR":
                value_language = "Arabic";
                break;
            case "AS":
                value_language = "Assamese";
                break;
            case "AV":
                value_language = "Avaric";
                break;
            case "AY":
                value_language = "Aymara";
                break;
            case "AZ":
                value_language = "Azerbaijani";
                break;
            case "BA":
                value_language = "Bashkir";
                break;
            case "BE":
                value_language = "Belarusian";
                break;
            case "BG":
                value_language = "Bulgarian";
                break;
            case "BH":
                value_language = "Bihari languages";
                break;
            case "BI":
                value_language = "Bislama";
                break;
            case "BM":
                value_language = "Bambara";
                break;
            case "BN":
                value_language = "Bengali";
                break;
            case "BO":
                value_language = "Tibetan";
                break;
            case "BR":
                value_language = "Breton";
                break;
            case "BS":
                value_language = "Bosnian";
                break;
            case "CA":
                value_language = "Catalan";
                break;
            case "CE":
                value_language = "Chechen";
                break;
            case "CH":
                value_language = "Chamorro";
                break;
            case "CO":
                value_language = "Corsican";
                break;
            case "CR":
                value_language = "Cree";
                break;
            case "CS":
                value_language = "Czech";
                break;
            case "CU":
                value_language = "Church Slavic";
                break;
            case "CV":
                value_language = "Chuvash";
                break;
            case "CY":
                value_language = "Welsh";
                break;
            case "DA":
                value_language = "Danish";
                break;
            case "DE":
                value_language = "German";
                break;
            case "DV":
                value_language = "Divehi";
                break;
            case "DZ":
                value_language = "Dzongkha";
                break;
            case "ES":
                value_language = "Spanish";
                break;
            case "ET":
                value_language = "Estonian";
                break;
            case "EO":
                value_language = "Esperanto";
                break;
            case "EU":
                value_language = "Basque";
                break;
            case "FA":
                value_language = "Persian";
                break;
            case "FF":
                value_language = "Fulah";
                break;
            case "FI":
                value_language = "Finnish";
                break;
            case "FJ":
                value_language = "Fijian";
                break;
            case "FO":
                value_language = "Faroese";
                break;
            case "FR":
                value_language = "French";
                break;
            case "FY":
                value_language = "Western Frisian";
                break;
            case "GA":
                value_language = "Irish";
                break;
            case "GD":
                value_language = "Gaelic";
                break;
            case "GL":
                value_language = "Galician";
                break;
            case "GN":
                value_language = "Guarani";
                break;
            case "GU":
                value_language = "Gujarati";
                break;
            case "GV":
                value_language = "Manx";
                break;
            case "HA":
                value_language = "Hausa";
                break;
            case "HE":
                value_language = "Hebrew";
                break;
            case "HI":
                value_language = "Hindi";
                break;
            case "HO":
                value_language = "Hiri Motu";
                break;
            case "HR":
                value_language = "Croatian";
                break;
            case "HT":
                value_language = "Haitian";
                break;
            case "HU":
                value_language = "Hungarian";
                break;
            case "HY":
                value_language = "Armenian";
                break;
            case "HZ":
                value_language = "Herero";
                break;
            case "IA":
                value_language = "Interlingua";
                break;
            case "ID":
                value_language = "Indonesian";
                break;
            case "IE":
                value_language = "Interlingue";
                break;
            case "IG":
                value_language = "Igbo";
                break;
            case "II":
                value_language = "Sichuan Yi";
                break;
            case "IK":
                value_language = "Inupiaq";
                break;
            case "IO":
                value_language = "Ido";
                break;
            case "IS":
                value_language = "Icelandic";
                break;
            case "IT":
                value_language = "Italian";
                break;
            case "IU":
                value_language = "Inuktitut";
                break;
            case "JA":
                value_language = "Japanese";
                break;
            case "JV":
                value_language = "Javanese";
                break;
            case "KA":
                value_language = "Georgian";
                break;
            case "KG":
                value_language = "Kongo";
                break;
            case "KI":
                value_language = "Kikuyu";
                break;
            case "KJ":
                value_language = "Kuanyama";
                break;
            case "KK":
                value_language = "Kazakh";
                break;
            case "KL":
                value_language = "Kalaallisut";
                break;
            case "KM":
                value_language = "Central Khmer";
                break;
            case "KN":
                value_language = "Kannada";
                break;
            case "KO":
                value_language = "Korean";
                break;
            case "KR":
                value_language = "Kanuri";
                break;
            case "KS":
                value_language = "Kashmiri";
                break;
            case "KU":
                value_language = "Kurdish";
                break;
            case "KV":
                value_language = "Komi";
                break;
            case "KW":
                value_language = "Cornish";
                break;
            case "KY":
                value_language = "Kirghiz";
                break;
            case "LA":
                value_language = "Latin";
                break;
            case "LB":
                value_language = "Luxembourgish";
                break;
            case "LG":
                value_language = "Ganda";
                break;
            case "LI":
                value_language = "Limburgan";
                break;
            case "LN":
                value_language = "Lingala";
                break;
            case "LO":
                value_language = "Lao";
                break;
            case "LT":
                value_language = "Lithuanian";
                break;
            case "LU":
                value_language = "Luba-Katanga";
                break;
            case "LV":
                value_language = "Latvian";
                break;
            case "MG":
                value_language = "Malagasy";
                break;
            case "MH":
                value_language = "Marshallese";
                break;
            case "MI":
                value_language = "Maori";
                break;
            case "MK":
                value_language = "Macedonian";
                break;
            case "ML":
                value_language = "Malayalam";
                break;
            case "MN":
                value_language = "Mongolian";
                break;
            case "MR":
                value_language = "Marathi";
                break;
            case "MS":
                value_language = "Malay";
                break;
            case "MT":
                value_language = "Maltese";
                break;
            case "MY":
                value_language = "Burmese";
                break;
            case "NA":
                value_language = "Nauru";
                break;
            case "NB":
                value_language = "Norwegian Bokmål";
                break;
            case "NG":
                value_language = "Ndonga";
                break;
            case "NL":
                value_language = "Dutch";
                break;
            case "ND":
                value_language = "North Ndebele";
                break;
            case "NE":
                value_language = "Nepali";
                break;
            case "NN":
                value_language = "Norwegian Nynorsk";
                break;
            case "NO":
                value_language = "Norwegian";
                break;
            case "NR":
                value_language = "Ndebele South";
                break;
            case "NV":
                value_language = "Navajo; Navaho";
                break;
            case "NY":
                value_language = "Nyanja";
                break;
            case "OC":
                value_language = "Occitan";
                break;
            case "OJ":
                value_language = "Ojibwa";
                break;
            case "OM":
                value_language = "Oromo";
                break;
            case "OR":
                value_language = "Oriya";
                break;
            case "OS":
                value_language = "Ossetian";
                break;
            case "PA":
                value_language = "Punjabi";
                break;
            case "PI":
                value_language = "Pali";
                break;
            case "PL":
                value_language = "Polish";
                break;
            case "PS":
                value_language = "Pushto";
                break;
            case "PT":
                value_language = "Portuguese";
                break;
            case "QU":
                value_language = "Quechua";
                break;
            case "RM":
                value_language = "Romansh";
                break;
            case "RN":
                value_language = "Rundi";
                break;
            case "RO":
                value_language = "Romanian";
                break;
            case "RU":
                value_language = "Russian";
                break;
            case "RW":
                value_language = "Kinyarwanda";
                break;
            case "SA":
                value_language = "Sanskrit";
                break;
            case "SC":
                value_language = "Sardinian";
                break;
            case "SD":
                value_language = "Sindhi";
                break;
            case "SE":
                value_language = "Northern Sami";
                break;
            case "SG":
                value_language = "Sango";
                break;
            case "SI":
                value_language = "Sinhalese";
                break;
            case "SK":
                value_language = "Slovak";
                break;
            case "SL":
                value_language = "Slovenian";
                break;
            case "SM":
                value_language = "Samoan";
                break;
            case "SN":
                value_language = "Shona";
                break;
            case "SO":
                value_language = "Somali";
                break;
            case "SQ":
                value_language = "Albanian";
                break;
            case "SR":
                value_language = "Serbian";
                break;
            case "SS":
                value_language = "Swati";
                break;
            case "ST":
                value_language = "Sotho Southern";
                break;
            case "SU":
                value_language = "Sundanese";
                break;
            case "SV":
                value_language = "Swedish";
                break;
            case "SW":
                value_language = "Swahili";
                break;
            case "TA":
                value_language = "Tamil";
                break;
            case "TE":
                value_language = "Telugu";
                break;
            case "TG":
                value_language = "Tajik";
                break;
            case "TH":
                value_language = "Thai";
                break;
            case "TI":
                value_language = "Tigrinya";
                break;
            case "TK":
                value_language = "Turkmen";
                break;
            case "TL":
                value_language = "Tagalog";
                break;
            case "TN":
                value_language = "Tswana";
                break;
            case "TO":
                value_language = "Tonga";
                break;
            case "TR":
                value_language = "Turkish";
                break;
            case "TS":
                value_language = "Tsonga";
                break;
            case "TT":
                value_language = "Tatar";
                break;
            case "TW":
                value_language = "Twi";
                break;
            case "TY":
                value_language = "Tahitian";
                break;
            case "UG":
                value_language = "Uighur";
                break;
            case "UK":
                value_language = "Ukrainian";
                break;
            case "UR":
                value_language = "Urdu";
                break;
            case "UZ":
                value_language = "Uzbek";
                break;
            case "VE":
                value_language = "Venda";
                break;
            case "VI":
                value_language = "Vietnamese";
                break;
            case "VO":
                value_language = "Volapük";
                break;
            case "WA":
                value_language = "Walloon";
                break;
            case "WO":
                value_language = "Wolof";
                break;
            case "XH":
                value_language = "Xhosa";
                break;
            case "YI":
                value_language = "Yiddish";
                break;
            case "YO":
                value_language = "Yoruba";
                break;
            case "ZA":
                value_language = "Zhuang";
                break;
            case "ZH":
                value_language = "Chinese";
                break;
            case "ZU":
                value_language = "Zulu";
                break;
            case "ZZ":
                value_language = "Multiple Languages (unspecified)";
                break;
        }
        return value_language;
    }

    public String[] getValue_PMC() {//created by Nishant @15 Jun 2020
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


}
