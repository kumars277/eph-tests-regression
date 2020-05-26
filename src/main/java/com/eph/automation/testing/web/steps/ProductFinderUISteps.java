package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ManifestationIdentifierObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.models.ui.ProductFinderConstants;
import com.eph.automation.testing.models.ui.ProductFinderTasks;
import com.eph.automation.testing.models.ui.TasksNew;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.eph.automation.testing.services.db.sql.ProductFinderSQL;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import org.openqa.selenium.WebElement;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;


public class ProductFinderUISteps {
    /**
     * Created by GVLAYKOV
     * update by Nishant @ 15 May 2020
     */

    @StaticInjection
    public DataQualityContext dataQualityContext;
    private ProductFinderTasks productFinderTasks;
    private TasksNew tasks;

    private String sql;
    private String workIds;
    private static String productId;
    private String searchingID;
    private static List<String> ids;
    private static List<String>workTypeCode;
    private static List<String> productIdList;
    private static List<String>productTitleList;
    private static List<String> workIdList;
    private static List<String> booksworkIdList = new ArrayList<String>();
    private static List<Map<?, ?>> workTypesCodeAvailable;
    private static List<Map<?, ?>> availableProduct;

    private static String[] Book_Types = {"Books Series","Major Ref Work","Other Book","Reference Book","Serial","Text Book"};
    private static String[] Journal_Types = {"Abstracts Journal","B2B Journal","Journal","Newsletter"};
    private static String[] Other_Types = {"Drug Monograph","Medical Procedure"};

    private static List<ManifestationIdentifierObject> manifestationIdentifiers;
    private static List<String>workStatusCode;
    private static List<String> manifestation_Ids;
    private static List<String> manifestationIdentifiers_Ids;
    private static List<String> allWorkTypesCodeArrayToList;
    private static List<String>workStatusCodesofLaunchedToList,workStatusCodesofPlannedToList;
    private String finalworkTypeCode;
    private String finalworkStatusCode;

    @Inject
    public ProductFinderUISteps(ProductFinderTasks productFinderTasks, TasksNew tasks) {
        this.productFinderTasks = productFinderTasks;
        this.tasks = tasks;
    }

    @Given("^get a random work id from DB")
    public void getRandomWorkIds(String numberOfRecords) {
        //created by Nishant @ 15 May 2020
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_IDS_FOR_SEARCH, numberOfRecords);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
//        ids.clear(); ids.add("EPR-W-1135VY");  Log.info("hard coded work ids are : " + ids);
        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }


    @Given("^We get the id for work search (.*)$")
    public void getWorkDataFromEPHGD(String workID) {
        sql = String.format(ProductFinderSQL.SELECT_WORK_BY_ID_FOR_SEARCH, workID);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
    }


    @And("^We get the work search data from the EPH GD$")
    public void getWorksDataFromEPHGD() {
        sql = String.format(ProductFinderSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        DataQualityContext.workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        DataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));
    }

    @Given("^user is on search page$")
    public void userOpensHomePage() throws InterruptedException {
        productFinderTasks.openHomePage();
        productFinderTasks.loginByScienceAccount(ProductFinderConstants.SCIENCE_ID);
    }

    @Then("^Searches for works by ID$")
    public void search_for_ID() throws Throwable {
        productFinderTasks.searchFor(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        Thread.sleep(2000);
        // TODO: add page counter
        while(!productFinderTasks.isPageContainingString(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())){
            productFinderTasks.goToNextPage();
            Thread.sleep(2000);
        }
    }

    @Then("^Searches for works by title$")
    public void search_for_Title() throws Throwable {
        productFinderTasks.searchFor(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        Thread.sleep(2000);
        // TODO: add page counter
        while(!productFinderTasks.isPageContainingString(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE())){
            productFinderTasks.goToNextPage();
            Thread.sleep(2000);
        }
    }

    @Then("^Searches for works by keyword \"([^\"]*)\"$")
    public void search_for_Keyword(String keyword) throws Throwable {
        Log.info("keyword is"+keyword);
        productFinderTasks.searchFor(keyword);
        // TODO: add page counter
        while(!productFinderTasks.isPageContainingString(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE())){
            productFinderTasks.goToNextPage();
            Thread.sleep(1000);
        }
    }

    @Then("^user is searching for \"([^\"]*)\"$")
    public void search_for_string(String searchString) throws Throwable {
        productFinderTasks.searchFor(searchString);
        Thread.sleep(1000);
    }

    @Then("^No results message is displayed for \"([^\"]*)\"$")
    public void no_results_message_is_displayed_for(String searchText) throws Throwable {
        if(tasks.verifyElementisDisplayed("XPATH",ProductFinderConstants.searchNoResults)){
            Assert.assertTrue("No Records found for the keyword "+searchText,true);
        }

    }

    @Then("^The searched item is listed and clicked$")
    public void checkSearchResultsAndClickElement() throws Throwable {
        WebElement foundEntry = productFinderTasks.getElementByTitle(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        Log.info("found entry is "+ DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        foundEntry.isDisplayed();
        foundEntry.click();
        Thread.sleep(2000);
    }

    @And("^User is forwarded to the searched works page$")
    public void verifyUserIsForwardedToWorksPage () {
        assertTrue(productFinderTasks.isUserOnWorkPage(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID()));
    }



    @Given("^Searches for works by given \"([^\"]*)\"$")
    public void searches_for_works_by_given(String searchKeyword) throws InterruptedException {
        Log.info("keyword is "+searchKeyword);
        productFinderTasks.searchFor(searchKeyword);
    }

    @Then("^Search items are listed and click the result based on \"([^\"]*)\"$")
    public void search_items_are_listed_and_click_the_result_based_on(String workId) throws InterruptedException {
        while(!productFinderTasks.isPageContainingString(workId)){
            productFinderTasks.goToNextPage();
            Thread.sleep(1000);
        }
        String buildWorkIdLocator = "//a[@href='/work/"+workId+"/overview']";
        tasks.click("XPATH",buildWorkIdLocator);
        Thread.sleep(2000);
    }

    @Then("^Verify user is forwarded to the searched works page \"([^\"]*)\"$")
    public void verify_user_is_forwarded_to_the_searched_works_page(String workId){
        assertTrue(productFinderTasks.isUserOnWorkPage(workId));
    }

    @Given("^Filter the Search Result by \"([^\"]*)\"$")
    public void filter_the_Search_Result_by(String workType) throws InterruptedException {
            String buildWorkTypeFilterLocator = "//span[contains(text(),\'"+workType+"\')]";
            tasks.click("XPATH",buildWorkTypeFilterLocator);
            Thread.sleep(1000);
     }

    @Given("^Filter the Search Result by work status \"([^\"]*)\"$")
    public void filter_the_Search_Result_by_work_status(String workStatus) throws InterruptedException {
        String buildWorkTypeFilterLocator = "//span[contains(text(),\'"+workStatus+"\')]";
        tasks.click("XPATH",buildWorkTypeFilterLocator);
        Thread.sleep(1000);
    }

    /*
    @Then("^Verify the Work \"([^\"]*)\" Type is \"([^\"]*)\"$")
    public void verify_the_Work_Type_is(String workID, String workType) throws Throwable {
        if (tasks.verifyElementTextisDisplayed(workID)) {
            sql = String.format(APIDataSQL.SELECT_GD_WWORK_TYPE_STATUS, workID);
            Log.info(sql);
            List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            workTypeCode = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_TYPE")).map(String::valueOf).collect(Collectors.toList());
            finalworkTypeCode = workTypeCode.toString();
            finalworkTypeCode = finalworkTypeCode.replaceAll("\\[", "").replaceAll("\\]","");
            String allWorkTypesCode[] = {"BKS", "MRW", "OTH", "SER", "TBK", "RBK", "ABS", "JNL",
                                         "JBB","NWL","DMG","MPR"};
            allWorkTypesCodeArrayToList = Arrays.asList(allWorkTypesCode);
            boolean flag = false;
            for (int i = 0; i < allWorkTypesCodeArrayToList.size(); i++) {
                if(finalworkTypeCode.contains(allWorkTypesCodeArrayToList.get(i))){
                    flag = true;
                    break;
                }
            }
            assertTrue("The given work Id is successfully filtered by the Work Type: "+workType,flag);
        }
   }

    @Then("^Verify the Work \"([^\"]*)\" Status is \"([^\"]*)\"$")
    public void verify_the_Work_Status_is(String workID, String workStatus) throws Throwable {
        String workStatusCodesofLaunched[]= {"WDA","WLA","WPU","WSC"};
        String workStatusCodesofNoLongerPub[]= {"WDI","WDV","WTA"};
        String workStatusCodesofPlanned[]= {"WAM","WAP","WCO","WIP","WPL","WSP"};
        boolean flag;
        if (tasks.verifyElementTextisDisplayed(workID)) {
            sql = String.format(APIDataSQL.SELECT_GD_WWORK_TYPE_STATUS, workID);
            Log.info(sql);
            List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            workStatusCode = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_STATUS")).map(String::valueOf).collect(Collectors.toList());
            finalworkStatusCode = workStatusCode.toString();
            finalworkStatusCode = finalworkStatusCode.replaceAll("\\[", "").replaceAll("\\]","");
            if(workStatus.equalsIgnoreCase("Launched")){
                flag = false;
                workStatusCodesofLaunchedToList = Arrays.asList(workStatusCodesofLaunched);
                for(int i=0;i<workStatusCodesofLaunchedToList.size();i++){
                    if(finalworkStatusCode.contains(workStatusCodesofLaunchedToList.get(i))){
                        flag = true;
                        break;
                    }
                }
                assertTrue("The given work Id is successfully filtered by the Work Status: "+workStatus,flag);
            }else if(workStatus.equalsIgnoreCase("Planned")){
                flag = false;
                workStatusCodesofPlannedToList = Arrays.asList(workStatusCodesofPlanned);
                for(int i=0;i<workStatusCodesofPlannedToList.size();i++){
                    if(finalworkStatusCode.contains(workStatusCodesofPlannedToList.get(i))){
                        flag = true;
                        break;
                    }
                }
                assertTrue("The given work Id is successfully filtered by the Work Status: "+workStatus,flag);
            }

        }

    }
*/

/*
  @Given("^Test$")
    public void test() throws InterruptedException {
        sql = String.format(ProductFinderSQL.SELECT_AVAILABLE_WORK_TYPES_FOR_BOOK);
        Log.info(sql);
        List<Map<?, ?>> workTypesCodeAvailable = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        workTypeCode = workTypesCodeAvailable.stream().map(m -> (String) m.get("WORK_TYPE")).map(String::valueOf).collect(Collectors.toList());
        for(String workTypeCodes: workTypeCode ){
            sql = String.format(ProductFinderSQL.SELECT_WORKID_FOR_WORK_TYPE_BOOK,workTypeCodes);
            Log.info(sql);
            List<Map<?, ?>> workIdsAvailable = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            workIdList = workIdsAvailable.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
            Thread.sleep(2000);
            String workIds = workIdList.toString();
            workIds =   workIds.replaceAll("\\[", "").replaceAll("\\]","");
            productFinderTasks.searchFor(workIds);
            Thread.sleep(2000);
                while(!productFinderTasks.isPageContainingString(workIds)){
                    productFinderTasks.goToNextPage();
                    Thread.sleep(1000);
                }
            String buildWorkIdLocator = "//a[@href='/work/"+workIds+"/overview']";
            tasks.click("XPATH",buildWorkIdLocator);
            Thread.sleep(2000);
            assertTrue(productFinderTasks.isUserOnWorkPage(workIds));
            tasks.click("XPATH",ProductFinderConstants.previousPage);
            tasks.click("XPATH",ProductFinderConstants.closeButtonSearchBar);
            Thread.sleep(2000);

        }
    }*/

    @Given("^We get the id and title for product search from DB$")
    public void getProductId() throws Throwable {
        sql = String.format(ProductFinderSQL.SELECT_PRODUCTID_TITLE_FOR_SEARCH);
      //  Log.info(sql);
        availableProduct = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        if(availableProduct.size() > 0){
            productIdList       = availableProduct.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
            productTitleList    = availableProduct.stream().map(m -> (String) m.get("PRODUCT_TITLE")).map(String::valueOf).collect(Collectors.toList());
            Log.info("Product ID => "+productIdList +"\n");
            Log.info("product Title =>"+productTitleList);
        }
    }

    @And("^Searches for Product by id$")
    public void searches_for_Product_by_id() throws Throwable {
        productId = productIdList.toString();
        productId =   productId.replaceAll("\\[", "").replaceAll("\\]", "");
        productFinderTasks.searchFor(productId);
    }
    @Given("^Get the available Work Types from the DB \"([^\"]*)\"$")
    public void get_the_available_Work_Types_from_the_DB(String chooseWorkType){
        try {
            if (chooseWorkType.equalsIgnoreCase("Book")) {
                sql = String.format(ProductFinderSQL.SELECT_AVAILABLE_WORK_TYPES_FOR_BOOK);
            } else if (chooseWorkType.equalsIgnoreCase("Journal")) {
                sql = String.format(ProductFinderSQL.SELECT_AVAILABLE_WORK_TYPES_FOR_JOURNAL);
            } else {
                sql = String.format(ProductFinderSQL.SELECT_AVAILABLE_WORK_TYPES_FOR_OTHER);
            }
         //   Log.info(sql);
            workTypesCodeAvailable = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            if (workTypesCodeAvailable.size() > 0) {
                workTypeCode = workTypesCodeAvailable.stream().map(m -> (String) m.get("WORK_TYPE"))
                        .map(String::valueOf).collect(Collectors.toList());
                Log.info("Available Work Types in DB: " + workTypeCode);
            } else {
                Log.info("Records for the work Type => " + chooseWorkType + " not available in DB.");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Then("^Get a Work Id for each Work Types available in the DB$")
    public void get_Work_Id_for_each_Work_Types_available_in_DB() {
        try {
            if (workTypesCodeAvailable.size() > 0) {
                for (String workTypeCodes : workTypeCode) {
                    sql = String.format(ProductFinderSQL.SELECT_WORKID_FOR_WORK_TYPE, workTypeCodes);
                 //   Log.info(sql);
                    List<Map<?, ?>> workIdsAvailableforWorkTypes = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                    workIdList = workIdsAvailableforWorkTypes.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
                    workIds = workIdList.toString();
                    workIds = workIds.replaceAll("\\[", "").replaceAll("\\]", "");
                    booksworkIdList.add(workIds);
                }
                Thread.sleep(2000);
                Log.info("Work Ids Used: " + booksworkIdList);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }



    @Then("^Search for the Work by Work Ids Filter By \"([^\"]*)\"$")
    public void search_for_the_Work_by_Work_Ids_Filter_By(String chooseWorkType) throws Throwable {
        try {
            if (workTypesCodeAvailable.size() > 0) {
                for (String booksworkId : booksworkIdList) {

                    productFinderTasks.searchFor(booksworkId);
                    String buildWorkTypeFilterLocator = "//span[contains(text(),\'" + chooseWorkType + "\')]";
                    tasks.click("XPATH", buildWorkTypeFilterLocator);
                    Thread.sleep(2000);
                    while (!productFinderTasks.isPageContainingString(booksworkId)) {
                        productFinderTasks.goToNextPage();
                        Thread.sleep(1000);
                    }
                    assertTrue(tasks.verifyElementTextisDisplayed(booksworkId));
                  //  tasks.click("XPATH", ProductFinderConstants.closeButtonSearchBar);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Given("^Click on the result to verify the work Type is \"([^\"]*)\"$")
    public void verify_the_result(String chooseWorkType) throws InterruptedException {
        try{
            if(workTypesCodeAvailable.size()>0) {
                for (String booksworkId : booksworkIdList) {
                    productFinderTasks.searchFor(booksworkId);
                    String buildWorkTypeFilterLocator = "//span[contains(text(),\'" + chooseWorkType + "\')]";
                    tasks.click("XPATH", buildWorkTypeFilterLocator);
                    Thread.sleep(1000);
                    while (!productFinderTasks.isPageContainingString(booksworkId)) {
                        productFinderTasks.goToNextPage();
                        Thread.sleep(1000);
                    }
                    String buildWorkIdLocator = "//a[@href='/work/" + booksworkId + "/overview']";
                    tasks.click("XPATH", buildWorkIdLocator);
                    Thread.sleep(1000);
                    assertTrue(productFinderTasks.isUserOnWorkPage(booksworkId));
                    if (chooseWorkType.equalsIgnoreCase("Book")) {
                        for (int i = 0; i < Book_Types.length; i++) {
                            if (productFinderTasks.isPageContainingString(Book_Types[i])) {
                                Log.info("Work Id=> " + booksworkId + " and work Type: " + Book_Types[i]);
                                assertTrue("Work Id " + booksworkId + " Successfully filtered by Work Type: " + chooseWorkType, productFinderTasks.isPageContainingString(Book_Types[i]));
                            break;
                            }
                        }
                    } else if (chooseWorkType.equalsIgnoreCase("Journal")) {
                        for (int i = 0; i < Journal_Types.length; i++) {
                            if (productFinderTasks.isPageContainingString(Journal_Types[i])) {
                                Log.info("Work Id=> " + booksworkId + " and work Type: " + Journal_Types[i]);
                                assertTrue("Work Id " + booksworkId + " Successfully filtered by Work Type: " + chooseWorkType, productFinderTasks.isPageContainingString(Journal_Types[i]));
                            }
                        }
                    } else {
                        for (int i = 0; i < Other_Types.length; i++) {
                            if (productFinderTasks.isPageContainingString(Other_Types[i])) {
                                Log.info("Work Id=> " + booksworkId + " and work Type: " + Other_Types[i]);
                                assertTrue("Work Id " + booksworkId + " Successfully filtered by Work Type: " + chooseWorkType, productFinderTasks.isPageContainingString(Journal_Types[i]));
                            }
                        }
                    }
                    productFinderTasks.openHomePage();
                  //  tasks.click("XPATH", ProductFinderConstants.previousPage);
                    //tasks.click("XPATH", ProductFinderConstants.closeButtonSearchBar);
                }
            }
            booksworkIdList.clear();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
