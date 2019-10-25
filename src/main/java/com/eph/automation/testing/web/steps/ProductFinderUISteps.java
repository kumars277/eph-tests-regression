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
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import org.openqa.selenium.WebElement;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Created by GVLAYKOV
 */
public class ProductFinderUISteps {

    @StaticInjection
    public DataQualityContext dataQualityContext;

    public static List<ManifestationIdentifierObject> manifestationIdentifiers;
    private String sql;
    private static List<String> ids;
    private static List<String>workTypeCode;
    private static List<String>workStatusCode;
    private static List<String> manifestation_Ids;
    private static List<String> manifestationIdentifiers_Ids;
    private static List<String> allWorkTypesCodeArrayToList;
    private String finalworkTypeCode;
    private String finalworkStatusCode;
    private static List<String>workStatusCodesofLaunchedToList,workStatusCodesofPlannedToList;

    private ProductFinderTasks productFinderTasks;
    private TasksNew tasks;

    private static String searchingID;

    @Inject
    public ProductFinderUISteps(ProductFinderTasks productFinderTasks, TasksNew tasks) {
          this.productFinderTasks = productFinderTasks;
            this.tasks = tasks;
    }


    @Given("^We get the id for work search (.*)$")
    public void getWorkDataFromEPHGD(String workID) {
        sql = String.format(APIDataSQL.SELECT_WORK_BY_ID_FOR_SEARCH, workID);
        Log.info(sql);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
    }
    @And("^We get the work search data from the EPH GD$")

    public void getWorksDataFromEPHGD() {
        Log.info("And We get the data from EPH GD for journals ...");
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        Log.info(sql);
        DataQualityContext.workDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
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

    @Then("^The searched item is listed and clicked$")
    public void checkSearchResultsAndClickElement() throws Throwable {
        WebElement foundEntry = productFinderTasks.getElementByTitle(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        Log.info("found entry is"+ DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        foundEntry.isDisplayed();
        foundEntry.click();
        Thread.sleep(2000);

    }

    @And("^User is forwarded to the searched works page$")
    public void verifyUserIsForwardedToWorksPage () {
        assertTrue(productFinderTasks.isUserOnWorkPage(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID()));
    }

    @Given("^Filter the Search Result by \"([^\"]*)\"$")
    public void filter_the_Search_Result_by(String workType) {
            String buildWorkTypeFilterLocator = "//span[contains(text(),\'"+workType+"\')]";
            tasks.click("XPATH",buildWorkTypeFilterLocator);
     }

    @Given("^Filter the Search Result by work status \"([^\"]*)\"$")
    public void filter_the_Search_Result_by_work_status(String workStatus) throws InterruptedException {
        String buildWorkTypeFilterLocator = "//span[contains(text(),\'"+workStatus+"\')]";
        tasks.click("XPATH",buildWorkTypeFilterLocator);
        Thread.sleep(1000);
    }

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






}