package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.api.WorkApiObject;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ManifestationIdentifierObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.models.ui.SearchTasks;
import com.google.inject.Inject;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.openqa.selenium.WebElement;

import java.util.List;


/**
 * Created by Georgi Vlaykov on 11/02/2019
 */
public class ProductFinderUISteps {

    @StaticInjection
    public DataQualityContext dataQualityContext;

    public static List<ManifestationIdentifierObject> manifestationIdentifiers;
    private String sql;
    private static List<String> ids;
    private static List<String> manifestation_Ids;
    private static List<String> manifestationIdentifiers_Ids;
    private WorkApiObject workApi_response;
    private static List<WorkDataObject> workIdentifiers;

    private SearchTasks searchTasks;

    private static String searchingID;

    @Inject
    public ProductFinderUISteps(final SearchTasks searchTasks) {
        this.searchTasks = searchTasks;
    }

    @Given("^user is on search page$")
    public void userOpensHomePage() throws InterruptedException {
        searchTasks.openHomePage();
        Thread.sleep(500);
    }

    @Given("^wants to search for \"([^\"]*)\"$")
    public void user_wants_to_search_for_type(String searchOption) throws Throwable {
        searchTasks.selectSearchOption(searchOption);
        Thread.sleep(500);

    }

    @Then("^Search for works by ID$")
    public void search_for_ID() throws Throwable {
        searchTasks.searchFor(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
        Thread.sleep(1000);
        // TODO: add page counter
        while(!searchTasks.isPageContainingString(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())){
            searchTasks.goToNextPage();
            Thread.sleep(1000);
        }
    }

    @Then("^user is searching for \"([^\"]*)\"$")
    public void search_for_string(String searchString) throws Throwable {
        searchTasks.searchFor(searchString);
        Thread.sleep(1000);
    }

    @Then("^No results message is displayed$")
    public void vrifyNoresultsMessageIsDisplayed() throws Throwable {
        searchTasks.assureNoresultsMessageIsDisplayed("There are no results that match your search for ‘abcdefg1234567890’");
        Thread.sleep(1000);
    }

    @Then("^Search for works by title$")
    public void search_for_Title() throws Throwable {
        searchTasks.searchFor(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        Thread.sleep(1000);
        // TODO: add page counter
        while(!searchTasks.isPageContainingString(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())){
            searchTasks.goToNextPage();
            Thread.sleep(1000);
        }
    }

    @Then("^Analyze results$")
    public void checkResults() throws Throwable {
        WebElement foundEntry = searchTasks.getElementByTitle(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        foundEntry.isDisplayed();
        foundEntry.click();

        Thread.sleep(500);

    }



}