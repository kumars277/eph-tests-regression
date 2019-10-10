/**
 * Copyright (C) Estafet Ltd
 */
package com.eph.automation.testing.models.ui;
/**
 * Created by GVLAYKOV
 */

import com.eph.automation.testing.configuration.Constants;
import com.google.inject.Inject;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;

import java.util.List;

public class SearchTasks {

    private Tasks tasks;

    @Inject
    public SearchTasks(final Tasks tasks){
        this.tasks = tasks;
    }

    public void openHomePage() {
        tasks.openPage(Constants.PRODUCT_FINDER_EPH_SIT_UI);
    }

    public void searchFor(final String searchID) {
        tasks.findElementByXpath(SearchConstants.searchBar).sendKeys(searchID, Keys.RETURN);
        tasks.findElementByXpath(SearchConstants.searchButton).click();
    }

    public void selectSearchOption(final String searchOption) {
        tasks.findElementByXpath(SearchConstants.searchOptions).click();
        if (searchOption.equals("Works")) {
            tasks.findElementByXpath(SearchConstants.searchOption_Works).click();
        } else if (searchOption.equals("People")){
            tasks.findElementByXpath(SearchConstants.searchOption_People).click();
        } else if (searchOption.equals("Manifestations")){
            tasks.findElementByXpath(SearchConstants.searchOption_Manifestations).click();
        } else if (searchOption.equals("Products")){
            tasks.findElementByXpath(SearchConstants.searchOption_Products).click();
        }
    }

    public List<WebElement> getSearchResults(){
        return tasks.findElementsByTagName(SearchConstants.searchResults);
    }

    public WebElement getElementByTitle(String title){
        return tasks.findElementByText(title);
    }

    public void assureNoresultsMessageIsDisplayed(String text){
        tasks.verifyElementContainsText(SearchConstants.searchNoResults, text);
    }

    public void goToNextPage(){
        tasks.findElementByClassName(SearchConstants.nextPageButton).click();
    }

    public boolean isPageContainingString(String text){
        return tasks.doesPageContainsText(text);
    }

    public boolean isUserOnWorkPage(String workID) {
        String targetURL = Constants.PRODUCT_FINDER_EPH_SIT_UI + "work/"+workID+"/overview";

        if(targetURL.equalsIgnoreCase(tasks.getCurrentPage())){
            return true;
        } else {
            return false;
        }
    }
}