package com.eph.automation.testing.models.ui;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.helper.Log;
import com.google.inject.Inject;
import org.openqa.selenium.WebElement;

public class ProductFinderTasks {

    private TasksNew tasks;

    @Inject
    public ProductFinderTasks(final TasksNew tasks){
        this.tasks = tasks;
    }


    public void openHomePage() {
        tasks.openPage(Constants.PRODUCT_FINDER_EPH_UAT_UI);
    }

    public void loginByScienceAccount(String scienceEmailId) throws InterruptedException{
        tasks.sendKeys("NAME",ProductFinderConstants.loginByEmail,scienceEmailId);
        tasks.click("ID",ProductFinderConstants.nextButton);

    }

    public void searchFor(final String searchID) {
        tasks.sendKeys("XPATH",ProductFinderConstants.searchBar,searchID);
        tasks.click("XPATH",ProductFinderConstants.searchButton);
    }

    public boolean isPageContainingString(String text){
        return tasks.doesPageContainsText(text);
    }

    public void goToNextPage(){
        tasks.click("CLASS",ProductFinderConstants.nextPageButton);
    }

    public WebElement getElementByTitle(String title){
        return tasks.findElementByText(title);
    }

    public boolean isUserOnWorkPage(String workID) {
        String targetURL = Constants.PRODUCT_FINDER_EPH_UAT_UI + "work/"+workID+"/overview";
        Log.info(targetURL);
        if(targetURL.equalsIgnoreCase(tasks.getCurrentPage())){
            return true;
        } else {
            return false;
        }
    }


}
