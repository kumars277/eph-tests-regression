package com.eph.automation.testing.models.ui;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.helper.Log;
import com.google.inject.Inject;
import org.openqa.selenium.WebElement;

public class ResearchPackagesTasks {
    private TasksNew tasks;

    @Inject
    public ResearchPackagesTasks(final TasksNew tasks){
        this.tasks = tasks;
    }

    public void openPage(){
        tasks.openPage(Constants.RESEARCH_PACKAGES_SIT_UI);
    }

    public void loginByScienceAccount(String scienceEmailId) throws InterruptedException{
        tasks.sendKeys("NAME", ResearchPackagesConstants.LoginByEmail,scienceEmailId);
        tasks.click("ID", ResearchPackagesConstants.NextButton);

    }

    public void impersonateUser() throws InterruptedException {
        String methodName = "impersonateUser";
        try{
        tasks.click("XPATH", ResearchPackagesConstants.USER_LOGIN_ICON);
        Thread.sleep(3000);
        tasks.click("XPATH", ResearchPackagesConstants.IMPERSONATE_BUTTON);
        tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_PRODUCT_OWNER);
        tasks.click("XPATH", ResearchPackagesConstants.SUBMIT_USER);}
        catch (Exception e){
            System.out.print("exception in"+methodName);
        }

    }
    public void specialCollectionJournals() throws InterruptedException {
        String methodName="specialCollectionJournals";
        try{
            tasks.click("XPATH", ResearchPackagesConstants.SELECT_DJC);
            tasks.click("XPATH", ResearchPackagesConstants.SELCT_SJC);
            tasks.click("XPATH", ResearchPackagesConstants.SELECT_CC);
            tasks.click("XPATH", ResearchPackagesConstants.SELECT_CC_BASIC);
            Thread.sleep(2000);
        }catch (Exception e){
            Log.info("Exception in "+methodName);

        }

    }

    public void addJournals(){
        tasks.click("XPATH", ResearchPackagesConstants.ADD_JOURNAL);
    }

    public void searchJournals(String issnValue){
        String methodname="specialCollectionJournals";
        try{
            tasks.sendKeys("XPATH", ResearchPackagesConstants.SEARCH_VALUE,issnValue);
            tasks.click("XPATH", ResearchPackagesConstants.SEARCH_SUBMIT);
        }catch (Exception e){
            Log.info("Exception in "+ methodname);
        }

    }

    public void includeJournal(){
        String methodname="includeJournal";
        try {
            tasks.click("XPATH", ResearchPackagesConstants.INCLUDE_JOURNAL);
            tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_DROPDOWN);
            tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_VALUE);
            tasks.click("XPATH", ResearchPackagesConstants.ADD);
        }catch (Exception e){
            Log.info("Exception in "+ methodname);
        }
    }

    public void excludeJournal(){
        String methodname="excludeJournal";
        try{
            tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_DROPDOWN);
            tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_FOR_EXCLUDE_VALUE);
            tasks.click("XPATH", ResearchPackagesConstants.EXCLUDE_ADD);

        }catch (Exception e){
            Log.info("Exception in "+ methodname);
        }
    }

    public void pendingJournal(){
        String methodname="pendingJournal";
        try {
           tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_DROPDOWN);
           WebElement reqElement =  tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_FOR_PENDING_VALUE);
           tasks.keyboardEvents(reqElement,"RETURN");
           tasks.sendKeys("XPATH", ResearchPackagesConstants.ADD_REMARKS,"Sample Test");
            tasks.click("XPATH", ResearchPackagesConstants.EXCLUDE_ADD);
        }catch (Exception e){
            Log.info("Exception in "+ methodname);
        }
    }

    public void saveCollections(){
        String methodname="saveCollections";
        try{
            tasks.click("XPATH", ResearchPackagesConstants.SAVE_COLLECTIONS);
        }catch (Exception e){
            Log.info("Exception in "+methodname);
        }

    }






}
