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

    public void loginByScienceAccount(String scienceEmailId) {
        String methodName="loginByScienceAccount";
        try{
            if(tasks.verifyElementisDisplayed("NAME",ResearchPackagesConstants.LoginByEmail)){
                tasks.sendKeys("NAME", ResearchPackagesConstants.LoginByEmail,scienceEmailId);
                tasks.click("ID", ResearchPackagesConstants.NextButton);
            }
        }catch (Exception e){
            Log.info("Exception in the method: "+methodName);
        }
    }

    public void impersonateUser() {
        String methodName = "impersonateUser";
        try{
         if(!tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.USER_ROLE)){
             if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.USER_LOGIN_ICON)){
                 tasks.click("XPATH", ResearchPackagesConstants.USER_LOGIN_ICON);
                 tasks.waitTime(2);
                 tasks.click("XPATH", ResearchPackagesConstants.IMPERSONATE_BUTTON);
                 if(tasks.verifyElementisDisplayed("XPATH", ResearchPackagesConstants.CHOOSE_PRODUCT_OWNER)){
                     tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_PRODUCT_OWNER);
                     tasks.click("XPATH", ResearchPackagesConstants.SUBMIT_USER);
                 }
             }
         }
        }catch (Exception e){
            Log.info("Exception in the Method: "+methodName);
        }
    }

    public void specialCollectionJournals() {
        String methodName="specialCollectionJournals";
        try{
            if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.SELECT_DJC)){
                tasks.click("XPATH", ResearchPackagesConstants.SELECT_DJC);
            }
            if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.SELCT_SJC)){
                tasks.click("XPATH", ResearchPackagesConstants.SELCT_SJC);
            }
            if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.SELECT_CC)){
                tasks.click("XPATH", ResearchPackagesConstants.SELECT_CC);
                tasks.click("XPATH", ResearchPackagesConstants.SELECT_CC_BASIC);
            }
        }catch (Exception e){
            Log.info("Exception in "+methodName);
        }
    }

    public boolean clickMathCoreCollections(){
        String methodName = "clickMathCoreCollections";
        boolean flag = false;
            try {
                if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.SELECT_DJC)) {
                    tasks.click("XPATH", ResearchPackagesConstants.SELECT_DJC);
                    if (tasks.verifyElementisDisplayed("XPATH", ResearchPackagesConstants.SELCT_MCC)) {
                        tasks.click("XPATH", ResearchPackagesConstants.SELCT_MCC);
                        flag = true;
                        return flag;
                    }
                }

            } catch (Exception e) {
                Log.info("Exception in "+methodName);
                flag=false;
                return flag;
            }
            return flag;
    }

    public void addJournals() throws InterruptedException {
        String methodName = "addJournals";
        try{
            if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.NO_COLLECTIONS)||
                    (tasks.verifyElementTextisDisplayed("There is currently no active collection")) ){
                tasks.click("XPATH", ResearchPackagesConstants.CREATE_PROSPECTIVE_LIST);
            }
            if(tasks.verifyElementisDisplayed("XPATH", ResearchPackagesConstants.ADD_JOURNAL)){
                tasks.click("XPATH", ResearchPackagesConstants.ADD_JOURNAL);
            }
        }catch (Exception e){
           Log.info("Exception in the Method :"+methodName);
        }
        tasks.waitTime(2);
    }

    public void searchJournalToAdd(String issnVal){
        String methodName="searchJournalToAdd";
        try{
            if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.SEARCH_VALUE)){
                tasks.sendKeys("XPATH", ResearchPackagesConstants.SEARCH_VALUE,issnVal);
                tasks.click("XPATH", ResearchPackagesConstants.SEARCH_SUBMIT);
                tasks.waitTime(2);
            }
        }catch (Exception e){
            Log.info("Exception in the Method "+ methodName);
        }
    }


    public boolean isAlertPresent()
    {
        try
        {
            tasks.acceptAlert();
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    public void includeJournalToAdd(String issnVal){
        String methodname="includeJournalToAdd";
        String buildLocatorVal = "//td[contains(text(),\'"+issnVal+"\')]";
        try {
            if(tasks.verifyElementisDisplayed("XPATH",buildLocatorVal)&&
                    tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.INCLUDE_JOURNAL_POPUP)){
                tasks.click("XPATH", ResearchPackagesConstants.INCLUDE_JOURNAL_POPUP);
            }if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.CHOOSE_REASON_DROPDOWN)){
                tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_DROPDOWN);
                tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_INCLUDE);
                tasks.click("XPATH", ResearchPackagesConstants.ADD);
            }
        }catch (Exception e){
            Log.info("Exception in "+ methodname);
        }
    }

    public void pendingJournalToAdd(String issnVal){
        String methodname="pendingJournalToAdd";
        String buildLocatorVal = "//td[contains(text(),\'"+issnVal+"\')]";
        try {
            if(tasks.verifyElementisDisplayed("XPATH",buildLocatorVal)&&
                    tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.PENDING_JOURNAL_POPUP)){
                    tasks.click("XPATH", ResearchPackagesConstants.PENDING_JOURNAL_POPUP);
            }if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.CHOOSE_REASON_DROPDOWN)){
                tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_DROPDOWN);
                tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_PENDING);
                tasks.sendKeys("XPATH",ResearchPackagesConstants.ADD_REMARKS,"test");
                tasks.click("XPATH", ResearchPackagesConstants.ADD);
            }
        }catch (Exception e){
            Log.info("Exception in "+ methodname);
        }
    }

    public void searchJournal(String val){
        String methodname="searchJournal";
        try{
            if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.NO_COLLECTIONS)){
                tasks.click("XPATH", ResearchPackagesConstants.CREATE_PROSPECTIVE_LIST);
               tasks.waitTime(2);
            }
            if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)){
                if(tasks.verifyElementisDisplayed("ID",ResearchPackagesConstants.SEARCH_FILTER)){
                    tasks.sendKeys("ID",ResearchPackagesConstants.SEARCH_FILTER,val);
                    tasks.waitTime(2);
                }
            }
        }catch (Exception e){
            Log.info("Exception in "+ methodname);
        }
    }


    public void excludeJournal(){
        String methodname="excludeJournal";
        try{
            if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.EXCLUDE_JOURNAL)){
                tasks.click("XPATH",ResearchPackagesConstants.EXCLUDE_JOURNAL);
                tasks.waitTime(1);
                tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_DROPDOWN);
                tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_EXCLUDE);
                tasks.click("XPATH", ResearchPackagesConstants.EXCLUDE_ADD);
            }
        }catch (Exception e){
            Log.info("Exception in "+ methodname);
        }
    }

    public void pendingJournal(){
        String methodname="pendingJournal";
        try {
            if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.PENDING_JOURNAL)){
                    tasks.click("XPATH",ResearchPackagesConstants.PENDING_JOURNAL);
                    tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_DROPDOWN);
                //    WebElement reqElement = tasks.findElement("XPATH", ResearchPackagesConstants.CHOOSE_REASON_PENDING);
                    tasks.keyboardEvents("XPATH", ResearchPackagesConstants.CHOOSE_REASON_PENDING,"RETURN");
                    tasks.sendKeys("XPATH", ResearchPackagesConstants.ADD_REMARKS,"Awaiting for the Response");
                    tasks.click("XPATH", ResearchPackagesConstants.EXCLUDE_ADD);
            }
            tasks.clearText("ID",ResearchPackagesConstants.SEARCH_FILTER);

        }catch (Exception e){
            Log.info("Exception in "+ methodname);
        }
    }

    public void includeJournal(){
        String methodname="includeJournal";
        try {
            if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.INCLUDE_JOURNAL)){
                tasks.click("XPATH",ResearchPackagesConstants.INCLUDE_JOURNAL);
                tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_DROPDOWN);
           //     WebElement reqElement = tasks.click("XPATH", ResearchPackagesConstants.CHOOSE_REASON_INCLUDE);
                tasks.keyboardEvents("XPATH", ResearchPackagesConstants.CHOOSE_REASON_INCLUDE,"RETURN");
                tasks.sendKeys("XPATH", ResearchPackagesConstants.ADD_REMARKS,"Adding as New Journal");
                tasks.click("XPATH", ResearchPackagesConstants.EXCLUDE_ADD);
            }

        }catch (Exception e){
            Log.info("Exception in "+ methodname);
        }
    }

    public void saveCollections(){
        String methodName="saveCollections";
        try{
            if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.SAVE_COLLECTIONS)){
                tasks.click("XPATH", ResearchPackagesConstants.SAVE_COLLECTIONS);
            }
        }catch (Exception e){
            Log.info("Exception in the method:  "+methodName);
        }

    }






}
