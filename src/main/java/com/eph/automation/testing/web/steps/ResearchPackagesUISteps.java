package com.eph.automation.testing.web.steps;


import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.ui.ResearchPackagesConstants;
import com.eph.automation.testing.models.ui.ResearchPackagesTasks;
import com.eph.automation.testing.models.ui.TasksNew;
import com.eph.automation.testing.services.db.sql.ResearchPackagesSQL;
import com.google.inject.Inject;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.WebElement;

import java.util.List;
import java.util.Map;

public class ResearchPackagesUISteps {
    @StaticInjection
    private ResearchPackagesTasks researchPackagesTasks;
    private TasksNew tasks;
    private String sql;
    private static String addJournalStatus;

    @Inject
    public ResearchPackagesUISteps(final ResearchPackagesTasks researchPackagesTasks, TasksNew tasks) {
        this.researchPackagesTasks = researchPackagesTasks;
        this.tasks = tasks;
    }

    @Then("^Enter into the SJC Clinical Collections$")
    public void enter_into_the_SJC_Clinical_Collections() throws Throwable {
        researchPackagesTasks.specialCollectionJournals();
    }


    @Then("^Verify the status of the journal \"([^\"]*)\" is excluded in DB$")
    public void verify_the_status_of_the_journal_is_excluded_in_DB(String ISSN, String journalStatus) throws Throwable {
        sql = String.format(ResearchPackagesSQL.SELECT_RECENT_STATUS_UPDATED_JOURNAL_FROM_EPH_RESEARCH_PACKAGES, ISSN);
        Log.info(sql);
        List<Map<?, ?>> excludeJournalCollectionStatus = DBManager.getDBResultMap(sql, Constants.WFT_URL);
        String statusOfCollection = (String)excludeJournalCollectionStatus.get(0).get("JOURNAL_STATUS");
        Log.info(statusOfCollection);
        Assert.assertTrue("Status of the Journal is Excluded",statusOfCollection.equalsIgnoreCase(journalStatus));
    }

    @Then("^Search the journal with \"([^\"]*)\" to Change into Pending$")
    public void search_the_journal_with_to_Change_into_Pending(String ISSN) throws Throwable {
        if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.NO_COLLECTIONS)){
            tasks.click("XPATH", ResearchPackagesConstants.CREATE_PROSPECTIVE_LIST);
        }
        tasks.sendKeys("ID",ResearchPackagesConstants.SEARCH_FILTER,ISSN);
        Thread.sleep(2000);
    }

    @Given("^User logged into the application as a Product Owner$")
    public void userLogin() throws Throwable {
        researchPackagesTasks.openPage();
        researchPackagesTasks.loginByScienceAccount(ResearchPackagesConstants.SCIENCE_ID);
        researchPackagesTasks.impersonateUser();
    }

    @Then("^Check for MCC availability and Click the same$")
    public void enterMathCoreCollection() throws Throwable {
        Assert.assertTrue("MCC are Available", researchPackagesTasks.clickMathCoreCollections());
    }

    @When("^Clicking on to the Add Journal$")
    public void clickAddJournal() throws InterruptedException {
        researchPackagesTasks.addJournals();
    }


    @Then("^Search for the journal with \"([^\"]*)\" inside Popup$")
    public void searchJournal(String issnValue){
        researchPackagesTasks.searchJournalToAdd(issnValue);
    }

    @Then("^Change the status of journal \"([^\"]*)\" to include and Add$")
    public void includeJournalFromResult(String issnValue){
        researchPackagesTasks.includeJournalToAdd(issnValue);
    }

    @Then("^Change the status of journal \"([^\"]*)\" to pending and Add$")
    public void pendingJournalFromResult(String issnValue){
        researchPackagesTasks.pendingJournalToAdd(issnValue);
    }

    @Then("^Save the Collections$")
    public void saveCollection(){
        researchPackagesTasks.saveCollections();
    }

    @Then("^Filter the journal with unsaved Status$")
    public void chooseUnsavedFilter(){
        researchPackagesTasks.filterJournalsByStatus(ResearchPackagesConstants.unsavedFilterCheckbox);
    }



    @And("^Verify the result displayed for the Unsaved Changes$")
     public void verifyUnsavedFilter() throws InterruptedException {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)) {
            String getTextUnsavedJournals = tasks.getTextofElement("XPATH",ResearchPackagesConstants.UNSAVED_JOURNAL_TEXT);
            String filterCountText  = tasks.getTextofElement("XPATH",ResearchPackagesConstants.UNSAVE_FILTER_COUNT);
            Log.info("Count Displayed in the Filter=> "+filterCountText);
            Log.info("Message displayed near Save button =>"+getTextUnsavedJournals);
            Assert.assertTrue("Count displayed near Filter and Count displayed near to Save button are equal",getTextUnsavedJournals.contains(filterCountText));
            int filterCount = Integer.parseInt(filterCountText);
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.UNSAVED_ROW).size();
            Log.info("Total Result Displayed =>"+ totItems);
            Assert.assertTrue("Results and count displayed are equal",filterCount==totItems);
            for(int i=0;i<totItems;i++) {
                Log.info("ISSN displayed in the Result => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).get(i).getText()+" is Unsaved");
            }
        }
    }


    @Then("^Choose the filter with Pending Status$")
    public void chooseFilterPending(){
        researchPackagesTasks.filterJournalsByStatus(ResearchPackagesConstants.pendingFilterCheckbox);
    }
    @Then("^Choose the filter with Excluded Status$")
    public void chooseFilterExcluded(){
        researchPackagesTasks.filterJournalsByStatus(ResearchPackagesConstants.excludeFilterCheckbox);
    }
    @Then("^Choose the filter with Included Status$")
    public void chooseFilterIncluded(){
        researchPackagesTasks.filterJournalsByStatus(ResearchPackagesConstants.includeFilterCheckbox);

    }
    @Then("^Verify the result displayed from multiple filters$")
    public void verifyResultByMultipleFilter() throws InterruptedException {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)) {
            String filterCountIncludeText  = tasks.getTextofElement("XPATH",ResearchPackagesConstants.INCLUDE_FILTER_COUNT);
            String filterCountOwnerText  = tasks.getTextofElement("XPATH",ResearchPackagesConstants.Ownership_Filter_ELSOWN_Count);
            String filterCountPubDirText  = tasks.getTextofElement("XPATH",ResearchPackagesConstants.PubDirector_Filter_Rapes_Relaxed_count);
            String filterCountPmgText  = tasks.getTextofElement("XPATH",ResearchPackagesConstants.PMG_FILTER_MCC_COUNT);
            int filterCountInclude = Integer.parseInt(filterCountIncludeText);
            int filterCountOwner = Integer.parseInt(filterCountOwnerText);
            int filterCountPubDir = Integer.parseInt(filterCountPubDirText);
            int filterCountPmg = Integer.parseInt(filterCountPmgText);
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.ROW_RESULT).size();
            Assert.assertTrue("Counts in the Include filters and result counts are matching",filterCountInclude==totItems);
            Assert.assertTrue("Counts in the PMG filters and result counts are matching",filterCountPmg==totItems);
            Assert.assertTrue("Counts in the Ownership filters and result counts are matching",filterCountOwner==totItems);
            Assert.assertTrue("Counts in the Pub Director filters and result counts are matching",filterCountPubDir==totItems);
            for(int i=0;i<totItems;i++) {
                Log.info("Results Displayed => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).get(i).getText()+"=> "
                           +tasks.multipleElements("XPATH",ResearchPackagesConstants.PMG_COLUMN).get(i).getText()+"=> "tasks.multipleElements("XPATH",ResearchPackagesConstants.OWNER_COLUMN).get(i).getText()
                            +"=> "+tasks.multipleElements("XPATH",ResearchPackagesConstants.PUBDIRECTOR_COLUMN).get(i).getText());
            }

        }
    }

    @And("^Verify the result displayed for the Included Status$")
    public void verifyIncludedFilter() throws InterruptedException {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)) {
            String filterCountText  = tasks.getTextofElement("XPATH",ResearchPackagesConstants.INCLUDE_FILTER_COUNT);
            int filterCount = Integer.parseInt(filterCountText);
            Log.info("Filter Count displayed for Included is => "+filterCount);
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.IS_INCLUDED).size();
            Log.info("Total Result Displayed for Included is =>"+ totItems);
            Assert.assertTrue("Results and count displayed are equal",filterCount==totItems);
            for(int i=0;i<totItems;i++) {
                Log.info("ISSN displayed in the Result => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).get(i).getText()+" is Included");
            }
        }
    }
    @And("^Verify the result displayed for the Excluded Status$")
    public void verifyExcludedFilter() throws InterruptedException {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)) {
            String filterCountText  = tasks.getTextofElement("XPATH",ResearchPackagesConstants.EXCLUDE_FILTER_COUNT);
            int filterCount = Integer.parseInt(filterCountText);
            Log.info("Filter Count displayed for Excluded is => "+filterCount);
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.IS_EXCLUDED).size();
            Log.info("Total Result Displayed for Excluded is =>"+ totItems);
            Assert.assertTrue("Results and count displayed are equal",filterCount==totItems);
            for(int i=0;i<totItems;i++) {
                Log.info("ISSN displayed in the Result => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).get(i).getText()+" is Excluded");
            }
        }
    }

    @And("^Verify the result displayed for the Pending Status$")
    public void verifyPendingFilter() throws InterruptedException {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)) {
            String filterCountText  = tasks.getTextofElement("XPATH",ResearchPackagesConstants.PENDING_FILTER_COUNT);
            int filterCount = Integer.parseInt(filterCountText);
            Log.info("Filter Count displayed for Pending is => "+filterCount);
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.IS_PENDING).size();
            Log.info("Total Result Displayed for Pending is =>"+ totItems);
            Assert.assertTrue("Results and count displayed are equal",filterCount==totItems);
            for(int i=0;i<totItems;i++) {
                Log.info("ISSN displayed in the Result => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).get(i).getText()+" in Pending");
            }
        }
    }

    @Then("^Verify the journal \"([^\"]*)\" is added to the collection$")
    public void journalAdded(String ISSN ){
        sql = String.format(ResearchPackagesSQL.SELECT_NEWLY_ADDED_JOURNAL_FROM_EPH_RESEARCH_PACKAGES, ISSN);
        Log.info(sql);
        List<Map<?, ?>> addJournalStatusList = DBManager.getDBResultMap(sql, Constants.WFT_URL);
        addJournalStatus = addJournalStatusList.get(0).get("NEWLY_ADDED_JOURNAL_STATUS").toString();
        Log.info("Journal "+ISSN+" Added to the Collection=> "+addJournalStatus.toString());
        Assert.assertTrue("Journal Added Successfully to the Collections", addJournalStatus.equalsIgnoreCase("true"));
    }


    @And("^Verify the status of the journal \"([^\"]*)\" is \"([^\"]*)\" in DB$")
    public void journalStatusInDb(String ISSN, String status) throws Throwable {
        sql = String.format(ResearchPackagesSQL.SELECT_RECENT_STATUS_UPDATED_JOURNAL_FROM_EPH_RESEARCH_PACKAGES, ISSN);
        Log.info(sql);
        List<Map<?, ?>> journalStatusList = DBManager.getDBResultMap(sql, Constants.WFT_URL);
        String journalStatus = (String)journalStatusList.get(0).get("JOURNAL_STATUS");
        Log.info("Status of the Journal: "+ISSN+"=> "+journalStatus);
        Assert.assertTrue("Status of the Journal Successfully Updated",journalStatus.equalsIgnoreCase(status));
    }

    @Then("^Exclude from the collection$")
    public void excludeJournal() throws Throwable {
        researchPackagesTasks.excludeJournal();
    }

    @Then("^Search for the journal with \"([^\"]*)\" given$")
    public void searchJournalIssn(String ISSN){
        researchPackagesTasks.searchJournal(ISSN);
    }

    @Then("^Search journalno of the journal with \"([^\"]*)\" given$")
    public void searchJournalno(String journalNoVal){
        researchPackagesTasks.searchJournal(journalNoVal);
    }

    @Then("^Search publisher of the journal with \"([^\"]*)\" given$")
        public void searchJournalPublisher(String publisher){
        researchPackagesTasks.searchJournal(publisher);
    }
    @Then("^Search title keyword of the journal with \"([^\"]*)\" given$")
    public void searchJournalbyTitle(String journaltitle){
            researchPackagesTasks.searchJournal(journaltitle);
    }



    @And("^Verify the journal with \"([^\"]*)\" is displayed$")
    public void journalDisplayedbyIssn(String issnVal) throws InterruptedException {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)){
            Log.info("Searched ISSN is => "+issnVal);
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).size();
            Log.info("Total Results Found => "+totItems);
            for(int i=0;i<totItems;i++){
                Log.info("ISSN displayed in the Result => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).get(i).getText());
                boolean assertionCondition = tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).get(i).getText().equalsIgnoreCase((issnVal));
                Assert.assertTrue("ISSN for the journals listed out matches with Searched Value",assertionCondition);
            }
        }
    }

    @And("^Verify the publisher of the journal with \"([^\"]*)\" is displayed$")
    public void journalDisplayedByPub(String pubVal) throws InterruptedException {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)){
            Log.info("Searched Publisher is => "+pubVal);
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.PUBLISHER_COLUMN).size();
            Log.info("Total Results Found => "+totItems);
            for(int i=0;i<totItems;i++){
                Log.info("Publishers displayed in the result with Journals => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.PUBLISHER_COLUMN).get(i).getText());
                boolean assertionCondition = tasks.multipleElements("XPATH",ResearchPackagesConstants.PUBLISHER_COLUMN).get(i).getText().equalsIgnoreCase((pubVal));
                Assert.assertTrue("Publisher for the journals listed out matches with Searched Value",assertionCondition);
            }
        }
    }

    @Then("^Choose the \"([^\"]*)\" filter$")
    public void chooseFilterForPubDirector(String pubDirectoVal) throws Throwable {
               if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.PubDirector_Filter_Rapes_Relaxed)) {
                   tasks.click("XPATH", ResearchPackagesConstants.PubDirector_Filter_Rapes_Relaxed);
               }
               Log.info("Filter By => "+pubDirectoVal);
    }
    @Then("^Choose the Ownership Type \"([^\"]*)\" filter$")
    public void chooseFilterForOwnership(String ownerShipVal) throws Throwable{
        if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.Ownership_Filter_ELSOWN)){
            tasks.click("XPATH",ResearchPackagesConstants.Ownership_Filter_ELSOWN);
        }
        Log.info("Filter by => "+ ownerShipVal);
    }

    @Then("^Verify the result displayed for the Ownership Type \"([^\"]*)\"$")
    public void verifyResultOfOwnership(String OwnershipVal) throws Throwable {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)){
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.OWNER_COLUMN).size();
            String filterCountText  = tasks.getTextofElement("XPATH",ResearchPackagesConstants.Ownership_Filter_ELSOWN_Count);
            int filterCount = Integer.parseInt(filterCountText);
            Log.info("Filtered by Ownership=> "+OwnershipVal);
            Log.info("Filter Count Displayed=> "+filterCount);
            Log.info("Total Results Found => "+totItems);
            Assert.assertTrue("Count in the filter and total result displayed matches",filterCount==totItems);
            for(int i=0;i<totItems;i++){
                Log.info("Ownership with ISSN displayed => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.OWNER_COLUMN).get(i).getText()+" - "+tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).get(i).getText());
                boolean assertionCondition = tasks.multipleElements("XPATH",ResearchPackagesConstants.OWNER_COLUMN).get(i).getText().equalsIgnoreCase((OwnershipVal));
                Assert.assertTrue("Ownership for the journals listed out matches with Searched Value",assertionCondition);
            }
        }
    }

    @Then("^Choose the PMG \"([^\"]*)\" filter$")
    public void chooseFilterForPmg(String pmgVal) throws Throwable{
        if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.PMG_Filter_MCC)){
            tasks.click("XPATH",ResearchPackagesConstants.PMG_Filter_MCC);
        }
        Log.info("Filter By => "+pmgVal);
    }

    @And("^Verify the result displayed for the PMG \"([^\"]*)\"$")
    public void verifyResultOfPmg(String pmgVal) throws Throwable {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)){
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.PMG_COLUMN).size();
            String filterCountText  = tasks.getTextofElement("XPATH",ResearchPackagesConstants.PMG_FILTER_MCC_COUNT);
            int filterCount = Integer.parseInt(filterCountText);
            Log.info("Filtered by PMG "+pmgVal);
            Log.info("Filter Count Displayed=> "+filterCount);
            Log.info("Total Results Found => "+totItems);
            Assert.assertTrue("Count in the filter and total result displayed matches",filterCount==totItems);
            for(int i=0;i<totItems;i++){
                Log.info("PMG with ISSN displayed => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.PMG_COLUMN).get(i).getText()+" - "+tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).get(i).getText());
                boolean assertionCondition = tasks.multipleElements("XPATH",ResearchPackagesConstants.PMG_COLUMN).get(i).getText().equalsIgnoreCase((pmgVal));
                Assert.assertTrue("PMG for the journals listed out matches with the Filtered Value",assertionCondition);
            }
        }
    }
    @Then("^Verify the result displayed for the \"([^\"]*)\"$")
    public void verifyResultOfPubDirector(String pubDirectorVal) throws Throwable {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)){
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.PUBDIRECTOR_COLUMN).size();
            String filterCountText  = tasks.getTextofElement("XPATH",ResearchPackagesConstants.PubDirector_Filter_Rapes_Relaxed_count);
            int filterCount = Integer.parseInt(filterCountText);
            Log.info("Filtered by Publishing Director "+pubDirectorVal);
            Log.info("Filter Count Displayed=> "+filterCount);
            Log.info("Total Results Found => "+totItems);
            Assert.assertTrue("Count in the filter and total result displayed matches",filterCount==totItems);
            for(int i=0;i<totItems;i++){
                Log.info("Publishing Directors with ISSN displayed => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.PUBDIRECTOR_COLUMN).get(i).getText()+" - "+tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).get(i).getText());
                boolean assertionCondition = tasks.multipleElements("XPATH",ResearchPackagesConstants.PUBDIRECTOR_COLUMN).get(i).getText().equalsIgnoreCase((pubDirectorVal));
                Assert.assertTrue("Publishing Director for the journals listed out matches with Searched Value",assertionCondition);
            }
        }
    }
    @And("^Verify the title of the journal with \"([^\"]*)\" is displayed$")
    public void journalDisplayedbyTitle(String journalTitle) throws InterruptedException {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)){
            Log.info("Searched keyword title is => "+journalTitle);
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.TITLE_COLUMN).size();
            Log.info("Total Results Found => "+totItems);
            for(int i=0;i<totItems;i++){
                Log.info("Title Keyword displayed in the Result => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.TITLE_COLUMN).get(i).getText());
                boolean assertionCondition = tasks.multipleElements("XPATH",ResearchPackagesConstants.TITLE_COLUMN).get(i).getText().contains(journalTitle);
                Assert.assertTrue("Journals displayed based on the Searched text",assertionCondition);
            }
        }
    }

    @And("^Verify the journalno of the journal with \"([^\"]*)\" is displayed$")
    public void journalDisplayedbyjournalno(String journalVal) throws InterruptedException {
        if(tasks.verifyElementisDisplayed("TAG",ResearchPackagesConstants.resultTable)){
            int totItems = tasks.multipleElements("XPATH",ResearchPackagesConstants.JOURNALNO_COLUMN).size();
            Log.info("Filtered by Journal Number "+journalVal);
            Log.info("Total Results Found => "+totItems);
            for(int i=0;i<totItems;i++){
                Log.info("Journal Number with ISSN displayed => "+tasks.multipleElements("XPATH",ResearchPackagesConstants.JOURNALNO_COLUMN).get(i).getText()+" -> "+tasks.multipleElements("XPATH",ResearchPackagesConstants.ISSN_COLUMN).get(i).getText());
                boolean assertionCondition = tasks.multipleElements("XPATH",ResearchPackagesConstants.JOURNALNO_COLUMN).get(i).getText().equalsIgnoreCase((journalVal));
                Assert.assertTrue("journals listed out matches with the given Journal Number",assertionCondition);
            }
        }
    }

    @Then("^change to Pending from the collection$")
    public void pendingJournal(){
        researchPackagesTasks.pendingJournal();
    }

    @And("^Verify the status of the journal before include \"([^\"]*)\" in DB$")
    public void journalStatusbeforeInDb(String ISSN) throws Throwable {
        sql = String.format(ResearchPackagesSQL.SELECT_RECENT_STATUS_UPDATED_JOURNAL_FROM_EPH_RESEARCH_PACKAGES, ISSN);
        Log.info(sql);
        List<Map<?, ?>> journalStatusList = DBManager.getDBResultMap(sql, Constants.WFT_URL);
        String journalStatus = (String)journalStatusList.get(0).get("JOURNAL_STATUS");
        Log.info("Status of the Journal before Include: "+ISSN+"=> "+journalStatus);
    }

    @Then("^change to Include from the collection$")
    public void includeJournal(){
        researchPackagesTasks.includeJournal();
    }

}
