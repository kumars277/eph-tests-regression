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
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class ResearchPackagesUISteps {
    @StaticInjection
    private ResearchPackagesTasks researchPackagesTasks;
    private TasksNew tasks;
    private String sql;
    private static String addedJournalStatus;

    @Inject
    public ResearchPackagesUISteps(final ResearchPackagesTasks researchPackagesTasks, TasksNew tasks) {
        this.researchPackagesTasks = researchPackagesTasks;
        this.tasks = tasks;
    }

    @Given("^User logged into the application$")
    public void user_logged_into_the_application() throws Throwable {
        researchPackagesTasks.openPage();
        researchPackagesTasks.loginByScienceAccount(ResearchPackagesConstants.SCIENCE_ID);
        Thread.sleep(3000);
        researchPackagesTasks.impersonateUser();
  }

    @Then("^Enter into the SJC Clinical Collections$")
    public void enter_into_the_SJC_Clinical_Collections() throws Throwable {
        researchPackagesTasks.specialCollectionJournals();
        Thread.sleep(1000);
    }

    @Then("^Clicking on to the Add Journal$")
    public void clicking_on_to_the_Add_Journal() throws InterruptedException {
       if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.NO_COLLECTIONS)){
           tasks.click("XPATH", ResearchPackagesConstants.CREATE_PROSPECTIVE_LIST);
       }
        if((tasks.verifyElementTextisDisplayed("There is currently no active collection"))){
              tasks.click("XPATH", ResearchPackagesConstants.CREATE_PROSPECTIVE_LIST);
        }
        researchPackagesTasks.addJournals();
            Thread.sleep(3000);
     }

    @When("^User searching a journal with \"([^\"]*)\"$")
    public void user_searching_a_journal_with(String issnNumber) throws InterruptedException {
        researchPackagesTasks.searchJournals(issnNumber);

    }

    @Then("^Select a Journal from the Result$")
    public void select_a_Journal_from_the_Result() throws Throwable {
        researchPackagesTasks.includeJournal();
        researchPackagesTasks.saveCollections();
    }

    @Then("^Adding the journal to the collections by including and verify in DB \"([^\"]*)\"$")
    public void adding_the_journal_to_the_collections_by_including_and_verify_in_DB(String ISSN) throws Throwable {
        sql = String.format(ResearchPackagesSQL.SELECT_NEWLY_ADDED_JOURNAL_FROM_EPH_RESEARCH_PACKAGES, ISSN);
        Log.info(sql);
        List<Map<?, ?>> addJournalCollectionStatus = DBManager.getDBResultMap(sql, Constants.WFT_URL);
        addedJournalStatus = addJournalCollectionStatus.get(0).get("NEWLY_ADDED_JOURNAL_STATUS").toString();
        Log.info(addedJournalStatus.toString());
        Assert.assertTrue("New Journal Added to the Collections", addedJournalStatus.equalsIgnoreCase("true"));
        tasks.closeBrowser();
    }

    @Then("^Search the journal with \"([^\"]*)\" to Exclude$")
    public void search_the_journal_with_to_Exclude(String ISSN) throws Throwable {
        if(tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.NO_COLLECTIONS)){
            tasks.click("XPATH", ResearchPackagesConstants.CREATE_PROSPECTIVE_LIST);
        }
        tasks.sendKeys("ID",ResearchPackagesConstants.SEARCH_FILTER,ISSN);
        Thread.sleep(2000);
    }

    @Then("^Exclude the Journal from the Clinical Collections$")
    public void exclude_the_Journal_from_the_Clinical_Collections() throws Throwable {
       if(!tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.CHECK_IS_EXCLUDE)){
            tasks.click("XPATH",ResearchPackagesConstants.EXCLUDE_JOURNAL);
           researchPackagesTasks.excludeJournal();
           researchPackagesTasks.saveCollections();
        }

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

    @Then("^Change the Journal to Pending from the Clinical Collections$")
    public void change_the_Journal_to_Pending_from_the_Clinical_Collections() throws Throwable {
        if(!tasks.verifyElementisDisplayed("XPATH",ResearchPackagesConstants.CHECK_IS_PENDING)){
            tasks.click("XPATH",ResearchPackagesConstants.PENDING_JOURNAL);
            researchPackagesTasks.pendingJournal();
            researchPackagesTasks.saveCollections();
        }
    }

    @Then("^Verify the status of the journal \"([^\"]*)\" is \"([^\"]*)\" in DB$")
    public void verify_the_status_of_the_journal_is_in_DB(String ISSN, String journalStatus) throws Throwable {
        sql = String.format(ResearchPackagesSQL.SELECT_RECENT_STATUS_UPDATED_JOURNAL_FROM_EPH_RESEARCH_PACKAGES, ISSN);
        Log.info(sql);
        List<Map<?, ?>> pendingJournalCollectionStatus = DBManager.getDBResultMap(sql, Constants.WFT_URL);
        String statusOfCollection = (String)pendingJournalCollectionStatus.get(0).get("JOURNAL_STATUS");
        Log.info(statusOfCollection);
        Assert.assertTrue("Status of the Journal is Pending",statusOfCollection.equalsIgnoreCase(journalStatus));
        tasks.closeBrowser();
    }
}
