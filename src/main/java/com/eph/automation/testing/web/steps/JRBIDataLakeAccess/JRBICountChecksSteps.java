package com.eph.automation.testing.web.steps.JRBIDataLakeAccess;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIDataLakeCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;


import java.util.List;
import java.util.Map;

public class JRBICountChecksSteps {
    private static String JRBIFullSQLSourceCount;
    private static int JRBIFullSourceCount;
    private static String JRBICurrentSQLCount;
    private static String JRBICurrentHistorySQLCount;
    private static int JRBICurrentCount;
    private static int JRBICurrentHistoryCount;
    private static String JRBIDiffDeltaCurrAndCurrHistSQLCount;
    private static int JRBIDiffDeltaCurrAndCurrHistCount;
    private static String JRBIExclSQLCount;
    private static int JRBIExclCount;
    private static String JRBISumSQLCount;
    private static int JRBISumCount;
    private static String JRBILatestSQLCount;
    private static int JRBILatestCount;
    private static String JRBIDiffCurrentPreviousSQLCount;
    private static int JRBIDiffCurrentPreviousCount;
    private static String JRBIPreviousSQLCount;
    private static int JRBIPreviousCount;
    private static String JRBIPreviousHistorySQLCount;
    private static int JRBIPreviousHistoryCount;
    private static String JRBIDeltaCurrentSQLCount;
    private static String JRBIDeltaCurrentHistorySQLCount;
    private static int JRBIDeltaCurrentCount;
    private static int JRBIDeltaCurrentHistoryCount;
    private static String JRBIworkExtendedSQLCount;
    private static int JRBIWorkExtCount;
    private static String JRBIManifExtendedSQLCount;
    private static String JRBIPersonExtendedSQLCount;
    private static String JRBIManifStitchingSQLCount;
    private static String JRBIWorkStitchingSQLCount;
    private static int JRBIManifExtCount;
    private static int JRBIPersonExtCount;
    private static int JRBIManifStchCount;
    private static int JRBIWorkStchCount;



    @Given("^Get the total count of JRBI Data from Full Load (.*)$")
    public void getJRBIFullLoadCount(String tableName) {
        switch (tableName){
            case "jrbi_transform_current_work":
                Log.info("Getting Source Table Count for Work...");
                JRBIFullSQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_SOURCE_COUNT;
                break;
            case "jrbi_transform_current_manifestation":
                Log.info("Getting Source Table Count for Manif...");
                JRBIFullSQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_SOURCE_COUNT;
                break;
            case "jrbi_transform_current_person":
                Log.info("Getting Source Table Count for Person...");
                JRBIFullSQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PERSON_SOURCE_COUNT;
                break;
        }
        Log.info(JRBIFullSQLSourceCount);
        List<Map<String, Object>> JRBIFullSourceTableCount = DBManager.getDBResultMap(JRBIFullSQLSourceCount, Constants.AWS_URL);
        JRBIFullSourceCount = ((Long) JRBIFullSourceTableCount.get(0).get("Source_Count")).intValue();
    }

    @Given("^We know the total count of Current JRBI data from (.*)$")
    public void getCountfromCurrentTables(String tableName){
        switch (tableName){
            case "jrbi_transform_current_work":
                Log.info("Getting Current Work Table Count...");
                JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_WORK_COUNT;
                break;

            case "jrbi_transform_current_manifestation":
                Log.info("Getting Current Manifestation Table Count...");
                JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_MANIF_COUNT;
                break;

            case "jrbi_transform_current_person":
                Log.info("Getting Current Person Table Count...");
                JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_PERSON_COUNT;
                break;

        }
        Log.info(JRBICurrentSQLCount);
        List<Map<String, Object>> JRBICurrentTableCount = DBManager.getDBResultMap(JRBICurrentSQLCount, Constants.AWS_URL);
        JRBICurrentCount = ((Long) JRBICurrentTableCount.get(0).get("Current_Count")).intValue();
    }

    @And("^Compare count of Full load with current (.*) table are identical$")
    public void compareFullAndCurrentCount(String tableName){
        Log.info("The count for table jrbi_journal_data_full => " + JRBIFullSourceCount + " and in "+tableName+" => " + JRBICurrentCount);
        Assert.assertEquals("The counts are not equal when compared with jrbi_journal_data_full and "+tableName, JRBICurrentCount, JRBIFullSourceCount);
    }

    @Then("^Get the count of transform_current_history (.*)$")
    public void getCountFromCurrentHistory(String tableName){
        Calendar cal = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String currentDate= dateFormat.format(cal.getTime());
        switch (tableName){
            case "jrbi_transform_current_work_history_part":
                Log.info("Getting Current Work History Table Count...");
                JRBICurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_WORK_HISTORY_COUNT;
                break;

            case "jrbi_transform_current_manifestation_history_part":
                Log.info("Getting Current Manifestation History Table Count...");
                JRBICurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_MANIF_HISTORY_COUNT;
                break;

            case "jrbi_transform_current_person_history_part":
                Log.info("Getting Current Person History Table Count...");
                JRBICurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_PERSON_HISTORY_COUNT;
                break;
        }
        Log.info("Current Date=> "+currentDate);
        Log.info(JRBICurrentHistorySQLCount);
        List<Map<String, Object>> JRBICurrentHistoryTableCount = DBManager.getDBResultMap(JRBICurrentHistorySQLCount, Constants.AWS_URL);
        JRBICurrentHistoryCount = ((Long)JRBICurrentHistoryTableCount.get(0).get("Current_History_Count")).intValue();
    }

    @And("^Check count of current table (.*) and current history (.*) are identical$")
    public void compareCurrentandCurrentHistoryCount(String SrctableName,String trgttableName){
        Log.info("The count for current table " + SrctableName + " => " + JRBICurrentCount + " and in current_history "+trgttableName+" => " + JRBICurrentHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with "+SrctableName+" and "+trgttableName, JRBICurrentHistoryCount, JRBICurrentCount);
    }

    @Given("^We know the total count of Previous tables from (.*)$")
    public void getCountFromPrevious(String tableName){
        switch (tableName) {
            case "jrbi_transform_previous_work":
                Log.info("Getting Previous Work Table Count...");
                JRBIPreviousSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_WORK_COUNT;
                break;
            case "jrbi_transform_previous_manifestation":
                Log.info("Getting Previous Manifestation Table Count...");
                JRBIPreviousSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_MANIF_COUNT;
                break;
            case "jrbi_transform_previous_person":
                Log.info("Getting Previous Person Table Count...");
                JRBIPreviousSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_PERSON_COUNT;
                break;
        }
        Log.info(JRBIPreviousSQLCount);
        List<Map<String, Object>> JRBIPreviousTableCount = DBManager.getDBResultMap(JRBIPreviousSQLCount, Constants.AWS_URL);
        JRBIPreviousCount = ((Long) JRBIPreviousTableCount.get(0).get("Previous_Count")).intValue();
    }


    @Then("^We Get the JRBI (.*) previous history data count$")
    public void getJRBIPreviousHistoryCount(String tableName) {
        Calendar cal = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        cal.add(Calendar.DATE, -1);
        String yesterdayDate = dateFormat.format(cal.getTime());
        //note there are no previous_history table, only current_history table fetching with previous date
        switch (tableName) {
            case "jrbi_transform_previous_work_history_part":
                Log.info("Getting Previous Work History Table Count...");
                JRBIPreviousHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_WORK_HISTORY_COUNT;
                break;

            case "jrbi_transform_previous_manifestation_history_part":
                Log.info("Getting Previous Manifestation History Table Count...");
                JRBIPreviousHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_MANIF_HISTORY_COUNT;
                break;

            case "jrbi_transform_previous_person_history_part":
                Log.info("Getting Previous Person History Table Count...");
                JRBIPreviousHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_PERSON_HISTORY_COUNT;
                break;

        }
        Log.info("Previous Date => " + yesterdayDate);
        Log.info(JRBIPreviousHistorySQLCount);
        List<Map<String, Object>> JRBIPreviousHistoryTableCount = DBManager.getDBResultMap(JRBIPreviousHistorySQLCount, Constants.AWS_URL);
        JRBIPreviousHistoryCount = ((Long) JRBIPreviousHistoryTableCount.get(0).get("Previous_History_Count")).intValue();
    }

    @And("^Check count of previous table (.*) and previous history (.*) are identical$")
    public void comparePreviousandPreviousHistoryCount(String SrctableName, String trgttableName){
        Log.info("The count for previous table " + SrctableName + " => " + JRBIPreviousCount + " and in previous_history "+trgttableName+" => " + JRBIPreviousHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with "+SrctableName+" and "+trgttableName, JRBIPreviousHistoryCount, JRBIPreviousCount);
    }


    @Given("^Get the total count difference between delta current and transform current history Table (.*)$")
    public void countDiffoFDeltaCurrentAndCurrentHistory(String targetTable ){
        Calendar cal = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String currentDate= dateFormat.format(cal.getTime());

        switch (targetTable){
            case "jrbi_transform_history_person_excl_delta":
                Log.info("Getting Count by finding the difference b/w delta_current_person and person_history... ");
                JRBIDiffDeltaCurrAndCurrHistSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_DIFF_PERSON_HISTORY_AND_DELTA_PERSON;
                break;
            case "jrbi_transform_history_work_excl_delta":
                Log.info("Getting Count by finding the difference b/w delta_current_work and work_history... ");
                JRBIDiffDeltaCurrAndCurrHistSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_DIFF_WORK_HISTORY_AND_DELTA_WORK;
                break;
            case "jrbi_transform_history_manifestation_excl_delta":
                Log.info("Getting Count by finding the difference b/w delta_current_manif and manif_history... ");
                JRBIDiffDeltaCurrAndCurrHistSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_DIFF_MANIF_HISTORY_AND_DELTA_MANIF;
                break;
        }

        Log.info("Current Date => " + currentDate);
        Log.info(JRBIDiffDeltaCurrAndCurrHistSQLCount);
        List<Map<String, Object>> JRBIDiffDeltaCurrAndCurrHistTableCount = DBManager.getDBResultMap(JRBIDiffDeltaCurrAndCurrHistSQLCount, Constants.AWS_URL);
        JRBIDiffDeltaCurrAndCurrHistCount = ((Long) JRBIDiffDeltaCurrAndCurrHistTableCount.get(0).get("source_count")).intValue();
    }


    @Then("^Get the JRBI (.*) exclude data count$")
    public void getExcludeTableCounts(String targetTable){
        switch (targetTable){
            case "jrbi_transform_history_person_excl_delta":
                Log.info("Getting Count from exclude person... ");
                JRBIExclSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PERSON_EXCL_COUNT;
                break;
            case "jrbi_transform_history_work_excl_delta":
                Log.info("Getting Count from exclude work... ");
                JRBIExclSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_EXCL_COUNT;
                break;
            case "jrbi_transform_history_manifestation_excl_delta":
                Log.info("Getting Count from exclude manifestation... ");
                JRBIExclSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_EXCL_COUNT;
                break;
        }
        Log.info(JRBIExclSQLCount);
        List<Map<String, Object>> JRBIExclTableCount = DBManager.getDBResultMap(JRBIExclSQLCount, Constants.AWS_URL);
        JRBIExclCount = ((Long) JRBIExclTableCount.get(0).get("Excl_Count")).intValue();
    }


    @And("^Compare exclude count of (.*) and (.*) with (.*) are identical$")
    public void compareExclcounts(String srcTableOne, String srcTableTwo, String trgtTable){
        Log.info("The counts from the difference of tables " + srcTableOne + " and "+srcTableTwo+" => " + JRBIDiffDeltaCurrAndCurrHistCount + " and in "+trgtTable+" => " + JRBIExclCount);
        Assert.assertEquals("The counts are not equal when compared difference of "+srcTableOne+" and "+srcTableTwo+" with "+trgtTable, JRBIExclCount, JRBIDiffDeltaCurrAndCurrHistCount);
    }

    @Given("^Get the sum of total count between delta current and and Current_Exclude Table (.*)$")
    public void countsumOfDeltacurrentandExclude(String table){

        switch (table){
            case "jrbi_transform_latest_person":
                Log.info("Getting Count by sum of delta_current_person and current_exclude_person... ");
                JRBISumSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_SUM_DELTA_PERSON_AND_PERSON_HISTORY;
                break;
            case "jrbi_transform_latest_work":
                Log.info("Getting Count by sum of delta_current_work and current_exclude_work... ");
                JRBISumSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_SUM_DELTA_WORK_AND_WORK_HISTORY;
                break;
            case "jrbi_transform_latest_manifestation":
                Log.info("Getting Count by sum of delta_current_manifestation and current_exclude_manifestation... ");
                JRBISumSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_SUM_DELTA_MANIF_AND_MANIF_EXCLUDE;
                break;
        }

        // Log.info("Current Date => " + currentDate);
        Log.info(JRBISumSQLCount);
        List<Map<String, Object>> JRBIsUMTableCount = DBManager.getDBResultMap(JRBISumSQLCount, Constants.AWS_URL);
        JRBISumCount = ((Long) JRBIsUMTableCount.get(0).get("source_count")).intValue();
    }

    @Then("^Get the JRBI (.*) latest data count$")
    public void latestCounts(String targetTable){
        switch (targetTable){
            case "jrbi_transform_latest_person":
                Log.info("Getting Count from Latest person... ");
                JRBILatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PERSON_LATEST_COUNT;
                break;
            case "jrbi_transform_latest_work":
                Log.info("Getting Count from Latest work... ");
                JRBILatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_LATEST_COUNT;
                break;
            case "jrbi_transform_latest_manifestation":
                Log.info("Getting Count from Latest manifestation... ");

                JRBILatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_LATEST_COUNT;
                break;
        }
        Log.info(JRBILatestSQLCount);
        List<Map<String, Object>> JRBILatestTableCount = DBManager.getDBResultMap(JRBILatestSQLCount, Constants.AWS_URL);
        JRBILatestCount = ((Long) JRBILatestTableCount.get(0).get("Target_Count")).intValue();
    }

    @And("^Compare latest counts of (.*) and (.*) with (.*) are identical$")
    public void compareLatestcounts(String srcTableOne, String srcTableTwo, String trgtTable){
        Log.info("The counts from the addition of tables " + srcTableOne + " and "+srcTableTwo+" => " + JRBISumCount + " and in "+trgtTable+" => " + JRBILatestCount);
        Assert.assertEquals("The counts are not equal when compared addition of "+srcTableOne+" and "+srcTableTwo+" with "+trgtTable, JRBILatestCount, JRBISumCount);
    }

    @Given("^Get the difference of total count between current and previous Table (.*)$")
    public void countDiffofCurrentAndPrevious(String table){

        switch (table){
            case "jrbi_delta_current_work":
                Log.info("Getting Count from difference of current_work and previous_work...");
                JRBIDiffCurrentPreviousSQLCount = JRBIDataLakeCountChecksSQL.GET_COUNT_DIFF_CURRENT_PREVIOUS_WORK;
                break;
            case "jrbi_delta_current_manifestation":
                Log.info("Getting Count from difference of current_manifestation and previous_manifestation...");
                JRBIDiffCurrentPreviousSQLCount = JRBIDataLakeCountChecksSQL.GET_COUNT_DIFF_CURRENT_PREVIOUS_MANIF;
                break;
            case "jrbi_delta_current_person":
                Log.info("Getting Count from difference of current_person and previous_person...");
                JRBIDiffCurrentPreviousSQLCount = JRBIDataLakeCountChecksSQL.GET_COUNT_DIFF_CURRENT_PREVIOUS_PERSON;
                break;
        }
        // Log.info("Current Date => " + currentDate);
        Log.info(JRBIDiffCurrentPreviousSQLCount);
        List<Map<String, Object>> JRBIDiffCurrentPreviousTableCount = DBManager.getDBResultMap(JRBIDiffCurrentPreviousSQLCount, Constants.AWS_URL);
        JRBIDiffCurrentPreviousCount = ((Long) JRBIDiffCurrentPreviousTableCount.get(0).get("source_count")).intValue();
    }

    @And("^Compare delta count of (.*) and (.*) with (.*) are identical$")
    public void compareCurrentDeltacounts(String srcTableOne, String srcTableTwo, String trgtTable){
        Log.info("The counts from the difference of tables " + srcTableOne + " and "+srcTableTwo+" => " + JRBIDiffCurrentPreviousCount + " and in "+trgtTable+" => " + JRBIDeltaCurrentCount);
        Assert.assertEquals("The counts are not equal when compared difference of "+srcTableOne+" and "+srcTableTwo+" with "+trgtTable, JRBIDeltaCurrentCount, JRBIDiffCurrentPreviousCount);
    }

    @Given("^We know the delta current count for tables (.*)$")
    public void getDeltaCurrentCount(String tableName){
        switch (tableName) {
            case "jrbi_delta_current_work":
                Log.info("Getting Delta Current Work Table Count...");
                JRBIDeltaCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_WORK_COUNT;
                break;

            case "jrbi_delta_current_manifestation":
                Log.info("Getting Delta Current Manifestation Table Count...");
                JRBIDeltaCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_MANIF_COUNT;
                break;

            case "jrbi_delta_current_person":
                Log.info("Getting Delta Current Person Table Count...");
                JRBIDeltaCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_PERSON_COUNT;
                break;
        }
        Log.info(JRBIDeltaCurrentSQLCount);
        List<Map<String, Object>> JRBIDeltaCurrentTableCount = DBManager.getDBResultMap(JRBIDeltaCurrentSQLCount, Constants.AWS_URL);
        JRBIDeltaCurrentCount = ((Long) JRBIDeltaCurrentTableCount.get(0).get("Delta_Current_Count")).intValue();
    }

    @Then("^Get the count of delta current history (.*) table$")
    public void getDeltaCurrentHistoryCount(String tableName){
        switch (tableName){
            case "jrbi_transform_delta_work_history_part":
                Log.info("Getting Delta Current Work History Table Count...");
                JRBIDeltaCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_WORK_HISTORY_COUNT;
                break;

            case "jrbi_transform_delta_manifestation_history_part":
                Log.info("Getting Delta Current Manifest History Table Count...");
                JRBIDeltaCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_MANIF_HISTORY_COUNT;
                break;

            case "jrbi_transform_delta_person_history_part":
                Log.info("Getting Delta Current person History Table Count...");

                JRBIDeltaCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_PERSON_HISTORY_COUNT;
                break;
        }
        Log.info(JRBIDeltaCurrentHistorySQLCount);
        List<Map<String, Object>> JRBIDeltaCurrentTableCount = DBManager.getDBResultMap(JRBIDeltaCurrentHistorySQLCount, Constants.AWS_URL);
        JRBIDeltaCurrentHistoryCount = ((Long) JRBIDeltaCurrentTableCount.get(0).get("Delta_History_Count")).intValue();
    }

    @And("^Compare delta current (.*) table and delta history (.*) are identical$")
    public void compareDeltaCurrentandDeltaHistoryCount(String SrctableName, String trgttableName){
        Log.info("The count for Delta table " + SrctableName + " => " + JRBIDeltaCurrentCount + " and in delta_history "+trgttableName+" => " + JRBIDeltaCurrentHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with "+SrctableName+" and "+trgttableName, JRBIDeltaCurrentHistoryCount, JRBIDeltaCurrentCount);
    }


    @Given("^Get the total count of work latest table$")
        public void getLatestWorkCount(){
        Log.info("Getting Count from Latest work... ");

        JRBILatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_LATEST_COUNT;
        Log.info(JRBILatestSQLCount);
        List<Map<String, Object>> JRBILatestTableCount = DBManager.getDBResultMap(JRBILatestSQLCount, Constants.AWS_URL);
        JRBILatestCount = ((Long) JRBILatestTableCount.get(0).get("Target_Count")).intValue();
    }

    @Then("^Get the total count of work extended table$")
    public void totalCountworkExtended(){
        Log.info("Getting Work Extended Table Count...");
        JRBIworkExtendedSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_EXTENDED_COUNT;
        Log.info(JRBIworkExtendedSQLCount);
        List<Map<String, Object>> JRBIWorkExtTableCount = DBManager.getDBResultMap(JRBIworkExtendedSQLCount, Constants.AWS_URL);
        JRBIWorkExtCount = ((Long) JRBIWorkExtTableCount.get(0).get("WORK_EXTENDED_COUNT")).intValue();
    }

    @And("^Compare the counts of work latest and work extended table are identical$")
    public void compareworkLatestExtended() {
        Log.info("The count for Work LAtest table => " + JRBILatestCount + " and in Work Extended => " + JRBIWorkExtCount);
        Assert.assertEquals("The counts are not equal when compared with Work Latest and Work Extended", JRBIWorkExtCount, JRBILatestCount);
    }

    @Given("^Get the total count of manif latest table$")
    public void getLatestManifCount(){
        Log.info("Getting Count from Latest manif... ");
        JRBILatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_LATEST_COUNT;
        Log.info(JRBILatestSQLCount);
        List<Map<String, Object>> JRBILatestTableCount = DBManager.getDBResultMap(JRBILatestSQLCount, Constants.AWS_URL);
        JRBILatestCount = ((Long) JRBILatestTableCount.get(0).get("Target_Count")).intValue();
    }

    @Then("^Get the total count of manif extended table$")
    public void totalCountManifExtended(){
        Log.info("Getting Manif Extended Table Count...");
        JRBIManifExtendedSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_EXTENDED_COUNT;
        Log.info(JRBIManifExtendedSQLCount);
        List<Map<String, Object>> JRBIManifExtTableCount = DBManager.getDBResultMap(JRBIManifExtendedSQLCount, Constants.AWS_URL);
        JRBIManifExtCount = ((Long) JRBIManifExtTableCount.get(0).get("MANIF_EXTENDED_COUNT")).intValue();
    }

    @And("^Compare the counts of manif latest and manif extended table are identical$")
    public void compareManifLatestExtended() {
        Log.info("The count for Manifestation LAtest table => " + JRBILatestCount + " and in Manif Extended => " + JRBIManifExtCount);
        Assert.assertEquals("The counts are not equal when compared with Manif Latest and Manif Extended", JRBIManifExtCount, JRBILatestCount);

    }


    @Given("^Get the total count of person latest table$")
    public void getLatestPersonCount(){
        Log.info("Getting Count from Latest Person... ");
        JRBILatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PERSON_LATEST_COUNT;
        Log.info(JRBILatestSQLCount);
        List<Map<String, Object>> JRBILatestTableCount = DBManager.getDBResultMap(JRBILatestSQLCount, Constants.AWS_URL);
        JRBILatestCount = ((Long) JRBILatestTableCount.get(0).get("Target_Count")).intValue();
    }

    @Then("^Get the total count of person extended table$")
    public void totalCountPersonExtended(){
        Log.info("Getting Person Extended Table Count...");
        JRBIPersonExtendedSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PERSON_EXTENDED_COUNT;
        Log.info(JRBIPersonExtendedSQLCount);
        List<Map<String, Object>> JRBIPersonExtTableCount = DBManager.getDBResultMap(JRBIPersonExtendedSQLCount, Constants.AWS_URL);
        JRBIPersonExtCount = ((Long) JRBIPersonExtTableCount.get(0).get("PERSON_EXTENDED_COUNT")).intValue();
    }

    @And("^Compare the counts of person latest and person extended table are identical$")
    public void comparePersonLatestExtended() {
        Log.info("The count for Person LAtest table => " + JRBILatestCount + " and in Person Extended => " + JRBIPersonExtCount);
        Assert.assertEquals("The counts are not equal when compared with Person Latest and Person Extended", JRBIPersonExtCount, JRBILatestCount);

    }

    @Then("^Get the total count of stitching manif json table$")
    public void totalCountStchManif(){
        Log.info("Getting Manif Stitching Table Count...");
        JRBIManifStitchingSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_STITCHING_COUNT;
        Log.info(JRBIManifStitchingSQLCount);
        List<Map<String, Object>> JRBIManifStschTableCount = DBManager.getDBResultMap(JRBIManifStitchingSQLCount, Constants.EPH_URL);
        JRBIManifStchCount = ((Long) JRBIManifStschTableCount.get(0).get("MANIF_STCH_COUNT")).intValue();
    }

    @And("^Compare the counts of stitching manif json and manif extended table are identical$")
    public void compareStchManifandManifExt() {
        Log.info("The count for Manif Ext table => " + JRBIManifExtCount + " and in Manif Stitching => " + JRBIManifStchCount);
        Assert.assertEquals("The counts are not equal when compared with Manif Ext and Manif Stitching", JRBIManifExtCount, JRBIManifStchCount);

    }

    @Then("^Get the total count of stitching work json table$")
    public void totalCountStchWork(){
        Log.info("Getting Work Stitching Table Count...");
        JRBIWorkStitchingSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_STITCHING_COUNT;
        Log.info(JRBIWorkStitchingSQLCount);
        List<Map<String, Object>> JRBIWorkStschTableCount = DBManager.getDBResultMap(JRBIWorkStitchingSQLCount, Constants.EPH_URL);
        JRBIWorkStchCount = ((Long) JRBIWorkStschTableCount.get(0).get("WORK_STCH_COUNT")).intValue();
    }

    @And("^Compare the counts of stitching work json and work extended table are identical$")
    public void compareStchWorkandWorkExt() {
        Log.info("The count for Work Ext table => " + JRBIWorkExtCount + " and in Work Stitching => " + JRBIWorkStchCount);
        Assert.assertEquals("The counts are not equal when compared with Work Ext and Work Stitching", JRBIWorkExtCount, JRBIWorkStchCount);

    }

}
