package com.eph.automation.testing.web.steps.JRBIDataLakeAccess;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLAccessObject;
import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIDataLakeCountChecksSQL;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import com.google.common.base.Joiner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
    private static String JRBIIssnRecSQLCount;
    private static String JRBIPreviousSQLCount;
    private static int JRBIPreviousCount;
    private static String JRBIPreviousHistorySQLCount;
    private static int JRBIPreviousHistoryCount;
    private static String JRBIDeltaCurrentSQLCount;
    private static String JRBIDeltaCurrentHistorySQLCount;
    private static String JRBIDuplicateLatestSQLCount;
    private static int JRBIDeltaCurrentCount;
    private static int JRBIDeltaCurrentHistoryCount;
    private static int JRBIDuplicateLatestCount;
    private static int JRBIIssntotalCount;
    private static String sql;
    private static int totalCountIssnIds;
    static ArrayList<String> IssnIds = new ArrayList<String>();
    static String[] issnIds;
    static List<String>IssnIdList = new ArrayList<String>();
    static Object[][] expectedIdentifiersFields;
    static Object[][] loadedIdentifiersFields;



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


    @Given("^Get the Duplicate count in (.*) table$")
    public void getDuplicateCount(String tableName){
        switch (tableName){
            case "jrbi_transform_latest_work":
                Log.info("Getting Duplicate Work Latest Table Count...");
                JRBIDuplicateLatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DUPLICATE_WORK_LAtest_COUNT;
                break;

            case "jrbi_transform_latest_manifestation":
                Log.info("Getting Duplicate Manifest Latest Table Count...");
                JRBIDuplicateLatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DUPLICATE_MANIF_LATEST_COUNT;
                break;

            case "jrbi_transform_latest_person":
                Log.info("Getting Duplicate person Latest Table Count...");
                JRBIDuplicateLatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DUPLICATE_PERSON_LATEST_COUNT;
                break;
        }
        Log.info(JRBIDuplicateLatestSQLCount);
        List<Map<String, Object>> JRBIDupLatestTableCount = DBManager.getDBResultMap(JRBIDuplicateLatestSQLCount, Constants.AWS_URL);
        JRBIDuplicateLatestCount = ((Long) JRBIDupLatestTableCount.get(0).get("Duplicate_Count")).intValue();
    }

    @Then("^Check the count should be equal to Zero (.*)$")
    public void checkDupCountZero(String tableName){
        Log.info("The Duplicate count for "+tableName+" => " + JRBIDuplicateLatestCount);
        Assert.assertEquals("There are Duplicate Count of EPR IDs in "+tableName,0,JRBIDuplicateLatestCount);

    }

   /* @Given("^Get the total count of JRBI Data from source file (.*)(.*)$")
    public void readSourceFile(String fileLocation, String fileName) throws Throwable{
        String splitBy = ",";
        String csvfile = fileLocation+fileName;
        BufferedReader br = new BufferedReader(new FileReader(csvfile));
        String line = "";
        while ((line = br.readLine()) != null) {
            while ((line = br.readLine()) != null) {
                IssnIds.add(line.toString());
            }
        }
        for (int i = 0; i < IssnIds.size(); i++) {
            issnIds = IssnIds.get(i).split(splitBy);
            IssnIdList.add(issnIds[1]);
        }
        totalCountIssnIds = IssnIdList.size();
    }*/

    @Then ("^Get total count of JRBI Data from the table (.*)$")
    public void readfromDb (String tablename){
        Log.info("Getting total issn ids Count from..."+tablename);
        JRBIIssnRecSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_ISSN_COUNT_FULL_LOAD;
        Log.info(JRBIIssnRecSQLCount);
        List<Map<String, Object>> JRBIIssnTableFullloadCount = DBManager.getDBResultMap(JRBIIssnRecSQLCount, Constants.AWS_URL);
        JRBIIssntotalCount = ((Long) JRBIIssnTableFullloadCount.get(0).get("issnCount")).intValue();
    }

    @And("^Compare count of JRBI data between (.*) and source file$")
    public void compareCounts(String tableName){
        Log.info("The count in excel file: " + totalCountIssnIds + " and in table " +tableName+":"+ JRBIIssntotalCount);
        Assert.assertEquals("The counts are not equal when compared with excel"+totalCountIssnIds+" and DB "+JRBIIssntotalCount, totalCountIssnIds, JRBIIssntotalCount);
    }

    @Given ("^Get the data of JRBI Data from source file (.*)(.*)$")
    public void readSourceFileData(String fileLocation, String fileName) throws Throwable {
        String splitBy = ",";
        String csvfile = fileLocation + fileName;
        BufferedReader br = new BufferedReader(new FileReader(csvfile));
        String line = "";
        while ((line = br.readLine()) != null) {
            while ((line = br.readLine()) != null) {
                IssnIds.add(line.toString());
            }
        }
        for (int i = 0; i < 5; i++) {
            issnIds = IssnIds.get(i).split(splitBy);
            IssnIdList.add(issnIds[1]);
        }
        sql = String.format(JRBIDataLakeCountChecksSQL.GET_JRBI_FULL_LOAD_DATA_REC, Joiner.on("','").join(IssnIdList));
        JRBIAccessDLContext.recordsFromExcel = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
        for (int i = 0; i < JRBIAccessDLContext.recordsFromExcel.size(); i++) {
            String[] RowValues = IssnIds.get(i).split(splitBy);
            expectedIdentifiersFields = new Object[][]{{"issn", RowValues[1]}, {"title", RowValues[0]}, {"journal_number", RowValues[2]},
                    {"journal_acronym", RowValues[3]}};
            loadedIdentifiersFields =
                    new Object[][]{{"issn", JRBIAccessDLContext.recordsFromExcel.get(i).getissn()}, {"title", JRBIAccessDLContext.recordsFromExcel.get(i).gettitle()},
                            {"journal_number", JRBIAccessDLContext.recordsFromExcel.get(i).getjournal_number()}, {"journal_acronym", JRBIAccessDLContext.recordsFromExcel.get(i).getjournal_acronym()}};

            for (int j = 0; j < loadedIdentifiersFields.length; j++) {
                Log.info(loadedIdentifiersFields[j][0] + " = DB => " + loadedIdentifiersFields[j][1] + " CSV => " + expectedIdentifiersFields[j][1]);
                String ExcelVal = String.valueOf(expectedIdentifiersFields[j][1]);
                String DBVal = String.valueOf(loadedIdentifiersFields[j][1]);
                Assert.assertEquals("Expected all columns to be with correct values loaded but for issn = "
                                + JRBIAccessDLContext.recordsFromExcel.get(i).getissn() + " there are not equal values for the field " + expectedIdentifiersFields[j][0],
                        ExcelVal, DBVal);
            }
        }

    }
}
