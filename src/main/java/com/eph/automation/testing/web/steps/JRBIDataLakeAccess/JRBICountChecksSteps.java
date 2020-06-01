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
    private static String JRBISQLSourceCount;
    private static int JRBISourceCount;
    private static String JRBICurrentSQLCount;
    private static String JRBICurrentHistorySQLCount;
    private static int JRBICurrentCount;
    private static int JRBICurrentHistoryCount;
    private static String JRBIDiffSQLCount;
    private static int JRBIDiffCount;
    private static String JRBIExclSQLCount;
    private static int JRBIExclCount;
    private static String JRBISumSQLCount;
    private static int JRBISumCount;
    private static String JRBILatestSQLCount;
    private static int JRBILatestCount;
    private static String JRBIDeltaCurrentSQLCount;
    private static int JRBIDeltaCurrentCount;
    private static String JRBIDiffCurrentPreviousSQLCount;
    private static int JRBIDiffCurrentPreviousCount;


    @Given("^Get the total count of JRBI Data from Full Load (.*)$")
    public void getJRBIFullLoadCount(String tableName) {
        switch (tableName){
            case "jrbi_transform_current_work":
                Log.info("Getting Source Table Count for Work...");
                JRBISQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_SOURCE_COUNT;
                break;
            case "jrbi_transform_current_manifestation":
                Log.info("Getting Source Table Count for Manif...");
                JRBISQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_SOURCE_COUNT;
                break;
            case "jrbi_transform_current_person":
                Log.info("Getting Source Table Count for Person...");
                JRBISQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PERSON_SOURCE_COUNT;
                break;
        }
        Log.info(JRBISQLSourceCount);
        List<Map<String, Object>> JRBISourceTableCount = DBManager.getDBResultMap(JRBISQLSourceCount, Constants.AWS_URL);
        JRBISourceCount = ((Long) JRBISourceTableCount.get(0).get("Source_Count")).intValue();
    }

    @Given("^We know the total count of JRBI data from (.*)$")
    public void getCountfromCurrentTransformTables(String tableName){
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
            case "jrbi_transform_previous_work":
                Log.info("Getting Previous Work Table Count...");
                JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_WORK_COUNT;
                break;
            case "jrbi_transform_previous_manifestation":
                Log.info("Getting Previous Manifestation Table Count...");
                JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_MANIF_COUNT;
                break;
            case "jrbi_transform_previous_person":
                Log.info("Getting Previous Person Table Count...");
                JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_PERSON_COUNT;
                break;

            case "jrbi_delta_current_work":
                Log.info("Getting Delta Current Work Table Count...");
                JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_WORK_COUNT;
                break;

            case "jrbi_delta_current_manifestation":
                Log.info("Getting Delta Current Work Table Count...");
                JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_MANIF_COUNT;
                break;

            case "jrbi_delta_current_person":
                Log.info("Getting Delta Current Work Table Count...");
                JRBICurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_PERSON_COUNT;
                break;


        }
        Log.info(JRBICurrentSQLCount);
        List<Map<String, Object>> JRBICurrentTableCount = DBManager.getDBResultMap(JRBICurrentSQLCount, Constants.AWS_URL);
        JRBICurrentCount = ((Long) JRBICurrentTableCount.get(0).get("Current_Count")).intValue();
    }

    @And("^The count of source table and (.*) table are identical$")
    public void compareFullLoadCurrentCount(String tableName){
        Log.info("The count for table jrbi_journal_data_full => " + JRBISourceCount + " and in "+tableName+" => " + JRBICurrentCount);
        Assert.assertEquals("The counts are not equal when compared with jrbi_journal_data_full and "+tableName, JRBICurrentCount, JRBISourceCount);
    }

    @Then("^Get the JRBI (.*) history data count$")
    public void getJRBICurrentHistoryTableCount(String tableName){
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

            case "jrbi_transform_delta_work_history_part":
                Log.info("Getting Delta Current Work History Table Count...");
                JRBICurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_WORK_HISTORY_COUNT;
                break;

            case "jrbi_transform_delta_manifestation_history_part":
                Log.info("Getting Delta Current Manifest History Table Count...");
                JRBICurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_MANIF_HISTORY_COUNT;
                break;

            case "jrbi_transform_delta_person_history_part":
                Log.info("Getting Delta Current Manifest History Table Count...");
                JRBICurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_PERSON_HISTORY_COUNT;
                break;


        }
        Log.info("Current Date=> "+currentDate);
        Log.info(JRBICurrentHistorySQLCount);
        List<Map<String, Object>> JRBICurrentHistoryTableCount = DBManager.getDBResultMap(JRBICurrentHistorySQLCount, Constants.AWS_URL);
        JRBICurrentHistoryCount = ((Long)JRBICurrentHistoryTableCount.get(0).get("Current_History_Count")).intValue();
    }

    @Then("^We Get the JRBI (.*) previous history data count$")
    public void getJRBIPreviousHistoryTableCount(String tableName) {
        Calendar cal = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        cal.add(Calendar.DATE, -1);
        String yesterdayDate = dateFormat.format(cal.getTime());
        switch (tableName) {
            case "jrbi_temp_transform_current_work_history_part":
                Log.info("Getting Current Work History Table Count...");
                JRBICurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_WORK_HISTORY_COUNT;
                break;

            case "jrbi_temp_transform_current_manifestation_history_part":
                Log.info("Getting Current Manifestation History Table Count...");
                JRBICurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_MANIF_HISTORY_COUNT;
                break;

            case "jrbi_temp_transform_current_person_history_part":
                Log.info("Getting Current Person History Table Count...");
                JRBICurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_PERSON_HISTORY_COUNT;
                break;

        }
        Log.info("Previous Date => " + yesterdayDate);
        Log.info(JRBICurrentHistorySQLCount);
        List<Map<String, Object>> JRBICurrentHistoryTableCount = DBManager.getDBResultMap(JRBICurrentHistorySQLCount, Constants.AWS_URL);
        JRBICurrentHistoryCount = ((Long) JRBICurrentHistoryTableCount.get(0).get("Current_History_Count")).intValue();
    }

    @And("^Compare count (.*) table and (.*) are identical$")
    public void compareCount(String SrctableName,String trgttableName){
        Log.info("The count for table " + SrctableName + " => " + JRBICurrentCount + " and in "+trgttableName+" => " + JRBICurrentHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with "+SrctableName+" and "+trgttableName, JRBICurrentHistoryCount, JRBICurrentCount);
    }

    @Given("^Get the total count difference between delta current and transform current history Table (.*)$")
    public void countDifferenceSourceTables(String targetTable ){

        Calendar cal = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String currentDate= dateFormat.format(cal.getTime());

        switch (targetTable){
            case "jrbi_transform_history_person_excl_delta":
                Log.info("Getting Count by finding the difference b/w delta_current_person and person_history... ");
                JRBIDiffSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_DIFF_PERSON_HISTORY_AND_DELTA_PERSON;
                break;
            case "jrbi_transform_history_work_excl_delta":
                Log.info("Getting Count by finding the difference b/w delta_current_work and work_history... ");
                JRBIDiffSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_DIFF_WORK_HISTORY_AND_DELTA_WORK;
                break;
            case "jrbi_transform_history_manifestation_excl_delta":
                Log.info("Getting Count by finding the difference b/w delta_current_manif and manif_history... ");
                JRBIDiffSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_DIFF_MANIF_HISTORY_AND_DELTA_MANIF;
                break;
        }

        Log.info("Current Date => " + currentDate);
        Log.info(JRBIDiffSQLCount);
        List<Map<String, Object>> JRBIDiffTableCount = DBManager.getDBResultMap(JRBIDiffSQLCount, Constants.AWS_URL);
        JRBIDiffCount = ((Long) JRBIDiffTableCount.get(0).get("source_count")).intValue();

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

    @Then("^Get the JRBI (.*) delta current data count$")
    public void deltaCurrentCounts(String targetTable){
        switch (targetTable){
        case "jrbi_delta_current_work":
        Log.info("Getting Delta Current Work Table Count...");
        JRBIDeltaCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_COUNT_DELTA_WORK;
        break;

        case "jrbi_delta_current_manifestation":
        Log.info("Getting Delta Current Work Table Count...");
            JRBIDeltaCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_COUNT_DELTA_MANIF;
        break;

        case "jrbi_delta_current_person":
        Log.info("Getting Delta Current Work Table Count...");
            JRBIDeltaCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_COUNT_DELTA_PERSON;
        break;


    }
    Log.info(JRBIDeltaCurrentSQLCount);
    List<Map<String, Object>> JRBIDeltaCurrentTableCount = DBManager.getDBResultMap(JRBIDeltaCurrentSQLCount, Constants.AWS_URL);
    JRBIDeltaCurrentCount = ((Long) JRBIDeltaCurrentTableCount.get(0).get("Target_Count")).intValue();
    }

    @Then("^Get the JRBI (.*) latest data count$")
    public void latestCounts(String targetTable){
        switch (targetTable){
            case "jrbi_transform_latest_person":
                Log.info("Getting Count from exclude person... ");
                JRBILatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PERSON_LATEST_COUNT;
                break;
            case "jrbi_transform_latest_work":
                Log.info("Getting Count from exclude work... ");
                JRBILatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_LATEST_COUNT;
                break;
            case "jrbi_transform_latest_manifestation":
                Log.info("Getting Count from exclude manifestation... ");
                JRBILatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_LATEST_COUNT;
                break;
        }
        Log.info(JRBILatestSQLCount);
        List<Map<String, Object>> JRBILatestTableCount = DBManager.getDBResultMap(JRBILatestSQLCount, Constants.AWS_URL);
        JRBILatestCount = ((Long) JRBILatestTableCount.get(0).get("Target_Count")).intValue();
    }


    @Then("^Get the JRBI (.*) exclude data count$")
    public void excludeCounts(String targetTable){
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
        JRBIExclCount = ((Long) JRBIExclTableCount.get(0).get("Target_Count")).intValue();
    }

    @And("^Compare exclude count of (.*) and (.*) with (.*) are identical$")
    public void compareExclcounts(String srcTableOne, String srcTableTwo, String trgtTable){
        Log.info("The counts from the difference of tables " + srcTableOne + " and "+srcTableTwo+" => " + JRBIDiffCount + " and in "+trgtTable+" => " + JRBIExclCount);
        Assert.assertEquals("The counts are not equal when compared difference of "+srcTableOne+" and "+srcTableTwo+" with "+trgtTable, JRBIExclCount, JRBIDiffCount);
    }

    @And("^Compare delta count of (.*) and (.*) with (.*) are identical$")
    public void compareCurrentDeltacounts(String srcTableOne, String srcTableTwo, String trgtTable){
        Log.info("The counts from the difference of tables " + srcTableOne + " and "+srcTableTwo+" => " + JRBIDiffCurrentPreviousCount + " and in "+trgtTable+" => " + JRBIDeltaCurrentCount);
        Assert.assertEquals("The counts are not equal when compared difference of "+srcTableOne+" and "+srcTableTwo+" with "+trgtTable, JRBIDeltaCurrentCount, JRBIDiffCurrentPreviousCount);
    }


    @And("^Compare latest counts of (.*) and (.*) with (.*) are identical$")
    public void compareLatestcounts(String srcTableOne, String srcTableTwo, String trgtTable){
        Log.info("The counts from the addition of tables " + srcTableOne + " and "+srcTableTwo+" => " + JRBISumCount + " and in "+trgtTable+" => " + JRBILatestCount);
        Assert.assertEquals("The counts are not equal when compared addition of "+srcTableOne+" and "+srcTableTwo+" with "+trgtTable, JRBILatestCount, JRBISumCount);
    }
}
