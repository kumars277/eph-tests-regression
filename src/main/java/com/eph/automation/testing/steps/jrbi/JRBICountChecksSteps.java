package com.eph.automation.testing.steps.jrbi;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLAccessObject;
import com.eph.automation.testing.services.db.jrbisql.JRBIDataLakeCountChecksSQL;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

public class JRBICountChecksSteps {
    private static String jrbiFullSQLSourceCount;
    private static int jrbiFullSourceCount;
    private static String jrbiCurrentSQLCount;
    private static String jrbiCurrentHistorySQLCount;
    private static int jrbiCurrentCount;
    private static int jrbiCurrentHistoryCount;
    private static String jrbiDiffDeltaCurrAndCurrHistSQLCount;
    private static int jrbiDiffDeltaCurrAndCurrHistCount;
    private static String jrbiExclSQLCount;
    private static int jrbiExclCount;
    private static String jrbiSumSQLCount;
    private static int jrbiSumCount;
    private static String jrbiLatestSQLCount;
    private static int jrbiLatestCount;
    private static String jrbiDiffCurrentPreviousSQLCount;
    private static int jrbiDiffCurrentPreviousCount;
    private static String jrbiPreviousSqlCount;
    private static int jrbiPreviousCount;
    private static String jrbiPreviousHistorySqlCount;
    private static int jrbiPreviousHistoryCount;
    private static String jrbiDeltaCurrentSQLCount;
    private static String jrbiDeltaCurrentHistorySQLCount;
    private static String jrbiDuplicateLatestSQLCount;
    private static String jrbideltaCurrentCount;
    private static int jrbiDeltaCurrentCount;
    private static int jrbiDeltaCurrentHistoryCount;
    private static int jrbiDuplicateLatestCount;
    private static int jrbiIssntotalCount;
    private static int totalCountIssnIds;

    @Given("^Get the total count of JRBI Data from Full Load (.*)$")
    public static void getJRBIFullLoadCount(String tableName) {
        switch (tableName){
            case "jrbi_transform_current_work":
                Log.info("Getting Source Table Count for Work...");
                jrbiFullSQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_SOURCE_COUNT;
                break;
            case "jrbi_transform_current_manifestation":
                Log.info("Getting Source Table Count for Manif...");
                jrbiFullSQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_SOURCE_COUNT;
                break;
            case "jrbi_transform_current_person":
                Log.info("Getting Source Table Count for Person...");
                jrbiFullSQLSourceCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PERSON_SOURCE_COUNT;
                break;
             default:
                 Log.info("No table found");
        }
        Log.info(jrbiFullSQLSourceCount);
        List<Map<String, Object>> jrbiFullSourceTableCount = DBManager.getDBResultMap(jrbiFullSQLSourceCount, Constants.AWS_URL);
        jrbiFullSourceCount = ((Long) jrbiFullSourceTableCount.get(0).get("Source_Count")).intValue();
    }

    @Then("^we get the total count of Current JRBI data from (.*)$")
    public static void getCountfromCurrentTables(String tableName){
        switch (tableName){
            case "jrbi_transform_current_work":
                Log.info("Getting Current Work Table Count...");
                jrbiCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_WORK_COUNT;
                break;
            case "jrbi_transform_current_manifestation":
                Log.info("Getting Current Manifestation Table Count...");
                jrbiCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_MANIF_COUNT;
                break;
            case "jrbi_transform_current_person":
                Log.info("Getting Current Person Table Count...");
                jrbiCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_PERSON_COUNT;
                break;
            default:
                Log.info("tables not found");

        }
        Log.info(jrbiCurrentSQLCount);
        List<Map<String, Object>> jrbiCurrentTableCount = DBManager.getDBResultMap(jrbiCurrentSQLCount, Constants.AWS_URL);
        jrbiCurrentCount = ((Long) jrbiCurrentTableCount.get(0).get("Current_Count")).intValue();
    }

    @And("^we Compare count of Full load with current (.*) table are identical$")
    public void compareFullAndCurrentCount(String tableName){
        Log.info("The count for table jrbi_journal_data_full => " + jrbiFullSourceCount + " and in "+tableName+" => " + jrbiCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with jrbi_journal_data_full and "+tableName, jrbiCurrentCount, jrbiFullSourceCount);
    }

    @Then("^Get the count of transform_current_history (.*)$")
    public static void getCountFromCurrentHistory(String tableName){
        Calendar cal = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String currentDate= dateFormat.format(cal.getTime());
        switch (tableName){
            case "jrbi_transform_current_work_history_part":
                Log.info("Getting Current Work History Table Count...");
                jrbiCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_WORK_HISTORY_COUNT;
                break;
            case "jrbi_transform_current_manifestation_history_part":
                Log.info("Getting Current Manifestation History Table Count...");
                jrbiCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_MANIF_HISTORY_COUNT;
                break;
            case "jrbi_transform_current_person_history_part":
                Log.info("Getting Current Person History Table Count...");
                jrbiCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_PERSON_HISTORY_COUNT;
                break;
            case "jrbi_transform_work_file_history_part":
                Log.info("Getting Current work file Table Count...");
                jrbiCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_WORK_FILE_COUNT;
                break;
            case "jrbi_transform_manifestation_file_history_part":
                Log.info("Getting Current manif file Table Count...");
                jrbiCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_MANIF_FILE_COUNT;
                break;
            case "jrbi_transform_person_file_history_part":
                Log.info("Getting Current Person file Table Count...");
                jrbiCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_PERSON_FILE_COUNT;
                break;
             default:
                 Log.info("no tables");
        }
        Log.info("Current Date=> "+currentDate);
        Log.info(jrbiCurrentHistorySQLCount);
        List<Map<String, Object>> jrbiCurrentHistoryTableCount = DBManager.getDBResultMap(jrbiCurrentHistorySQLCount, Constants.AWS_URL);
        jrbiCurrentHistoryCount = ((Long)jrbiCurrentHistoryTableCount.get(0).get("Current_History_Count")).intValue();
    }

    @And("^Check count of current table (.*) and current history (.*) are identical$")
    public void compareCurrentandCurrentHistoryCount(String srctableName,String trgttableName){
        Log.info("The count for " + srctableName + " => " + jrbiCurrentCount + " and in "+trgttableName+" => " + jrbiCurrentHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctableName+" and "+trgttableName, jrbiCurrentHistoryCount, jrbiCurrentCount);
    }

    @Given("^We know the total count of Previous tables from (.*)$")
    public static void getCountFromPrevious(String tableName){
        switch (tableName) {
            case "jrbi_transform_previous_work":
                Log.info("Getting Previous Work Table Count...");
                jrbiPreviousSqlCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_WORK_COUNT;
                break;
            case "jrbi_transform_previous_manifestation":
                Log.info("Getting Previous Manifestation Table Count...");
                jrbiPreviousSqlCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_MANIF_COUNT;
                break;
            case "jrbi_transform_previous_person":
                Log.info("Getting Previous Person Table Count...");
                jrbiPreviousSqlCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_PERSON_COUNT;
                break;
             default:
                 Log.info("table not found");
        }
        Log.info(jrbiPreviousSqlCount);
        List<Map<String, Object>> jrbiPreviousTableCount = DBManager.getDBResultMap(jrbiPreviousSqlCount, Constants.AWS_URL);
        jrbiPreviousCount = ((Long) jrbiPreviousTableCount.get(0).get("Previous_Count")).intValue();
    }


    @Then("^We Get the JRBI (.*) previous history data count$")
    public static void getjrbiPreviousHistoryCount(String tableName) {
        Calendar cal = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        cal.add(Calendar.DATE, -1);
        String yesterdayDate = dateFormat.format(cal.getTime());
        //note there are no previous_history table, only current_history table fetching with previous date
        switch (tableName) {
            case "jrbi_transform_previous_work_history_part":
                Log.info("Getting Previous Work History Table Count...");
                jrbiPreviousHistorySqlCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_WORK_HISTORY_COUNT;
                break;

            case "jrbi_transform_previous_manifestation_history_part":
                Log.info("Getting Previous Manifestation History Table Count...");
                jrbiPreviousHistorySqlCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_MANIF_HISTORY_COUNT;
                break;

            case "jrbi_transform_previous_person_history_part":
                Log.info("Getting Previous Person History Table Count...");
                jrbiPreviousHistorySqlCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PREVIOUS_PERSON_HISTORY_COUNT;
                break;
            default:
                Log.info("no tables found");

        }
        Log.info("Previous Date => " + yesterdayDate);
        Log.info(jrbiPreviousHistorySqlCount);
        List<Map<String, Object>> jrbiPreviousHistoryTableCount = DBManager.getDBResultMap(jrbiPreviousHistorySqlCount, Constants.AWS_URL);
        jrbiPreviousHistoryCount = ((Long) jrbiPreviousHistoryTableCount.get(0).get("Previous_History_Count")).intValue();
    }

    @And("^Check count of previous table (.*) and previous history (.*) are identical$")
    public void comparePreviousandPreviousHistoryCount(String srctableName, String trgttableName){
        Log.info("The count for previous table " + srctableName + " => " + jrbiPreviousCount + " and in previous_history "+trgttableName+" => " + jrbiPreviousHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctableName+" and "+trgttableName, jrbiPreviousHistoryCount, jrbiPreviousCount);
    }


    @Given("^Get the total count difference between delta current and transform current history Table (.*)$")
    public static void countDiffoFDeltaCurrentAndCurrentHistory(String targetTable ){
        Calendar cal = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String currentDate= dateFormat.format(cal.getTime());

        switch (targetTable){
            case "jrbi_transform_history_person_excl_delta":
                Log.info("Getting Count by finding the difference b/w delta_current_person and person_history... ");
                jrbiDiffDeltaCurrAndCurrHistSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_DIFF_PERSON_HISTORY_AND_DELTA_PERSON;
                break;
            case "jrbi_transform_history_work_excl_delta":
                Log.info("Getting Count by finding the difference b/w delta_current_work and work_history... ");
                jrbiDiffDeltaCurrAndCurrHistSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_DIFF_WORK_HISTORY_AND_DELTA_WORK;
                break;
            case "jrbi_transform_history_manifestation_excl_delta":
                Log.info("Getting Count by finding the difference b/w delta_current_manif and manif_history... ");
                jrbiDiffDeltaCurrAndCurrHistSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_DIFF_MANIF_HISTORY_AND_DELTA_MANIF;
                break;
            default:
                Log.info("table not found");
        }

        Log.info("Current Date => " + currentDate);
        Log.info(jrbiDiffDeltaCurrAndCurrHistSQLCount);
        List<Map<String, Object>> jrbiDiffDeltaCurrAndCurrHistTableCount = DBManager.getDBResultMap(jrbiDiffDeltaCurrAndCurrHistSQLCount, Constants.AWS_URL);
        jrbiDiffDeltaCurrAndCurrHistCount = ((Long) jrbiDiffDeltaCurrAndCurrHistTableCount.get(0).get("source_count")).intValue();
    }


    @Then("^Get the JRBI (.*) exclude data count$")
    public static void getExcludeTableCounts(String targetTable){
        switch (targetTable){
            case "jrbi_transform_history_person_excl_delta":
                Log.info("Getting Count from exclude person... ");
                jrbiExclSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PERSON_EXCL_COUNT;
                break;
            case "jrbi_transform_history_work_excl_delta":
                Log.info("Getting Count from exclude work... ");
                jrbiExclSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_EXCL_COUNT;
                break;
            case "jrbi_transform_history_manifestation_excl_delta":
                Log.info("Getting Count from exclude manifestation... ");
                jrbiExclSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_EXCL_COUNT;
                break;
            default:
                Log.info("tables not found");
        }
        Log.info(jrbiExclSQLCount);
        List<Map<String, Object>> jrbiExclTableCount = DBManager.getDBResultMap(jrbiExclSQLCount, Constants.AWS_URL);
        jrbiExclCount = ((Long) jrbiExclTableCount.get(0).get("Excl_Count")).intValue();
    }


    @And("^Compare exclude count of (.*) and (.*) with (.*) are identical$")
    public void compareExclcounts(String srcTableOne, String srcTableTwo, String trgtTable){
        Log.info("The counts from the difference of tables " + srcTableOne + " and "+srcTableTwo+" => " + jrbiDiffDeltaCurrAndCurrHistCount + " and in "+trgtTable+" => " + jrbiExclCount);
        Assert.assertEquals("The counts are not equal when compared difference of "+srcTableOne+" and "+srcTableTwo+" with "+trgtTable, jrbiExclCount, jrbiDiffDeltaCurrAndCurrHistCount);
    }

    @Given("^Get the sum of total count between delta current and and Current_Exclude Table (.*)$")
    public static void countsumOfDeltacurrentandExclude(String table){
        switch (table){
            case "jrbi_transform_latest_person":
                Log.info("Getting Count by sum of delta_current_person and current_exclude_person... ");
                jrbiSumSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_SUM_DELTA_PERSON_AND_PERSON_HISTORY;
                break;
            case "jrbi_transform_latest_work":
                Log.info("Getting Count by sum of delta_current_work and current_exclude_work... ");
                jrbiSumSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_SUM_DELTA_WORK_AND_WORK_HISTORY;
                break;
            case "jrbi_transform_latest_manifestation":
                Log.info("Getting Count by sum of delta_current_manifestation and current_exclude_manifestation... ");
                jrbiSumSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_COUNT_SUM_DELTA_MANIF_AND_MANIF_EXCLUDE;
                break;
            default:
               Log.info("no table found");
        }
        Log.info(jrbiSumSQLCount);
        List<Map<String, Object>> jrbiSumTableCount = DBManager.getDBResultMap(jrbiSumSQLCount, Constants.AWS_URL);
        jrbiSumCount = ((Long) jrbiSumTableCount.get(0).get("source_count")).intValue();
    }

    @Then("^Get the JRBI (.*) latest data count$")
    public static void latestCounts(String targetTable){
        switch (targetTable){
            case "jrbi_transform_latest_person":
                Log.info("Getting Count from Latest person... ");
                jrbiLatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_PERSON_LATEST_COUNT;
                break;
            case "jrbi_transform_latest_work":
                Log.info("Getting Count from Latest work... ");
                jrbiLatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_WORK_LATEST_COUNT;
                break;
            case "jrbi_transform_latest_manifestation":
                Log.info("Getting Count from Latest manifestation... ");
                jrbiLatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_MANIF_LATEST_COUNT;
                break;
             default:
                 Log.info("table not found");
        }
        Log.info(jrbiLatestSQLCount);
        List<Map<String, Object>> jrbiLatestTableCount = DBManager.getDBResultMap(jrbiLatestSQLCount, Constants.AWS_URL);
        jrbiLatestCount = ((Long) jrbiLatestTableCount.get(0).get("Target_Count")).intValue();
    }

    @And("^Compare latest counts of (.*) and (.*) with (.*) are identical$")
    public void compareLatestcounts(String srcTableOne, String srcTableTwo, String trgtTable){
        Log.info("The counts from the addition of tables " + srcTableOne + " and "+srcTableTwo+" => " + jrbiSumCount + " and in "+trgtTable+" => " + jrbiLatestCount);
        Assert.assertEquals("The counts are not equal when compared addition of "+srcTableOne+" and "+srcTableTwo+" with "+trgtTable, jrbiLatestCount, jrbiSumCount);
    }

    @Given("^Get the difference of total count between current and previous Table (.*)$")
    public static void countDiffofCurrentAndPrevious(String table){
        switch (table){
            case "jrbi_delta_current_work":
                Log.info("Getting Count from difference of current_work and previous_work...");
                jrbiDiffCurrentPreviousSQLCount = JRBIDataLakeCountChecksSQL.GET_COUNT_DIFF_CURRENT_PREVIOUS_WORK;
                break;
            case "jrbi_delta_current_manifestation":
                Log.info("Getting Count from difference of current_manifestation and previous_manifestation...");
                jrbiDiffCurrentPreviousSQLCount = JRBIDataLakeCountChecksSQL.GET_COUNT_DIFF_CURRENT_PREVIOUS_MANIF;
                break;
            case "jrbi_delta_current_person":
                Log.info("Getting Count from difference of current_person and previous_person...");
                jrbiDiffCurrentPreviousSQLCount = JRBIDataLakeCountChecksSQL.GET_COUNT_DIFF_CURRENT_PREVIOUS_PERSON;
                break;
            default:
                Log.info("no such tables found");
        }
        Log.info(jrbiDiffCurrentPreviousSQLCount);
        List<Map<String, Object>> jrbiDiffCurrentPreviousTableCount = DBManager.getDBResultMap(jrbiDiffCurrentPreviousSQLCount, Constants.AWS_URL);
        jrbiDiffCurrentPreviousCount = ((Long) jrbiDiffCurrentPreviousTableCount.get(0).get("source_count")).intValue();
    }

    @And("^Compare delta count of (.*) and (.*) with (.*) are identical$")
    public void compareCurrentDeltacounts(String srcTableOne, String srcTableTwo, String trgtTable){
        Log.info("The counts from the difference of tables " + srcTableOne + " and "+srcTableTwo+" => " + jrbiDiffCurrentPreviousCount + " and in "+trgtTable+" => " + jrbiDeltaCurrentCount);
        Assert.assertEquals("The counts are not equal when compared difference of "+srcTableOne+" and "+srcTableTwo+" with "+trgtTable, jrbiDeltaCurrentCount, jrbiDiffCurrentPreviousCount);
    }

    @Given("^We know the delta current count for tables (.*)$")
    public static void getDeltaCurrentCount(String tableName){
        switch (tableName) {
            case "jrbi_delta_current_work":
                Log.info("Getting Delta Current Work Table Count...");
                jrbiDeltaCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_WORK_COUNT;
                break;

            case "jrbi_delta_current_manifestation":
                Log.info("Getting Delta Current Manifestation Table Count...");
                jrbiDeltaCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_MANIF_COUNT;
                break;

            case "jrbi_delta_current_person":
                Log.info("Getting Delta Current Person Table Count...");
                jrbiDeltaCurrentSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_PERSON_COUNT;
                break;
             default:
                 Log.info("tables not found");
        }
        Log.info(jrbiDeltaCurrentSQLCount);
        List<Map<String, Object>> jrbiDeltaCurrentTableCount = DBManager.getDBResultMap(jrbiDeltaCurrentSQLCount, Constants.AWS_URL);
        jrbiDeltaCurrentCount = ((Long) jrbiDeltaCurrentTableCount.get(0).get("Delta_Current_Count")).intValue();
    }

    @Then("^Get the count of delta current history (.*) table$")
    public static void getDeltaCurrentHistoryCount(String tableName){
        switch (tableName){
            case "jrbi_transform_delta_work_history_part":
                Log.info("Getting Delta Current Work History Table Count...");
                jrbiDeltaCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_WORK_HISTORY_COUNT;
                break;

            case "jrbi_transform_delta_manifestation_history_part":
                Log.info("Getting Delta Current Manifest History Table Count...");
                jrbiDeltaCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_MANIF_HISTORY_COUNT;
                break;

            case "jrbi_transform_delta_person_history_part":
                Log.info("Getting Delta Current person History Table Count...");
                jrbiDeltaCurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_PERSON_HISTORY_COUNT;
                break;

            default:
                Log.info("no tables found");
        }
        Log.info(jrbiDeltaCurrentHistorySQLCount);
        List<Map<String, Object>> jrbiDeltaCurrentTableCount = DBManager.getDBResultMap(jrbiDeltaCurrentHistorySQLCount, Constants.AWS_URL);
        jrbiDeltaCurrentHistoryCount = ((Long) jrbiDeltaCurrentTableCount.get(0).get("Delta_History_Count")).intValue();
    }

    @And("^Compare delta current (.*) table and delta history (.*) are identical$")
    public void compareDeltaCurrentandDeltaHistoryCount(String srctableName, String trgttableName){
        Log.info("The count for Delta table " + srctableName + " => " + jrbiDeltaCurrentCount + " and in delta_history "+trgttableName+" => " + jrbiDeltaCurrentHistoryCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctableName+" and "+trgttableName, jrbiDeltaCurrentHistoryCount, jrbiDeltaCurrentCount);
    }


    @Given("^Get the Duplicate count in (.*) table$")
    public static void getDuplicateCount(String tableName){
        switch (tableName){
            case "jrbi_transform_latest_work":
                Log.info("Getting Duplicate Work Latest Table Count...");
                jrbiDuplicateLatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DUPLICATE_WORK_LATEST_COUNT;
                break;

            case "jrbi_transform_latest_manifestation":
                Log.info("Getting Duplicate Manifest Latest Table Count...");
                jrbiDuplicateLatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DUPLICATE_MANIF_LATEST_COUNT;
                break;

            case "jrbi_transform_latest_person":
                Log.info("Getting Duplicate person Latest Table Count...");
                jrbiDuplicateLatestSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DUPLICATE_PERSON_LATEST_COUNT;
                break;

                default:
                    Log.info("no tables found");
        }
        Log.info(jrbiDuplicateLatestSQLCount);
        List<Map<String, Object>> jrbiDupLatestTableCount = DBManager.getDBResultMap(jrbiDuplicateLatestSQLCount, Constants.AWS_URL);
        jrbiDuplicateLatestCount = ((Long) jrbiDupLatestTableCount.get(0).get("Duplicate_Count")).intValue();
    }

    @Then("^Check the count should be equal to Zero (.*)$")
    public void checkDupCountZero(String tableName){
        Log.info("The Duplicate count for "+tableName+" => " + jrbiDuplicateLatestCount);
        Assert.assertEquals("There are Duplicate Count of EPR IDs in "+tableName,0,jrbiDuplicateLatestCount);

    }

    @Given("^Get the count of delta current (.*) table$")
    public static void getDeltCurrentTableCount(String tableName){
        switch (tableName){
            case "jrbi_delta_current_work":
                Log.info("Getting jrbi_delta_current_work Table Count...");
                jrbideltaCurrentCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_CURR_WORK_COUNT;
                break;

            case "jrbi_delta_current_manifestation":
                Log.info("Getting jrbi_delta_current_manifestation Table Count...");
                jrbideltaCurrentCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_CURR_MANIF_COUNT;
                break;

            case "jrbi_delta_current_person":
                Log.info("Getting jrbi_delta_current_person Table Count...");
                jrbideltaCurrentCount = JRBIDataLakeCountChecksSQL.GET_JRBI_DELTA_CURR_PERSON_COUNT;
                break;

            default:
                Log.info("no tables found");
        }
        Log.info(jrbideltaCurrentCount);
        List<Map<String, Object>> jrbiDeltaTableCount = DBManager.getDBResultMap(jrbideltaCurrentCount, Constants.AWS_URL);
        int jrbiDeltaCurrCount = ((Long) jrbiDeltaTableCount.get(0).get("delta_current_count")).intValue();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();

        Log.info("The counts for "+tableName+" on "+dtf.format(now)+"=> "+jrbiDeltaCurrCount);
    }

   /* @Given("^Get the total count of jrbi Data from source file (.*)(.*)$")
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
    public static void readfromDb (String tablename){
        String jrbiIssnRecSQLCount;
        Log.info("Getting total issn ids Count from..."+tablename);
        jrbiIssnRecSQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_ISSN_COUNT_FULL_LOAD;
        Log.info(jrbiIssnRecSQLCount);
        List<Map<String, Object>> jrbiIssnTableFullloadCount = DBManager.getDBResultMap(jrbiIssnRecSQLCount, Constants.AWS_URL);
        jrbiIssntotalCount = ((Long) jrbiIssnTableFullloadCount.get(0).get("issnCount")).intValue();
    }

    @And("^Compare count of JRBI data between (.*) and source file$")
    public void compareCounts(String tableName){
        Log.info("The count in excel file: " + totalCountIssnIds + " and in table " +tableName+":"+ jrbiIssntotalCount);
        Assert.assertEquals("The counts are not equal when compared with excel"+totalCountIssnIds+" and DB "+jrbiIssntotalCount, totalCountIssnIds, jrbiIssntotalCount);
    }

    @Given ("^Get the data of JRBI Data from source file (.*)(.*)$")
    public static void readSourceFileData(String fileLocation, String fileName) throws Throwable {
        ArrayList<String> issnId = new ArrayList<>();
        String[] issnIds;
        List<String>issnIdList = new ArrayList<>();
        Object[][] expectedIdentifiersFields;
        Object[][] loadedIdentifiersFields;

        String sql;
        String splitBy = ",";
        String csvfile = fileLocation + fileName;
        BufferedReader br = new BufferedReader(new FileReader(csvfile));
        String line =  br.readLine();
        while ((line) != null) {
            while ((line) != null) {
                issnId.add(line);
            }
        }
        for (int i = 0; i < 5; i++) {
            issnIds = issnId.get(i).split(splitBy);
            issnIdList.add(issnIds[1]);
        }
        sql = String.format(JRBIDataLakeCountChecksSQL.GET_JRBI_FULL_LOAD_DATA_REC, String.join("','",issnId));
        JRBIAccessDLContext.recordsFromExcel = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
        for (int i = 0; i < JRBIAccessDLContext.recordsFromExcel.size(); i++) {
            String[] rowValues = issnId.get(i).split(splitBy);
            expectedIdentifiersFields = new Object[][]{{"issn", rowValues[1]}, {"title", rowValues[0]}, {"journal_number", rowValues[2]},
                    {"journal_acronym", rowValues[3]}};
            loadedIdentifiersFields =
                    new Object[][]{{"issn", JRBIAccessDLContext.recordsFromExcel.get(i).getissn()}, {"title", JRBIAccessDLContext.recordsFromExcel.get(i).gettitle()},
                            {"journal_number", JRBIAccessDLContext.recordsFromExcel.get(i).getjournal_number()}, {"journal_acronym", JRBIAccessDLContext.recordsFromExcel.get(i).getjournal_acronym()}};

            for (int j = 0; j < loadedIdentifiersFields.length; j++) {
                Log.info(loadedIdentifiersFields[j][0] + " = DB => " + loadedIdentifiersFields[j][1] + " CSV => " + expectedIdentifiersFields[j][1]);
                String excelVal = String.valueOf(expectedIdentifiersFields[j][1]);
                String dbVal = String.valueOf(loadedIdentifiersFields[j][1]);
                Assert.assertEquals("Expected all columns to be with correct values loaded but for issn = "
                                + JRBIAccessDLContext.recordsFromExcel.get(i).getissn() + " there are not equal values for the field " + expectedIdentifiersFields[j][0],
                        excelVal, dbVal);
            }
        }

    }
}
