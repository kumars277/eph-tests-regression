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
            case "jrbi_temp_transform_current_work_history_part":
                Log.info("Getting Current Work History Table Count...");
                JRBICurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_WORK_HISTORY_COUNT;
                break;

            case "jrbi_temp_transform_current_manifestation_history_part":
                Log.info("Getting Current Manifestation History Table Count...");
                JRBICurrentHistorySQLCount = JRBIDataLakeCountChecksSQL.GET_JRBI_CURRENT_MANIF_HISTORY_COUNT;
                break;

            case "jrbi_temp_transform_current_person_history_part":
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

}
