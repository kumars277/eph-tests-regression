package com.eph.automation.testing.web.steps.JRBIDataLakeAccess;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLPersonAccessObject;
import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIPersonDataChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class JRBIPersonDataChecksSteps {

    public JRBIAccessDLContext dataQualityJRBIContext;
    private static String sql;
    private static List<String> Ids;
    private JRBIPersonDataChecksSQL jrbiObj = new JRBIPersonDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^We get the (.*) random Person EPR ids (.*)$")
    public void getRandomPersonEPRIds(String numberOfRecords, String tableName) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Person EPR Ids...");
        switch (tableName) {
            case "jrbi_journal_data_full":
                sql = String.format(JRBIPersonDataChecksSQL.GET_EPR_IDS_PERSON_FULLLOAD, numberOfRecords);
                List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_CURRENT_PERSON_EPR_ID, numberOfRecords);
                List<Map<?, ?>> randomCurrentEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomCurrentEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_previous_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_PREVIOUS_PERSON_EPR_ID, numberOfRecords);
                List<Map<?, ?>> randomPreviousEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPreviousEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the records from data full load for Person (.*)$")
    public void getPersonRecordsFromFullLoad(String tableName) {
        Log.info("We get the FULL Load Person records...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_PERSON_RECORDS_FULL_LOAD, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDataFullLoadPerson = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform current person (.*)$")
    public void getRecordsFromCurrentPerson(String tableName) {
        Log.info("We get the records from Current Person...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_CURRENT_PERSON_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentPerson = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @Then("^We Get the records from transform current person History (.*)$")
    public void getRecordsFromCurrentPersonHistory(String tableName) {
        Log.info("We get the records from Current Person History...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_CURRENT_PERSON_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentPersonHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @When("^Get the records from transform previous person (.*)$")
    public void getRecordsFromPreviousPerson(String tableName) {
        Log.info("We get the records from Previous Person...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_PREVIOUS_PERSON_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousPerson = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

   @Then("^We Get the records from transform previous person History (.*)$")
   public void getRecordsFromPreviousPersonHistory(String tableName) {
       Log.info("We get the records from Previous Person History...");
       sql = String.format(JRBIPersonDataChecksSQL.GET_PREVIOUS_PERSON_HISTORY_RECORDS, Joiner.on("','").join(Ids));
       Log.info(sql);
       dataQualityJRBIContext.recordsFromFromPreviousPersonHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
   }

    @And("^Compare the records of person full load and current person$")
    public void compareDataFullandCurrentPerson() {
       if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Person Full Load and Current Person...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDataFullLoadPerson.size(); i++) {

                dataQualityJRBIContext.recordsFromDataFullLoadPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));

                Log.info("Full Load -> EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        "Current_Person -> EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() + " is missing/not found in Current_Manif table",
                            dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " RECORD_TYPE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getRECORD_TYPE() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " ROLE_CODE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_CODE() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_CODE());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_CODE() != null)) {
                    Assert.assertEquals("The ROLE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_CODE(),
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " U_KEY => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getU_KEY() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getU_KEY());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getU_KEY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_KEY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getU_KEY(),
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getU_KEY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " ROLE_DESCRIPTION => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_DESCRIPTION() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_DESCRIPTION());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_DESCRIPTION() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_DESCRIPTION() != null)) {
                    Assert.assertEquals("The ROLE_DESCRIPTION is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_DESCRIPTION(),
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_DESCRIPTION());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " GIVEN_NAME => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getGIVEN_NAME() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getGIVEN_NAME());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getGIVEN_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getGIVEN_NAME() != null)) {
                    Assert.assertEquals("The GIVEN_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getGIVEN_NAME(),
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getGIVEN_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " FAMILY_NAME => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getFAMILY_NAME() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getFAMILY_NAME());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getFAMILY_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getFAMILY_NAME() != null)) {
                    Assert.assertEquals("The FAMILY_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getFAMILY_NAME(),
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getFAMILY_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " PEOPLEHUB_ID => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getPEOPLEHUB_ID() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getPEOPLEHUB_ID());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getPEOPLEHUB_ID() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getPEOPLEHUB_ID() != null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getPEOPLEHUB_ID(),
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getPEOPLEHUB_ID());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " EMAIL => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEMAIL() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEMAIL());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEMAIL() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEMAIL(),
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEMAIL());
                }
            }
        }
    }

    @And("^Compare the records of previous person and previous person history$")
    public void comparePreviousPersonandHistory() {
        if (dataQualityJRBIContext.recordsFromFromPreviousPerson.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Previous Person and Previous Person history...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromPreviousPerson.size(); i++) {

                dataQualityJRBIContext.recordsFromFromPreviousPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));

                Log.info("Current_person -> EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() +
                        "Previous_person_History -> EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() + " is missing/not found in Current_Manif table",
                            dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() +
                        " RECORD_TYPE => Previous_person =" + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getRECORD_TYPE() +
                        " Previous_person_History=" + dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() +
                        " ROLE_CODE => Previous_person =" + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getROLE_CODE() +
                        " Previous_person_History=" + dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getROLE_CODE());

                if (dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getROLE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getROLE_CODE() != null)) {
                    Assert.assertEquals("The ROLE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getROLE_CODE(),
                            dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getROLE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() +
                        " U_KEY => Previous_person =" + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getU_KEY() +
                        " Previous_person_History=" + dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getU_KEY());

                if (dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getU_KEY() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_KEY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getU_KEY(),
                            dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getU_KEY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() +
                        " ROLE_DESCRIPTION => Previous_person =" + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getROLE_DESCRIPTION() +
                        " Previous_person_History=" + dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getROLE_DESCRIPTION());

                if (dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getROLE_DESCRIPTION() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getROLE_DESCRIPTION() != null)) {
                    Assert.assertEquals("The ROLE_DESCRIPTION is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getROLE_DESCRIPTION(),
                            dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getROLE_DESCRIPTION());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() +
                        " GIVEN_NAME => Previous_person =" + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getGIVEN_NAME() +
                        " Previous_person_History=" + dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getGIVEN_NAME());

                if (dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getGIVEN_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getGIVEN_NAME() != null)) {
                    Assert.assertEquals("The GIVEN_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getGIVEN_NAME(),
                            dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getGIVEN_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() +
                        " FAMILY_NAME => Previous_person =" + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getFAMILY_NAME() +
                        " Previous_person_History=" + dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getFAMILY_NAME());

                if (dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getFAMILY_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getFAMILY_NAME() != null)) {
                    Assert.assertEquals("The FAMILY_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getFAMILY_NAME(),
                            dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getFAMILY_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() +
                        " PEOPLEHUB_ID => Previous_person =" + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getPEOPLEHUB_ID() +
                        " Previous_person_History=" + dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getPEOPLEHUB_ID());

                if (dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getPEOPLEHUB_ID() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getPEOPLEHUB_ID() != null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getPEOPLEHUB_ID(),
                            dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getPEOPLEHUB_ID());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() +
                        " EMAIL => Previous_person =" + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEMAIL() +
                        " Previous_person_History=" + dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getEMAIL());

                if (dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEMAIL() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEMAIL(),
                            dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getEMAIL());
                }
            }
        }
    }

    @And("^Compare the records of current person and current person history$")
    public void compareCurrentPersonandHistory(String tableName) {
        if (dataQualityJRBIContext.recordsFromFromCurrentPerson.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Current Person and Current Person history...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromCurrentPerson.size(); i++) {

                dataQualityJRBIContext.recordsFromFromCurrentPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));

                Log.info("Current_person -> EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() +
                        "Current_Person_History -> EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() + " is missing/not found in Current_Manif table",
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() +
                        " RECORD_TYPE => Current_person =" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getRECORD_TYPE() +
                        " Current_Person_History=" + dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() +
                        " ROLE_CODE => Current_person =" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_CODE() +
                        " Current_Person_History=" + dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getROLE_CODE());

                if (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getROLE_CODE() != null)) {
                    Assert.assertEquals("The ROLE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_CODE(),
                            dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getROLE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() +
                        " U_KEY => Current_person =" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getU_KEY() +
                        " Current_Person_History=" + dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getU_KEY());

                if (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getU_KEY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_KEY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getU_KEY(),
                            dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getU_KEY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() +
                        " ROLE_DESCRIPTION => Current_person =" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_DESCRIPTION() +
                        " Current_Person_History=" + dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getROLE_DESCRIPTION());

                if (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_DESCRIPTION() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getROLE_DESCRIPTION() != null)) {
                    Assert.assertEquals("The ROLE_DESCRIPTION is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_DESCRIPTION(),
                            dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getROLE_DESCRIPTION());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() +
                        " GIVEN_NAME => Current_person =" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getGIVEN_NAME() +
                        " Current_Person_History=" + dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getGIVEN_NAME());

                if (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getGIVEN_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getGIVEN_NAME() != null)) {
                    Assert.assertEquals("The GIVEN_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getGIVEN_NAME(),
                            dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getGIVEN_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() +
                        " FAMILY_NAME => Current_person =" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getFAMILY_NAME() +
                        " Current_Person_History=" + dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getFAMILY_NAME());

                if (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getFAMILY_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getFAMILY_NAME() != null)) {
                    Assert.assertEquals("The FAMILY_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getFAMILY_NAME(),
                            dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getFAMILY_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() +
                        " PEOPLEHUB_ID => Current_person =" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getPEOPLEHUB_ID() +
                        " Current_Person_History=" + dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getPEOPLEHUB_ID());

                if (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getPEOPLEHUB_ID() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getPEOPLEHUB_ID() != null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getPEOPLEHUB_ID(),
                            dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getPEOPLEHUB_ID());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() +
                        " EMAIL => Current_person =" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEMAIL() +
                        " Current_Person_History=" + dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getEMAIL());

                if (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEMAIL() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEMAIL(),
                            dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getEMAIL());
                }
            }
        }
    }
}