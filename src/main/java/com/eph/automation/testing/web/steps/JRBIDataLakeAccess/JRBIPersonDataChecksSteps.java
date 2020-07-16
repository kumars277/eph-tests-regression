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
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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
                List<Map<?, ?>> randomCurrentPersonEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomCurrentPersonEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_previous_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_PREVIOUS_PERSON_EPR_ID, numberOfRecords);
                List<Map<?, ?>> randomPreviousPersonEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPreviousPersonEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_delta_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_DELTA_PERSON_EPR_ID, numberOfRecords);
                List<Map<?, ?>> randomDeltapersonEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomDeltapersonEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_history_person_excl_delta":
                sql = String.format(JRBIPersonDataChecksSQL.GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_PERSON, numberOfRecords);
                List<Map<?, ?>> randomExclEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomExclEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_latest_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_EPR_FOR_PERSON_LATEST, numberOfRecords);
                List<Map<?, ?>> randomLatestEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomLatestEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_current_previous_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_RANDOM_EPR_DELTA_PERSON, numberOfRecords);
                List<Map<?, ?>> randomDeltaEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomDeltaEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the records from data full load for Person$")
    public void getPersonRecordsFromFullLoad() {
        Log.info("We get the FULL Load Person records...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_PERSON_RECORDS_FULL_LOAD, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDataFullLoadPerson = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform current person$")
    public void getRecordsFromCurrentPerson() {
        Log.info("We get the records from Current Person table...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_CURRENT_PERSON_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentPerson = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of person full load and current person$")
    public void compareDataFullandCurrentPerson() {
        if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Person Full Load and Current Person...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDataFullLoadPerson.size(); i++) {

                dataQualityJRBIContext.recordsFromDataFullLoadPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort primarykey data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));
                dataQualityJRBIContext.recordsFromDataFullLoadPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY));

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
                    String recordType = dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getRECORD_TYPE();
                    if(recordType.isEmpty()){
                        recordType = null;
                    }
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            recordType,dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " ROLE_CODE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_CODE() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_CODE());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_CODE() != null)) {

                    String roleCode = dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_CODE();
                    if(roleCode.isEmpty()){
                        roleCode = null;
                    }

                    Assert.assertEquals("The ROLE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            roleCode,dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " U_KEY => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getU_KEY() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getU_KEY());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getU_KEY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getU_KEY() != null)) {
                    String uKey = dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getU_KEY();
                    if(uKey.isEmpty()){
                        uKey = null;
                    }
                    Assert.assertEquals("The U_KEY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            uKey,dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getU_KEY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " ROLE_DESCRIPTION => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_DESCRIPTION() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_DESCRIPTION());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_DESCRIPTION() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_DESCRIPTION() != null)) {

                    String roleDescription = dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getROLE_DESCRIPTION();
                    if(roleDescription.isEmpty()){
                        roleDescription = null;
                    }

                    Assert.assertEquals("The ROLE_DESCRIPTION is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            roleDescription,dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getROLE_DESCRIPTION());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " GIVEN_NAME => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getGIVEN_NAME() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getGIVEN_NAME());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getGIVEN_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getGIVEN_NAME() != null)) {

                    String givenName = dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getGIVEN_NAME();
                    if(givenName.isEmpty()){
                        givenName = null;
                    }
                    Assert.assertEquals("The GIVEN_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            givenName,dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getGIVEN_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " FAMILY_NAME => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getFAMILY_NAME() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getFAMILY_NAME());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getFAMILY_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getFAMILY_NAME() != null)) {
                    String familyName = dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getFAMILY_NAME();
                    if(familyName.isEmpty()){
                        familyName = null;
                    }
                    Assert.assertEquals("The FAMILY_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            familyName,dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getFAMILY_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " PEOPLEHUB_ID => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getPEOPLEHUB_ID() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getPEOPLEHUB_ID());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getPEOPLEHUB_ID() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getPEOPLEHUB_ID() != null)) {

                    String peopleHub = dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getPEOPLEHUB_ID();
                    if(peopleHub.isEmpty()){
                        peopleHub = null;
                    }

                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            peopleHub,dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getPEOPLEHUB_ID());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() +
                        " EMAIL => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEMAIL() +
                        " Current_Person=" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEMAIL());

                if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEMAIL() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEMAIL() != null)) {

                    String email = dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEMAIL();
                    if(email.isEmpty()){
                        email = null;
                    }
                    Assert.assertEquals("The EMAIL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR() ,
                            email,dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEMAIL());
                }
            }
        }
    }

    @Then("^We Get the records from transform current person History$")
    public void getRecordsCurrentPersonHistory() {
        Log.info("We get the records from Current Person History...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_CURRENT_PERSON_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentPersonHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of current person and current person history$")
    public void compareCurrentPersonandPersonHistory() {
        if (dataQualityJRBIContext.recordsFromFromCurrentPerson.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Current Person and Current Person history...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromCurrentPerson.size(); i++) {

                dataQualityJRBIContext.recordsFromFromCurrentPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));
                dataQualityJRBIContext.recordsFromFromCurrentPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY));
                dataQualityJRBIContext.recordsFromFromCurrentPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID));


                Log.info("Current_person -> EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() +
                        "Current_Person_History -> EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() + " is missing/not found in Current_Person_History table",
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

    @When("^Get the records from transform previous person$")
    public void getRecordsFromPreviousPerson() {
        Log.info("We get the records from Previous Person...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_PREVIOUS_PERSON_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousPerson = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @Then("^We Get the records from transform previous person History$")
    public void getRecordsFromPreviousPersonHistory() {
        Log.info("We get the records from Previous Person History...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_PREVIOUS_PERSON_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousPersonHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
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
                dataQualityJRBIContext.recordsFromFromPreviousPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY));
                dataQualityJRBIContext.recordsFromFromPreviousPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID));


                Log.info("Previous_person -> EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() +
                        "Previous_person_History -> EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() + " is missing/not found in Previous Person History table",
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

    @When("^Get the records from the difference of current_person and previous_person$")
    public void getDiffCurrentPreviousPersonRec() {
        Log.info("We get the Difference of Current and Previous Person records...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_DIFF_REC_PREVIOUS_CURRENT_PREVIOUS_PERSON, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @When("^Get the records from transform Delta Current person$")
    public void getDeltaPersonRecords() {
        Log.info("We get the Delta Person current records...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_DELTA_PERSON_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaPerson = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of Delta Current person with difference of current and previous person$")
    public void compareDeltawithCurrentPreviousPerson() {
        if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between DElta Person and Current,Previous tables...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.size(); i++) {

                dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));
                dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY));
                dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID));



                Log.info("Current_PRevious -> EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                        "Delta_person -> EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() + " is missing/not found in Delta_PErson table",
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                        " RECORD_TYPE => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getRECORD_TYPE() +
                        " Delta_person=" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getRECORD_TYPE());
                }

  /*              Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                        " ROLE_CODE => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getROLE_CODE() +
                        " Delta_person=" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_CODE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getROLE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_CODE() != null)) {
                    Assert.assertEquals("The ROLE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getROLE_CODE(),
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_CODE());
                }

  */              Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                        " U_KEY => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getU_KEY() +
                        " Delta_person=" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getU_KEY());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getU_KEY() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_KEY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getU_KEY(),
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getU_KEY());
                }
/*

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                        " ROLE_DESCRIPTION => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getROLE_DESCRIPTION() +
                        " Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_DESCRIPTION());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getROLE_DESCRIPTION() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_DESCRIPTION() != null)) {
                    Assert.assertEquals("The ROLE_DESCRIPTION is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getROLE_DESCRIPTION(),
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_DESCRIPTION());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                        " GIVEN_NAME => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getGIVEN_NAME() +
                        " Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getGIVEN_NAME());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getGIVEN_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getGIVEN_NAME() != null)) {
                    Assert.assertEquals("The GIVEN_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getGIVEN_NAME(),
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getGIVEN_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                        " FAMILY_NAME => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getFAMILY_NAME() +
                        " Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getFAMILY_NAME());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getFAMILY_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getFAMILY_NAME() != null)) {
                    Assert.assertEquals("The FAMILY_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getFAMILY_NAME(),
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getFAMILY_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                        " PEOPLEHUB_ID => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getPEOPLEHUB_ID() +
                        " Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getPEOPLEHUB_ID());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getPEOPLEHUB_ID() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getPEOPLEHUB_ID() != null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getPEOPLEHUB_ID(),
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getPEOPLEHUB_ID());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                        " EMAIL => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEMAIL() +
                        " Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEMAIL());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEMAIL() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEMAIL(),
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEMAIL());
                }
*/

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                        " TYPE => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getTYPE() +
                        " Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getTYPE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getTYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getTYPE() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getTYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getTYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                        " DELTA_MODE => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getDELTA_MODE() +
                        " Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getDELTA_MODE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getDELTA_MODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getDELTA_MODE() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getDELTA_MODE(),
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getDELTA_MODE());
                }
            }
        }
    }


    @Then("^We Get the records from transform Delta person History$")
    public void getDeltaPersonHistoryRecords() {
        Log.info("We get the Delta Person history records...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_DELTA_PERSON_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaPersonHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }


    @And ("^Compare the records of delta person and delta person history$")
    public void compareDeltaPersonandDeltapersonHistory() {
        if (dataQualityJRBIContext.recordsFromFromDeltaPerson.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Delta Person and Delta Person history...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromDeltaPerson.size(); i++) {

                dataQualityJRBIContext.recordsFromFromDeltaPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));
                dataQualityJRBIContext.recordsFromFromDeltaPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY));

                Log.info("Delta_person -> EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                        "Delta_person_History -> EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() + " is missing/not found in Delta_Person_History table",
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                        " RECORD_TYPE => Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getRECORD_TYPE() +
                        " Delta_person_History=" + dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                        " ROLE_CODE => Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_CODE() +
                        " Delta_person_History=" + dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getROLE_CODE());

                if (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getROLE_CODE() != null)) {
                    Assert.assertEquals("The ROLE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_CODE(),
                            dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getROLE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                        " U_KEY => Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getU_KEY() +
                        " Delta_person_History=" + dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getU_KEY());

                if (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getU_KEY() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_KEY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getU_KEY(),
                            dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getU_KEY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                        " ROLE_DESCRIPTION => Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_DESCRIPTION() +
                        " Delta_person_History=" + dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getROLE_DESCRIPTION());

                if (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_DESCRIPTION() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getROLE_DESCRIPTION() != null)) {
                    Assert.assertEquals("The ROLE_DESCRIPTION is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getROLE_DESCRIPTION(),
                            dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getROLE_DESCRIPTION());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                        " GIVEN_NAME => Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getGIVEN_NAME() +
                        " Delta_person_History=" + dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getGIVEN_NAME());

                if (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getGIVEN_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getGIVEN_NAME() != null)) {
                    Assert.assertEquals("The GIVEN_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getGIVEN_NAME(),
                            dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getGIVEN_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                        " FAMILY_NAME => Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getFAMILY_NAME() +
                        " Delta_person_History=" + dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getFAMILY_NAME());

                if (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getFAMILY_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getFAMILY_NAME() != null)) {
                    Assert.assertEquals("The FAMILY_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getFAMILY_NAME(),
                            dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getFAMILY_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                        " PEOPLEHUB_ID => Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getPEOPLEHUB_ID() +
                        " Delta_person_History=" + dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getPEOPLEHUB_ID());

                if (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getPEOPLEHUB_ID() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getPEOPLEHUB_ID() != null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getPEOPLEHUB_ID(),
                            dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getPEOPLEHUB_ID());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                        " EMAIL => Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEMAIL() +
                        " Delta_person_History=" + dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getEMAIL());

                if (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEMAIL() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEMAIL(),
                            dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getEMAIL());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                        " TYPE => Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getTYPE() +
                        " Delta_person_History=" + dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getTYPE());

                if (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getTYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getTYPE() != null)) {
                    Assert.assertEquals("The TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getTYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getTYPE());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                        " DELTA_MODE => Delta_person =" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getDELTA_MODE() +
                        " Delta_person_History=" + dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getDELTA_MODE());

                if (dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getDELTA_MODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getDELTA_MODE() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getDELTA_MODE(),
                            dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i).getDELTA_MODE());
                }
            }
        }
    }

    @When("^Get the records from the difference of Delta_current_person and person_history$")
    public void getRecordsofDeltaPErsonandPersonHistory(){
        Log.info("We get the difference of Delta Person and PErson History with current time records...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_PERSON, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from person exclude table$")
    public void getExcludePersonRecords(){
        Log.info("We get the records fromPerson Exclude...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_RECORDS_FROM_PERSON_EXCLUDE, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromExcludePerson = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of Person Exclude with difference of Delta_current_person and person_history$")
    public void compareDataForPersonExclude() {
        if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Delta Person & PersonHistory with Person Exclude...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.size(); i++) {

                dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromExcludePerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));
                dataQualityJRBIContext.recordsFromExcludePerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY));
                dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY));
                dataQualityJRBIContext.recordsFromExcludePerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID));
                dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID));

                Log.info("Diff of Delt Person and History -> EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() +
                        "Person_Exclude -> EPR => " + dataQualityJRBIContext.recordsFromExcludePerson.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromExcludePerson.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() + " is missing/not found in PErson Exclude table",
                            dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromExcludePerson.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() +
                        " RECORD_TYPE => Diff of Delt Person and History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getRECORD_TYPE() +
                        " Person_Exclude=" + dataQualityJRBIContext.recordsFromExcludePerson.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludePerson.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromExcludePerson.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() +
                        " ROLE_CODE => Diff of Delt Person and History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getROLE_CODE() +
                        " Person_Exclude=" + dataQualityJRBIContext.recordsFromExcludePerson.get(i).getROLE_CODE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getROLE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludePerson.get(i).getROLE_CODE() != null)) {
                    Assert.assertEquals("The ROLE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getROLE_CODE(),
                            dataQualityJRBIContext.recordsFromExcludePerson.get(i).getROLE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() +
                        " U_KEY => Diff of Delt Person and History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getU_KEY() +
                        " Person_Exclude=" + dataQualityJRBIContext.recordsFromExcludePerson.get(i).getU_KEY());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getU_KEY() != null ||
                        (dataQualityJRBIContext.recordsFromExcludePerson.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_KEY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getU_KEY(),
                            dataQualityJRBIContext.recordsFromExcludePerson.get(i).getU_KEY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() +
                        " ROLE_DESCRIPTION => Diff of Delt Person and History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getROLE_DESCRIPTION() +
                        " Person_Exclude=" + dataQualityJRBIContext.recordsFromExcludePerson.get(i).getROLE_DESCRIPTION());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getROLE_DESCRIPTION() != null ||
                        (dataQualityJRBIContext.recordsFromExcludePerson.get(i).getROLE_DESCRIPTION() != null)) {
                    Assert.assertEquals("The ROLE_DESCRIPTION is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getROLE_DESCRIPTION(),
                            dataQualityJRBIContext.recordsFromExcludePerson.get(i).getROLE_DESCRIPTION());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() +
                        " GIVEN_NAME => Diff of Delt Person and History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getGIVEN_NAME() +
                        " Person_Exclude=" + dataQualityJRBIContext.recordsFromExcludePerson.get(i).getGIVEN_NAME());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getGIVEN_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromExcludePerson.get(i).getGIVEN_NAME() != null)) {
                    Assert.assertEquals("The GIVEN_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getGIVEN_NAME(),
                            dataQualityJRBIContext.recordsFromExcludePerson.get(i).getGIVEN_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() +
                        " FAMILY_NAME => Diff of Delt Person and History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getFAMILY_NAME() +
                        " Person_Exclude=" + dataQualityJRBIContext.recordsFromExcludePerson.get(i).getFAMILY_NAME());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getFAMILY_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromExcludePerson.get(i).getFAMILY_NAME() != null)) {
                    Assert.assertEquals("The FAMILY_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getFAMILY_NAME(),
                            dataQualityJRBIContext.recordsFromExcludePerson.get(i).getFAMILY_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() +
                        " PEOPLEHUB_ID => Diff of Delt Person and History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getPEOPLEHUB_ID() +
                        " Person_Exclude=" + dataQualityJRBIContext.recordsFromExcludePerson.get(i).getPEOPLEHUB_ID());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getPEOPLEHUB_ID() != null ||
                        (dataQualityJRBIContext.recordsFromExcludePerson.get(i).getPEOPLEHUB_ID() != null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getPEOPLEHUB_ID(),
                            dataQualityJRBIContext.recordsFromExcludePerson.get(i).getPEOPLEHUB_ID());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() +
                        " EMAIL => Diff of Delt Person and History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEMAIL() +
                        " Person_Exclude=" + dataQualityJRBIContext.recordsFromExcludePerson.get(i).getEMAIL());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEMAIL() != null ||
                        (dataQualityJRBIContext.recordsFromExcludePerson.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEMAIL(),
                            dataQualityJRBIContext.recordsFromExcludePerson.get(i).getEMAIL());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() +
                        " LAST_UPDATED_DATE => Diff of Delt Person and History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getLAST_UPDATED_DATE() +
                        " Person_Exclude=" + dataQualityJRBIContext.recordsFromExcludePerson.get(i).getLAST_UPDATED_DATE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getLAST_UPDATED_DATE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludePerson.get(i).getLAST_UPDATED_DATE() != null)) {
                    Assert.assertEquals("The LAST_UPDATED_DATE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getLAST_UPDATED_DATE(),
                            dataQualityJRBIContext.recordsFromExcludePerson.get(i).getLAST_UPDATED_DATE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() +
                        " DELETE_FLAG => Diff of Delt Person and History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getDELETE_FLAG() +
                        " Person_Exclude=" + dataQualityJRBIContext.recordsFromExcludePerson.get(i).getDELETE_FLAG());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getDELETE_FLAG() != null ||
                        (dataQualityJRBIContext.recordsFromExcludePerson.get(i).getDELETE_FLAG() != null)) {
                    Assert.assertEquals("The DELETE_FLAG is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getDELETE_FLAG(),
                            dataQualityJRBIContext.recordsFromExcludePerson.get(i).getDELETE_FLAG());
                }
            }
        }
    }


    @When("^Get the records from the addition of Delta_Person and Person_Exclude$")
    public void getRecordsofDeltaPErsonandExclude(){
        Log.info("We get the Addition of Delta Person and Person Exclude records...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_JRBI_REC_SUM_DELTA_PERSON_AND_PERSON_HISTORY, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from Person latest table$")
    public void getRecordsofPersonLatest(){
        Log.info("We Person Latest records...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_JRBI_PERSON_LATEST_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromLAtestPerson = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }



    @And("^Compare the records of Person Latest with addition of Delta_current_Person and Person_Exclude$")
    public void compareDataForPersonLatest() {
        if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records of Delta Person & Person_Latest with Person LAtest...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.size(); i++) {

                dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromLAtestPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));
                dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY)); //sort data in the lists
                dataQualityJRBIContext.recordsFromLAtestPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY));
                dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID)); //sort data in the lists
                dataQualityJRBIContext.recordsFromLAtestPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID));


                Log.info("Delta_PERSON_EXclude -> EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                        "Person_Latest -> EPR => " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() + " is missing/not found in PErson_latest table",
                            dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                        " RECORD_TYPE => Delta_PERSON_EXclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getRECORD_TYPE() +
                        " Person_Latest=" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                        " ROLE_CODE => Delta_PERSON_EXclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getROLE_CODE() +
                        " Person_Latest=" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_CODE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getROLE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_CODE() != null)) {
                    Assert.assertEquals("The ROLE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getROLE_CODE(),
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                        " U_KEY => Delta_PERSON_EXclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getU_KEY() +
                        " Person_Latest=" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getU_KEY());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getU_KEY() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_KEY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getU_KEY(),
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getU_KEY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                        " ROLE_DESCRIPTION => Delta_PERSON_EXclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getROLE_DESCRIPTION() +
                        " Person_Latest=" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_DESCRIPTION());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getROLE_DESCRIPTION() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_DESCRIPTION() != null)) {
                    Assert.assertEquals("The ROLE_DESCRIPTION is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getROLE_DESCRIPTION(),
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_DESCRIPTION());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                        " GIVEN_NAME => Delta_PERSON_EXclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getGIVEN_NAME() +
                        " Person_Latest=" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getGIVEN_NAME());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getGIVEN_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getGIVEN_NAME() != null)) {
                    Assert.assertEquals("The GIVEN_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getGIVEN_NAME(),
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getGIVEN_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                        " FAMILY_NAME => Delta_PERSON_EXclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getFAMILY_NAME() +
                        " Person_Latest=" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getFAMILY_NAME());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getFAMILY_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getFAMILY_NAME() != null)) {
                    Assert.assertEquals("The FAMILY_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getFAMILY_NAME(),
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getFAMILY_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                        " PEOPLEHUB_ID => Delta_PERSON_EXclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getPEOPLEHUB_ID() +
                        " Person_Latest=" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getPEOPLEHUB_ID());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getPEOPLEHUB_ID() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getPEOPLEHUB_ID() != null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getPEOPLEHUB_ID(),
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getPEOPLEHUB_ID());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                        " EMAIL => Delta_PERSON_EXclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEMAIL() +
                        " Person_Latest=" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEMAIL());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEMAIL() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEMAIL(),
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEMAIL());
                }
/*

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                        " LAST_UPDATED_DATE => Delta_PERSON_EXclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getLAST_UPDATED_DATE() +
                        " Person_Latest=" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getLAST_UPDATED_DATE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getLAST_UPDATED_DATE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getLAST_UPDATED_DATE() != null)) {
                    Assert.assertEquals("The LAST_UPDATED_DATE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getLAST_UPDATED_DATE(),
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getLAST_UPDATED_DATE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                        " DELETE_FLAG => Delta_PERSON_EXclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getDELETE_FLAG() +
                        " Person_Latest=" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getDELETE_FLAG());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getDELETE_FLAG() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getDELETE_FLAG() != null)) {
                    Assert.assertEquals("The DELETE_FLAG is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getDELETE_FLAG(),
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getDELETE_FLAG());
                }
*/
            }
        }
    }

    @Then("^Get the records from person extended table$")
    public void getRecordsFromProductDBManifestationExtended() {
        Log.info("We get the records from Person Extended Roles...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_JRBI_PERSON_EXTENDED_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromPersonExtended = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of transform latest person and person extended$")
    public void comparePersonLatestandExtended(){
        if (dataQualityJRBIContext.recordsFromLAtestPerson.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records of Person Extended & Person_Latest...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromLAtestPerson.size(); i++) {

                dataQualityJRBIContext.recordsFromLAtestPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromPersonExtended.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));
                dataQualityJRBIContext.recordsFromPersonExtended.sort(Comparator.comparing(JRBIDLPersonAccessObject::getROLE_CODE)); //sort data in the lists
                dataQualityJRBIContext.recordsFromLAtestPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getROLE_CODE));
                dataQualityJRBIContext.recordsFromPersonExtended.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID)); //sort data in the lists
                dataQualityJRBIContext.recordsFromLAtestPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getPEOPLEHUB_ID));


                Log.info("Person_Latest -> EPR => " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() +
                        "Person_Extended -> EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromPersonExtended.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() + " is missing/not found in Person Extended table",
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromPersonExtended.get(i).getEPR());
                }


                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() +
                        " ROLE_CODE => Person_Latest =" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_CODE() +
                        " Person_Extended=" + dataQualityJRBIContext.recordsFromPersonExtended.get(i).getROLE_CODE());

                if (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromPersonExtended.get(i).getROLE_CODE() != null)) {
                    Assert.assertEquals("The ROLE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_CODE(),
                            dataQualityJRBIContext.recordsFromPersonExtended.get(i).getROLE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() +
                        " ROLE_NAME => Person_Latest =" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_DESCRIPTION() +
                        " Person_Extended=" + dataQualityJRBIContext.recordsFromPersonExtended.get(i).getROLE_NAME());

                if (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_DESCRIPTION() != null ||
                        (dataQualityJRBIContext.recordsFromPersonExtended.get(i).getROLE_NAME() != null)) {
                    Assert.assertEquals("The ROLE_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getROLE_DESCRIPTION(),
                            dataQualityJRBIContext.recordsFromPersonExtended.get(i).getROLE_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() +
                        " FIRST_NAME => Person_Latest =" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getGIVEN_NAME() +
                        " Person_Extended=" + dataQualityJRBIContext.recordsFromPersonExtended.get(i).getFIRST_NAME());

                if (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getGIVEN_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromPersonExtended.get(i).getFIRST_NAME() != null)) {
                    Assert.assertEquals("The FIRST_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getGIVEN_NAME(),
                            dataQualityJRBIContext.recordsFromPersonExtended.get(i).getFIRST_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() +
                        " LAST_NAME => Person_Latest =" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getFAMILY_NAME() +
                        "Person_Extended =" + dataQualityJRBIContext.recordsFromPersonExtended.get(i).getLAST_NAME());

                if (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getFAMILY_NAME() != null ||
                        (dataQualityJRBIContext.recordsFromPersonExtended.get(i).getLAST_NAME() != null)) {
                    Assert.assertEquals("The SECOND_NAME is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getFAMILY_NAME(),
                            dataQualityJRBIContext.recordsFromPersonExtended.get(i).getLAST_NAME());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() +
                        " PEOPLEHUB_ID => Person_Latest =" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getPEOPLEHUB_ID() +
                        " Person_Extended=" + dataQualityJRBIContext.recordsFromPersonExtended.get(i).getPEOPLEHUB_ID());

                if (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getPEOPLEHUB_ID() != null ||
                        (dataQualityJRBIContext.recordsFromPersonExtended.get(i).getPEOPLEHUB_ID() != null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getPEOPLEHUB_ID(),
                            dataQualityJRBIContext.recordsFromPersonExtended.get(i).getPEOPLEHUB_ID());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() +
                        " EMAIL => Person_Latest =" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEMAIL() +
                        " Person_Extended=" + dataQualityJRBIContext.recordsFromPersonExtended.get(i).getEMAIL());

                if (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEMAIL() != null ||
                        (dataQualityJRBIContext.recordsFromPersonExtended.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEMAIL(),
                            dataQualityJRBIContext.recordsFromPersonExtended.get(i).getEMAIL());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() +
                        " LAST_UPDATED => Person_Latest =" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getLAST_UPDATED_DATE() +
                        " Person_Extended=" + dataQualityJRBIContext.recordsFromPersonExtended.get(i).getLAST_UPDATED_DATE());

                if (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getLAST_UPDATED_DATE() != null ||
                        (dataQualityJRBIContext.recordsFromPersonExtended.get(i).getLAST_UPDATED_DATE() != null)) {
                    Assert.assertEquals("The LAST_UPDATED_DATE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getLAST_UPDATED_DATE(),
                            dataQualityJRBIContext.recordsFromPersonExtended.get(i).getLAST_UPDATED_DATE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() +
                        " DELETE_FLAG => Person_Latest =" + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getLAST_UPDATED_DATE() +
                        " Person_Extended=" + dataQualityJRBIContext.recordsFromPersonExtended.get(i).getLAST_UPDATED_DATE());

                if (dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getDELETE_FLAG() != null ||
                        (dataQualityJRBIContext.recordsFromPersonExtended.get(i).getDELETE_FLAG() != null)) {
                    Assert.assertEquals("The DELETE_FLAG is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestPerson.get(i).getDELETE_FLAG(),
                            dataQualityJRBIContext.recordsFromPersonExtended.get(i).getDELETE_FLAG());
                }

            }
        }
    }



}