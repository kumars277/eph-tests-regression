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

    @Given("^We get the (.*) random EPR ids for Person from data full load (.*)$")
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

    @And("^Compare the records of person full load and current person (.*)$")
    public void compareDataFullandCurrentPerson(String tableName) {
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


}