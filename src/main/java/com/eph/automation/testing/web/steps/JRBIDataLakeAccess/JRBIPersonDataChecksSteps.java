package com.eph.automation.testing.web.steps.JRBIDataLakeAccess;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLPersonAccessObject;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLWorkAccessObject;
import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIPersonDataChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
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
    public void compareDataFullandCurrentPerson() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromDataFullLoadPerson.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Person Full Load and Current Person...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDataFullLoadPerson.size(); i++) {

                dataQualityJRBIContext.recordsFromDataFullLoadPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort primarykey data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));
                dataQualityJRBIContext.recordsFromDataFullLoadPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY));

                String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID"};

                for (String strTemp : etl_curr_persext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLPersonAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i);
                    JRBIDLPersonAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                            " " + strTemp + " => ful_load_person = " + method.invoke(objectToCompare1) +
                            " currentPerson = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in currentperson for EPR:" + dataQualityJRBIContext.recordsFromDataFullLoadPerson.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
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
    public void compareCurrentPersonandPersonHistory() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
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

                String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID"};

                for (String strTemp : etl_curr_persext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLPersonAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i);
                    JRBIDLPersonAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromCurrentPersonHistory.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR() +
                            " " + strTemp + " => Current_person = " + method.invoke(objectToCompare1) +
                            " PersonCurrent_history = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PersonCurrent_history for EPR:" + dataQualityJRBIContext.recordsFromFromCurrentPerson.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
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
    public void comparePreviousPersonandHistory() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
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


                String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID"};

                for (String strTemp : etl_curr_persext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLPersonAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i);
                    JRBIDLPersonAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromPreviousPersonHistory.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR() +
                            " " + strTemp + " => Previous_person = " + method.invoke(objectToCompare1) +
                            " PersonCurrent_history = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PersonCurrent_history for EPR:" + dataQualityJRBIContext.recordsFromFromPreviousPerson.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
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
    public void compareDeltawithCurrentPreviousPerson() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
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


                String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID","getTYPE","getDELTA_MODE"};

                for (String strTemp : etl_curr_persext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLPersonAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i);
                    JRBIDLPersonAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR() +
                            " " + strTemp + " => Current&Previous_person = " + method.invoke(objectToCompare1) +
                            " DeltaPerson = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in DeltaPerson for EPR:" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousPerson.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
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
    public void compareDeltaPersonandDeltapersonHistory() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromFromDeltaPerson.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Delta Person and Delta Person history...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromDeltaPerson.size(); i++) {

                dataQualityJRBIContext.recordsFromFromDeltaPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getEPR));
                dataQualityJRBIContext.recordsFromFromDeltaPerson.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.sort(Comparator.comparing(JRBIDLPersonAccessObject::getU_KEY));


                String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID","getTYPE","getDELTA_MODE"};

                for (String strTemp : etl_curr_persext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLPersonAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i);
                    JRBIDLPersonAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromDeltaPersonHistory.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                            " " + strTemp + " => DeltaPerson = " + method.invoke(objectToCompare1) +
                            " DeltaPErsonHist = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in DeltaPerson for EPR:" + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
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
    @And("^Get the records from person exclude jrbi ext table$")
    public void getExcludePersonRecords(){
        Log.info("We get the records fromPerson Exclude...");
        sql = String.format(JRBIPersonDataChecksSQL.GET_RECORDS_FROM_PERSON_EXCLUDE, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromExcludePerson = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of Person Exclude with difference of Delta_current_person and person_history$")
    public void compareDataForPersonExclude() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
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

                String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID","getLAST_UPDATED_DATE","getDELETE_FLAG"};

                for (String strTemp : etl_curr_persext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLPersonAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i);
                    JRBIDLPersonAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromExcludePerson.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaPerson.get(i).getEPR() +
                            " " + strTemp + " => DeltaPersonHist = " + method.invoke(objectToCompare1) +
                            " ExcludePerson = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in ExcludePerson for EPR:" + dataQualityJRBIContext.recordsFromDiffDeltaAndPersonHistory.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
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
    public void compareDataForPersonLatest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
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


                String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID","getLAST_UPDATED_DATE","getDELETE_FLAG"};

                for (String strTemp : etl_curr_persext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLPersonAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i);
                    JRBIDLPersonAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromLAtestPerson.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR() +
                            " " + strTemp + " => Delta&ExclPerson = " + method.invoke(objectToCompare1) +
                            " LatestPErson = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in LatestPErson for EPR:" + dataQualityJRBIContext.recordsFromAddDeltaAndPersonExclude.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

 }