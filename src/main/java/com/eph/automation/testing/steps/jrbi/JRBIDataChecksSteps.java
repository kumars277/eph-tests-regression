package com.eph.automation.testing.steps.jrbi;

import com.amazonaws.services.iot.model.CACertificate;
import com.amazonaws.services.iot.model.CACertificateStatus;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.*;

import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIManifestationDataChecksSQL;
import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIPersonDataChecksSQL;
import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIWorkDataChecksSQL;
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


public class JRBIDataChecksSteps {

    public JRBIAccessDLContext dataQualityJRBIContext;
    private static String sql;
    private static List<String> Ids;
    private JRBIWorkDataChecksSQL jrbiObj = new JRBIWorkDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^We get the(.*) random EPR ids from full load(.*)$")
    public void getRandomSourceEPRIds(String numberOfRecords, String tableName) {
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Source EPR Ids...");
        switch (tableName) {
            case "jrbi_transform_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_EPR_IDS_FULLLOAD, numberOfRecords);
                break;
            case "jrbi_transform_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_EPR_IDS_MANIF_FULLLOAD, numberOfRecords);
                break;
            case "jrbi_transform_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_EPR_IDS_PERSON_FULLLOAD, numberOfRecords);
                break;

        }
        Log.info(sql);
        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
    }

    @When("^We get the records from data jrbi_journal_data_full (.*)$")
    public void getRecordsFromFullLoad(String tableName) {
        Log.info("We get the FULL Load records...");
        switch (tableName) {
            case "jrbi_transform_current_work":
            sql = String.format(JRBIWorkDataChecksSQL.GET_WORK_RECORDS_FULL_LOAD, Joiner.on("','").join(Ids));
            break;
            case "jrbi_transform_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_MANIF_RECORDS_FULL_LOAD, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_PERSON_RECORDS_FULL_LOAD, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDataFullLoad = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }


    @Then("^We get the records from transform (.*)$")
    public void getRecords(String table) {
        Log.info("We get the records...");
        switch (table) {
            case "jrbi_transform_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_CURRENT_PERSON_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_previous_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_previous_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_previous_person":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_delta_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_CURRENT_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_delta_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_DELTA_MANIF_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_delta_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_DELTA_PERSON_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_history_work_excl_delta":
                sql = String.format(JRBIWorkDataChecksSQL.GET_RECORDS_FROM_WORK_EXCLUDE, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_history_manifestation_excl_delta":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_RECORDS_FROM_MANIF_EXCLUDE, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_history_person_excl_delta":
                sql = String.format(JRBIPersonDataChecksSQL.GET_RECORDS_FROM_PERSON_EXCLUDE, Joiner.on("','").join(Ids));
                break;

        }
        Log.info(sql);
        dataQualityJRBIContext.recordsFromSource = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }


    @And("^Compare the records of jrbi_journal_data_full and transform current (.*)")
    public void compareDataFullandCurrent(String table) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        if (dataQualityJRBIContext.recordsFromDataFullLoad.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Full Load and Current table...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDataFullLoad.size(); i++) {

                switch (table){
                    case "jrbi_transform_current_work":

                        dataQualityJRBIContext.recordsFromDataFullLoad.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etl_curr_workext = {"getEPR","getRECORD_TYPE","getWORK_TYPE","getPRIMARY_SITE_SYSTEM","getPRIMARY_SITE_ACRONYM","getPRIMARY_SITE_SUPPORT_LEVEL","getFULFILMENT_SYSTEM",
                                "getFULFILMENT_JOURNAL_ACRONYM","getISSUE_PROD_TYPE_CODE","getCATALOGUE_ISSUES_QTY","getCATALOGUE_VOLUME_FROM","getCATALOGUE_VOLUME_TO",
                                "getRF_ISSUES_QTY","getRF_TOTAL_PAGES_QTY","getRF_FVI","getRF_LVI","getBUSINESS_UNIT_DESC"};
                        for (String strTemp : etl_curr_workext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDataFullLoad.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                                    " " + strTemp + " => work_full_load = " + method.invoke(objectToCompare1) +
                                    " current_work = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:"+dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                    break;
                    case "jrbi_transform_current_manifestation":
                        dataQualityJRBIContext.recordsFromDataFullLoad.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE"};
                        for (String strTemp : etl_curr_manifest) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDataFullLoad.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                                    " " + strTemp + " => Manif_Full_Load = " + method.invoke(objectToCompare1) +
                                    " Current_manifest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                            break;
                    case "jrbi_transform_current_person":
                        dataQualityJRBIContext.recordsFromDataFullLoad.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort primarykey data in the lists
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        dataQualityJRBIContext.recordsFromDataFullLoad.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY));

                        String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID"};

                        for (String strTemp : etl_curr_persext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDataFullLoad.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                                    " " + strTemp + " => ful_load_person = " + method.invoke(objectToCompare1) +
                                    " currentPerson = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in currentperson for EPR:" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }

    @Given("^We get the (.*) random EPR ids from (.*)$")
    public void getRandomEPRIds(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Source EPR Ids...");
        switch (tableName) {
            case "jrbi_transform_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_EPR_ID, numberOfRecords);
                break;
            case "jrbi_transform_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_EPR_ID, numberOfRecords);
                break;
            case "jrbi_transform_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_CURRENT_PERSON_EPR_ID, numberOfRecords);
                break;
            case "jrbi_transform_previous_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_EPR_ID, numberOfRecords);
                break;
            case "jrbi_transform_previous_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_EPR_ID, numberOfRecords);
                break;
            case "jrbi_transform_previous_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_PREVIOUS_PERSON_EPR_ID, numberOfRecords);
                break;
            case "jrbi_delta_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_EPR_ID, numberOfRecords);
                break;
            case "jrbi_delta_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_DELTA_MANIF_EPR_ID, numberOfRecords);
                break;
            case "jrbi_delta_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_DELTA_PERSON_EPR_ID, numberOfRecords);
                break;
            case "jrbi_transform_history_work_excl_delta":
                sql = String.format(JRBIWorkDataChecksSQL.GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_WORK, numberOfRecords);
                break;
            case "jrbi_transform_history_manifestation_excl_delta":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_MANIF, numberOfRecords);
                break;
            case "jrbi_transform_history_person_excl_delta":
                sql = String.format(JRBIPersonDataChecksSQL.GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_PERSON, numberOfRecords);
                break;
            case "jrbi_transform_latest_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_EPR_LATEST_WORK, numberOfRecords);
                break;
            case "jrbi_transform_latest_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_JRBI_EPR_MANIF_LATEST, numberOfRecords);
                break;
            case "jrbi_transform_latest_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_EPR_FOR_PERSON_LATEST, numberOfRecords);
                break;
        }
        Log.info(sql);
        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
    }


    @Then("^Get the records from transform history (.*)$")
    public void getRecordsFromCurrentWorkHistory(String table) {
        Log.info("We get the records from Current history ..");
        switch (table){
            case "jrbi_transform_current_work_history_part":
                sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_current_manifestation_history_part":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_HISTORY_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_current_person_history_part":
                sql = String.format(JRBIPersonDataChecksSQL.GET_CURRENT_PERSON_HISTORY_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_previous_work_history_part":
                sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_previous_manifestation_history_part":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_HISTORY_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_previous_person_history_part":
                sql = String.format(JRBIPersonDataChecksSQL.GET_PREVIOUS_PERSON_HISTORY_RECORDS, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        dataQualityJRBIContext.recordsFromTarget = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^compare the records of (.*) and (.*)$")
    public void compareCurrentandHistory(String sourcetable, String targetTable)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromSource.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between current and Current Hitory...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromSource.size(); i++) {
                switch (sourcetable){
                    case "jrbi_transform_current_work":
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etl_curr_workext = {"getEPR","getRECORD_TYPE","getWORK_TYPE","getPRIMARY_SITE_SYSTEM","getPRIMARY_SITE_ACRONYM","getPRIMARY_SITE_SUPPORT_LEVEL","getFULFILMENT_SYSTEM",
                                "getFULFILMENT_JOURNAL_ACRONYM","getISSUE_PROD_TYPE_CODE","getCATALOGUE_ISSUES_QTY","getCATALOGUE_VOLUME_FROM","getCATALOGUE_VOLUME_TO",
                                "getRF_ISSUES_QTY","getRF_TOTAL_PAGES_QTY","getRF_FVI","getRF_LVI","getBUSINESS_UNIT_DESC"};
                        for (String strTemp : etl_curr_workext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  dataQualityJRBIContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => current_work = " + method.invoke(objectToCompare1) +
                                    " work_history = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:"+dataQualityJRBIContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_current_manifestation":
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE"};
                        for (String strTemp : etl_curr_manifest) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  dataQualityJRBIContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => Current_manifest = " + method.invoke(objectToCompare1) +
                                    " manifest_history = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:"+dataQualityJRBIContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jrbi_transform_current_person":
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY));
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getPEOPLEHUB_ID)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getPEOPLEHUB_ID));

                        String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID"};

                        for (String strTemp : etl_curr_persext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + dataQualityJRBIContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => Current_person = " + method.invoke(objectToCompare1) +
                                    " PersonCurrent_history = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:" + dataQualityJRBIContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jrbi_transform_previous_work":
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etl_prev_workext = {"getEPR","getRECORD_TYPE","getWORK_TYPE","getPRIMARY_SITE_SYSTEM","getPRIMARY_SITE_ACRONYM","getPRIMARY_SITE_SUPPORT_LEVEL","getFULFILMENT_SYSTEM",
                                "getFULFILMENT_JOURNAL_ACRONYM","getISSUE_PROD_TYPE_CODE","getCATALOGUE_ISSUES_QTY","getCATALOGUE_VOLUME_FROM","getCATALOGUE_VOLUME_TO",
                                "getRF_ISSUES_QTY","getRF_TOTAL_PAGES_QTY","getRF_FVI","getRF_LVI","getBUSINESS_UNIT_DESC"};
                        for (String strTemp : etl_prev_workext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  dataQualityJRBIContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => previous_work = " + method.invoke(objectToCompare1) +
                                    " work_history = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:"+dataQualityJRBIContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_previous_manifestation":
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etl_prev_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE"};
                        for (String strTemp : etl_prev_manifest) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  dataQualityJRBIContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => Previous_Manif = " + method.invoke(objectToCompare1) +
                                    " Manif_History = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:"+dataQualityJRBIContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_previous_person":
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY));
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getPEOPLEHUB_ID)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getPEOPLEHUB_ID));

                        String[] etl_prev_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID"};

                        for (String strTemp : etl_prev_persext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + dataQualityJRBIContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => Previous_person = " + method.invoke(objectToCompare1) +
                                    " PersonCurrent_history = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:" + dataQualityJRBIContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }


    @When("^Get the records from the difference of current and previous tables (.*)$")
    public void getDiffCurrentPreviousWorkRec(String table) {
        Log.info("We get the Difference of Current and Previous records...");
        switch (table){
            case "jrbi_delta_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_DIFF_REC_PREVIOUS_CURRENT_WORK, Joiner.on("','").join(Ids));
             break;
            case "jrbi_delta_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_DIFF_REC_PREVIOUS_CURRENT_PREVIOUS_MANIF, Joiner.on("','").join(Ids));
                break;
            case "jrbi_delta_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_DIFF_REC_PREVIOUS_CURRENT_PREVIOUS_PERSON, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^commpare records of (.*) with difference of(.*) and (.*)$")
    public void compareDeltaWithcurrentPrevious( String fsourceTable, String ssourceTable, String tTrgttable)  throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between DElta and Current,Previous tables...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.size(); i++) {

                switch (tTrgttable){
                    case "jrbi_delta_current_work":
                        dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etl_curr_workext = {"getEPR","getRECORD_TYPE","getWORK_TYPE","getPRIMARY_SITE_SYSTEM","getPRIMARY_SITE_ACRONYM","getPRIMARY_SITE_SUPPORT_LEVEL","getFULFILMENT_SYSTEM",
                                "getFULFILMENT_JOURNAL_ACRONYM","getISSUE_PROD_TYPE_CODE","getCATALOGUE_ISSUES_QTY","getCATALOGUE_VOLUME_FROM","getCATALOGUE_VOLUME_TO",
                                "getRF_ISSUES_QTY","getRF_TOTAL_PAGES_QTY","getRF_FVI","getRF_LVI","getBUSINESS_UNIT_DESC","getDELTA_MODE","getTYPE"};

                        for (String strTemp : etl_curr_workext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR() +
                                    " " + strTemp + " => Diff_curr_prev = " + method.invoke(objectToCompare1) +
                                    " Deltawork = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+tTrgttable+" for EPR:" + dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_delta_current_manifestation":
                        dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE","getTYPE","getDELTA_MODE"};
                        for (String strTemp : etl_curr_manifest) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR() +
                                    " " + strTemp + " => Curr_Previous_Manif = " + method.invoke(objectToCompare1) +
                                    " Delta_Manif = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+tTrgttable+" for EPR:"+dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jrbi_delta_current_person":
                        dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY));
                        dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.sort(Comparator.comparing(JRBIDLAccessObject::getPEOPLEHUB_ID)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getPEOPLEHUB_ID));

                        String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID","getTYPE","getDELTA_MODE"};

                        for (String strTemp : etl_curr_persext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR() +
                                    " " + strTemp + " => Current&Previous_person = " + method.invoke(objectToCompare1) +
                                    " DeltaPerson = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in DeltaPerson for EPR:" + dataQualityJRBIContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }


            }
        }
    }

    @When("^Get the records from the difference of (.*) and (.*)$")
    public void getRecordsDeltaWorkandHistory(String ftable, String stable) {
        Log.info("We get the difference of Delta Current and Current History records...");
        String combTable = ftable+stable;
        switch (combTable){
            case "jrbi_delta_current_workjrbi_transform_current_work_history_part":
                sql = String.format(JRBIWorkDataChecksSQL.GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_WORK, Joiner.on("','").join(Ids));
                break;

            case "jrbi_delta_current_manifestationjrbi_transform_current_manifestation_history_part":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_MANIF, Joiner.on("','").join(Ids));
                break;

            case "jrbi_delta_current_personjrbi_transform_current_person_history_part":
                sql = String.format(JRBIPersonDataChecksSQL.GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_PERSON, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffDeltaAndHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }







/*
    @Given("^We get the (.*) random EPR ids (.*)$")
    public void getRandomEPRIds(String numberOfRecords, String tableName) {
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random EPR Ids...");
        switch (tableName) {

            case "jrbi_transform_latest_work":

                List<Map<?, ?>> randomLatestWorkEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomLatestWorkEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;

            case "jrbi_transform_previous_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_RANDOM_DELTA_EPR_WORK, numberOfRecords);
                List<Map<?, ?>> randomCurrentPreviousWorkEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomCurrentPreviousWorkEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }*/













    @Then("^Get the records from transform delta work history$")
    public void getRecDeltaWorkHistory(){
        Log.info("We get the records from Delta Current Work History..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

/*

    @And("^Compare the records of delta work and delta work history$")
    public void compareDeltaWorkandDeltaHistory()  throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromFromDeltaWork.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between DElta Work and Delta Work History...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromDeltaWork.size(); i++) {

                dataQualityJRBIContext.recordsFromFromDeltaWork.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_workext = {"getEPR", "getRECORD_TYPE", "getWORK_TYPE", "getPRIMARY_SITE_SYSTEM", "getPRIMARY_SITE_ACRONYM", "getPRIMARY_SITE_SUPPORT_LEVEL", "getFULFILMENT_SYSTEM",
                        "getFULFILMENT_JOURNAL_ACRONYM", "getISSUE_PROD_TYPE_CODE", "getCATALOGUE_ISSUES_QTY", "getCATALOGUE_VOLUME_FROM", "getCATALOGUE_VOLUME_TO",
                        "getRF_ISSUES_QTY", "getRF_TOTAL_PAGES_QTY", "getRF_FVI", "getRF_LVI", "getBUSINESS_UNIT_DESC", "getDELTA_MODE", "getTYPE"};

                for (String strTemp : etl_curr_workext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromFromDeltaWork.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                            " " + strTemp + " => Deltawork = " + method.invoke(objectToCompare1) +
                            " Deltawork_hist = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

*/

    @And("^compare the records of Exclude with difference of Delta_current and current_history(.*)$")
    public void compareExcludeWork(String table) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Exclude Work with Delta_current and current_history...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.size(); i++) {

                switch (table){
                    case "jrbi_transform_history_work_excl_delta":
                        dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etl_curr_workext = {"getEPR", "getRECORD_TYPE", "getWORK_TYPE", "getPRIMARY_SITE_SYSTEM", "getPRIMARY_SITE_ACRONYM", "getPRIMARY_SITE_SUPPORT_LEVEL", "getFULFILMENT_SYSTEM",
                                "getFULFILMENT_JOURNAL_ACRONYM", "getISSUE_PROD_TYPE_CODE", "getCATALOGUE_ISSUES_QTY", "getCATALOGUE_VOLUME_FROM", "getCATALOGUE_VOLUME_TO",
                                "getRF_ISSUES_QTY", "getRF_TOTAL_PAGES_QTY", "getRF_FVI", "getRF_LVI", "getBUSINESS_UNIT_DESC", "getLAST_UPDATED_DATE", "getDELETE_FLAG"};

                        for (String strTemp : etl_curr_workext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.get(i).getEPR() +
                                    " " + strTemp + " => DeltaworkHist = " + method.invoke(objectToCompare1) +
                                    " ExcludeWork = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+table+" for EPR:" + dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_history_manifestation_excl_delta":

                    dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                    dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                    String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE","getLAST_UPDATED_DATE","getDELETE_FLAG"};
                    for (String strTemp : etl_curr_manifest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;
                        JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.get(i);
                        JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromSource.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPR => " +  dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.get(i).getEPR() +
                                " " + strTemp + " => DeltaManif_Hist = " + method.invoke(objectToCompare1) +
                                " Exclude_Manif = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifest_hsistory for EPR:"+dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.get(i).getEPR(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                        break;

                    case "jrbi_transform_history_person_excl_delta":

                        dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY));
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY));
                        dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.sort(Comparator.comparing(JRBIDLAccessObject::getPEOPLEHUB_ID));
                        dataQualityJRBIContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getPEOPLEHUB_ID));

                        String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID","getLAST_UPDATED_DATE","getDELETE_FLAG"};

                        for (String strTemp : etl_curr_persext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.get(i).getEPR() +
                                    " " + strTemp + " => DeltaPersonHist = " + method.invoke(objectToCompare1) +
                                    " ExcludePerson = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in ExcludePerson for EPR:" + dataQualityJRBIContext.recordsFromDiffDeltaAndHistory.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }

            }
        }
    }


    @When("^Get the records from the addition of delta current and exclude (.*)$")
    public void getAddRecDeltaCurrentWorkandExclude( String SecondSourceTable){
        Log.info("We get the Addition of Delta Current and Exclude records...");
        switch (SecondSourceTable){
            case "jrbi_transform_latest_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_JRBI_REC_SUM_DELTA_WORK_AND_EXCLUDE, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_latest_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_JRBI_REC_SUM_DELTA_MANIF_AND_MANIF_EXCLUDE, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_latest_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_JRBI_REC_SUM_DELTA_PERSON_AND_PERSON_EXCL, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);

    }

    @Then("^Get the records from latest table (.*)$")
    public void getLatestWorkRec(String table){
        Log.info("We get the Latest records...");
        switch (table){
            case "jrbi_transform_latest_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_JRBI_WORK_LATEST_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_latest_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_JRBI_MANIF_LATEST_RECORDS, Joiner.on("','").join(Ids));
                break;
            case "jrbi_transform_latest_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_JRBI_PERSON_LATEST_RECORDS, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        dataQualityJRBIContext.recordsFromLAtest = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records for Latest tables (.*)$")
    public void compareLatestWork(String table) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Latest with addition of Delta_current and Exclude tables...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.size(); i++) {

                switch (table){
                    case "jrbi_transform_latest_work":
                        dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromLAtest.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etl_curr_workext = {"getEPR", "getRECORD_TYPE", "getWORK_TYPE", "getPRIMARY_SITE_SYSTEM", "getPRIMARY_SITE_ACRONYM", "getPRIMARY_SITE_SUPPORT_LEVEL", "getFULFILMENT_SYSTEM",
                                "getFULFILMENT_JOURNAL_ACRONYM", "getISSUE_PROD_TYPE_CODE", "getCATALOGUE_ISSUES_QTY", "getCATALOGUE_VOLUME_FROM", "getCATALOGUE_VOLUME_TO",
                                "getRF_ISSUES_QTY", "getRF_TOTAL_PAGES_QTY", "getRF_FVI", "getRF_LVI", "getBUSINESS_UNIT_DESC","getLAST_UPDATED_DATE","getDELETE_FLAG"};

                        for (String strTemp : etl_curr_workext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromLAtest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR() +
                                    " " + strTemp + " => DeltaWorkExclude = " + method.invoke(objectToCompare1) +
                                    " LatestWork = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:" + dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_latest_manifestation":
                        dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromLAtest.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE"};
                        for (String strTemp : etl_curr_manifest) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromLAtest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR() +
                                    " " + strTemp + " => Delta_Exclde_Manif = " + method.invoke(objectToCompare1) +
                                    " Latest_Manif = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifest_hsistory for EPR:"+dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_latest_person":

                        dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromLAtest.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromLAtest.sort(Comparator.comparing(JRBIDLAccessObject::getU_KEY));
                        dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.sort(Comparator.comparing(JRBIDLAccessObject::getPEOPLEHUB_ID)); //sort data in the lists
                        dataQualityJRBIContext.recordsFromLAtest.sort(Comparator.comparing(JRBIDLAccessObject::getPEOPLEHUB_ID));


                        String[] etl_curr_persext = {"getEPR","getRECORD_TYPE","getROLE_CODE","getU_KEY","getROLE_DESCRIPTION","getGIVEN_NAME","getFAMILY_NAME","getEMAIL","getPEOPLEHUB_ID","getLAST_UPDATED_DATE","getDELETE_FLAG"};

                        for (String strTemp : etl_curr_persext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.get(i);
                            JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromLAtest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR() +
                                    " " + strTemp + " => Delta&ExclPerson = " + method.invoke(objectToCompare1) +
                                    " LatestPErson = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in LatestPErson for EPR:" + dataQualityJRBIContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                      break;
                   }
            }
        }
    }

}