package com.eph.automation.testing.steps.jrbi;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.*;

import com.eph.automation.testing.services.db.jrbisql.JRBIManifestationDataChecksSQL;
import com.eph.automation.testing.services.db.jrbisql.JRBIPersonDataChecksSQL;
import com.eph.automation.testing.services.db.jrbisql.JRBIWorkDataChecksSQL;
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

    private static String sql;
    private static List<String> ids;
    private static String nomsg = "tables not found";

    @Given("^We get the(.*) random EPR ids from full load(.*)$")
    public static void getRandomSourceEPRIds(String numberOfRecords, String tableName) {
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
             default:
                 Log.info(nomsg);
        }
        Log.info(sql);
        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
    }

    @When("^We get the records from data jrbi_journal_data_full (.*)$")
    public static void getRecordsFromFullLoad(String tableName) {
        Log.info("We get the FULL Load records...");
        switch (tableName) {
            case "jrbi_transform_current_work":
            sql = String.format(JRBIWorkDataChecksSQL.GET_WORK_RECORDS_FULL_LOAD, String.join("','",ids));
            break;
            case "jrbi_transform_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_MANIF_RECORDS_FULL_LOAD, String.join("','",ids));
                break;
            case "jrbi_transform_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_PERSON_RECORDS_FULL_LOAD, String.join("','",ids));
                break;
             default:
                 Log.info("tables not found");
        }
        Log.info(sql);
        JRBIAccessDLContext.recordsFromDataFullLoad = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }


    @When("^We get the records from transform (.*)$")
    public static void getRecords(String table) {
        Log.info("We get the records...");
        switch (table) {
            case "jrbi_transform_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_CURRENT_PERSON_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_previous_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_previous_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_previous_person":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_RECORDS, String.join("','",ids));
                break;
            case "jrbi_delta_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_CURRENT_RECORDS, String.join("','",ids));
                break;
            case "jrbi_delta_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_DELTA_MANIF_RECORDS, String.join("','",ids));
                break;
            case "jrbi_delta_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_DELTA_PERSON_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_history_work_excl_delta":
                sql = String.format(JRBIWorkDataChecksSQL.GET_RECORDS_FROM_WORK_EXCLUDE, String.join("','",ids));
                break;
            case "jrbi_transform_history_manifestation_excl_delta":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_RECORDS_FROM_MANIF_EXCLUDE, String.join("','",ids));
                break;
            case "jrbi_transform_history_person_excl_delta":
                sql = String.format(JRBIPersonDataChecksSQL.GET_RECORDS_FROM_PERSON_EXCLUDE, String.join("','",ids));
                break;
            default:
                Log.info(nomsg);


        }
        Log.info(sql);
        JRBIAccessDLContext.recordsFromSource = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }


    @And("^Compare the records of jrbi_journal_data_full and transform current (.*)")
    public void compareDataFullandCurrent(String table) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        if (JRBIAccessDLContext.recordsFromDataFullLoad.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Full Load and Current table...");
            for (int i = 0; i < JRBIAccessDLContext.recordsFromDataFullLoad.size(); i++) {
                switch (table){
                    case "jrbi_transform_current_work":
                        JRBIAccessDLContext.recordsFromDataFullLoad.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        String[] etlCurrWorkext = {"getEPR","getrecordType","getworkType","getprimarySiteSystem","getprimarySiteAcronym","getprimarySiteSupportLevel","getfulfilmentSystem",
                                "getfulfilmentJournalAcronym","getissueProdTypeCode","getcatalogueIssuesqty","getcatalogueVolumeFrom","getcatalogueVolumeTo",
                                "getrfIssuesQty","getrfTotalPagesQty","getrfFvi","getrfLvi","getbusinessUnitDesc"};
                        for (String strTemp : etlCurrWorkext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromDataFullLoad.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  JRBIAccessDLContext.recordsFromDataFullLoad.get(i).getEPR() +
                                    " " + strTemp + " => work_full_load = " + method.invoke(objectToCompare1) +
                                    " current_work = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:"+JRBIAccessDLContext.recordsFromDataFullLoad.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                    break;
                    case "jrbi_transform_current_manifestation":
                        JRBIAccessDLContext.recordsFromDataFullLoad.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etlCurrManifest = {"getEPR","getrecordType","getmanifestationType","getJOURNAL_ISSUE_TRIM_SIZE","getwarReference"};
                        for (String strTemp : etlCurrManifest) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromDataFullLoad.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + JRBIAccessDLContext.recordsFromDataFullLoad.get(i).getEPR() +
                                    " " + strTemp + " => Manif_Full_Load = " + method.invoke(objectToCompare1) +
                                    " Current_manifest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:" + JRBIAccessDLContext.recordsFromDataFullLoad.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jrbi_transform_current_person":
                        JRBIAccessDLContext.recordsFromDataFullLoad.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort primarykey data in the lists
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        JRBIAccessDLContext.recordsFromDataFullLoad.sort(Comparator.comparing(JRBIDLAccessObject::getuKey)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getuKey));

                        String[] etlCurrPersext = {"getEPR","getrecordType","getroleCode","getuKey","getroleDescription","getgivenName","getfamilyName","getemail","getpeopleHubId"};

                        for (String strTemp : etlCurrPersext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromDataFullLoad.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + JRBIAccessDLContext.recordsFromDataFullLoad.get(i).getEPR() +
                                    " " + strTemp + " => ful_load_person = " + method.invoke(objectToCompare1) +
                                    " currentPerson = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in currentperson for EPR:" + JRBIAccessDLContext.recordsFromDataFullLoad.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                       default:
                           Log.info(nomsg);
                }
            }
        }
    }

    @Given("^We get the (.*) random EPR ids from (.*)$")
    public static void getRandomEPRIds(String numberOfRecords, String tableName) {
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
            default:
                Log.info(nomsg);
        }
        Log.info(sql);
        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
    }


    @Then("^we get the records from transform history (.*)$")
    public static void getRecordsFromCurrentWorkHistory(String table) {
        Log.info("We get the records from Current history ..");
        switch (table){
            case "jrbi_transform_current_work_history_part":
                sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_HISTORY_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_current_manifestation_history_part":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_HISTORY_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_current_person_history_part":
                sql = String.format(JRBIPersonDataChecksSQL.GET_CURRENT_PERSON_HISTORY_RECORDS, String.join("','",ids));
                break;
//            case "jrbi_transform_previous_work_history_part":
//                sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_HISTORY_RECORDS, String.join("','",ids));
//                break;
//            case "jrbi_transform_previous_manifestation_history_part":
//                sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_HISTORY_RECORDS, String.join("','",ids));
//                break;
//            case "jrbi_transform_previous_person_history_part":
//                sql = String.format(JRBIPersonDataChecksSQL.GET_PREVIOUS_PERSON_HISTORY_RECORDS, String.join("','",ids));
//                break;
           case "jrbi_transform_work_file_history_part":
                sql = String.format(JRBIWorkDataChecksSQL.GET_WORK_FILE_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_manifestation_file_history_part":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_MANIF_FILE_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_person_file_history_part":
                sql = String.format(JRBIPersonDataChecksSQL.GET_PERSON_FILE_RECORDS, String.join("','",ids));
                break;
            default:
                Log.info(nomsg);
        }
        Log.info(sql);
        JRBIAccessDLContext.recordsFromTarget = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^compare the records of (.*) and (.*)$")
    public void compareCurrentandHistory(String sourcetable, String targetTable)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JRBIAccessDLContext.recordsFromSource.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between current and Current Hitory...");
            for (int i = 0; i < JRBIAccessDLContext.recordsFromSource.size(); i++) {
                switch (sourcetable){
                    case "jrbi_transform_current_work":
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etlCurrWorkext = {"getEPR","getrecordType","getworkType","getprimarySiteSystem","getprimarySiteAcronym","getprimarySiteSupportLevel","getfulfilmentSystem",
                                "getfulfilmentJournalAcronym","getissueProdTypeCode","getcatalogueIssuesqty","getcatalogueVolumeFrom","getcatalogueVolumeTo",
                                "getrfIssuesQty","getrfTotalPagesQty","getrfFvi","getrfLvi","getbusinessUnitDesc"};
                        for (String strTemp : etlCurrWorkext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  JRBIAccessDLContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => "+sourcetable +" = " + method.invoke(objectToCompare1) +
                                    targetTable+" = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:"+JRBIAccessDLContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_current_manifestation":
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etlCurrManifest = {"getEPR","getrecordType","getmanifestationType","getjournal_issue_trim_size","getwarReference"};
                        for (String strTemp : etlCurrManifest) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  JRBIAccessDLContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => "+sourcetable +" = " + method.invoke(objectToCompare1) +
                                    targetTable+" = " + method2.invoke(objectToCompare2));                           if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:"+JRBIAccessDLContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jrbi_transform_current_person":
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getuKey)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getuKey));
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getpeopleHubId)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getpeopleHubId));

                        String[] etlCurrPersext = {"getEPR","getrecordType","getroleCode","getuKey","getroleDescription","getgivenName","getfamilyName","getemail","getpeopleHubId"};

                        for (String strTemp : etlCurrPersext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + JRBIAccessDLContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => "+sourcetable +" = " + method.invoke(objectToCompare1) +
                                     targetTable+" = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:" + JRBIAccessDLContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jrbi_transform_previous_work":
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etlPrevWorkext = {"getEPR","getrecordType","getworkType","getprimarySiteSystem","getprimarySiteAcronym","getprimarySiteSupportLevel","getfulfilmentSystem",
                                "getfulfilmentJournalAcronym","getissueProdTypeCode","getcatalogueIssuesqty","getcatalogueVolumeFrom","getcatalogueVolumeTo",
                                "getrfIssuesQty","getrfTotalPagesQty","getrfFvi","getrfLvi","getbusinessUnitDesc"};
                        for (String strTemp : etlPrevWorkext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  JRBIAccessDLContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => previous_work = " + method.invoke(objectToCompare1) +
                                    " work_history = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:"+JRBIAccessDLContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_previous_manifestation":
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etlPrevManifest = {"getEPR","getrecordType","getmanifestationType","getJOURNAL_ISSUE_TRIM_SIZE","getwarReference"};
                        for (String strTemp : etlPrevManifest) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  JRBIAccessDLContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => Previous_Manif = " + method.invoke(objectToCompare1) +
                                    " Manif_History = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:"+JRBIAccessDLContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_previous_person":
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getuKey)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getuKey));
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getpeopleHubId)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromTarget.sort(Comparator.comparing(JRBIDLAccessObject::getpeopleHubId));

                        String[] etlPrevPersext = {"getEPR","getrecordType","getroleCode","getuKey","getroleDescription","getgivenName","getfamilyName","getemail","getpeopleHubId"};

                        for (String strTemp : etlPrevPersext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromSource.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromTarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + JRBIAccessDLContext.recordsFromSource.get(i).getEPR() +
                                    " " + strTemp + " => Previous_person = " + method.invoke(objectToCompare1) +
                                    " PersonCurrent_history = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+targetTable+" for EPR:" + JRBIAccessDLContext.recordsFromSource.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    default:
                        Log.info(nomsg);
                }
            }
        }
    }


    @When("^Get the records from the difference of current and previous tables (.*)$")
    public static void getDiffCurrentPreviousWorkRec(String table) {
        Log.info("We get the Difference of Current and Previous records...");
        switch (table){
            case "jrbi_delta_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_DIFF_REC_PREVIOUS_CURRENT_WORK, String.join("','",ids));
             break;
            case "jrbi_delta_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_DIFF_REC_PREVIOUS_CURRENT_PREVIOUS_MANIF, String.join("','",ids));
                break;
            case "jrbi_delta_current_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_DIFF_REC_PREVIOUS_CURRENT_PREVIOUS_PERSON, String.join("','",ids));
                break;
            default:
                Log.info(nomsg);
        }
        Log.info(sql);
        JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^commpare records of (.*) with difference of(.*) and (.*)$")
    public void compareDeltaWithcurrentPrevious( String fsourceTable, String ssourceTable, String tTrgttable)  throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between DElta and Current,Previous tables...");
            for (int i = 0; i < JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.size(); i++) {

                switch (tTrgttable){
                    case "jrbi_delta_current_work":
                        JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etlCurrWorkext = {"getEPR","getrecordType","getworkType","getprimarySiteSystem","getprimarySiteAcronym","getprimarySiteSupportLevel","getfulfilmentSystem",
                                "getfulfilmentJournalAcronym","getissueProdTypeCode","getcatalogueIssuesqty","getcatalogueVolumeFrom","getcatalogueVolumeTo",
                                "getrfIssuesQty","getrfTotalPagesQty","getrfFvi","getrfLvi","getbusinessUnitDesc","getdeltaMode","gettype"};

                        for (String strTemp : etlCurrWorkext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR() +
                                    " " + strTemp + " => Diff_curr_prev = " + method.invoke(objectToCompare1) +
                                    " Deltawork = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+tTrgttable+" for EPR:" + JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_delta_current_manifestation":
                        JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etlCurrManifest = {"getEPR","getrecordType","getmanifestationType","getJOURNAL_ISSUE_TRIM_SIZE","getwarReference","gettype","getdeltaMode"};
                        for (String strTemp : etlCurrManifest) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR() +
                                    " " + strTemp + " => Curr_Previous_Manif = " + method.invoke(objectToCompare1) +
                                    " Delta_Manif = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+tTrgttable+" for EPR:"+JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jrbi_delta_current_person":
                        JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.sort(Comparator.comparing(JRBIDLAccessObject::getuKey)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getuKey));
                        JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.sort(Comparator.comparing(JRBIDLAccessObject::getpeopleHubId)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getpeopleHubId));

                        String[] etlCurrPersext = {"getEPR","getrecordType","getroleCode","getuKey","getroleDescription","getgivenName","getfamilyName","getemail","getpeopleHubId","gettype","getdeltaMode"};

                        for (String strTemp : etlCurrPersext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR() +
                                    " " + strTemp + " => Current&Previous_person = " + method.invoke(objectToCompare1) +
                                    " DeltaPerson = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in DeltaPerson for EPR:" + JRBIAccessDLContext.recordsFromDiffCurrentAndPrevious.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                     default:
                         Log.info(nomsg);
                }
            }
        }
    }

    @When("^Get the records from the difference of (.*) and (.*)$")
    public static void getRecordsDeltaWorkandHistory(String ftable, String stable) {
        Log.info("We get the difference of Delta Current and Current History records...");
        String combTable = ftable+stable;
        switch (combTable){
            case "jrbi_delta_current_workjrbi_transform_current_work_history_part":
                sql = String.format(JRBIWorkDataChecksSQL.GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_WORK, String.join("','",ids));
                break;
            case "jrbi_delta_current_manifestationjrbi_transform_current_manifestation_history_part":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_MANIF, String.join("','",ids));
                break;
            case "jrbi_delta_current_personjrbi_transform_current_person_history_part":
                sql = String.format(JRBIPersonDataChecksSQL.GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_PERSON, String.join("','",ids));
                break;
            default:
                Log.info(nomsg);
        }
        Log.info(sql);
        JRBIAccessDLContext.recordsFromDiffDeltaAndHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform delta work history$")
    public static void getRecDeltaWorkHistory(){
        Log.info("We get the records from Delta Current Work History..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_HISTORY_RECORDS, String.join("','",ids));
        Log.info(sql);
        JRBIAccessDLContext.recordsFromFromDeltaWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

   @And("^Compare the records of Exclude with difference of Delta_current and current_history(.*)$")
    public void compareExcludeWork(String table) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Exclude Work with Delta_current and current_history...");
            for (int i = 0; i < JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.size(); i++) {
                switch (table){
                    case "jrbi_transform_history_work_excl_delta":
                        JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etlCurrworkext = {"getEPR", "getrecordType", "getworkType", "getprimarySiteSystem", "getprimarySiteAcronym", "getprimarySiteSupportLevel", "getfulfilmentSystem",
                                "getfulfilmentJournalAcronym", "getissueProdTypeCode", "getcatalogueIssuesqty", "getcatalogueVolumeFrom", "getcatalogueVolumeTo",
                                "getrfIssuesQty", "getrfTotalPagesQty", "getrfFvi", "getrfLvi", "getbusinessUnitDesc", "getlastUpdatedDate", "getdeleteFlag"};

                        for (String strTemp : etlCurrworkext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.get(i).getEPR() +
                                    " " + strTemp + " => DeltaworkHist = " + method.invoke(objectToCompare1) +
                                    " ExcludeWork = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+table+" for EPR:" + JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_history_manifestation_excl_delta":
                    JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                    JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                    String[] etlCurrManifest = {"getEPR","getrecordType","getmanifestationType","getJOURNAL_ISSUE_TRIM_SIZE","getwarReference","getlastUpdatedDate","getdeleteFlag"};
                    for (String strTemp : etlCurrManifest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;
                        JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.get(i);
                        JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromSource.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPR => " +  JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.get(i).getEPR() +
                                " " + strTemp + " => DeltaManif_Hist = " + method.invoke(objectToCompare1) +
                                " Exclude_Manif = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifest_hsistory for EPR:"+JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.get(i).getEPR(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                        break;

                    case "jrbi_transform_history_person_excl_delta":

                        JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.sort(Comparator.comparing(JRBIDLAccessObject::getuKey));
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getuKey));
                        JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.sort(Comparator.comparing(JRBIDLAccessObject::getpeopleHubId));
                        JRBIAccessDLContext.recordsFromSource.sort(Comparator.comparing(JRBIDLAccessObject::getpeopleHubId));

                        String[] etlCurrPersext = {"getEPR","getrecordType","getroleCode","getuKey","getroleDescription","getgivenName","getfamilyName","getemail","getpeopleHubId","getlastUpdatedDate","getdeleteFlag"};

                        for (String strTemp : etlCurrPersext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromSource.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.get(i).getEPR() +
                                    " " + strTemp + " => DeltaPersonHist = " + method.invoke(objectToCompare1) +
                                    " ExcludePerson = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in ExcludePerson for EPR:" + JRBIAccessDLContext.recordsFromDiffDeltaAndHistory.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                      default:
                          Log.info(nomsg);
                }

            }
        }
    }


    @When("^we get the records from the addition of delta current and exclude (.*)$")
    public static void getAddRecDeltaCurrentWorkandExclude( String secondSourceTable){
        Log.info("We get the Addition of Delta Current and Exclude records...");
        switch (secondSourceTable){
            case "jrbi_transform_latest_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_JRBI_REC_SUM_DELTA_WORK_AND_EXCLUDE, String.join("','",ids));
                break;
            case "jrbi_transform_latest_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_JRBI_REC_SUM_DELTA_MANIF_AND_MANIF_EXCLUDE, String.join("','",ids));
                break;
            case "jrbi_transform_latest_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_JRBI_REC_SUM_DELTA_PERSON_AND_PERSON_EXCL, String.join("','",ids));
                break;
             default:
                 Log.info(nomsg);
        }

        Log.info(sql);
        JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);

    }

    @Then("^we get the records from latest table (.*)$")
    public static void getLatestWorkRec(String table){
        Log.info("We get the Latest records...");
        switch (table){
            case "jrbi_transform_latest_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_JRBI_WORK_LATEST_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_latest_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_JRBI_MANIF_LATEST_RECORDS, String.join("','",ids));
                break;
            case "jrbi_transform_latest_person":
                sql = String.format(JRBIPersonDataChecksSQL.GET_JRBI_PERSON_LATEST_RECORDS, String.join("','",ids));
                break;
              default:
                  Log.info(nomsg);
        }
        Log.info(sql);
        JRBIAccessDLContext.recordsFromLAtest = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records for Latest tables (.*)$")
    public void compareLatestWork(String table) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Latest with addition of Delta_current and Exclude tables...");
            for (int i = 0; i < JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.size(); i++) {

                switch (table){
                    case "jrbi_transform_latest_work":
                        JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromLAtest.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etlCurrWorkext = {"getEPR", "getrecordType", "getworkType", "getprimarySiteSystem", "getprimarySiteAcronym", "getprimarySiteSupportLevel", "getfulfilmentSystem",
                                "getfulfilmentJournalAcronym", "getissueProdTypeCode", "getcatalogueIssuesqty", "getcatalogueVolumeFrom", "getcatalogueVolumeTo",
                                "getrfIssueQty", "getrfTotalPagesQty", "getrfFvi", "getrfLvi", "getbusinessUnitDesc","getlastUpdatedDate","getdeleteFlag"};

                        for (String strTemp : etlCurrWorkext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromLAtest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR() +
                                    " " + strTemp + " => DeltaWorkExclude = " + method.invoke(objectToCompare1) +
                                    " LatestWork = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:" + JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_latest_manifestation":
                        JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromLAtest.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                        String[] etlCurrManifest = {"getEPR","getrecordType","getmanifestationType","getjournal_issue_trim_size","getwarReference"};
                        for (String strTemp : etlCurrManifest) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromLAtest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " +  JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR() +
                                    " " + strTemp + " => Delta_Exclde_Manif = " + method.invoke(objectToCompare1) +
                                    " Latest_Manif = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifest_hsistory for EPR:"+JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "jrbi_transform_latest_person":

                        JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromLAtest.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));
                        JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.sort(Comparator.comparing(JRBIDLAccessObject::getuKey)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromLAtest.sort(Comparator.comparing(JRBIDLAccessObject::getuKey));
                        JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.sort(Comparator.comparing(JRBIDLAccessObject::getpeopleHubId)); //sort data in the lists
                        JRBIAccessDLContext.recordsFromLAtest.sort(Comparator.comparing(JRBIDLAccessObject::getpeopleHubId));


                        String[] etlCurrPersext = {"getEPR","getrecordType","getroleCode","getuKey","getroleDescription","getgivenName","getfamilyName","getemail","getpeopleHubId","getlastUpdatedDate","getdeleteFlag"};

                        for (String strTemp : etlCurrPersext) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            JRBIDLAccessObject objectToCompare1 = JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.get(i);
                            JRBIDLAccessObject objectToCompare2 = JRBIAccessDLContext.recordsFromLAtest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPR => " + JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR() +
                                    " " + strTemp + " => Delta&ExclPerson = " + method.invoke(objectToCompare1) +
                                    " LatestPErson = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in LatestPErson for EPR:" + JRBIAccessDLContext.recordsFromAddDeltaCurrAndExclude.get(i).getEPR(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                      break;
                    default:
                        Log.info(nomsg);

                   }
            }
        }
    }

}