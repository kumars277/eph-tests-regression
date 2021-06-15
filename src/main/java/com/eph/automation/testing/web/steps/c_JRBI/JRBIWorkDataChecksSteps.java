package com.eph.automation.testing.web.steps.c_JRBI;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.*;

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


public class JRBIWorkDataChecksSteps {

    public JRBIAccessDLContext dataQualityJRBIContext;
    private static String sql;
    private static List<String> Ids;
    private JRBIWorkDataChecksSQL jrbiObj = new JRBIWorkDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^We get the (.*) random EPR ids (.*)$")
    public void getRandomEPRIds(String numberOfRecords, String tableName) {
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random EPR Ids...");
        switch (tableName) {
            case "jrbi_journal_data_full":
                sql = String.format(JRBIWorkDataChecksSQL.GET_EPR_IDS_FULLLOAD, numberOfRecords);
                List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_EPR_ID, numberOfRecords);
                List<Map<?, ?>> randomCurrentWorkEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomCurrentWorkEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_previous_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_EPR_ID, numberOfRecords);
                List<Map<?, ?>> randomPreviousWorkEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPreviousWorkEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_delta_current_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_EPR_ID, numberOfRecords);
                List<Map<?, ?>> randomDeltaWorkEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomDeltaWorkEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_history_work_excl_delta":
                sql = String.format(JRBIWorkDataChecksSQL.GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_WORK, numberOfRecords);
                List<Map<?, ?>> randomExclWorkEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomExclWorkEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_latest_work":
                sql = String.format(JRBIWorkDataChecksSQL.GET_EPR_LATEST_WORK, numberOfRecords);
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
    }


    @When("^We get the records from data jrbi_journal_data_full$")
    public void getRecordsFromFullLoad() {
        Log.info("We get the FULL Load records...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_WORK_RECORDS_FULL_LOAD, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDataFullLoad = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^We get the records from transform current work$")
    public void getRecordsFromCurrentWork() {
        Log.info("We get the records from Current Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentWork = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of jrbi_journal_data_full and transform current work$")
    public void compareDataFullandCurrentWork() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        if (dataQualityJRBIContext.recordsFromDataFullLoad.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Full Load and Current Work...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDataFullLoad.size(); i++) {

                dataQualityJRBIContext.recordsFromDataFullLoad.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentWork.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_workext = {"getEPR","getRECORD_TYPE","getWORK_TYPE","getPRIMARY_SITE_SYSTEM","getPRIMARY_SITE_ACRONYM","getPRIMARY_SITE_SUPPORT_LEVEL","getFULFILMENT_SYSTEM",
                        "getFULFILMENT_JOURNAL_ACRONYM","getISSUE_PROD_TYPE_CODE","getCATALOGUE_ISSUES_QTY","getCATALOGUE_VOLUME_FROM","getCATALOGUE_VOLUME_TO",
                        "getRF_ISSUES_QTY","getRF_TOTAL_PAGES_QTY","getRF_FVI","getRF_LVI","getBUSINESS_UNIT_DESC"};
                for (String strTemp : etl_curr_workext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDataFullLoad.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromCurrentWork.get(i);

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
            }
        }
    }

    @Then("^Get the records from transform current work history$")
    public void getRecordsFromCurrentWorkHistory() {
        Log.info("We get the records from Current history Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of current work and current work history$")
    public void compareCurrentWorkandCurrentHistoryWork()throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromFromCurrentWork.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Full Load and Current Work...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromCurrentWork.size(); i++) {

                dataQualityJRBIContext.recordsFromFromCurrentWork.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_workext = {"getEPR","getRECORD_TYPE","getWORK_TYPE","getPRIMARY_SITE_SYSTEM","getPRIMARY_SITE_ACRONYM","getPRIMARY_SITE_SUPPORT_LEVEL","getFULFILMENT_SYSTEM",
                        "getFULFILMENT_JOURNAL_ACRONYM","getISSUE_PROD_TYPE_CODE","getCATALOGUE_ISSUES_QTY","getCATALOGUE_VOLUME_FROM","getCATALOGUE_VOLUME_TO",
                        "getRF_ISSUES_QTY","getRF_TOTAL_PAGES_QTY","getRF_FVI","getRF_LVI","getBUSINESS_UNIT_DESC"};
                for (String strTemp : etl_curr_workext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromFromCurrentWork.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " +  dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                            " " + strTemp + " => current_work = " + method.invoke(objectToCompare1) +
                            " work_history = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:"+dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

    @When("^We get the records from transform previous work$")
    public void getRecordsFromPreviousWork() {
        Log.info("We get the records from Previous Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousWork = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }


    @Then("^Get the records from transform previous work history$")
    public void getRecordsFromPreviousWorkHistory() {
        Log.info("We get the records from Previous history Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of previous work and previous work history$")
    public void comparePreviousWorkandPreviousHistoryWork() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromFromPreviousWork.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Previous Work and Previous Work History...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromPreviousWork.size(); i++) {

                dataQualityJRBIContext.recordsFromFromPreviousWork.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_workext = {"getEPR","getRECORD_TYPE","getWORK_TYPE","getPRIMARY_SITE_SYSTEM","getPRIMARY_SITE_ACRONYM","getPRIMARY_SITE_SUPPORT_LEVEL","getFULFILMENT_SYSTEM",
                        "getFULFILMENT_JOURNAL_ACRONYM","getISSUE_PROD_TYPE_CODE","getCATALOGUE_ISSUES_QTY","getCATALOGUE_VOLUME_FROM","getCATALOGUE_VOLUME_TO",
                        "getRF_ISSUES_QTY","getRF_TOTAL_PAGES_QTY","getRF_FVI","getRF_LVI","getBUSINESS_UNIT_DESC"};
                for (String strTemp : etl_curr_workext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromFromPreviousWork.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " +  dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                            " " + strTemp + " => previous_work = " + method.invoke(objectToCompare1) +
                            " work_history = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:"+dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

    @When("^Get the records from the difference of current_work and previous_work$")
    public void getDiffCurrentPreviousWorkRec() {
        Log.info("We get the Difference of Current work and Previous Work records...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_DIFF_REC_PREVIOUS_CURRENT_WORK, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @When("^We get the records from transform delta current work$")
    public void getRecordsfromDeltaWork(){
        Log.info("We get the records from Delta Current Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_CURRENT_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaWork = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }


    @And("^Compare the records of Delta Current work with difference of current and previous work$")
    public void compareDeltawithCurrentPreviousWork()  throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between DElta Work and Current,Previous tables...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.size(); i++) {

                dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaWork.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_workext = {"getEPR","getRECORD_TYPE","getWORK_TYPE","getPRIMARY_SITE_SYSTEM","getPRIMARY_SITE_ACRONYM","getPRIMARY_SITE_SUPPORT_LEVEL","getFULFILMENT_SYSTEM",
                        "getFULFILMENT_JOURNAL_ACRONYM","getISSUE_PROD_TYPE_CODE","getCATALOGUE_ISSUES_QTY","getCATALOGUE_VOLUME_FROM","getCATALOGUE_VOLUME_TO",
                        "getRF_ISSUES_QTY","getRF_TOTAL_PAGES_QTY","getRF_FVI","getRF_LVI","getBUSINESS_UNIT_DESC","getDELTA_MODE","getTYPE"};

                for (String strTemp : etl_curr_workext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromDeltaWork.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                            " " + strTemp + " => Diff_curr_prev = " + method.invoke(objectToCompare1) +
                            " Deltawork = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

    @Then("^Get the records from transform delta work history$")
    public void getRecDeltaWorkHistory(){
        Log.info("We get the records from Delta Current Work History..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }


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

    @When("^Get the records from the difference of Delta_current_work and work_history$")
    public void getRecordsDeltaWorkandHistory() {
        Log.info("We get the difference of Delta Current work and Current History records...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_WORK, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from work jrbi ext exclude table for data check$")
    public void getRecordsFromExcludeJrbiWork() {
        Log.info("We get the records from Excude Work...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_RECORDS_FROM_WORK_EXCLUDE, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromExcludeWork = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of Work Exclude with difference of Delta_current_work and work_history$")
    public void compareExcludeWork() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Exclude Work with Delta_current_work and work_history...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.size(); i++) {

                dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromExcludeWork.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_workext = {"getEPR", "getRECORD_TYPE", "getWORK_TYPE", "getPRIMARY_SITE_SYSTEM", "getPRIMARY_SITE_ACRONYM", "getPRIMARY_SITE_SUPPORT_LEVEL", "getFULFILMENT_SYSTEM",
                        "getFULFILMENT_JOURNAL_ACRONYM", "getISSUE_PROD_TYPE_CODE", "getCATALOGUE_ISSUES_QTY", "getCATALOGUE_VOLUME_FROM", "getCATALOGUE_VOLUME_TO",
                        "getRF_ISSUES_QTY", "getRF_TOTAL_PAGES_QTY", "getRF_FVI", "getRF_LVI", "getBUSINESS_UNIT_DESC", "getLAST_UPDATED_DATE", "getDELETE_FLAG"};

                for (String strTemp : etl_curr_workext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromExcludeWork.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                            " " + strTemp + " => DeltaworkHist = " + method.invoke(objectToCompare1) +
                            " ExcludeWork = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }

            }
        }
    }


    @When("^Get the records from the addition of Delta_current_work and work_Exclude$")
    public void getAddRecDeltaCurrentWorkandExclude(){
        Log.info("We get the Addition of Delta Current work and Work Exclude records...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_JRBI_REC_SUM_DELTA_WORK_AND_WORK_HISTORY, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);

    }

    @Then("^Get the records from work latest table$")
    public void getLatestWorkRec(){
        Log.info("We get the Latest Work records...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_JRBI_WORK_LATEST_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromLAtestWork = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);

    }

    @And("^Compare the records of Work Latest with addition of Delta_current_work and work_Exclude$")
    public void compareLatestWork() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Work Latest with addition of Delta_current_work and work_Exclude...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.size(); i++) {

                dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromLAtestWork.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_workext = {"getEPR", "getRECORD_TYPE", "getWORK_TYPE", "getPRIMARY_SITE_SYSTEM", "getPRIMARY_SITE_ACRONYM", "getPRIMARY_SITE_SUPPORT_LEVEL", "getFULFILMENT_SYSTEM",
                        "getFULFILMENT_JOURNAL_ACRONYM", "getISSUE_PROD_TYPE_CODE", "getCATALOGUE_ISSUES_QTY", "getCATALOGUE_VOLUME_FROM", "getCATALOGUE_VOLUME_TO",
                        "getRF_ISSUES_QTY", "getRF_TOTAL_PAGES_QTY", "getRF_FVI", "getRF_LVI", "getBUSINESS_UNIT_DESC","getLAST_UPDATED_DATE","getDELETE_FLAG"};

                for (String strTemp : etl_curr_workext) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromLAtestWork.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                            " " + strTemp + " => DeltaWorkExclude = " + method.invoke(objectToCompare1) +
                            " LatestWork = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }



}