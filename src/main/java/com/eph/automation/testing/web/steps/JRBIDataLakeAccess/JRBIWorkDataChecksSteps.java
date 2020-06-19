package com.eph.automation.testing.web.steps.JRBIDataLakeAccess;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLWorkAccessObject;
import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIWorkDataChecksSQL;
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

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the records from the addition of Delta_current_work and work_Exclude$")
    public void getAddRecDeltaCurrentWorkandExclude(){
        Log.info("We get the Addition of Delta Current work and Work Exclude records...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_JRBI_REC_SUM_DELTA_WORK_AND_WORK_HISTORY, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);

    }

    @Then("^Get the records from work latest table$")
    public void getLatestWorkRec(){
        Log.info("We get the Latest Work records...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_JRBI_WORK_LATEST_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromLAtestWork = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);

    }

    @When("^Get the records from the difference of Delta_current_work and work_history$")
    public void getRecordsFromDeltaWorkandHistory() {
        Log.info("We get the difference of Delta Current work and Current History records...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_WORK, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @When("^Get the records from work exclude table$")
    public void getRecordsFromExcludeWork() {
        Log.info("We get the records from Excude Work...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_RECORDS_FROM_WORK_EXCLUDE, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromExcludeWork = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @When("^We get the records from data full load (.*)$")
    public void getRecordsFromFullLoad(String tableName) {
        Log.info("We get the FULL Load records...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_WORK_RECORDS_FULL_LOAD, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDataFullLoad = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @Then("^We get the records from transform current work (.*)$")
    public void getRecordsFromCurrentWork(String tableName) {
        Log.info("We get the records from Current Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentWork = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform current work history (.*)$")
    public void getRecordsFromCurrentWorkHistory(String tableName) {
        Log.info("We get the records from Current history Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @When("^We get the records from transform previous work (.*)$")
    public void getRecordsFromPreviousWork(String tableName) {
        Log.info("We get the records from Previous Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousWork = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform previous work history (.*)$")
    public void getRecordsFromPreviousWorkHistory(String tableName) {
        Log.info("We get the records from Current history Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @When("^We get the records from transform delta current work$")
    public void getRecordsfromDeltaWork(){
        Log.info("We get the records from Delta Current Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_CURRENT_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaWork = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform delta work history$")
    public void getRecordsfromDeltaWorkHistory(){
        Log.info("We get the records from Delta Current Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }


    @And("^Compare the records of previous work and previous work history (.*)$")
    public void comparePrevioustWorkandPreviousHistoryWork(String tableName) {
        if (dataQualityJRBIContext.recordsFromFromPreviousWork.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Previous Work and Previous Work History...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromPreviousWork.size(); i++) {

                dataQualityJRBIContext.recordsFromFromPreviousWork.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR));

                Log.info("Previous_Work -> EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        "Previous_Work_History -> EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() + " is missing/not found in Current_Work_history table",
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " RECORD_TYPE => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRECORD_TYPE() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " PRIMARY_SITE_SYSTEM => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getPRIMARY_SITE_SYSTEM() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getPRIMARY_SITE_SYSTEM());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getPRIMARY_SITE_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getPRIMARY_SITE_SYSTEM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getPRIMARY_SITE_SYSTEM(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getPRIMARY_SITE_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " PRIMARY_SITE_ACRONYM => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getPRIMARY_SITE_ACRONYM() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getPRIMARY_SITE_ACRONYM());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getPRIMARY_SITE_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getPRIMARY_SITE_ACRONYM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_ACRONYM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getPRIMARY_SITE_ACRONYM(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getPRIMARY_SITE_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " PRIMARY_SITE_SUPPORT_LEVEL => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SUPPORT_LEVEL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " FULFILMENT_SYSTEM => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getFULFILMENT_SYSTEM() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getFULFILMENT_SYSTEM());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getFULFILMENT_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getFULFILMENT_SYSTEM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getFULFILMENT_SYSTEM(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getFULFILMENT_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " FULFILMENT_JOURNAL_ACRONYM => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getFULFILMENT_JOURNAL_ACRONYM(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " ISSUE_PROD_TYPE_CODE => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getISSUE_PROD_TYPE_CODE() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getISSUE_PROD_TYPE_CODE());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getISSUE_PROD_TYPE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getISSUE_PROD_TYPE_CODE() != null)) {
                    Assert.assertEquals("The ISSUE_PROD_TYPE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getISSUE_PROD_TYPE_CODE(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getISSUE_PROD_TYPE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_QTY => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_VOLUME_QTY() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_VOLUME_QTY());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_VOLUME_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_VOLUME_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_VOLUME_QTY(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_VOLUME_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " CATALOGUE_ISSUES_QTY => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_ISSUES_QTY() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_FROM => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_VOLUME_FROM() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_VOLUME_FROM());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_VOLUME_FROM() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_VOLUME_FROM() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_FROM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_VOLUME_FROM(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_VOLUME_FROM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_TO => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_VOLUME_TO() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_VOLUME_TO());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_VOLUME_TO() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_VOLUME_TO() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_TO is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getCATALOGUE_VOLUME_TO(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getCATALOGUE_VOLUME_TO());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " RF_ISSUES_QTY => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_ISSUES_QTY() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The RF_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " RF_TOTAL_PAGES_QTY => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_TOTAL_PAGES_QTY() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_TOTAL_PAGES_QTY());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_TOTAL_PAGES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_TOTAL_PAGES_QTY() != null)) {
                    Assert.assertEquals("The RF_TOTAL_PAGES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_TOTAL_PAGES_QTY(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_TOTAL_PAGES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " RF_FVI => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_FVI() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_FVI());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_FVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_FVI() != null)) {
                    Assert.assertEquals("The RF_FVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_FVI(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_FVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " RF_LVI => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_LVI() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_LVI());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_LVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_LVI() != null)) {
                    Assert.assertEquals("The RF_LVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getRF_LVI(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getRF_LVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() +
                        " BUSINESS_UNIT_DESC => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getBUSINESS_UNIT_DESC() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getBUSINESS_UNIT_DESC());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getBUSINESS_UNIT_DESC() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getBUSINESS_UNIT_DESC() != null)) {
                    Assert.assertEquals("The BUSINESS_UNIT_DESC is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getBUSINESS_UNIT_DESC(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getBUSINESS_UNIT_DESC());
                }
            }
        }
    }


    @And("^Compare the records of current work and current work history (.*)$")
    public void compareCurrentWorkandCurrentHistoryWork(String tableName) {
        if (dataQualityJRBIContext.recordsFromFromCurrentWork.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Full Load and Current Work...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromCurrentWork.size(); i++) {

                dataQualityJRBIContext.recordsFromFromCurrentWork.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR));

                Log.info("Current_Work -> EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        "Current_Work_History -> EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() + " is missing/not found in Current_Work_history table",
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " RECORD_TYPE => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRECORD_TYPE() +
                        " Current_Work_History=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " PRIMARY_SITE_SYSTEM => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SYSTEM() +
                        " Current_Work_History=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getPRIMARY_SITE_SYSTEM());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getPRIMARY_SITE_SYSTEM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SYSTEM(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getPRIMARY_SITE_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " PRIMARY_SITE_ACRONYM => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_ACRONYM() +
                        " Current_Work_History=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getPRIMARY_SITE_ACRONYM());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getPRIMARY_SITE_ACRONYM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_ACRONYM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_ACRONYM(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getPRIMARY_SITE_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " PRIMARY_SITE_SUPPORT_LEVEL => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() +
                        " Current_Work_History=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SUPPORT_LEVEL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " FULFILMENT_SYSTEM => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_SYSTEM() +
                        " Current_Work_History=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getFULFILMENT_SYSTEM());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getFULFILMENT_SYSTEM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_SYSTEM(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getFULFILMENT_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " FULFILMENT_JOURNAL_ACRONYM => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() +
                        " Current_Work_History=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_JOURNAL_ACRONYM(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " ISSUE_PROD_TYPE_CODE => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getISSUE_PROD_TYPE_CODE() +
                        " Current_Work_history=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getISSUE_PROD_TYPE_CODE());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getISSUE_PROD_TYPE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getISSUE_PROD_TYPE_CODE() != null)) {
                    Assert.assertEquals("The ISSUE_PROD_TYPE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getISSUE_PROD_TYPE_CODE(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getISSUE_PROD_TYPE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_QTY => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_QTY() +
                        " Current_Work_history=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_VOLUME_QTY());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_VOLUME_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_QTY(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_VOLUME_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " CATALOGUE_ISSUES_QTY => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_ISSUES_QTY() +
                        " Current_Work_history=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_FROM => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_FROM() +
                        " Current_Work_history=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_VOLUME_FROM());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_FROM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_VOLUME_FROM() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_FROM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_FROM(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_VOLUME_FROM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_TO => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_TO() +
                        " Current_Work_history=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_VOLUME_TO());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_TO() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_VOLUME_TO() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_TO is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_TO(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getCATALOGUE_VOLUME_TO());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " RF_ISSUES_QTY => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_ISSUES_QTY() +
                        " Current_Work_history=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The RF_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " RF_TOTAL_PAGES_QTY => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_TOTAL_PAGES_QTY() +
                        " Current_Work_history=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_TOTAL_PAGES_QTY());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_TOTAL_PAGES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_TOTAL_PAGES_QTY() != null)) {
                    Assert.assertEquals("The RF_TOTAL_PAGES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_TOTAL_PAGES_QTY(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_TOTAL_PAGES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " RF_FVI => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_FVI() +
                        " Current_Work_history=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_FVI());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_FVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_FVI() != null)) {
                    Assert.assertEquals("The RF_FVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_FVI(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_FVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " RF_LVI => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_LVI() +
                        " Current_Work_history=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_LVI());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_LVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_LVI() != null)) {
                    Assert.assertEquals("The RF_LVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_LVI(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getRF_LVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() +
                        " BUSINESS_UNIT_DESC => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getBUSINESS_UNIT_DESC() +
                        " Current_Work_history=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getBUSINESS_UNIT_DESC());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getBUSINESS_UNIT_DESC() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getBUSINESS_UNIT_DESC() != null)) {
                    Assert.assertEquals("The BUSINESS_UNIT_DESC is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getBUSINESS_UNIT_DESC(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getBUSINESS_UNIT_DESC());
                }
            }
        }
    }

    @And("^Compare the records of data full load and transform current work (.*)$")
    public void compareDataFullandCurrentWork(String tableName) {
       if (dataQualityJRBIContext.recordsFromDataFullLoad.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Full Load and Current Work...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDataFullLoad.size(); i++) {

                dataQualityJRBIContext.recordsFromDataFullLoad.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentWork.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR));

                Log.info("Full Load -> EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        "Current_Work -> EPR => " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() + " is missing/not found in Current_Work table",
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " RECORD_TYPE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRECORD_TYPE() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " PRIMARY_SITE_SYSTEM => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SYSTEM() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SYSTEM());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SYSTEM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SYSTEM(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " PRIMARY_SITE_ACRONYM => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_ACRONYM() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_ACRONYM());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_ACRONYM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_ACRONYM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_ACRONYM(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " PRIMARY_SITE_SUPPORT_LEVEL => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SUPPORT_LEVEL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SUPPORT_LEVEL(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " FULFILMENT_SYSTEM => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_SYSTEM() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_SYSTEM());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_SYSTEM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_SYSTEM(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " FULFILMENT_JOURNAL_ACRONYM => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_JOURNAL_ACRONYM() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_JOURNAL_ACRONYM());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_JOURNAL_ACRONYM(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_JOURNAL_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " ISSUE_PROD_TYPE_CODE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getISSUE_PROD_TYPE_CODE() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getISSUE_PROD_TYPE_CODE());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getISSUE_PROD_TYPE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getISSUE_PROD_TYPE_CODE() != null)) {
                    Assert.assertEquals("The ISSUE_PROD_TYPE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getISSUE_PROD_TYPE_CODE(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getISSUE_PROD_TYPE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " CATALOGUE_VOLUME_QTY => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_QTY() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_QTY());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_QTY(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " CATALOGUE_ISSUES_QTY => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_ISSUES_QTY() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " CATALOGUE_VOLUME_FROM => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_FROM() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_FROM());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_FROM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_FROM() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_FROM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_FROM(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_FROM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " CATALOGUE_VOLUME_TO => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_TO() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_TO());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_TO() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_TO() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_TO is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_TO(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_TO());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " RF_ISSUES_QTY => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_ISSUES_QTY() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The RF_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " RF_TOTAL_PAGES_QTY => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_TOTAL_PAGES_QTY() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_TOTAL_PAGES_QTY());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_TOTAL_PAGES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_TOTAL_PAGES_QTY() != null)) {
                    Assert.assertEquals("The RF_TOTAL_PAGES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_TOTAL_PAGES_QTY(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_TOTAL_PAGES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " RF_FVI => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_FVI() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_FVI());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_FVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_FVI() != null)) {
                    Assert.assertEquals("The RF_FVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_FVI(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_FVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " RF_LVI => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_LVI() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_LVI());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_LVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_LVI() != null)) {
                    Assert.assertEquals("The RF_LVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_LVI(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_LVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " BUSINESS_UNIT_DESC => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getBUSINESS_UNIT_DESC() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getBUSINESS_UNIT_DESC());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getBUSINESS_UNIT_DESC() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getBUSINESS_UNIT_DESC() != null)) {
                    Assert.assertEquals("The BUSINESS_UNIT_DESC is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getBUSINESS_UNIT_DESC(),
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getBUSINESS_UNIT_DESC());
                }
            }
        }
    }

    @And("^Compare the records of delta work and delta work history (.*)$")
    public void compareDeltaWorkandDeltaHistory(String tablename) {
        if (dataQualityJRBIContext.recordsFromFromDeltaWork.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between DElta Work and Delta Work History...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromDeltaWork.size(); i++) {

                dataQualityJRBIContext.recordsFromFromDeltaWork.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR));

                Log.info("Delta_Work -> EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        "Delta_Work_History -> EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() + " is missing/not found in Current_Work_history table",
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " RECORD_TYPE => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRECORD_TYPE() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " PRIMARY_SITE_SYSTEM => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SYSTEM() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getPRIMARY_SITE_SYSTEM());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getPRIMARY_SITE_SYSTEM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SYSTEM(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getPRIMARY_SITE_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " PRIMARY_SITE_ACRONYM => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_ACRONYM() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getPRIMARY_SITE_ACRONYM());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getPRIMARY_SITE_ACRONYM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_ACRONYM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_ACRONYM(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getPRIMARY_SITE_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " PRIMARY_SITE_SUPPORT_LEVEL => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SUPPORT_LEVEL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " FULFILMENT_SYSTEM => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_SYSTEM() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getFULFILMENT_SYSTEM());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getFULFILMENT_SYSTEM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_SYSTEM(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getFULFILMENT_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " FULFILMENT_JOURNAL_ACRONYM => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_JOURNAL_ACRONYM(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " ISSUE_PROD_TYPE_CODE => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getISSUE_PROD_TYPE_CODE() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getISSUE_PROD_TYPE_CODE());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getISSUE_PROD_TYPE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getISSUE_PROD_TYPE_CODE() != null)) {
                    Assert.assertEquals("The ISSUE_PROD_TYPE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getISSUE_PROD_TYPE_CODE(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getISSUE_PROD_TYPE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_QTY => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_QTY() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_VOLUME_QTY());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_VOLUME_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_QTY(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_VOLUME_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " CATALOGUE_ISSUES_QTY => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_ISSUES_QTY() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_FROM => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_FROM() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_VOLUME_FROM());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_FROM() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_VOLUME_FROM() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_FROM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_FROM(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_VOLUME_FROM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_TO => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_TO() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_VOLUME_TO());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_TO() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_VOLUME_TO() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_TO is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_TO(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getCATALOGUE_VOLUME_TO());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " RF_ISSUES_QTY => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_ISSUES_QTY() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The RF_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " RF_TOTAL_PAGES_QTY => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_TOTAL_PAGES_QTY() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_TOTAL_PAGES_QTY());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_TOTAL_PAGES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_TOTAL_PAGES_QTY() != null)) {
                    Assert.assertEquals("The RF_TOTAL_PAGES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_TOTAL_PAGES_QTY(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_TOTAL_PAGES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " RF_FVI => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_FVI() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_FVI());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_FVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_FVI() != null)) {
                    Assert.assertEquals("The RF_FVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_FVI(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_FVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " RF_LVI => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_LVI() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_LVI());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_LVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_LVI() != null)) {
                    Assert.assertEquals("The RF_LVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_LVI(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getRF_LVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " BUSINESS_UNIT_DESC => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getBUSINESS_UNIT_DESC() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getBUSINESS_UNIT_DESC());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getBUSINESS_UNIT_DESC() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getBUSINESS_UNIT_DESC() != null)) {
                    Assert.assertEquals("The BUSINESS_UNIT_DESC is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getBUSINESS_UNIT_DESC(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getBUSINESS_UNIT_DESC());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " DELTA_MODE => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getDELTA_MODE() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getDELTA_MODE());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getDELTA_MODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getDELTA_MODE() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getDELTA_MODE(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getDELTA_MODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " TYPE => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getTYPE() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getTYPE());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getTYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getTYPE() != null)) {
                    Assert.assertEquals("The TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getTYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getTYPE());
                }
            }
        }
    }

    @And("^Compare the records of Work Exclude with difference of Delta_current_work and work_history$")
    public void compareExcludeWork() {
        if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Exclude Work with Delta_current_work and work_history...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.size(); i++) {

                dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromExcludeWork.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR));

                Log.info("Delta_Work_Current_History -> EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        "Work_Exclude -> EPR => " + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() + " is missing/not found in Current_Work_history table",
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " RECORD_TYPE => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRECORD_TYPE() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " PRIMARY_SITE_SYSTEM => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getPRIMARY_SITE_SYSTEM() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getPRIMARY_SITE_SYSTEM());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getPRIMARY_SITE_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getPRIMARY_SITE_SYSTEM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getPRIMARY_SITE_SYSTEM(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getPRIMARY_SITE_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " PRIMARY_SITE_ACRONYM => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getPRIMARY_SITE_ACRONYM() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getPRIMARY_SITE_ACRONYM());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getPRIMARY_SITE_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getPRIMARY_SITE_ACRONYM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_ACRONYM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getPRIMARY_SITE_ACRONYM(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getPRIMARY_SITE_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " PRIMARY_SITE_SUPPORT_LEVEL => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SUPPORT_LEVEL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getPRIMARY_SITE_SUPPORT_LEVEL(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " FULFILMENT_SYSTEM => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getFULFILMENT_SYSTEM() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getFULFILMENT_SYSTEM());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getFULFILMENT_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getFULFILMENT_SYSTEM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getFULFILMENT_SYSTEM(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getFULFILMENT_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " FULFILMENT_JOURNAL_ACRONYM => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getFULFILMENT_JOURNAL_ACRONYM());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getFULFILMENT_JOURNAL_ACRONYM(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getFULFILMENT_JOURNAL_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " ISSUE_PROD_TYPE_CODE => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getISSUE_PROD_TYPE_CODE() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getISSUE_PROD_TYPE_CODE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getISSUE_PROD_TYPE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getISSUE_PROD_TYPE_CODE() != null)) {
                    Assert.assertEquals("The ISSUE_PROD_TYPE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getISSUE_PROD_TYPE_CODE(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getISSUE_PROD_TYPE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " CATALOGUE_VOLUME_QTY => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_VOLUME_QTY() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_VOLUME_QTY());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_VOLUME_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_VOLUME_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_VOLUME_QTY(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_VOLUME_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " CATALOGUE_ISSUES_QTY => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_ISSUES_QTY() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " CATALOGUE_VOLUME_FROM => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_VOLUME_FROM() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_VOLUME_FROM());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_VOLUME_FROM() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_VOLUME_FROM() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_FROM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_VOLUME_FROM(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_VOLUME_FROM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " CATALOGUE_VOLUME_TO => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_VOLUME_TO() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_VOLUME_TO());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_VOLUME_TO() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_VOLUME_TO() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_TO is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getCATALOGUE_VOLUME_TO(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getCATALOGUE_VOLUME_TO());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " RF_ISSUES_QTY => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_ISSUES_QTY() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The RF_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " RF_TOTAL_PAGES_QTY => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_TOTAL_PAGES_QTY() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_TOTAL_PAGES_QTY());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_TOTAL_PAGES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_TOTAL_PAGES_QTY() != null)) {
                    Assert.assertEquals("The RF_TOTAL_PAGES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_TOTAL_PAGES_QTY(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_TOTAL_PAGES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " RF_FVI => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_FVI() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_FVI());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_FVI() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_FVI() != null)) {
                    Assert.assertEquals("The RF_FVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_FVI(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_FVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " RF_LVI => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_LVI() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_LVI());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_LVI() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_LVI() != null)) {
                    Assert.assertEquals("The RF_LVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getRF_LVI(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getRF_LVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " BUSINESS_UNIT_DESC => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getBUSINESS_UNIT_DESC() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getBUSINESS_UNIT_DESC());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getBUSINESS_UNIT_DESC() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getBUSINESS_UNIT_DESC() != null)) {
                    Assert.assertEquals("The BUSINESS_UNIT_DESC is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getBUSINESS_UNIT_DESC(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getBUSINESS_UNIT_DESC());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " LAST_UPDATED_DATE => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getLAST_UPDATED_DATE() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getLAST_UPDATED_DATE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getLAST_UPDATED_DATE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getLAST_UPDATED_DATE() != null)) {
                    Assert.assertEquals("The LAST_UPDATED_DATE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getLAST_UPDATED_DATE(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getLAST_UPDATED_DATE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() +
                        " DELETE_FLAG => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getDELETE_FLAG() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getDELETE_FLAG());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getDELETE_FLAG() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getDELETE_FLAG() != null)) {
                    Assert.assertEquals("The DELETE_FLAG is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getDELETE_FLAG(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getDELETE_FLAG());
                }
            }
        }
    }

    @And("^Compare the records of Work Latest with addition of Delta_current_work and work_Exclude$")
    public void compareLatestWork() {
        if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Work Latest with addition of Delta_current_work and work_Exclude...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.size(); i++) {

                dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromLAtestWork.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR));

                Log.info("Delta_Work_Exclude -> EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        "Work_Latest -> EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() + " is missing/not found in Current_Work_history table",
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " RECORD_TYPE => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRECORD_TYPE() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " PRIMARY_SITE_SYSTEM => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getPRIMARY_SITE_SYSTEM() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_SYSTEM());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getPRIMARY_SITE_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_SYSTEM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getPRIMARY_SITE_SYSTEM(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " PRIMARY_SITE_ACRONYM => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getPRIMARY_SITE_ACRONYM() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_ACRONYM());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getPRIMARY_SITE_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_ACRONYM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_ACRONYM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getPRIMARY_SITE_ACRONYM(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " PRIMARY_SITE_SUPPORT_LEVEL => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SUPPORT_LEVEL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getPRIMARY_SITE_SUPPORT_LEVEL(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " FULFILMENT_SYSTEM => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getFULFILMENT_SYSTEM() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getFULFILMENT_SYSTEM());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getFULFILMENT_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getFULFILMENT_SYSTEM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getFULFILMENT_SYSTEM(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getFULFILMENT_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " FULFILMENT_JOURNAL_ACRONYM => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getFULFILMENT_JOURNAL_ACRONYM() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getFULFILMENT_JOURNAL_ACRONYM());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getFULFILMENT_JOURNAL_ACRONYM(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getFULFILMENT_JOURNAL_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " ISSUE_PROD_TYPE_CODE => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getISSUE_PROD_TYPE_CODE() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getISSUE_PROD_TYPE_CODE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getISSUE_PROD_TYPE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getISSUE_PROD_TYPE_CODE() != null)) {
                    Assert.assertEquals("The ISSUE_PROD_TYPE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getISSUE_PROD_TYPE_CODE(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getISSUE_PROD_TYPE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " CATALOGUE_VOLUME_QTY => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_VOLUME_QTY() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_QTY());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_VOLUME_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_VOLUME_QTY(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " CATALOGUE_ISSUES_QTY => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_ISSUES_QTY() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " CATALOGUE_VOLUME_FROM => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_VOLUME_FROM() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_FROM());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_VOLUME_FROM() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_FROM() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_FROM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_VOLUME_FROM(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_FROM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " CATALOGUE_VOLUME_TO => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_VOLUME_TO() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_TO());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_VOLUME_TO() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_TO() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_TO is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getCATALOGUE_VOLUME_TO(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_TO());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " RF_ISSUES_QTY => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_ISSUES_QTY() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The RF_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " RF_TOTAL_PAGES_QTY => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_TOTAL_PAGES_QTY() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_TOTAL_PAGES_QTY());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_TOTAL_PAGES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_TOTAL_PAGES_QTY() != null)) {
                    Assert.assertEquals("The RF_TOTAL_PAGES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_TOTAL_PAGES_QTY(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_TOTAL_PAGES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " RF_FVI => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_FVI() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_FVI());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_FVI() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_FVI() != null)) {
                    Assert.assertEquals("The RF_FVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_FVI(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_FVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " RF_LVI => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_LVI() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_LVI());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_LVI() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_LVI() != null)) {
                    Assert.assertEquals("The RF_LVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getRF_LVI(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_LVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " BUSINESS_UNIT_DESC => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getBUSINESS_UNIT_DESC() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getBUSINESS_UNIT_DESC());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getBUSINESS_UNIT_DESC() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getBUSINESS_UNIT_DESC() != null)) {
                    Assert.assertEquals("The BUSINESS_UNIT_DESC is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getBUSINESS_UNIT_DESC(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getBUSINESS_UNIT_DESC());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " DELETE_FLAG => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getDELETE_FLAG() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getDELETE_FLAG());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getDELETE_FLAG() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getDELETE_FLAG() != null)) {
                    Assert.assertEquals("The DELETE_FLAG is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getDELETE_FLAG(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getDELETE_FLAG());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
                        " LAST_UPDATED_DATE => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getLAST_UPDATED_DATE() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getLAST_UPDATED_DATE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getLAST_UPDATED_DATE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getLAST_UPDATED_DATE() != null)) {
                    Assert.assertEquals("The LAST_UPDATED_DATE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getLAST_UPDATED_DATE(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getLAST_UPDATED_DATE());
                }

            }
        }
    }

    @Then("^Get the records from work extended table$")
    public void getExtendedWorkRec(){
        Log.info("We get the Extended Work records...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_JRBI_WORK_EXTENDED_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromExtendeWork = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);

    }

    @And("^Compare the records of Work Latest with work_Extended$")
    public void compareWorkLatestandExtended(){

       if (dataQualityJRBIContext.recordsFromLAtestWork.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Work Latest with Work_Extended...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromLAtestWork.size(); i++) {


                dataQualityJRBIContext.recordsFromLAtestWork.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromExtendeWork.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR_ID));

                Log.info("Work_Latest -> EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        "Work_Extended -> EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID());
                if (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() != null)) {
                    Assert.assertEquals("The EPR is => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() + " is missing/not found in Work_extended table",
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " WORK_TYPE => Work_Latest =" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getWORK_TYPE() +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getWORK_TYPE());

                if (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getWORK_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getWORK_TYPE(),
                            dataQualityJRBIContext.recordsFromExtendeWork.get(i).getWORK_TYPE());
                }


                String primarySiteExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_SYSTEM();
                String primarySiteAcrExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_ACRONYM();
                String primarySiteSupportLevelExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL();
                String issueProdTypeCodeExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getISSUE_PROD_TYPE_CODE();
                String catalogueVolQtyExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_QTY();
                String catlogueIssueQtyExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_ISSUES_QTY();
                String catalogueVolFromExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_FROM();
                String catalogueVolToExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getCATALOGUE_VOLUME_TO();
                String rfIssueQtyExt =  dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_ISSUES_QTY();
                String rfTotalPageQtyExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_TOTAL_PAGES_QTY();
                String rfFviExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_FVI();
                String rfLviExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getRF_LVI();
                String busineesUnitDescExt = dataQualityJRBIContext.recordsFromLAtestWork.get(i).getBUSINESS_UNIT_DESC();

                if(dataQualityJRBIContext.recordsFromLAtestWork.get(i).getDELETE_FLAG()=="1"){
                    primarySiteExt = null;
                    primarySiteAcrExt = null;
                    primarySiteSupportLevelExt = null;
                    issueProdTypeCodeExt = null;
                    catalogueVolQtyExt = null;
                    catlogueIssueQtyExt = null;
                    catalogueVolFromExt = null;
                    catalogueVolToExt = null;
                    rfIssueQtyExt = null;
                    rfTotalPageQtyExt = null;
                    rfFviExt = null;
                    rfLviExt = null;
                    busineesUnitDescExt = null;
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " PRIMARY_SITE_SYSTEM => Work_Latest =" + primarySiteExt +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SYSTEM());

                if (primarySiteExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SYSTEM() != null) {
                    Assert.assertEquals("The PRIMARY_SITE_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            primarySiteExt, dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " PRIMARY_SITE_ACRONYM => Work_latest =" + primarySiteAcrExt +
                        " Work_Extended=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_ACRONYM());

                if (primarySiteAcrExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_ACRONYM() != null) {
                    Assert.assertEquals("The PRIMARY_SITE_ACRONYM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            primarySiteAcrExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " PRIMARY_SITE_SUPPORT_LEVEL => Work_Latest =" + primarySiteSupportLevelExt +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());

                if (primarySiteSupportLevelExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null) {
                    Assert.assertEquals("The PRIMARY_SITE_SUPPORT_LEVEL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            primarySiteSupportLevelExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " ISSUE_PROD_TYPE_CODE => Work_Latest =" + issueProdTypeCodeExt +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getISSUE_PROD_TYPE_CODE());

                if (issueProdTypeCodeExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getISSUE_PROD_TYPE_CODE() != null) {
                    Assert.assertEquals("The ISSUE_PROD_TYPE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            issueProdTypeCodeExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getISSUE_PROD_TYPE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_QTY => Work_Latest =" + catalogueVolQtyExt +
                        " Work_Extend =" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_QTY());

                if (catalogueVolQtyExt != null ||dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_QTY() != null) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            catalogueVolQtyExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " CATALOGUE_ISSUES_QTY => Work_Latest =" + catlogueIssueQtyExt +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_ISSUES_QTY());

                if (catlogueIssueQtyExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_ISSUES_QTY() != null) {
                    Assert.assertEquals("The CATALOGUE_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            catlogueIssueQtyExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_FROM => Work_Latest =" + catalogueVolFromExt +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_FROM());

                if (catalogueVolFromExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_FROM() != null) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_FROM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            catalogueVolFromExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_FROM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_TO => Work_Latest =" + catalogueVolToExt +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_TO());

                if (catalogueVolToExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_TO() != null) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_TO is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            catalogueVolToExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_TO());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " RF_ISSUES_QTY => Work_Latest =" + rfIssueQtyExt +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_ISSUES_QTY());

                if (rfIssueQtyExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_ISSUES_QTY() != null) {
                    Assert.assertEquals("The RF_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            rfIssueQtyExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " RF_TOTAL_PAGES_QTY => Work_Latest =" + rfTotalPageQtyExt +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_TOTAL_PAGES_QTY());

                if (rfTotalPageQtyExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_TOTAL_PAGES_QTY() != null) {
                    Assert.assertEquals("The RF_TOTAL_PAGES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            rfTotalPageQtyExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_TOTAL_PAGES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " RF_FVI => Work_Latest =" + rfFviExt +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_FVI());

                if (rfFviExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_FVI() != null) {
                    Assert.assertEquals("The RF_FVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            rfFviExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_FVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " RF_LVI => Work_Latest =" + rfLviExt +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_LVI());

                if (rfLviExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_LVI() != null) {
                    Assert.assertEquals("The RF_LVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            rfLviExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_LVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " BUSINESS_UNIT_DESC => Work_Latest =" + busineesUnitDescExt +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getBUSINESS_UNIT_DESC());

                if (busineesUnitDescExt != null || dataQualityJRBIContext.recordsFromExtendeWork.get(i).getBUSINESS_UNIT_DESC() != null) {
                    Assert.assertEquals("The BUSINESS_UNIT_DESC is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            busineesUnitDescExt,dataQualityJRBIContext.recordsFromExtendeWork.get(i).getBUSINESS_UNIT_DESC());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " DELETE_FLAG => Work_Latest =" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getDELETE_FLAG() +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getDELETE_FLAG());

                if (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getDELETE_FLAG() != null ||
                        (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getDELETE_FLAG() != null)) {
                    Assert.assertEquals("The DELETE_FLAG is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getDELETE_FLAG(),
                            dataQualityJRBIContext.recordsFromExtendeWork.get(i).getDELETE_FLAG());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() +
                        " LAST_UPDATED_DATE => Work_Latest =" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getLAST_UPDATED_DATE() +
                        " Work_Extend=" + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getLAST_UPDATED_DATE());

                if (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getLAST_UPDATED_DATE() != null ||
                        (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getLAST_UPDATED_DATE() != null)) {
                    Assert.assertEquals("The LAST_UPDATED_DATE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getLAST_UPDATED_DATE(),
                            dataQualityJRBIContext.recordsFromExtendeWork.get(i).getLAST_UPDATED_DATE());
                }

            }
        }


    }





}