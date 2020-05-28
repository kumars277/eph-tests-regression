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
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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

        }
        Log.info(sql);
        Log.info(Ids.toString());
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


}