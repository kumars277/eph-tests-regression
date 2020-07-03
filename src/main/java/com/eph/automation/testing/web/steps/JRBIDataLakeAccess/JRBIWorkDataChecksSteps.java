package com.eph.automation.testing.web.steps.JRBIDataLakeAccess;

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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.google.gson.Gson;


public class JRBIWorkDataChecksSteps {

    public JRBIAccessDLContext dataQualityJRBIContext;
    private static String sql;
    private static List<String> Ids;
    private JRBIWorkDataChecksSQL jrbiObj = new JRBIWorkDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^We get the (.*) random EPR ids (.*)$")
    public void getRandomEPRIds(String numberOfRecords, String tableName) {
      //  numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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

            case "work_extended":
                sql = String.format(JRBIWorkDataChecksSQL.GET_RANDOM_EPR_WORK_EXTENDED, numberOfRecords);
                List<Map<?, ?>> randomWorkExtendedEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkExtendedEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
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
        dataQualityJRBIContext.recordsFromDataFullLoad = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @Then("^We get the records from transform current work$")
    public void getRecordsFromCurrentWork() {
        Log.info("We get the records from Current Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentWork = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of jrbi_journal_data_full and transform current work$")
    public void compareDataFullandCurrentWork() {

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
                    String fullRecordType = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRECORD_TYPE();
                    if(fullRecordType.isEmpty()){
                        fullRecordType = null;
                    }
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            fullRecordType,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " WORK_TYPE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getWORK_TYPE() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getWORK_TYPE());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getWORK_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getWORK_TYPE() != null)) {
                    String fullWorkType = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getWORK_TYPE();
                    if(fullWorkType.isEmpty()){
                        fullWorkType = null;
                    }
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            fullWorkType,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getWORK_TYPE());
                }


                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " PRIMARY_SITE_SYSTEM => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SYSTEM() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SYSTEM());
                String primarySiteSys = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SYSTEM();
                if(primarySiteSys.isEmpty()){
                    primarySiteSys = null;
                }
                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SYSTEM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            primarySiteSys,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " PRIMARY_SITE_ACRONYM => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_ACRONYM() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_ACRONYM());
                String primarySiteAcronym = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_ACRONYM();
                if(primarySiteAcronym.isEmpty()){
                    primarySiteAcronym = null;
                }
                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_ACRONYM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_ACRONYM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            primarySiteAcronym,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " PRIMARY_SITE_SUPPORT_LEVEL => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());
                String primarySiteSupportLevel = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SUPPORT_LEVEL();
                if(primarySiteSupportLevel.isEmpty()){
                    primarySiteSupportLevel = null;
                }
                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SUPPORT_LEVEL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            primarySiteSupportLevel,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " FULFILMENT_SYSTEM => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_SYSTEM() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_SYSTEM());
                String fulfilmentSys = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_SYSTEM();
                if(fulfilmentSys.isEmpty()){
                    fulfilmentSys = null;
                }
                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_SYSTEM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            fulfilmentSys,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " FULFILMENT_JOURNAL_ACRONYM => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_JOURNAL_ACRONYM() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_JOURNAL_ACRONYM());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null)) {
                    String fulfilmentJournalAcr = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getFULFILMENT_JOURNAL_ACRONYM();
                    if(fulfilmentJournalAcr.isEmpty()){
                        fulfilmentJournalAcr = null;
                    }
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            fulfilmentJournalAcr,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getFULFILMENT_JOURNAL_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " ISSUE_PROD_TYPE_CODE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getISSUE_PROD_TYPE_CODE() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getISSUE_PROD_TYPE_CODE());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getISSUE_PROD_TYPE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getISSUE_PROD_TYPE_CODE() != null)) {
                    String issueProdTypeCode = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getISSUE_PROD_TYPE_CODE();
                    if(issueProdTypeCode.isEmpty()){
                        issueProdTypeCode = null;
                    }
                    Assert.assertEquals("The ISSUE_PROD_TYPE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            issueProdTypeCode,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getISSUE_PROD_TYPE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " CATALOGUE_VOLUME_QTY => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_QTY() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_QTY());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_QTY() != null)) {
                    String catalogue_volume_qty = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_QTY();
                    if(catalogue_volume_qty.isEmpty()){
                        catalogue_volume_qty = null;
                    }
                    Assert.assertEquals("The CATALOGUE_VOLUME_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            catalogue_volume_qty,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " CATALOGUE_ISSUES_QTY => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_ISSUES_QTY() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_ISSUES_QTY() != null)) {

                    String catalogue_issue_qty = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_ISSUES_QTY();
                    if(catalogue_issue_qty.isEmpty()){
                        catalogue_issue_qty = null;
                    }
                    Assert.assertEquals("The CATALOGUE_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            catalogue_issue_qty,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " CATALOGUE_VOLUME_FROM => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_FROM() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_FROM());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_FROM() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_FROM() != null)) {

                    String catalogue_vol_from = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_FROM();
                    if(catalogue_vol_from.isEmpty()){
                        catalogue_vol_from = null;
                    }

                    Assert.assertEquals("The CATALOGUE_VOLUME_FROM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            catalogue_vol_from,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_FROM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " CATALOGUE_VOLUME_TO => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_TO() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_TO());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_TO() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_TO() != null)) {

                    String catalogue_vol_to = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getCATALOGUE_VOLUME_TO();
                    if(catalogue_vol_to.isEmpty()){
                        catalogue_vol_to = null;
                    }

                    Assert.assertEquals("The CATALOGUE_VOLUME_TO is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            catalogue_vol_to,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getCATALOGUE_VOLUME_TO());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " RF_ISSUES_QTY => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_ISSUES_QTY() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_ISSUES_QTY() != null)) {

                    String RfIssueQty = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_ISSUES_QTY();
                    if(RfIssueQty.isEmpty()){
                        RfIssueQty = null;
                    }

                    Assert.assertEquals("The RF_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            RfIssueQty,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " RF_TOTAL_PAGES_QTY => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_TOTAL_PAGES_QTY() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_TOTAL_PAGES_QTY());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_TOTAL_PAGES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_TOTAL_PAGES_QTY() != null)) {

                    String RfIssuetotPage = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_TOTAL_PAGES_QTY();
                    if(RfIssuetotPage.isEmpty()){
                        RfIssuetotPage = null;
                    }

                    Assert.assertEquals("The RF_TOTAL_PAGES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            RfIssuetotPage,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_TOTAL_PAGES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " RF_FVI => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_FVI() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_FVI());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_FVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_FVI() != null)) {
                    String RfVI = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_FVI();
                    if(RfVI.isEmpty()){
                        RfVI = null;
                    }
                    Assert.assertEquals("The RF_FVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            RfVI,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_FVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " RF_LVI => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_LVI() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_LVI());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_LVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_LVI() != null)) {
                    String RfLVI = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getRF_LVI();
                    if(RfLVI.isEmpty()){
                        RfLVI = null;
                    }
                    Assert.assertEquals("The RF_LVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            RfLVI,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getRF_LVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() +
                        " BUSINESS_UNIT_DESC => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getBUSINESS_UNIT_DESC() +
                        " Current_Work=" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getBUSINESS_UNIT_DESC());

                if (dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getBUSINESS_UNIT_DESC() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getBUSINESS_UNIT_DESC() != null)) {

                    String BusinessUnitDes = dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getBUSINESS_UNIT_DESC();
                    if(BusinessUnitDes.isEmpty()){
                        BusinessUnitDes = null;
                    }

                    Assert.assertEquals("The BUSINESS_UNIT_DESC is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoad.get(i).getEPR() ,
                            BusinessUnitDes,dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getBUSINESS_UNIT_DESC());
                }
            }
        }
    }

    @Then("^Get the records from transform current work history$")
    public void getRecordsFromCurrentWorkHistory() {
        Log.info("We get the records from Current history Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_CURRENT_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of current work and current work history$")
    public void compareCurrentWorkandCurrentHistoryWork() {
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
                        " WORK_TYPE => Current_Work =" + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getWORK_TYPE() +
                        " Current_Work_History=" + dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getWORK_TYPE());

                if (dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getWORK_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentWork.get(i).getWORK_TYPE(),
                            dataQualityJRBIContext.recordsFromFromCurrentWorkHistory.get(i).getWORK_TYPE());
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

    @When("^We get the records from transform previous work$")
    public void getRecordsFromPreviousWork() {
        Log.info("We get the records from Previous Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousWork = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }


    @Then("^Get the records from transform previous work history$")
    public void getRecordsFromPreviousWorkHistory() {
        Log.info("We get the records from Previous history Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_PREVIOUS_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of previous work and previous work history$")
    public void comparePreviousWorkandPreviousHistoryWork() {
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
                        " WORK_TYPE => Previous_Work =" + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getWORK_TYPE() +
                        " Previous_Work_History=" + dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getWORK_TYPE());

                if (dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getWORK_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousWork.get(i).getWORK_TYPE(),
                            dataQualityJRBIContext.recordsFromFromPreviousWorkHistory.get(i).getWORK_TYPE());
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

    @When("^Get the records from the difference of current_work and previous_work$")
    public void getDiffCurrentPreviousWorkRec() {
        Log.info("We get the Difference of Current work and Previous Work records...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_DIFF_REC_PREVIOUS_CURRENT_WORK, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }

    @When("^We get the records from transform delta current work$")
    public void getRecordsfromDeltaWork(){
        Log.info("We get the records from Delta Current Work..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_CURRENT_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaWork = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }


    @And("^Compare the records of Delta Current work with difference of current and previous work$")
    public void compareDeltawithCurrentPreviousWork() {
        if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between DElta Work and Current,Previous tables...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.size(); i++) {

                dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaWork.sort(Comparator.comparing(JRBIDLWorkAccessObject::getEPR));

                Log.info("Current_previous -> EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        "Delta_Work -> EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() + " is missing/not found in Delta_Current table",
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " RECORD_TYPE => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRECORD_TYPE() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " WORK_TYPE => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getWORK_TYPE() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getWORK_TYPE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getWORK_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getWORK_TYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getWORK_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
                        " PRIMARY_SITE_SYSTEM => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getPRIMARY_SITE_SYSTEM() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SYSTEM());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getPRIMARY_SITE_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SYSTEM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getPRIMARY_SITE_SYSTEM(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " PRIMARY_SITE_ACRONYM => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getPRIMARY_SITE_ACRONYM() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_ACRONYM());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getPRIMARY_SITE_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_ACRONYM() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_ACRONYM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getPRIMARY_SITE_ACRONYM(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " PRIMARY_SITE_SUPPORT_LEVEL => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null)) {
                    Assert.assertEquals("The PRIMARY_SITE_SUPPORT_LEVEL is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " FULFILMENT_SYSTEM => Current_Previous =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getFULFILMENT_SYSTEM() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_SYSTEM());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getFULFILMENT_SYSTEM() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_SYSTEM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getFULFILMENT_SYSTEM(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_SYSTEM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " FULFILMENT_JOURNAL_ACRONYM => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_JOURNAL_ACRONYM());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_JOURNAL_ACRONYM() != null)) {
                    Assert.assertEquals("The FULFILMENT_SYSTEM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getFULFILMENT_JOURNAL_ACRONYM(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getFULFILMENT_JOURNAL_ACRONYM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " ISSUE_PROD_TYPE_CODE => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getISSUE_PROD_TYPE_CODE() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getISSUE_PROD_TYPE_CODE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getISSUE_PROD_TYPE_CODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getISSUE_PROD_TYPE_CODE() != null)) {
                    Assert.assertEquals("The ISSUE_PROD_TYPE_CODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getISSUE_PROD_TYPE_CODE(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getISSUE_PROD_TYPE_CODE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_QTY => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_VOLUME_QTY() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_QTY());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_VOLUME_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_VOLUME_QTY(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " CATALOGUE_ISSUES_QTY => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_ISSUES_QTY() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The CATALOGUE_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_FROM => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_VOLUME_FROM() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_FROM());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_VOLUME_FROM() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_FROM() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_FROM is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_VOLUME_FROM(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_FROM());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " CATALOGUE_VOLUME_TO => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_VOLUME_TO() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_TO());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_VOLUME_TO() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_TO() != null)) {
                    Assert.assertEquals("The CATALOGUE_VOLUME_TO is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getCATALOGUE_VOLUME_TO(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getCATALOGUE_VOLUME_TO());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " RF_ISSUES_QTY => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_ISSUES_QTY() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_ISSUES_QTY());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_ISSUES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_ISSUES_QTY() != null)) {
                    Assert.assertEquals("The RF_ISSUES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_ISSUES_QTY(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_ISSUES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " RF_TOTAL_PAGES_QTY => current_previous =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_TOTAL_PAGES_QTY() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_TOTAL_PAGES_QTY());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_TOTAL_PAGES_QTY() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_TOTAL_PAGES_QTY() != null)) {
                    Assert.assertEquals("The RF_TOTAL_PAGES_QTY is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_TOTAL_PAGES_QTY(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_TOTAL_PAGES_QTY());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " RF_FVI => current_previous =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_FVI() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_FVI());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_FVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_FVI() != null)) {
                    Assert.assertEquals("The RF_FVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_FVI(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_FVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " RF_LVI => current_previous =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_LVI() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_LVI());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_LVI() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_LVI() != null)) {
                    Assert.assertEquals("The RF_LVI is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getRF_LVI(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getRF_LVI());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " BUSINESS_UNIT_DESC => current_previous =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getBUSINESS_UNIT_DESC() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getBUSINESS_UNIT_DESC());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getBUSINESS_UNIT_DESC() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getBUSINESS_UNIT_DESC() != null)) {
                    Assert.assertEquals("The BUSINESS_UNIT_DESC is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getBUSINESS_UNIT_DESC(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getBUSINESS_UNIT_DESC());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " TYPE => current_previous =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getTYPE() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getTYPE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getTYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getTYPE() != null)) {
                    Assert.assertEquals("The TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getTYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getTYPE());
                }


                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() +
                        " DELTA_MODE => current_previous =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getDELTA_MODE() +
                        " Delta_Work=" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getDELTA_MODE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getDELTA_MODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getDELTA_MODE() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousWork.get(i).getDELTA_MODE(),
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getDELTA_MODE());
                }
            }
        }
    }

    @Then("^Get the records from transform delta work history$")
    public void getRecDeltaWorkHistory(){
        Log.info("We get the records from Delta Current Work History..");
        sql = String.format(JRBIWorkDataChecksSQL.GET_DELTA_WORK_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaWorkHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLWorkAccessObject.class, Constants.AWS_URL);
    }


    @And("^Compare the records of delta work and delta work history$")
    public void compareDeltaWorkandDeltaHistory() {
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
                    Assert.assertEquals("The EPR is => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() + " is missing/not found in Delta_Work_history table",
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
                        " WORK_TYPE => Delta_Work =" + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getWORK_TYPE() +
                        " Delta_Work_History=" + dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getWORK_TYPE());

                if (dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getWORK_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getWORK_TYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaWorkHistory.get(i).getWORK_TYPE());
                }

/*                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaWork.get(i).getEPR() +
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
                }*/

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

    @When("^Get the records from the difference of Delta_current_work and work_history$")
    public void getRecordsDeltaWorkandHistory() {
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
                    Assert.assertEquals("The EPR is => " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() + " is missing/not found in Work_Exclude table",
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
                        " WORK_TYPE => Delta_Work_Current_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getWORK_TYPE() +
                        " Work_Exclude=" + dataQualityJRBIContext.recordsFromExcludeWork.get(i).getWORK_TYPE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getWORK_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeWork.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndWorkHistory.get(i).getWORK_TYPE(),
                            dataQualityJRBIContext.recordsFromExcludeWork.get(i).getWORK_TYPE());
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
                    Assert.assertEquals("The EPR is => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() + " is missing/not found in Current_Latest table",
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
                        " WORK_TYPE => Delta_Work_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getWORK_TYPE() +
                        " Work_Latest=" + dataQualityJRBIContext.recordsFromLAtestWork.get(i).getWORK_TYPE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getWORK_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestWork.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getWORK_TYPE(),
                            dataQualityJRBIContext.recordsFromLAtestWork.get(i).getWORK_TYPE());
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
               /* Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndWorkExclude.get(i).getEPR() +
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
                }*/

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

    @ Then("^Get the records from work extended person role table$")
    public void getRecPersonRole(String workId){
        Log.info("We get the records from Person Extended Roles...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_JRBI_PERSON_ROLE_REC, workId);
        Log.info(sql);
        JRBIAccessDLContext.recordsFromPersonExtended = DBManager.getDBResultAsBeanList(sql, JRBIDLPersonAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from work extended stitching table$")
    public void getworkExtendedJSONRec(String workId){
        Log.info("We get the JSON from Work Stitching Tables...");
        sql = String.format(JRBIWorkDataChecksSQL.GET_WORK_JSON_RECORDS, workId);
        Log.info(sql);
        List<Map<String,String>> jsonValue=DBManager.getDBResultMap(sql,Constants.EPH_URL);
        JRBIAccessDLContext.recordsFromWorkStitching = new Gson().fromJson(jsonValue.get(0).get("json"), JRBIWorkExtJsonObject.class);
    }

    @And("^compare work extended and work extended person role with work stitching table$")
    public  void compareWorkExtendedAndStitching() {
        if (dataQualityJRBIContext.recordsFromExtendeWork.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < dataQualityJRBIContext.recordsFromExtendeWork.size(); i++) {
                String workId=dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID();
                getRecPersonRole(workId);
                getworkExtendedJSONRec(workId);
                Log.info("Work_Extended -> EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                        " Work_JSON -> EPR => " + JRBIAccessDLContext.recordsFromWorkStitching.getId());
               if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() != null ||
                        (JRBIAccessDLContext.recordsFromWorkStitching.getId() != null)) {
                    Assert.assertEquals("The EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR() + " is missing/not found in Work_Stitching table",
                            dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                            JRBIAccessDLContext.recordsFromWorkStitching.getId());
                }

                if(dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SYSTEM()== null){
                    Log.info("Primary Site System not available");
                }else{
                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                            " Work_Extended -> PrimarySiteSystem => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SYSTEM() +
                            " Work_JSON -> PrimarySiteSystem => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPrimarySiteSystem());
                    if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SYSTEM() != null ||
                            (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPrimarySiteSystem() != null)) {
                        Assert.assertEquals("The PrimarySiteSystem is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SYSTEM(),
                                JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPrimarySiteSystem());
                    }
                }
                if(dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_ACRONYM()==null){
                    Log.info("PrimarySiteAcronym not available");
                }else{
                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                    " Work_Extended -> PrimarySiteAcronym => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_ACRONYM() +
                            " Work_JSON -> PrimarySiteAcronym => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPrimarySiteAcronym());
                    if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_ACRONYM() != null ||
                            (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPrimarySiteAcronym() != null)) {
                        Assert.assertEquals("The PrimarySiteAcronym is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_ACRONYM(),
                                JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPrimarySiteAcronym());
                    }
                }

                if(dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SYSTEM().isEmpty()){
                    Log.info("PrimarySiteSupportLevel not available");
                }else{
                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                    " Work_Extended -> PrimarySiteSupportLevel => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() +
                            " Work_JSON -> PrimarySiteSupportLevel => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPrimarySiteSupportLevel());
                    if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null ||
                            (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPrimarySiteSupportLevel() != null)) {
                        Assert.assertEquals("The PrimarySiteSupportLevel is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL(),
                                JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPrimarySiteSupportLevel());
                    }
                }

                if(dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_QTY().isEmpty()){
                    Log.info("catalogueVolumesQty not available");
                }else{
                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                    " Work_Extended -> catalogueVolumesQty => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_QTY() +
                            " Work_JSON -> catalogueVolumesQty => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueVolumesQty());
                    if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_QTY() != null ||
                            (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueVolumesQty() != null)) {
                        Assert.assertEquals("The catalogueVolumesQty is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_QTY(),
                                JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueVolumesQty());
                    }
                }

                if(dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_ISSUES_QTY().isEmpty()){
                    Log.info("catalogueIssuesQty not available");
                }else{
                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                    " Work_Extended -> catalogueIssuesQty => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_ISSUES_QTY() +
                            " Work_JSON -> catalogueIssuesQty => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueIssuesQty());
                    if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_QTY() != null ||
                            (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueIssuesQty() != null)) {
                        Assert.assertEquals("The catalogueVolumesQty is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_ISSUES_QTY(),
                                JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueIssuesQty());
                    }
                }

                if(dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_FROM().isEmpty()){
                    Log.info("catalogueVolumeFrom not available");
                }else{
                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                    " Work_Extended -> catalogueVolumeFrom => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_FROM() +
                            " Work_JSON -> catalogueVolumeFrom => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueVolumeFrom());
                    if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_QTY() != null ||
                            (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueVolumeFrom() != null)) {
                        Assert.assertEquals("The catalogueVolumeFrom is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_FROM(),
                                JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueVolumeFrom());
                    }
                }

                if(dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_TO().isEmpty()){
                    Log.info("catalogueVolumeTo not available");
                }else{
                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                    " Work_Extended -> catalogueVolumeTo => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_TO() +
                            " Work_JSON -> catalogueVolumeTo => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueVolumeTo());
                    if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_TO() != null ||
                            (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueVolumeTo() != null)) {
                        Assert.assertEquals("The catalogueVolumeTo is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromExtendeWork.get(i).getCATALOGUE_VOLUME_TO(),
                                JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getCatalogueVolumeTo());
                    }
                }

                if(dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_ISSUES_QTY().isEmpty()){
                    Log.info("RF_ISSUES_QTY not available");
                }else{
                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                    " Work_Extended -> catalogueVolumeTo => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_ISSUES_QTY() +
                            " Work_JSON -> catalogueVolumeTo => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfIssuesQty());
                    if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_ISSUES_QTY() != null ||
                            (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfIssuesQty() != null)) {
                        Assert.assertEquals("The RfIssuesQty is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_ISSUES_QTY(),
                                JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfIssuesQty());
                    }
                }

                if(dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_TOTAL_PAGES_QTY().isEmpty()){
                    Log.info("RF_TOTAL_PAGES_QTY not available");
                }else{
                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                    " Work_Extended -> RF_TOTAL_PAGES_QTY => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_TOTAL_PAGES_QTY() +
                            " Work_JSON -> RF_TOTAL_PAGES_QTY => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfTotalPagesQty());
                    if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_TOTAL_PAGES_QTY() != null ||
                            (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfTotalPagesQty() != null)) {
                        Assert.assertEquals("The RF_TOTAL_PAGES_QTY is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_TOTAL_PAGES_QTY(),
                                JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfTotalPagesQty());
                    }
                }

                if(dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_FVI().isEmpty()){
                    Log.info("RF_FVI not available");
                }else{
                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                    " Work_Extended -> RF_FVI => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_FVI() +
                            " Work_JSON -> RF_FVI => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfFvi());
                    if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_FVI() != null ||
                            (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfFvi() != null)) {
                        Assert.assertEquals("The RfFvi is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_FVI(),
                                JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfFvi());
                    }
                }

                if(dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_LVI().isEmpty()){
                    Log.info("RF_LVI not available");
                }else{
                    Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                    " Work_Extended -> RF_FVI => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_LVI() +
                            " Work_JSON -> RF_FVI => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfLvi());
                    if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_LVI() != null ||
                            (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfLvi() != null)) {
                        Assert.assertEquals("The RF_LVI is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromExtendeWork.get(i).getRF_LVI(),
                                JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getRfLvi());
                    }
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
               " Work_Extended -> issueProdTypeCode => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getISSUE_PROD_TYPE_CODE() +
                        " Work_JSON -> issueProdTypeCode => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getIssueProdTypeCode());
            if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getPRIMARY_SITE_SUPPORT_LEVEL() != null ||
                        (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPrimarySiteSupportLevel() != null)) {
                    Assert.assertEquals("The issueProdTypeCode is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                            dataQualityJRBIContext.recordsFromExtendeWork.get(i).getISSUE_PROD_TYPE_CODE(),
                            JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getIssueProdTypeCode());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID() +
                " Work_Extended -> PtsBusinessUnitDesc => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getBUSINESS_UNIT_DESC() +
                        " Work_JSON -> PtsBusinessUnitDesc => " + JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPtsBusinessUnitDesc());
              if (dataQualityJRBIContext.recordsFromExtendeWork.get(i).getBUSINESS_UNIT_DESC() != null ||
                        (JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPtsBusinessUnitDesc() != null)) {
                    Assert.assertEquals("The PtsBusinessUnitDesc is incorrect for EPR => " + dataQualityJRBIContext.recordsFromExtendeWork.get(i).getEPR_ID(),
                            dataQualityJRBIContext.recordsFromExtendeWork.get(i).getBUSINESS_UNIT_DESC(),
                            JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getPtsBusinessUnitDesc());
                }
            if(JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedPersons()!=null) {
                JRBIPersonExtJson[] extPerson_temp = JRBIAccessDLContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedPersons().clone();
                for(int j=0;j<extPerson_temp.length;j++)
                    {
                       /* Log.info( extPerson_temp[j].getExtendedRole().toString());
                        Log.info(JRBIAccessDLContext.recordsFromPersonExtended.get(j).getROLE_NAME());
                        Log.info(extPerson_temp[j].getExtendedRole().get("code").toString());*/

                        Log.info("EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID() +
                        " Role_Extended -> Role Code => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getROLE_CODE() +
                                " Role_JSON -> Role Code => " + extPerson_temp[j].getExtendedRole().get("code").toString());
                        Assert.assertEquals("The Code is incorrect for EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromPersonExtended.get(j).getROLE_CODE(),
                                extPerson_temp[j].getExtendedRole().get("code").toString());

                        Log.info("EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID() +
                                " Role_Extended -> Role Name => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getROLE_NAME() +
                                " Role_JSON -> Role Name => " + extPerson_temp[j].getExtendedRole().get("code").toString());
                        Assert.assertEquals("The Role name is incorrect for EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromPersonExtended.get(j).getROLE_NAME(),
                                extPerson_temp[j].getExtendedRole().get("name").toString());

                        Log.info("EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID() +
                                " Person_Extended -> firstName => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getFIRST_NAME() +
                                " Person_JSON -> firstName => " + extPerson_temp[j].getExtendedRole().get("firstName").toString());
                        Assert.assertEquals("The First name is incorrect for EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromPersonExtended.get(j).getFIRST_NAME(),
                                extPerson_temp[j].getExtendedPerson().get("firstName").toString());

                        Log.info("EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID() +
                                " Person_Extended -> lastName => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getLAST_NAME() +
                                " Person_JSON -> lastName => " + extPerson_temp[j].getExtendedRole().get("lastName").toString());
                        Assert.assertEquals("The Last name is incorrect for EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromPersonExtended.get(j).getLAST_NAME(),
                                extPerson_temp[j].getExtendedPerson().get("lastName").toString());

                        Log.info("EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID() +
                                " Person_Extended -> peoplehubId => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getPEOPLEHUB_ID() +
                                " Person_JSON -> peoplehubId => " + extPerson_temp[j].getExtendedRole().get("peoplehubId").toString());
                        Assert.assertEquals("The peoplehubId is incorrect for EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromPersonExtended.get(j).getPEOPLEHUB_ID(),
                                extPerson_temp[j].getExtendedPerson().get("peoplehubId").toString());

                        Log.info("EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID() +
                                " Person_Extended -> email => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEMAIL() +
                                " Person_JSON -> email => " + extPerson_temp[j].getExtendedRole().get("email").toString());
                        Assert.assertEquals("The email is incorrect for EPR => " + dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEPR_ID(),
                                dataQualityJRBIContext.recordsFromPersonExtended.get(j).getEMAIL(),
                                extPerson_temp[j].getExtendedPerson().get("email").toString());

                    }
                }

            }
        }
    }






}