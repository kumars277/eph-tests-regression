package com.eph.automation.testing.web.steps.JRBIDataLakeAccess;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLManifestationAccessObject;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLWorkAccessObject;
import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIManifestationDataChecksSQL;
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


public class JRBIManifestationDataChecksSteps {

    public JRBIAccessDLContext dataQualityJRBIContext;
    private static String sql;
    private static List<String> Ids;
    private JRBIManifestationDataChecksSQL jrbiObj = new JRBIManifestationDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^We get the (.*) random manifestation EPR ids (.*)$")
    public void getRandomManifEPRIds(String numberOfRecords, String tableName) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Manif EPR Ids...");
        switch (tableName) {
            case "jrbi_journal_data_full":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_EPR_IDS_MANIF_FULLLOAD, numberOfRecords);
                List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_EPR_ID, numberOfRecords);
                List<Map<?, ?>> randommanifEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randommanifEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_previous_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_EPR_ID, numberOfRecords);
                List<Map<?, ?>> randomPreviousmanifEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPreviousmanifEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the records from data full load for Manifestation (.*)$")
    public void getManifRecordsFromFullLoad(String tableName) {
        Log.info("We get the FULL Load Manif records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_MANIF_RECORDS_FULL_LOAD, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDataFullLoadManif = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform current manifestation (.*)$")
    public void getRecordsFromCurrentManif(String tableName) {
        Log.info("We get the records from Current Manif...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentManif = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @Then("^We Get the records from transform current manifestation History (.*)$")
    public void getRecordsFromCurrentManifHistory(String tableName) {
        Log.info("We get the records from Current Manif History...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentManifHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @When("^We get the records from transform previous manifestation (.*)$")
    public void getRecordsFromPreviousManif(String tableName) {
        Log.info("We get the records from PRevious Manif...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousManif = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform previous manifestation history (.*)$")
    public void getRecordsFromPreviousManifHistory(String tableName) {
        Log.info("We get the records from PRevious Manif History...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousManifHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }
    @And("^Compare the records of previous manifestation and previous manifestation history$")
    public void comparePReviousManifandHistory() {
        if (dataQualityJRBIContext.recordsFromFromPreviousManif.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Previous Manif and Previous Manif History...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromPreviousManif.size(); i++) {

                dataQualityJRBIContext.recordsFromFromPreviousManif.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromPreviousManifHistory.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR));

                Log.info("Previous_Manif -> EPR => " + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() +
                        "Previous_Manif_hist -> EPR => " + dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() + " is missing/not found in Previous_Manif table",
                            dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() +
                        " RECORD_TYPE => Previous_Manif =" + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getRECORD_TYPE() +
                        " Previous_Manif_hist=" + dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() +
                        " JOURNAL_PROD_SITE => Previous_Manif =" + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getJOURNAL_PROD_SITE() +
                        " Previous_Manif_hist=" + dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getJOURNAL_PROD_SITE());

                if (dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getJOURNAL_PROD_SITE() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getJOURNAL_PROD_SITE() != null)) {
                    Assert.assertEquals("The JOURNAL_PROD_SITE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getJOURNAL_PROD_SITE(),
                            dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getJOURNAL_PROD_SITE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() +
                        " JOURNAL_ISSUE_TRIM_SIZE => Previous_Manif =" + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() +
                        " Previous_Manif_hist=" + dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE());

                if (dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null)) {
                    Assert.assertEquals("The JOURNAL_ISSUE_TRIM_SIZE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE(),
                            dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() +
                        " WAR_REFERENCE => Previous_Manif =" + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getWAR_REFERENCE() +
                        " Previous_Manif_hist=" + dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getWAR_REFERENCE());

                if (dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getWAR_REFERENCE() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getWAR_REFERENCE() != null)) {
                    Assert.assertEquals("The WAR_REFERENCE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getWAR_REFERENCE(),
                            dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getWAR_REFERENCE());
                }

            }
        }
    }

    @And("^Compare the records of current manifestation and current manifestation history$")
    public void compareCurrentManifandHistory() {
        if (dataQualityJRBIContext.recordsFromFromCurrentManif.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Current Manif and Current Manif History...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromCurrentManif.size(); i++) {

                dataQualityJRBIContext.recordsFromFromCurrentManif.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentManifHistory.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR));

                Log.info("Current_Manif -> EPR => " + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() +
                        "Current_Manif_hist -> EPR => " + dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() + " is missing/not found in Current_Manif table",
                            dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() +
                        " RECORD_TYPE => Current_Manif =" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getRECORD_TYPE() +
                        " Current_Manif_hist=" + dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() +
                        " JOURNAL_PROD_SITE => Current_Manif =" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_PROD_SITE() +
                        " Current_Manif_hist=" + dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getJOURNAL_PROD_SITE());

                if (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_PROD_SITE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getJOURNAL_PROD_SITE() != null)) {
                    Assert.assertEquals("The JOURNAL_PROD_SITE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_PROD_SITE(),
                            dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getJOURNAL_PROD_SITE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() +
                        " JOURNAL_ISSUE_TRIM_SIZE => Current_Manif =" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() +
                        " Current_Manif_hist=" + dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE());

                if (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null)) {
                    Assert.assertEquals("The JOURNAL_ISSUE_TRIM_SIZE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE(),
                            dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() +
                        " WAR_REFERENCE => Current_Manif =" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getWAR_REFERENCE() +
                        " Current_Manif_hist=" + dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getWAR_REFERENCE());

                if (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getWAR_REFERENCE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getWAR_REFERENCE() != null)) {
                    Assert.assertEquals("The WAR_REFERENCE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getWAR_REFERENCE(),
                            dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getWAR_REFERENCE());
                }

            }
        }
    }

    @And("^Compare the records of manifestation full load and current manifestation$")
    public void compareDataFullandCurrentManif() {
       if (dataQualityJRBIContext.recordsFromDataFullLoadManif.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Manif Full Load and Current Manif...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDataFullLoadManif.size(); i++) {

                dataQualityJRBIContext.recordsFromDataFullLoadManif.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentManif.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR));

                Log.info("Full Load -> EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() +
                        "Current_Manif -> EPR => " + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() + " is missing/not found in Current_Manif table",
                            dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() +
                        " RECORD_TYPE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getRECORD_TYPE() +
                        " Current_Manif=" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() +
                        " JOURNAL_PROD_SITE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_PROD_SITE() +
                        " Current_Manif=" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_PROD_SITE());

                if (dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_PROD_SITE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_PROD_SITE() != null)) {
                    Assert.assertEquals("The JOURNAL_PROD_SITE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_PROD_SITE(),
                            dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_PROD_SITE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() +
                        " JOURNAL_ISSUE_TRIM_SIZE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() +
                        " Current_Manif=" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());

                if (dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null)) {
                    Assert.assertEquals("The JOURNAL_ISSUE_TRIM_SIZE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE(),
                            dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() +
                        " WAR_REFERENCE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getWAR_REFERENCE() +
                        " Current_Manif=" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getWAR_REFERENCE());

                if (dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getWAR_REFERENCE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getWAR_REFERENCE() != null)) {
                    Assert.assertEquals("The WAR_REFERENCE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getWAR_REFERENCE(),
                            dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getWAR_REFERENCE());
                }

            }
        }
    }


}