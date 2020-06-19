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
            case "jrbi_delta_current_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_DELTA_MANIF_EPR_ID, numberOfRecords);
                List<Map<?, ?>> randomPDeltamanifEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPDeltamanifEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_history_manifestation_excl_delta":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_MANIF, numberOfRecords);
                List<Map<?, ?>> randomExclmanifEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomExclmanifEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_transform_latest_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_JRBI_EPR_MANIF_LATEST, numberOfRecords);
                List<Map<?, ?>> randomLatestmanifEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomLatestmanifEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_current_previous_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_RANDOM_EPR_DELTA_PERSON, numberOfRecords);
                List<Map<?, ?>> randomCurrPrevManifEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomCurrPrevManifEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }




    @When("^Get the records from data full load for Manifestation$")
    public void getManifRecordsFromFullLoad() {
        Log.info("We get the FULL Load Manif records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_MANIF_RECORDS_FULL_LOAD, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDataFullLoadManif = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform current manifestation$")
    public void getRecordsFromCurrentManif() {
        Log.info("We get the records from Current Manif...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentManif = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
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

                   String recordType = dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getRECORD_TYPE();
                   if(recordType.isEmpty()){
                      recordType = null;
                   }
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() ,
                            recordType,dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() +
                        " MANIFESTATION_TYPE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getMANIFESTATION_TYPE() +
                        " Current_Manif=" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getMANIFESTATION_TYPE());

                if (dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getMANIFESTATION_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getMANIFESTATION_TYPE() != null)) {

                    String manifType = dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getMANIFESTATION_TYPE();
                    if(manifType.isEmpty()){
                        manifType = null;
                    }
                    Assert.assertEquals("The MANIFESTATION_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() ,
                            manifType,dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getMANIFESTATION_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() +
                        " JOURNAL_PROD_SITE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_PROD_SITE() +
                        " Current_Manif=" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_PROD_SITE());

                if (dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_PROD_SITE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_PROD_SITE() != null)) {

                    String journalProdSite = dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_PROD_SITE();
                    if(journalProdSite.isEmpty()){
                        journalProdSite = null;
                    }
          Assert.assertEquals("The JOURNAL_PROD_SITE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() ,
                  journalProdSite,dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_PROD_SITE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() +
                        " JOURNAL_ISSUE_TRIM_SIZE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() +
                        " Current_Manif=" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());

                if (dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null)) {
                    String journalIssueTrim = dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE();
                    if(journalIssueTrim.isEmpty()){
                        journalIssueTrim = null;
                    }
                    Assert.assertEquals("The JOURNAL_ISSUE_TRIM_SIZE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() ,
                            journalIssueTrim,dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() +
                        " WAR_REFERENCE => Full_Load =" + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getWAR_REFERENCE() +
                        " Current_Manif=" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getWAR_REFERENCE());

                if (dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getWAR_REFERENCE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getWAR_REFERENCE() != null)) {

                    String war_reference = dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getWAR_REFERENCE();
                    if(war_reference.isEmpty()){
                        war_reference = null;
                    }
                    Assert.assertEquals("The WAR_REFERENCE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() ,
                            war_reference,dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getWAR_REFERENCE());
                }
            }
        }
    }

    @Then("^We Get the records from transform current manifestation History$")
    public void getRecordsCurrentManifHistory() {
        Log.info("We get the records from Current Manif History...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentManifHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of current manifestation and current manifestation history$")
    public void compareCurrentManifandManifHistory() {
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
                        " MANIFESTATION_TYPE => Current_Manif =" + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getMANIFESTATION_TYPE() +
                        " Current_Manif_hist=" + dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getMANIFESTATION_TYPE());

                if (dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getMANIFESTATION_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getMANIFESTATION_TYPE() != null)) {
                    Assert.assertEquals("The MANIFESTATION_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getMANIFESTATION_TYPE(),
                            dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i).getMANIFESTATION_TYPE());
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

    @When("^Get the records from the difference of current_manifestation and previous_manifestation$")
    public void getDiffCurrentPreviousRecords() {
        Log.info("We get the difference of Current Manif and Previous Manif records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_DIFF_REC_PREVIOUS_CURRENT_PREVIOUS_MANIF, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @When("^We get the records from transform delta manifestation$")
    public void getDeltaCurrentManifRecords() {
        Log.info("We get the Delta current Manif records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_DELTA_MANIF_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaManif = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of Delta Current manifestation with difference of current and previous manifestation$")
    public void compareDeltawithCrrentPReviousManifRecords() {
        if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Delta Manif...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.size(); i++) {

                dataQualityJRBIContext.recordsFromFromDeltaManif.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR));

                Log.info("Current_PRevious -> EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() +
                        "Delta_Manif -> EPR => " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() + " is missing/not found in Delta_Manif table",
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() +
                        " RECORD_TYPE => Current_Previous =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getRECORD_TYPE() +
                        " Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getRECORD_TYPE());
                }
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() +
                        " MANIFESTATION_TYPE => Current_Previous =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getMANIFESTATION_TYPE() +
                        " Delta_Manif=" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getMANIFESTATION_TYPE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getMANIFESTATION_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getMANIFESTATION_TYPE() != null)) {
                    Assert.assertEquals("The MANIFESTATION_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getMANIFESTATION_TYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getMANIFESTATION_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() +
                        " JOURNAL_PROD_SITE => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getJOURNAL_PROD_SITE() +
                        " Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_PROD_SITE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getJOURNAL_PROD_SITE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_PROD_SITE() != null)) {
                    Assert.assertEquals("The JOURNAL_PROD_SITE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getJOURNAL_PROD_SITE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_PROD_SITE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() +
                        " JOURNAL_ISSUE_TRIM_SIZE => Current_Previous =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() +
                        " Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null)) {
                    Assert.assertEquals("The JOURNAL_ISSUE_TRIM_SIZE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() +
                        " WAR_REFERENCE => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getWAR_REFERENCE() +
                        " Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getWAR_REFERENCE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getWAR_REFERENCE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getWAR_REFERENCE() != null)) {
                    Assert.assertEquals("The WAR_REFERENCE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getWAR_REFERENCE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getWAR_REFERENCE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() +
                        " TYPE => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getTYPE() +
                        " Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getTYPE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getTYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getTYPE() != null)) {
                    Assert.assertEquals("The TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getTYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getTYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() +
                        " DELTA_MODE => Current_PRevious =" + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getDELTA_MODE() +
                        " Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getDELTA_MODE());

                if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getDELTA_MODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getDELTA_MODE() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i).getDELTA_MODE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getDELTA_MODE());
                }
            }
        }
    }

    @Then("^Get the records from transform delta manifestation history$")
    public void getDeltaManifRecordsHistory() {
        Log.info("We get the Delta Manif History records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_DELTA_MANIF_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaManifHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }


    @And("^Compare the records of delta manifestation and delta manifestation history$")
    public void compareDeltaManifandDeltaManifHistory() {
        if (dataQualityJRBIContext.recordsFromFromDeltaManif.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Delta Manif and Delta Manif History...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromDeltaManif.size(); i++) {

                dataQualityJRBIContext.recordsFromFromDeltaManif.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaManifHistory.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR));

                Log.info("Delta_Manif -> EPR => " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() +
                        "Delta_Manif_History -> EPR => " + dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() + " is missing/not found in Delta_Manif table",
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() +
                        " RECORD_TYPE => Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getRECORD_TYPE() +
                        " Delta_Manif_History=" + dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() +
                        " MANIFESTATION_TYPE => Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getMANIFESTATION_TYPE() +
                        " Delta_Manif_History=" + dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getMANIFESTATION_TYPE());

                if (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getMANIFESTATION_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getMANIFESTATION_TYPE() != null)) {
                    Assert.assertEquals("The MANIFESTATION_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getMANIFESTATION_TYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getMANIFESTATION_TYPE());
                }


           /*     Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() +
                        " JOURNAL_PROD_SITE => Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_PROD_SITE() +
                        " Delta_Manif_History=" + dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getJOURNAL_PROD_SITE());

                if (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_PROD_SITE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getJOURNAL_PROD_SITE() != null)) {
                    Assert.assertEquals("The JOURNAL_PROD_SITE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_PROD_SITE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getJOURNAL_PROD_SITE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() +
                        " JOURNAL_ISSUE_TRIM_SIZE => Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() +
                        " Delta_Manif_History=" + dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE());

                if (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null)) {
                    Assert.assertEquals("The JOURNAL_ISSUE_TRIM_SIZE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() +
                        " WAR_REFERENCE => Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getWAR_REFERENCE() +
                        " Delta_Manif_History=" + dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getWAR_REFERENCE());

                if (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getWAR_REFERENCE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getWAR_REFERENCE() != null)) {
                    Assert.assertEquals("The WAR_REFERENCE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getWAR_REFERENCE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getWAR_REFERENCE());
                }
*/
                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() +
                        " TYPE => Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getTYPE() +
                        " Delta_Manif_History=" + dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getTYPE());

                if (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getTYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getTYPE() != null)) {
                    Assert.assertEquals("The TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getTYPE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getTYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() +
                        " DELTA_MODE => Delta_Manif =" + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getDELTA_MODE() +
                        " Delta_Manif_History=" + dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getDELTA_MODE());

                if (dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getDELTA_MODE() != null ||
                        (dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getDELTA_MODE() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getDELTA_MODE(),
                            dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i).getDELTA_MODE());
                }
            }
        }
    }

    @When("^Get the records from the difference of Delta_current_manif and manif_history$")
    public void getDeltaManifandHistoryRecords() {
        Log.info("We get the Delta Manif and Manif Histroy difference records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_MANIF, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from manifestation exclude table$")
    public void getManifExcludeRecords() {
        Log.info("We get the Manif Exclude records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_RECORDS_FROM_MANIF_EXCLUDE, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromExcludeManif = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of Manif Exclude with difference of Delta_current_manif and manif_history$")
    public void compareExcludeManifRecords() {
        if (dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records Manif Exclude...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.size(); i++) {

                dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromExcludeManif.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR));

                Log.info("ManifDelta_History -> EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() +
                        "Manif_Exclude -> EPR => " + dataQualityJRBIContext.recordsFromExcludeManif.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeManif.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() + " is missing/not found in ManifDelta_History table",
                            dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromExcludeManif.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() +
                        " RECORD_TYPE => ManifDelta_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getRECORD_TYPE() +
                        " Manif_Exclude=" + dataQualityJRBIContext.recordsFromExcludeManif.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeManif.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromExcludeManif.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() +
                        " MANIFESTATION_TYPE => ManifDelta_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getMANIFESTATION_TYPE() +
                        " Manif_Exclude=" + dataQualityJRBIContext.recordsFromExcludeManif.get(i).getMANIFESTATION_TYPE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getMANIFESTATION_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeManif.get(i).getMANIFESTATION_TYPE() != null)) {
                    Assert.assertEquals("The MANIFESTATION_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getMANIFESTATION_TYPE(),
                            dataQualityJRBIContext.recordsFromExcludeManif.get(i).getMANIFESTATION_TYPE());
                }


                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() +
                        " JOURNAL_PROD_SITE => ManifDelta_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getJOURNAL_PROD_SITE() +
                        " Manif_Exclude=" + dataQualityJRBIContext.recordsFromExcludeManif.get(i).getJOURNAL_PROD_SITE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getJOURNAL_PROD_SITE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeManif.get(i).getJOURNAL_PROD_SITE() != null)) {
                    Assert.assertEquals("The JOURNAL_PROD_SITE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getJOURNAL_PROD_SITE(),
                            dataQualityJRBIContext.recordsFromExcludeManif.get(i).getJOURNAL_PROD_SITE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() +
                        " JOURNAL_ISSUE_TRIM_SIZE => ManifDelta_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE() +
                        " Manif_Exclude=" + dataQualityJRBIContext.recordsFromExcludeManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null)) {
                    Assert.assertEquals("The JOURNAL_ISSUE_TRIM_SIZE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getJOURNAL_ISSUE_TRIM_SIZE(),
                            dataQualityJRBIContext.recordsFromExcludeManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() +
                        " WAR_REFERENCE => ManifDelta_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getWAR_REFERENCE() +
                        " Manif_Exclude=" + dataQualityJRBIContext.recordsFromExcludeManif.get(i).getWAR_REFERENCE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getWAR_REFERENCE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeManif.get(i).getWAR_REFERENCE() != null)) {
                    Assert.assertEquals("The WAR_REFERENCE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getWAR_REFERENCE(),
                            dataQualityJRBIContext.recordsFromExcludeManif.get(i).getWAR_REFERENCE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() +
                        " LAST_UPDATED_DATE => ManifDelta_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getLAST_UPDATED_DATE() +
                        " Manif_Exclude=" + dataQualityJRBIContext.recordsFromExcludeManif.get(i).getLAST_UPDATED_DATE());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getLAST_UPDATED_DATE() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeManif.get(i).getLAST_UPDATED_DATE() != null)) {
                    Assert.assertEquals("The LAST_UPDATED_DATE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getLAST_UPDATED_DATE(),
                            dataQualityJRBIContext.recordsFromExcludeManif.get(i).getLAST_UPDATED_DATE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() +
                        " DELETE_FLAG => ManifDelta_History =" + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getDELETE_FLAG() +
                        " Manif_Exclude=" + dataQualityJRBIContext.recordsFromExcludeManif.get(i).getDELETE_FLAG());

                if (dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getDELETE_FLAG() != null ||
                        (dataQualityJRBIContext.recordsFromExcludeManif.get(i).getDELETE_FLAG() != null)) {
                    Assert.assertEquals("The DELETE_FLAG is incorrect for EPR = " + dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getDELETE_FLAG(),
                            dataQualityJRBIContext.recordsFromExcludeManif.get(i).getDELETE_FLAG());
                }
            }
        }
    }


    @When("^We get the records from transform previous manifestation$")
    public void getRecordsFromPreviousManif() {
        Log.info("We get the records from PRevious Manif...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousManif = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform previous manifestation history$")
    public void getRecordsFromPreviousManifHistory() {
        Log.info("We get the records from PRevious Manif History...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousManifHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of previous manifestation and previous manifestation history$")
    public void comparePreviousManifandHistory() {
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
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() + " is missing/not found in Previous_Manif_hist table",
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
                        " MANIFESTATION_TYPE => Previous_Manif =" + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getMANIFESTATION_TYPE() +
                        " Previous_Manif_hist=" + dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getMANIFESTATION_TYPE());

                if (dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getMANIFESTATION_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getMANIFESTATION_TYPE() != null)) {
                    Assert.assertEquals("The MANIFESTATION_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getMANIFESTATION_TYPE(),
                            dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i).getMANIFESTATION_TYPE());
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
    @When("^Get the records from the addition of Delta_manifestation_work and manifestation_Exclude$")
    public void getDeltaManifandExcludeRecords() {
        Log.info("We get the Delta Manif and Manif Exclude records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_JRBI_REC_SUM_DELTA_MANIF_AND_MANIF_EXCLUDE, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from manifestation latest table$")
    public void getLatestManifRecords() {
        Log.info("We get the Latest Manif records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_JRBI_MANIF_LATEST_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromLAtestManif = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of Manifestation Latest with addition of Delta_current_Manifestation and Manifestation_Exclude$")
    public void compareLatestManifRecords() {
        if (dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records Manif Exclude...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.size(); i++) {

                dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromLAtestManif.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR));

                Log.info("Delta_Current_Exclude -> EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() +
                        "Manif_Latest -> EPR => " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR());
                if (dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() + " is missing/not found in Manif_latest table",
                            dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() +
                        " MANIFESTATION_TYPE => Delta_Current_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getMANIFESTATION_TYPE() +
                        " Manif_Latest=" + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getMANIFESTATION_TYPE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getMANIFESTATION_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getMANIFESTATION_TYPE() != null)) {
                    Assert.assertEquals("The MANIFESTATION_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getMANIFESTATION_TYPE(),
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getMANIFESTATION_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() +
                        " RECORD_TYPE => Delta_Current_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getRECORD_TYPE() +
                        " Manif_Latest=" + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getRECORD_TYPE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getRECORD_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getRECORD_TYPE() != null)) {
                    Assert.assertEquals("The RECORD_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getRECORD_TYPE(),
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getRECORD_TYPE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() +
                        " JOURNAL_PROD_SITE => Delta_Current_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getJOURNAL_PROD_SITE() +
                        " Manif_Latest=" + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getJOURNAL_PROD_SITE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getJOURNAL_PROD_SITE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getJOURNAL_PROD_SITE() != null)) {
                    Assert.assertEquals("The JOURNAL_PROD_SITE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getJOURNAL_PROD_SITE(),
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getJOURNAL_PROD_SITE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() +
                        " JOURNAL_ISSUE_TRIM_SIZE => Delta_Current_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getJOURNAL_ISSUE_TRIM_SIZE() +
                        " Manif_Latest=" + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null)) {
                    Assert.assertEquals("The JOURNAL_ISSUE_TRIM_SIZE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getJOURNAL_ISSUE_TRIM_SIZE(),
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() +
                        " WAR_REFERENCE => Delta_Current_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getWAR_REFERENCE() +
                        " Manif_Latest=" + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getWAR_REFERENCE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getWAR_REFERENCE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getWAR_REFERENCE() != null)) {
                    Assert.assertEquals("The WAR_REFERENCE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getWAR_REFERENCE(),
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getWAR_REFERENCE());
                }

            /*    Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() +
                        " LAST_UPDATED_DATE => Delta_Current_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getLAST_UPDATED_DATE() +
                        " Manif_Latest=" + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getLAST_UPDATED_DATE());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getLAST_UPDATED_DATE() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getLAST_UPDATED_DATE() != null)) {
                    Assert.assertEquals("The LAST_UPDATED_DATE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getLAST_UPDATED_DATE(),
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getLAST_UPDATED_DATE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() +
                        " DELETE_FLAG => Delta_Current_Exclude =" + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getDELETE_FLAG() +
                        " Manif_Latest=" + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getDELETE_FLAG());

                if (dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getDELETE_FLAG() != null ||
                        (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getDELETE_FLAG() != null)) {
                    Assert.assertEquals("The DELETE_FLAG is incorrect for EPR = " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getDELETE_FLAG(),
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getDELETE_FLAG());
                }*/
            }
        }
    }

    @Then("^Get the records from manif extended table$")
    public void getExtendedManifRecords() {
        Log.info("We get the Manif Extended records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_JRBI_MANIF_EXTENDED_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromLAtestManif = DBManager.getDBResultAsBeanList(sql, JRBIDLManifestationAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of manif Latest with manif_Extended")
    public void compareManifExtendedRecords() {
        if (dataQualityJRBIContext.recordsFromLAtestManif.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records Manif Extended...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromLAtestManif.size(); i++) {

                dataQualityJRBIContext.recordsFromLAtestManif.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromExtendedManif.sort(Comparator.comparing(JRBIDLManifestationAccessObject::getEPR_ID));

                Log.info("Manif_Latest -> EPR => " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() +
                        "Manif_Extended -> EPR => " + dataQualityJRBIContext.recordsFromExtendedManif.get(i).getEPR_ID());
                if (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() != null ||
                        (dataQualityJRBIContext.recordsFromExtendedManif.get(i).getEPR() != null)) {
                    Assert.assertEquals("The EPR is =" + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() + " is missing/not found in Manif_latest table",
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR(),
                            dataQualityJRBIContext.recordsFromExtendedManif.get(i).getEPR());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() +
                        " MANIFESTATION_TYPE => Manif_Latest =" + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getMANIFESTATION_TYPE() +
                        " Manif_Extended=" + dataQualityJRBIContext.recordsFromExtendedManif.get(i).getMANIFESTATION_TYPE());

                if (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getMANIFESTATION_TYPE() != null ||
                        (dataQualityJRBIContext.recordsFromExtendedManif.get(i).getMANIFESTATION_TYPE() != null)) {
                    Assert.assertEquals("The MANIFESTATION_TYPE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getMANIFESTATION_TYPE(),
                            dataQualityJRBIContext.recordsFromExtendedManif.get(i).getMANIFESTATION_TYPE());
                }

                String journalProdCodeExt = dataQualityJRBIContext.recordsFromLAtestManif.get(i).getJOURNAL_PROD_SITE();
                String journalIssueTrimSize = dataQualityJRBIContext.recordsFromLAtestManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE();
                String warRefExt = dataQualityJRBIContext.recordsFromLAtestManif.get(i).getWAR_REFERENCE();
                if(dataQualityJRBIContext.recordsFromLAtestManif.get(i).getDELETE_FLAG()=="1"){
                    journalProdCodeExt = null;
                    journalIssueTrimSize = null;
                    warRefExt = null;
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() +
                        " JOURNAL_PROD_SITE => Manif_Latest =" + journalProdCodeExt +
                        " Manif_Extended=" + dataQualityJRBIContext.recordsFromExtendedManif.get(i).getJOURNAL_PROD_SITE());

                if (journalProdCodeExt != null || dataQualityJRBIContext.recordsFromExtendedManif.get(i).getJOURNAL_PROD_SITE() != null) {
                    Assert.assertEquals("The JOURNAL_PROD_SITE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() ,
                            journalProdCodeExt,dataQualityJRBIContext.recordsFromExtendedManif.get(i).getJOURNAL_PROD_SITE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() +
                        " JOURNAL_ISSUE_TRIM_SIZE => Delta_Current_Exclude =" + journalIssueTrimSize +
                        " Manif_Latest=" + dataQualityJRBIContext.recordsFromExtendedManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());

                if (journalIssueTrimSize != null || dataQualityJRBIContext.recordsFromExtendedManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE() != null) {
                    Assert.assertEquals("The JOURNAL_ISSUE_TRIM_SIZE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() ,
                            journalIssueTrimSize,dataQualityJRBIContext.recordsFromExtendedManif.get(i).getJOURNAL_ISSUE_TRIM_SIZE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() +
                        " WAR_REFERENCE => MAnif_Latest =" + warRefExt +
                        " Manif_Extend=" + dataQualityJRBIContext.recordsFromExtendedManif.get(i).getWAR_REFERENCE());

                if (warRefExt != null || dataQualityJRBIContext.recordsFromExtendedManif.get(i).getWAR_REFERENCE() != null) {
                    Assert.assertEquals("The WAR_REFERENCE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() ,
                            warRefExt,dataQualityJRBIContext.recordsFromLAtestManif.get(i).getWAR_REFERENCE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() +
                        " LAST_UPDATED_DATE => Manif_Latest =" + dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getLAST_UPDATED_DATE() +
                        " Manif_Extended=" + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getLAST_UPDATED_DATE());

                if (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getLAST_UPDATED_DATE() != null ||
                        (dataQualityJRBIContext.recordsFromExtendedManif.get(i).getLAST_UPDATED_DATE() != null)) {
                    Assert.assertEquals("The LAST_UPDATED_DATE is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getLAST_UPDATED_DATE(),
                            dataQualityJRBIContext.recordsFromExtendedManif.get(i).getLAST_UPDATED_DATE());
                }

                Log.info("EPR => " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() +
                        " DELETE_FLAG => Manif_Latest =" + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getDELETE_FLAG() +
                        " Manif_Extended=" + dataQualityJRBIContext.recordsFromExtendedManif.get(i).getDELETE_FLAG());

                if (dataQualityJRBIContext.recordsFromLAtestManif.get(i).getDELETE_FLAG() != null ||
                        (dataQualityJRBIContext.recordsFromExtendedManif.get(i).getDELETE_FLAG() != null)) {
                    Assert.assertEquals("The DELETE_FLAG is incorrect for EPR = " + dataQualityJRBIContext.recordsFromLAtestManif.get(i).getEPR() ,
                            dataQualityJRBIContext.recordsFromLAtestManif.get(i).getDELETE_FLAG(),
                            dataQualityJRBIContext.recordsFromExtendedManif.get(i).getDELETE_FLAG());
                }
            }
        }
    }


}