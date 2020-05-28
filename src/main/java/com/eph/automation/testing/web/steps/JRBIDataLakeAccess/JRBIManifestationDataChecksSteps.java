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

    @Given("^We get the (.*) random EPR ids for Manifestation from data full load (.*)$")
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

    @And("^Compare the records of manifestation full load and current manifestation (.*)$")
    public void compareDataFullandCurrentManif(String tableName) {
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