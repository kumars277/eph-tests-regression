package com.eph.automation.testing.web.steps.c_JRBI;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.JRBIManifestationDataChecksSQL;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.*;
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


public class JRBIManifestationDataChecksSteps {

    public JRBIAccessDLContext dataQualityJRBIContext;
    private static String sql;
    private static List<String> Ids;
    private JRBIManifestationDataChecksSQL jrbiObj = new JRBIManifestationDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^We get the (.*) random manifestation EPR ids (.*)$")
    public void getRandomManifEPRIds(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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
                List<Map<?, ?>> randomLatestManifEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomLatestManifEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "jrbi_current_previous_manifestation":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_RANDOM_EPR_DELTA_PERSON, numberOfRecords);
                List<Map<?, ?>> randomCurrPrevManifEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomCurrPrevManifEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;
            case "manifestation_extended":
                sql = String.format(JRBIManifestationDataChecksSQL.GET_RANDOM_EPR_MANIF_EXTENDED, numberOfRecords);
                List<Map<?, ?>> randomManifExtendedEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifExtendedEPRIds.stream().map(m -> (String) m.get("EPR")).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the records from data full load for Manifestation$")
    public void getManifRecordsFromFullLoad() {
        Log.info("We get the FULL Load Manif records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_MANIF_RECORDS_FULL_LOAD, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDataFullLoadManif = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform current manifestation$")
    public void getRecordsFromCurrentManif() {
        Log.info("We get the records from Current Manif...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentManif = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of manifestation full load and current manifestation$")
    public void compareDataFullandCurrentManif() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromDataFullLoadManif.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Manif Full Load and Current Manif...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDataFullLoadManif.size(); i++) {

                dataQualityJRBIContext.recordsFromDataFullLoadManif.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentManif.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE"};
                for (String strTemp : etl_curr_manifest) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromCurrentManif.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " +  dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR() +
                            " " + strTemp + " => Manif_Full_Load = " + method.invoke(objectToCompare1) +
                            " Current_manifest = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Current_manifest for EPR:"+dataQualityJRBIContext.recordsFromDataFullLoadManif.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

    @Then("^We Get the records from transform current manifestation History$")
    public void getRecordsCurrentManifHistory() {
        Log.info("We get the records from Current Manif History...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_CURRENT_MANIF_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromCurrentManifHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of current manifestation and current manifestation history$")
    public void compareCurrentManifandManifHistory() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromFromCurrentManif.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Current Manif and Current Manif History...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromCurrentManif.size(); i++) {

                dataQualityJRBIContext.recordsFromFromCurrentManif.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromCurrentManifHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE"};
                for (String strTemp : etl_curr_manifest) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromFromCurrentManif.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromCurrentManifHistory.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " +  dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR() +
                            " " + strTemp + " => Current_manifest = " + method.invoke(objectToCompare1) +
                            " manifest_hsistory = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifest_hsistory for EPR:"+dataQualityJRBIContext.recordsFromFromCurrentManif.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

    @When("^Get the records from the difference of current_manifestation and previous_manifestation$")
    public void getDiffCurrentPreviousRecords() {
        Log.info("We get the difference of Current Manif and Previous Manif records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_DIFF_REC_PREVIOUS_CURRENT_PREVIOUS_MANIF, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @When("^We get the records from transform delta manifestation$")
    public void getDeltaCurrentManifRecords() {
        Log.info("We get the Delta current Manif records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_DELTA_MANIF_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaManif = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of Delta Current manifestation with difference of current and previous manifestation$")
    public void compareDeltawithCrrentPReviousManifRecords() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Delta Manif...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.size(); i++) {

                dataQualityJRBIContext.recordsFromFromDeltaManif.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE","getTYPE","getDELTA_MODE"};
                for (String strTemp : etl_curr_manifest) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromFromDeltaManif.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromDiffCurrentAndPreviousManif.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " +  dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() +
                            " " + strTemp + " => DeltaManif = " + method.invoke(objectToCompare1) +
                            " Curr_Previous_Manif = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifest_hsistory for EPR:"+dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

    @Then("^Get the records from transform delta manifestation history$")
    public void getDeltaManifRecordsHistory() {
        Log.info("We get the Delta Manif History records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_DELTA_MANIF_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromDeltaManifHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }


    @And("^Compare the records of delta manifestation and delta manifestation history$")
    public void compareDeltaManifandDeltaManifHistory() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromFromDeltaManif.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Delta Manif and Delta Manif History...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromDeltaManif.size(); i++) {

                dataQualityJRBIContext.recordsFromFromDeltaManif.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromDeltaManifHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE","getTYPE","getDELTA_MODE"};
                for (String strTemp : etl_curr_manifest) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromFromDeltaManif.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromDeltaManifHistory.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " +  dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR() +
                            " " + strTemp + " => DeltaManif = " + method.invoke(objectToCompare1) +
                            " Delta_Manfi_hist = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifest_hsistory for EPR:"+dataQualityJRBIContext.recordsFromFromDeltaManif.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

    @When("^Get the records from the difference of Delta_current_manif and manif_history$")
    public void getDeltaManifandHistoryRecords() {
        Log.info("We get the Delta Manif and Manif Histroy difference records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_MANIF, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And ("^Get the records from manifestation jrbi exclude extended tables$")
    public void getManifExcludeRecords() {
        Log.info("We get the Manif Exclude records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_RECORDS_FROM_MANIF_EXCLUDE, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromExcludeManif = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of Manif Exclude with difference of Delta_current_manif and manif_history$")
    public void compareExcludeManifRecords()throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records Manif Exclude...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.size(); i++) {

                dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromExcludeManif.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE","getLAST_UPDATED_DATE","getDELETE_FLAG"};
                for (String strTemp : etl_curr_manifest) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromExcludeManif.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " +  dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR() +
                            " " + strTemp + " => DeltaManif_Hist = " + method.invoke(objectToCompare1) +
                            " Exclude_Manif = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifest_hsistory for EPR:"+dataQualityJRBIContext.recordsFromDiffDeltaAndManifHistory.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

    @When("^We get the records from transform previous manifestation$")
    public void getRecordsFromPreviousManif() {
        Log.info("We get the records from PRevious Manif...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousManif = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform previous manifestation history$")
    public void getRecordsFromPreviousManifHistory() {
        Log.info("We get the records from PRevious Manif History...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_PREVIOUS_MANIF_HISTORY_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromFromPreviousManifHistory = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of previous manifestation and previous manifestation history$")
    public void comparePreviousManifandHistory() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromFromPreviousManif.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records between Previous Manif and Previous Manif History...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromFromPreviousManif.size(); i++) {

                dataQualityJRBIContext.recordsFromFromPreviousManif.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromFromPreviousManifHistory.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE"};
                for (String strTemp : etl_curr_manifest) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromFromPreviousManif.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromFromPreviousManifHistory.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " +  dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR() +
                            " " + strTemp + " => Previous_Manif = " + method.invoke(objectToCompare1) +
                            " Manif_History = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifest_hsistory for EPR:"+dataQualityJRBIContext.recordsFromFromPreviousManif.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

    @When("^Get the records from the addition of Delta_manifestation_work and manifestation_Exclude$")
    public void getDeltaManifandExcludeRecords() {
        Log.info("We get the Delta Manif and Manif Exclude records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_JRBI_REC_SUM_DELTA_MANIF_AND_MANIF_EXCLUDE, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from manifestation latest table$")
    public void getLatestManifRecords() {
        Log.info("We get the Latest Manif records...");
        sql = String.format(JRBIManifestationDataChecksSQL.GET_JRBI_MANIF_LATEST_RECORDS, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJRBIContext.recordsFromLAtestManif = DBManager.getDBResultAsBeanList(sql, JRBIDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of Manifestation Latest with addition of Delta_current_Manifestation and Manifestation_Exclude$")
    public void compareLatestManifRecords() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the EPR Ids to compare the records Manif Exclude...");
            for (int i = 0; i < dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.size(); i++) {

                dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.sort(Comparator.comparing(JRBIDLAccessObject::getEPR)); //sort data in the lists
                dataQualityJRBIContext.recordsFromLAtestManif.sort(Comparator.comparing(JRBIDLAccessObject::getEPR));

                String[] etl_curr_manifest = {"getEPR","getRECORD_TYPE","getMANIFESTATION_TYPE","getJOURNAL_ISSUE_TRIM_SIZE","getWAR_REFERENCE"};
                for (String strTemp : etl_curr_manifest) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    JRBIDLAccessObject objectToCompare1 = dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i);
                    JRBIDLAccessObject objectToCompare2 = dataQualityJRBIContext.recordsFromLAtestManif.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPR => " +  dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR() +
                            " " + strTemp + " => Delta_Exclde_Manif = " + method.invoke(objectToCompare1) +
                            " Latest_Manif = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifest_hsistory for EPR:"+dataQualityJRBIContext.recordsFromAddDeltaAndManifExclude.get(i).getEPR(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }
}

