package com.eph.automation.testing.web.steps.ExtendedTableToStitchingExtTables;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.StitchingExtContext;
import com.eph.automation.testing.models.dao.StitchingExtended.ManifestationExtAccessObject;
import com.eph.automation.testing.models.dao.StitchingExtended.ManifExtJsonObject;
import com.eph.automation.testing.services.db.StitchingExtendedSQL.StitchingExtDataChecksSQL;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class ManifestationExtDataChecksSteps {

    public StitchingExtContext dataQualityStitchContext;
    private static String sql;
    private static List<String> Ids;
    private StitchingExtDataChecksSQL StchObj = new StitchingExtDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");



    @Given("^We get the (.*) random manifestation Ext EPR ids (.*)$")
    public void getRandomManifExtEPRIds(String numberOfRecords, String tableName) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Manif Ext EPR Ids...");
        switch (tableName) {
            case "manifestation_extended":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_MANIF_EXTENDED, numberOfRecords);
                List<Map<?, ?>> randomManifExtendedEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifExtendedEPRIds.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @Then("^Get the records from Manifestation extended table$")
    public void getRecordsFromManifExtendedTable() {
        Log.info("We get the records from Manif Extended Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_MANIF_EXT_REC, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityStitchContext.recordsFromManifExtended = DBManager.getDBResultAsBeanList(sql, ManifestationExtAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from Manifestation extended stitching table$")
    public void getManifExtendedJSONRec(String manifId) {
        Log.info("We get the JSON from Manifestation Ext Stitching...");
        Log.info(manifId);
        sql = String.format(StitchingExtDataChecksSQL.GET_MANIF_EXT_JSON_REC, manifId);
        Log.info(sql);
        List<Map<String, String>> jsonValue = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        StitchingExtContext.recordsFromManifStitching = new Gson().fromJson(jsonValue.get(0).get("json"), ManifExtJsonObject.class);
    }

    public void getType(String manifId){
        Log.info("We get the type from Manif Stitching Extended Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_MANIF_EXT_JSON_REC, manifId);
        Log.info(sql);
        dataQualityStitchContext.recFromManifStitchExtended = DBManager.getDBResultAsBeanList(sql, ManifestationExtAccessObject.class, Constants.EPH_URL);
    }

    @And("^Compare Manif Extended and Manif Extended Stitching Table$")
    public void compareManifExtendedAndStitchingManifExt() {
        if (dataQualityStitchContext.recordsFromManifExtended.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < dataQualityStitchContext.recordsFromManifExtended.size(); i++) {
                String manifId = dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id();
                getManifExtendedJSONRec(manifId);
                getType(manifId);
                Log.info("Manif_Extended -> EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_JSON -> EPR => " + dataQualityStitchContext.recordsFromManifStitching.getId());
                if (dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() != null ||
                        (dataQualityStitchContext.recordsFromManifStitching.getId() != null)) {
                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() + " is missing/not found in Manif_Stitching table",
                            dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromManifStitching.getId());
                }

                Log.info(" EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> manif_type => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getmanifestation_type() +
                        " Manif_JSON -> Type => " + dataQualityStitchContext.recFromManifStitchExtended.get(i).gettype());
                if (dataQualityStitchContext.recordsFromManifExtended.get(i).getmanifestation_type() != null ||
                        (dataQualityStitchContext.recFromManifStitchExtended.get(i).gettype() != null)) {
                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() + " is missing/not found in Manif_Stitching table",
                            dataQualityStitchContext.recordsFromManifExtended.get(i).getmanifestation_type(),
                            dataQualityStitchContext.recFromManifStitchExtended.get(i).gettype());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> journalIssueTrimSize => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getjournal_issue_trim_size() +
                        " Manif_JSON -> journalIssueTrimSize => " + dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getjournalIssueTrimSize());
                if (dataQualityStitchContext.recordsFromManifExtended.get(i).getjournal_issue_trim_size() != null ||
                        (dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getjournalIssueTrimSize() != null)) {
                    Assert.assertEquals("The journalIssueTrimSize is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromManifExtended.get(i).getjournal_issue_trim_size(),
                            dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getjournalIssueTrimSize());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> journalIssueTrimSize => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getjournal_issue_trim_size() +
                        " Manif_JSON -> journalIssueTrimSize => " + dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getjournalIssueTrimSize());
                if (dataQualityStitchContext.recordsFromManifExtended.get(i).getjournal_issue_trim_size() != null ||
                        (dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getjournalIssueTrimSize() != null)) {
                    Assert.assertEquals("The journalIssueTrimSize is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromManifExtended.get(i).getjournal_issue_trim_size(),
                            dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getjournalIssueTrimSize());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> ukTextbookInd => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getuk_textbook_ind() +
                        " Manif_JSON -> ukTextbookInd => " + dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getukTextbookInd());
                if (dataQualityStitchContext.recordsFromManifExtended.get(i).getuk_textbook_ind() != null ||
                        (dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getukTextbookInd() != null)) {
                    Assert.assertEquals("The ukTextbookInd is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromManifExtended.get(i).getuk_textbook_ind(),
                            dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getukTextbookInd());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> usTextbookInd => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getus_textbook_ind() +
                        " Manif_JSON -> usTextbookInd => " + dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getusTextbookInd());
                if (dataQualityStitchContext.recordsFromManifExtended.get(i).getus_textbook_ind() != null ||
                        (dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getusTextbookInd() != null)) {
                    Assert.assertEquals("The usTextbookInd is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromManifExtended.get(i).getus_textbook_ind(),
                            dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getusTextbookInd());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> commodityCode => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getcommodity_code() +
                        " Manif_JSON -> commodityCode => " + dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getcommodityCode());
                if (dataQualityStitchContext.recordsFromManifExtended.get(i).getcommodity_code() != null ||
                        (dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getcommodityCode() != null)) {
                    Assert.assertEquals("The commodityCode is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromManifExtended.get(i).getcommodity_code(),
                            dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getcommodityCode());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> discount_code_emea => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getdiscount_code_emea() +
                        " Manif_JSON -> discount_code_emea => " + dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getdiscountCodeEMEA());
                if (dataQualityStitchContext.recordsFromManifExtended.get(i).getdiscount_code_emea() != null ||
                        (dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getdiscountCodeEMEA() != null)) {
                    Assert.assertEquals("The discount_code_emea is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromManifExtended.get(i).getdiscount_code_emea(),
                            dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getdiscountCodeEMEA());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> discountCodeUS => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getdiscount_code_us() +
                        " Manif_JSON -> discountCodeUS => " + dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getdiscountCodeUS());
                if (dataQualityStitchContext.recordsFromManifExtended.get(i).getdiscount_code_us() != null ||
                        (dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getdiscountCodeUS() != null)) {
                    Assert.assertEquals("The discountCodeUS is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromManifExtended.get(i).getdiscount_code_us(),
                            dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getdiscountCodeUS());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> war_reference => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getwar_reference() +
                        " Manif_JSON -> war_reference => " + dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getWarReference());
                if (dataQualityStitchContext.recordsFromManifExtended.get(i).getwar_reference() != null ||
                        (dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getWarReference() != null)) {
                    Assert.assertEquals("The war_reference is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromManifExtended.get(i).getwar_reference(),
                            dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getWarReference());
                }
            }

        }

    }

}

