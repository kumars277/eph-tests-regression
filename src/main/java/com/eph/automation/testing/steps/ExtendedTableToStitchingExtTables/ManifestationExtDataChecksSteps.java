package com.eph.automation.testing.steps.ExtendedTableToStitchingExtTables;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.StitchingExtContext;
import com.eph.automation.testing.models.dao.StitchingExtended.ManifExtPgCountJson;
import com.eph.automation.testing.models.dao.StitchingExtended.ManifExtRestrictionJson;
import com.eph.automation.testing.models.dao.StitchingExtended.ManifestationExtAccessObject;
import com.eph.automation.testing.models.dao.StitchingExtended.ManifExtJsonObject;
import com.eph.automation.testing.services.db.StitchingExtendedSQL.StitchingExtDataChecksSQL;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

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
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Manif Ext EPR Ids...");
        switch (tableName) {
            case "manifestation_extended":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_MANIF_EXTENDED,tableName, numberOfRecords);
                List<Map<?,?>> randomManifExtendedEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifExtendedEPRIds.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @And("^Records from Manifestation extended table$")
    public void getRecordsFromManifExtendedTable() {
        Log.info("We get the records from Manif Extended Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_MANIF_EXT_REC, Joiner.on("','").join(Ids));
       // Log.info(sql);
        dataQualityStitchContext.recordsFromManifExtended = DBManager.getDBResultAsBeanList(sql, ManifestationExtAccessObject.class, Constants.AWS_URL);
    }

    @And("^Records from manifestation summary table$")
    public void getRecFromManifSummaryTable() {
        Log.info("We get the records from Manif Summary for manf type Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_MANIF_SUMMARY_EXT_REC, Joiner.on("','").join(Ids));
        // Log.info(sql);
        dataQualityStitchContext.recordsFromManifExtSummary = DBManager.getDBResultAsBeanList(sql, ManifestationExtAccessObject.class, Constants.AWS_URL);
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
       // Log.info(sql);
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
                        " Manif_Extended -> manif_type => " + dataQualityStitchContext.recordsFromManifExtSummary.get(i).getmanifestation_type() +
                        " Manif_JSON -> Type => " + dataQualityStitchContext.recFromManifStitchExtended.get(0).gettype());
                if (dataQualityStitchContext.recordsFromManifExtSummary.get(i).getmanifestation_type() != null ||
                        (dataQualityStitchContext.recFromManifStitchExtended.get(i).gettype() != null)) {
                    Assert.assertEquals("The Manif_type for EPR => " + dataQualityStitchContext.recordsFromManifExtSummary.get(i).getepr_id() + " is missing/not matching",
                            dataQualityStitchContext.recordsFromManifExtSummary.get(i).getmanifestation_type(),
                            dataQualityStitchContext.recFromManifStitchExtended.get(0).gettype());
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
                String uk_textbook_ind;
                if(dataQualityStitchContext.recordsFromManifExtended.get(i).getuk_textbook_ind()!=null ||
                        !dataQualityStitchContext.recordsFromManifExtended.get(i).getuk_textbook_ind().isEmpty()){
                        if(dataQualityStitchContext.recordsFromManifExtended.get(i).getuk_textbook_ind()=="1"){
                            uk_textbook_ind ="true";
                        }else{
                            uk_textbook_ind="false";
                        }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> ukTextbookInd => " + uk_textbook_ind +
                        " Manif_JSON -> ukTextbookInd => " + dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getukTextbookInd());
                     Assert.assertEquals("The ukTextbookInd is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            uk_textbook_ind, dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getukTextbookInd());
                }

                String us_textbook_ind;
                if(dataQualityStitchContext.recordsFromManifExtended.get(i).getus_textbook_ind()!=null ||
                        !dataQualityStitchContext.recordsFromManifExtended.get(i).getus_textbook_ind().isEmpty()){
                    if(dataQualityStitchContext.recordsFromManifExtended.get(i).getus_textbook_ind()=="1")
                    {
                        us_textbook_ind ="true";
                    }else{
                        us_textbook_ind="false";
                    }
                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> usTextbookInd => " + us_textbook_ind +
                        " Manif_JSON -> usTextbookInd => " + dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getusTextbookInd());
                        Assert.assertEquals("The usTextbookInd is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            us_textbook_ind,dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getusTextbookInd());
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
                String exportWebInd;
                if(dataQualityStitchContext.recordsFromManifExtended.get(i).getexport_to_web_ind()!=null ||
                        !dataQualityStitchContext.recordsFromManifExtended.get(i).getexport_to_web_ind().isEmpty() ){
                   if(dataQualityStitchContext.recordsFromManifExtended.get(i).getexport_to_web_ind()=="1"){
                         exportWebInd ="true";
                   }else{
                        exportWebInd="false";
                    }
                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id() +
                        " Manif_Extended -> exportToWebInd => " + exportWebInd +
                        " Manif_JSON -> exportToWebInd => " + dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getexportToWebInd());
                    Assert.assertEquals("The exportToWebInd is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtended.get(i).getepr_id(),
                            exportWebInd,dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getexportToWebInd());
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

    @Given("^We get the (.*) random manifestation Ext page count EPR ids (.*)$")
    public void getRandomManifExtPgCountIds(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Manif Ext Page count EPR Ids...");
        switch (tableName) {
            case "manifestation_extended_page_count":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_MANIF_EXT_PAGE_COUNT,tableName, numberOfRecords);
                List<Map<?, ?>> randomManifExtendedEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifExtendedEPRIds.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());


                break;
        }
      //  Log.info(sql);
      Log.info(Ids.toString());
    }


    @Then("^Records from Manifestation extended page count table$")
    public void getRecordsFromManifExtPageCountTable() {
        Log.info("We get the records from Manif Extended Page count Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_MANIF_EXT_PAFE_COUNT_REC, Joiner.on("','").join(Ids));
      //  Log.info(sql);
        dataQualityStitchContext.recordsFromManifExtPageCount = DBManager.getDBResultAsBeanList(sql, ManifestationExtAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare Manif Extended page count and Manif Extended Stitching Table$")
    public void compareManifExtPageCountAndStitchingManifExt() {
        if (dataQualityStitchContext.recordsFromManifExtPageCount.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            //List <Integer> ignore = new ArrayList();
            for (int i = 0; i < dataQualityStitchContext.recordsFromManifExtPageCount.size(); i++) {
                String manifId = dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id();
                getManifExtendedJSONRec(manifId);
                getType(manifId);
                if (dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getManifestationExtendedPageCounts() != null) {
                    ManifExtPgCountJson[] extmanifPgCount_temp = dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getManifestationExtendedPageCounts().clone();
                    for (int j = 0; j < extmanifPgCount_temp.length; j++) {
                      //  if(ignore.contains(j)) continue;
                            String sourceCode = dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getcount_type_code();
                            String trgetCode =String.valueOf(extmanifPgCount_temp[j].getExtendedPageCount().getType().get("code"));
                            if(sourceCode.equals(trgetCode)){
                                Log.info("Manif_Ext_PageCount -> EPR => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id() +
                                        " Manif_JSON -> EPR => " + dataQualityStitchContext.recordsFromManifStitching.getId());
                                if (dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id() != null ||
                                        (dataQualityStitchContext.recordsFromManifStitching.getId() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id() + " is missing/not found in Manif_Stitching table",
                                            dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromManifStitching.getId());
                                }
                                Log.info(" EPR => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id() +
                                        " Manif_Ext_PageCount -> manif_type => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getmanifestation_type() +
                                        " Manif_JSON -> Type => " + dataQualityStitchContext.recFromManifStitchExtended.get(0).gettype());
                                if (dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getmanifestation_type() != null ||
                                        (dataQualityStitchContext.recFromManifStitchExtended.get(0).gettype() != null)) {
                                    Assert.assertEquals("The manif_type for EPR => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id() + " is missing/not matching in Manif_Stitching table",
                                            dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getmanifestation_type(),
                                            dataQualityStitchContext.recFromManifStitchExtended.get(0).gettype());
                                }
                                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id() +
                                        " Manif_Ext_PageCount -> Country_Code => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getcount_type_code() +
                                        " Manif_JSON -> Country_Code => " + extmanifPgCount_temp[j].getExtendedPageCount().getType().get("code"));
                                Assert.assertEquals("The CountryCode is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getcount_type_code(),
                                        extmanifPgCount_temp[j].getExtendedPageCount().getType().get("code"));

                               Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id() +
                                       " Manif_Ext_PageCount -> Country_name => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getcount_type_name() +
                                       " Manif_JSON -> Country_name => " + extmanifPgCount_temp[j].getExtendedPageCount().getType().get("name"));
                               Assert.assertEquals("The CountryName is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id(),
                                       dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getcount_type_name(),
                                       extmanifPgCount_temp[j].getExtendedPageCount().getType().get("name"));

                               Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id() +
                                       " Manif_Ext_PageCount -> page Count => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getcount() +
                                       " Manif_JSON -> page Count => " + extmanifPgCount_temp[j].getExtendedPageCount().getCount());
                               Assert.assertEquals("The page Count is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getepr_id(),
                                       dataQualityStitchContext.recordsFromManifExtPageCount.get(i).getcount(),
                                       extmanifPgCount_temp[j].getExtendedPageCount().getCount());
                                j=0;
                               // ignore.add(j);
                                break;
                            }else{
                                j= j++;
                            }
                       // }

                    }
                }
            }
        }
    }


    @Given("^We get the (.*) random manifestation Ext restrictions EPR ids (.*)$")
    public void getRandomManifExtRestrictIds(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Manif Ext Restrict count EPR Ids...");
        switch (tableName) {
            case "manifestation_extended_restriction":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_MANIF_EXT_RESTRICT_COUNT, numberOfRecords);
                List<Map<?, ?>> randomManifExtendedEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifExtendedEPRIds.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());
                break;

        }
       // Log.info(sql);
        Log.info(Ids.toString());
    }

    @Then("^Records from Manifestation extended restrictions table$")
    public void getRecordsFromManifExtRestrictTable() {
        Log.info("We get the records from Manif Extended Restrict Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_MANIF_EXT_RESTRICT_REC, Joiner.on("','").join(Ids));
       // Log.info(sql);
        dataQualityStitchContext.recordsFromManifExtRestrict = DBManager.getDBResultAsBeanList(sql, ManifestationExtAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare Manif Extended restrictions and Manif Extended Stitching Table$")
    public void compareManifExtRestrictAndStitchingManifExt() {
        if (dataQualityStitchContext.recordsFromManifExtRestrict.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
          //  List <Integer> ignore = new ArrayList();
            for (int i = 0; i < dataQualityStitchContext.recordsFromManifExtRestrict.size(); i++) {
                String manifId = dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getepr_id();
                getManifExtendedJSONRec(manifId);
                getType(manifId);
                if (dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getManifestationExtendedRestrictions() != null) {
                    ManifExtRestrictionJson[] extmanifRestrict_temp = dataQualityStitchContext.recordsFromManifStitching.getManifestationExtended().getManifestationExtendedRestrictions().clone();
                    for (int j = 0; j < extmanifRestrict_temp.length; j++) {
                    //    if(ignore.contains(j)) continue;
                          String sourceCode = dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getrestriction_code();
                            String trgetCode =String.valueOf(extmanifRestrict_temp[j].getExtendedRestriction().getType().get("code"));
                            if(sourceCode.equals(trgetCode)){
                                Log.info("Manif_Ext_Restrict -> EPR => " + dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getepr_id() +
                                        " Manif_JSON_Restrict -> EPR => " + dataQualityStitchContext.recordsFromManifStitching.getId());
                                if (dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getepr_id() != null ||
                                        (dataQualityStitchContext.recordsFromManifStitching.getId() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getepr_id() + " is missing/not found in Manif_Stitching table",
                                            dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromManifStitching.getId());
                                }
                                Log.info(" EPR => " + dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getepr_id() +
                                        " Manif_Ext_Restrict -> manif_type => " + dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getmanifestation_type() +
                                        " Manif_JSON_Restrict -> Type => " + dataQualityStitchContext.recFromManifStitchExtended.get(0).gettype());
                                if (dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getmanifestation_type() != null ||
                                        (dataQualityStitchContext.recFromManifStitchExtended.get(0).gettype() != null)) {
                                    Assert.assertEquals("The manif_type for EPR => " + dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getepr_id() + " is missing/not matching in Manif_Stitching table",
                                            dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getmanifestation_type(),
                                            dataQualityStitchContext.recFromManifStitchExtended.get(0).gettype());
                                }

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getepr_id() +
                                        " Manif_Ext_Restrict -> Restrict_COde => " + dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getrestriction_code() +
                                        " Manif_JSON -> Restrict_COde => " + extmanifRestrict_temp[j].getExtendedRestriction().getType().get("code"));
                                Assert.assertEquals("The Restrict_COde is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getrestriction_code(),
                                        extmanifRestrict_temp[j].getExtendedRestriction().getType().get("code"));

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getepr_id() +
                                        " Manif_Ext_Restrict -> Restrict_name => " + dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getrestriction_name() +
                                        " Manif_JSON -> Restrict_name => " + extmanifRestrict_temp[j].getExtendedRestriction().getType().get("name"));
                                Assert.assertEquals("The Restrict_name is incorrect for EPR => " + dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromManifExtRestrict.get(i).getrestriction_name(),
                                        extmanifRestrict_temp[j].getExtendedRestriction().getType().get("name"));
                                j=0;
                              //  ignore.add(j);
                                break;
                            }else{
                                j= j++;
                            }
                       // }

                    }
                }
            }
        }
    }


}
