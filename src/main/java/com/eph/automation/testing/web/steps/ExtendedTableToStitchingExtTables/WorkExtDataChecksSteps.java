package com.eph.automation.testing.web.steps.ExtendedTableToStitchingExtTables;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.StitchingExtContext;
import com.eph.automation.testing.models.dao.StitchingExtended.WorkExtJsonObject;
import com.eph.automation.testing.models.dao.StitchingExtended.WorkExtAccessObject;
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


public class WorkExtDataChecksSteps {

    public StitchingExtContext dataQualityStitchContext;
    private static String sql;
    private static List<String> Ids;
    private StitchingExtDataChecksSQL StchObj = new StitchingExtDataChecksSQL();
    private static String WorkExtTableSQLSourceCount,WorkExtStitchingSQLSourceCount;
    private static int WorkExtTableSourceCount,WorkExtStitchingSourceCount;

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");


    @Given ("^We get the counts from the WorkExtended Table$")
    public void getCountWorkExtTable(){
        Log.info("Getting Count for Work extended...");
        WorkExtTableSQLSourceCount = StitchingExtDataChecksSQL.GET_COUNT_WORK_EXT_TABLE;
        Log.info(WorkExtTableSQLSourceCount);
        List<Map<String, Object>> WorkExtTableCount = DBManager.getDBResultMap(WorkExtTableSQLSourceCount, Constants.AWS_URL);
        WorkExtTableSourceCount = ((Long) WorkExtTableCount.get(0).get("Source_count")).intValue();
    }

    @And ("^Get the counts from Work Stitching Table$")
    public void getCountWorkStitchingTable(){
        Log.info("Getting Count for Work extended...");
        WorkExtStitchingSQLSourceCount = StitchingExtDataChecksSQL.GET_COUNT_WORK_EXT_STITCH_TABLE;
        Log.info(WorkExtStitchingSQLSourceCount);
        List<Map<String, Object>> WorkExtStitchingTableCount = DBManager.getDBResultMap(WorkExtStitchingSQLSourceCount, Constants.EPH_URL);
        WorkExtStitchingSourceCount = ((Long) WorkExtStitchingTableCount.get(0).get("Target_count")).intValue();
    }

    @Then ("^Compare work Extended and work Extended Stitching Table Counts$")
    public void compareCountWorkExtAndWorkStitchingExt(){
        Log.info("The count for table work_extended table => " + WorkExtTableSourceCount + " and in Work Ext Stitching => " + WorkExtStitchingSourceCount);
        Assert.assertEquals("The counts are not equal when compared with work_ext table and work_ext_stitch", WorkExtTableSourceCount,WorkExtStitchingSourceCount );
    }

    @Given("^We get the (.*) random work Ext EPR ids (.*)$")
    public void getRandomManifExtEPRIds(String numberOfRecords, String tableName) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Work Ext EPR Ids...");
        switch (tableName) {
            case "work_extended":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_WORK_EXTENDED, numberOfRecords);
                List<Map<?, ?>> randomWorkExtendedEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkExtendedEPRIds.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @Then("^Get the records from Work extended table$")
    public void getRecordsFromWorkExtendedTable() {
        Log.info("We get the records from Work Extended Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_REC, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityStitchContext.recordsFromWorkExtended = DBManager.getDBResultAsBeanList(sql, WorkExtAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from Work extended stitching table$")
    public void getWorkExtendedJSONRec(String workId) {
        Log.info("We get the JSON from Work Ext Stitching...");
        Log.info(workId);
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_JSON_REC, workId);
        Log.info(sql);
        List<Map<String, String>> jsonValue = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        StitchingExtContext.recordsFromWorkStitching = new Gson().fromJson(jsonValue.get(0).get("json"), WorkExtJsonObject.class);
    }

    public void getWorkType(String workId){
        Log.info("We get the type from Work Stitching Extended Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_JSON_REC, workId);
        Log.info(sql);
        dataQualityStitchContext.recFromWorkTypeStitchExtended = DBManager.getDBResultAsBeanList(sql, WorkExtAccessObject.class, Constants.EPH_URL);
    }

    @And("^Compare work Extended and work Extended Stitching Table$")
    public void compareWorkExtendedAndStitchingWorkExt() {
        if (dataQualityStitchContext.recordsFromWorkExtended.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < dataQualityStitchContext.recordsFromWorkExtended.size(); i++) {
                String workId = dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id();
                getWorkExtendedJSONRec(workId);
                getWorkType(workId);
                Log.info("Work_Extended -> EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() + " is missing/not found in Work_Stitching table",
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkStitching.getId());
                }

                Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> work_type => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getwork_type() +
                        " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(i).gettype());
                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getwork_type() != null ||
                        (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(i).gettype() != null)) {
                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() + " is missing/not found in Work_Stitching table",
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getwork_type(),
                            dataQualityStitchContext.recFromWorkTypeStitchExtended.get(i).gettype());
                }
                //journalElsComInd not passing in to the stitching json if the value is false, it seems like only true and null no false value holding in DB.
               String journalElsComInd_workExtTable;
                if(dataQualityStitchContext.recordsFromWorkExtended.get(i).getjournal_els_com_ind()){
                    journalElsComInd_workExtTable = "true";
                }else{
                    //journalElsComInd_workExtTable = "false";
                    journalElsComInd_workExtTable = null;
                }
                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> journalElsComInd => " + journalElsComInd_workExtTable +
                        " Work_JSON -> journalElsComInd => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalElsComInd());

                if (journalElsComInd_workExtTable!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalElsComInd() != null)) {
                    Assert.assertEquals("The journalElsComInd is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            journalElsComInd_workExtTable,
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalElsComInd());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> journs_aims_scope => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope() +
                        " Work_JSON -> journs_aims_scope => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope() != null)) {
                    Assert.assertEquals("The journs_aims_scope is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> imageFileRef => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getimage_file_ref() +
                        " Work_JSON -> imageFileRef => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getimageFileRef());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getimage_file_ref()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getimageFileRef() != null)) {
                    Assert.assertEquals("The imageFileRef is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getimage_file_ref(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getimageFileRef());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> master_isbn => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn() +
                        " Work_JSON -> master_isbn => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN() != null)) {
                    Assert.assertEquals("The master_isbn is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> authorByLineText => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text() +
                        " Work_JSON -> authorByLineText => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText() != null)) {
                    Assert.assertEquals("The authorByLineText is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> bookSubBusinessUnit => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getbook_sub_business_unit() +
                        " Work_JSON -> bookSubBusinessUnit => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getbookSubBusinessUnit());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getbook_sub_business_unit()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getbookSubBusinessUnit() != null)) {
                    Assert.assertEquals("The bookSubBusinessUnit is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getbook_sub_business_unit(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getbookSubBusinessUnit());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> rfLvi => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_lvi() +
                        " Work_JSON -> rfLvi => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfLvi());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_lvi()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfLvi() != null)) {
                    Assert.assertEquals("The rfLvi is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_lvi(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfLvi());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> business_unit_desc => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getbusiness_unit_desc() +
                        " Work_JSON -> business_unit_desc => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getptsBusinessUnitDesc());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getbusiness_unit_desc()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getptsBusinessUnitDesc() != null)) {
                    Assert.assertEquals("The business_unit_desc is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getbusiness_unit_desc(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getptsBusinessUnitDesc());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> rfFvi => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_fvi() +
                        " Work_JSON -> rfFvi => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfFvi());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_fvi()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfFvi() != null)) {
                    Assert.assertEquals("The rfFvi is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_fvi(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfFvi());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> rfTotalPagesQty => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_total_pages_qty() +
                        " Work_JSON -> rfTotalPagesQty => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfTotalPagesQty());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_total_pages_qty()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfTotalPagesQty() != null)) {
                    Assert.assertEquals("The rfTotalPagesQty is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_total_pages_qty(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfTotalPagesQty());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> rf_issues_qty => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_issues_qty() +
                        " Work_JSON -> rf_issues_qty => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfIssuesQty());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_issues_qty()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfIssuesQty() != null)) {
                    Assert.assertEquals("The rf_issues_qty is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_issues_qty(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfIssuesQty());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> catalogueVolumeTo => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volume_to() +
                        " Work_JSON -> catalogueVolumeTo => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumeTo());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volume_to()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumeTo() != null)) {
                    Assert.assertEquals("The catalogueVolumeTo is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volume_to(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumeTo());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> catalogueIssuesQty    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_issues_qty() +
                        " Work_JSON -> catalogueIssuesQty => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueIssuesQty());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_issues_qty()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueIssuesQty() != null)) {
                    Assert.assertEquals("The catalogueIssuesQty is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_issues_qty(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueIssuesQty());
                }
                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> catalogueVolumesQty    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volumes_qty() +
                        " Work_JSON -> catalogueVolumesQty => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumesQty());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volumes_qty()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumesQty() != null)) {
                    Assert.assertEquals("The catalogueVolumesQty is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volumes_qty(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumesQty());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> issue_prod_type_code    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getissue_prod_type_code() +
                        " Work_JSON -> issue_prod_type_code => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getissueProdTypeCode());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getissue_prod_type_code()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getissueProdTypeCode() != null)) {
                    Assert.assertEquals("The issue_prod_type_code is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getissue_prod_type_code(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getissueProdTypeCode());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> primarySiteSupportLevel    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_support_level() +
                        " Work_JSON -> primarySiteSupportLevel => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSupportLevel());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_support_level()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSupportLevel() != null)) {
                    Assert.assertEquals("The primarySiteSupportLevel is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_support_level(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSupportLevel());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> primarySiteAcronym    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_acronym() +
                        " Work_JSON -> primarySiteAcronym => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteAcronym());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_acronym()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteAcronym() != null)) {
                    Assert.assertEquals("The primarySiteAcronym is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_acronym(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteAcronym());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> primarySiteSystem    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_system() +
                        " Work_JSON -> primarySiteSystem => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSystem());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_system()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSystem() != null)) {
                    Assert.assertEquals("The primarySiteSystem is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_system(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSystem());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> journalProdSiteCode    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getjournal_prod_site_code() +
                        " Work_JSON -> journalProdSiteCode => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalProdSiteCode());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getjournal_prod_site_code()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalProdSiteCode() != null)) {
                    Assert.assertEquals("The journalProdSiteCode is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getjournal_prod_site_code(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalProdSiteCode());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> journs_aims_scope    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope() +
                        " Work_JSON -> journs_aims_scope => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope() != null)) {
                    Assert.assertEquals("The journs_aims_scope is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> profitCentre    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getprofit_centre() +
                        " Work_JSON -> profitCentre => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprofitCentre());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getprofit_centre()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprofitCentre() != null)) {
                    Assert.assertEquals("The profitCentre is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getprofit_centre(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprofitCentre());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> text_ref_trade    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade() +
                        " Work_JSON -> text_ref_trade => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade() != null)) {
                    Assert.assertEquals("The text_ref_trade is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> text_ref_trade    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade() +
                        " Work_JSON -> text_ref_trade => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade() != null)) {
                    Assert.assertEquals("The text_ref_trade is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> internalElsDiv    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getinternal_els_div() +
                        " Work_JSON -> internalElsDiv => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getinternalElsDiv());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getinternal_els_div()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getinternalElsDiv() != null)) {
                    Assert.assertEquals("The internalElsDiv is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getinternal_els_div(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getinternalElsDiv());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> audienceText    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getaudience_text() +
                        " Work_JSON -> audienceText => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getaudienceText());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getaudience_text()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getaudienceText() != null)) {
                    Assert.assertEquals("The audienceText is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getaudience_text(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getaudienceText());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> tocShort    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_short() +
                        " Work_JSON -> tocShort => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocShort());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_short()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocShort() != null)) {
                    Assert.assertEquals("The tocShort is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_short(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocShort());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> tocLong    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_long() +
                        " Work_JSON -> tocLong => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocLong());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_long()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocLong() != null)) {
                    Assert.assertEquals("The tocLong is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_long(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocLong());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> productLongDesc    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getproduct_long_desc() +
                        " Work_JSON -> productLongDesc => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getproductLongDesc());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getproduct_long_desc()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getproductLongDesc() != null)) {
                    Assert.assertEquals("The productLongDesc is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getproduct_long_desc(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getproductLongDesc());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> keyFeatures    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getkey_features() +
                        " Work_JSON -> keyFeatures => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getkeyFeatures());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getkey_features()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getkeyFeatures() != null)) {
                    Assert.assertEquals("The keyFeatures is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getkey_features(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getkeyFeatures());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> authorByLineText    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text() +
                        " Work_JSON -> authorByLineText => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText() != null)) {
                    Assert.assertEquals("The authorByLineText is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> masterISBN    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn() +
                        " Work_JSON -> masterISBN => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn()!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN() != null)) {
                    Assert.assertEquals("The masterISBN is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN());
                }

            }

        }

    }


}