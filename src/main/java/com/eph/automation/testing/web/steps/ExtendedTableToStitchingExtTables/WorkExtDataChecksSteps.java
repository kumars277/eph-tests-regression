package com.eph.automation.testing.web.steps.ExtendedTableToStitchingExtTables;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.StitchingExtContext;
import com.eph.automation.testing.models.dao.StitchingExtended.*;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.StitchingExtendedSQL.StitchingExtDataChecksSQL;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class WorkExtDataChecksSteps {

    public StitchingExtContext dataQualityStitchContext;
    private static String sql;
    private static List<String> Ids;
    private StitchingExtDataChecksSQL StchObj = new StitchingExtDataChecksSQL();
    private static String WorkExtTableSQLSourceCount, WorkExtStitchingSQLSourceCount;
    private static int WorkExtTableSourceCount, WorkExtStitchingSourceCount;

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");


    @Given("^We get the counts from the WorkExtended Table$")
    public void getCountWorkExtTable() {
        Log.info("Getting Count for Work extended...");
        WorkExtTableSQLSourceCount = StitchingExtDataChecksSQL.GET_COUNT_WORK_EXT_TABLE;
        Log.info(WorkExtTableSQLSourceCount);
        List<Map<String, Object>> WorkExtTableCount = DBManager.getDBResultMap(WorkExtTableSQLSourceCount, Constants.AWS_URL);
        WorkExtTableSourceCount = ((Long) WorkExtTableCount.get(0).get("Source_count")).intValue();
    }

    @And("^Get the counts from Work Stitching Table$")
    public void getCountWorkStitchingTable() {
        Log.info("Getting Count for Work extended...");
        WorkExtStitchingSQLSourceCount = StitchingExtDataChecksSQL.GET_COUNT_WORK_EXT_STITCH_TABLE;
        Log.info(WorkExtStitchingSQLSourceCount);
        List<Map<String, Object>> WorkExtStitchingTableCount = DBManager.getDBResultMap(WorkExtStitchingSQLSourceCount, Constants.EPH_URL);
        WorkExtStitchingSourceCount = ((Long) WorkExtStitchingTableCount.get(0).get("Target_count")).intValue();
    }

    @Then("^Compare work Extended and work Extended Stitching Table Counts$")
    public void compareCountWorkExtAndWorkStitchingExt() {
        Log.info("The count for table work_extended table => " + WorkExtTableSourceCount + " and in Work Ext Stitching => " + WorkExtStitchingSourceCount);
        Assert.assertEquals("The counts are not equal when compared with work_ext table and work_ext_stitch", WorkExtTableSourceCount, WorkExtStitchingSourceCount);
    }

    @Given("^We get the (.*) random work EPR ids (.*)$")
    public void getRandomWorkExtEPRIds(String numberOfRecords, String tableName) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Work Ext EPR Ids...");
        switch (tableName) {
            case "work_extended":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_WORK_EXTENDED, numberOfRecords);
                break;
            case "work_extended_metric":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_WORK_EXT_METRIC, numberOfRecords);
                break;
            case "work_extended_url":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_WORK_EXT_URL, numberOfRecords);
                break;
            case "work_extended_subject_area":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_WORK_EXT_SUBJ_AREA, numberOfRecords);
                break;
            case "work_extended_person_role":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_WORK_EXT_PERSON_ROLE, numberOfRecords);
                break;
            case "work_extended_relationship_sibling":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_WORK_EXT_RELATIONSHIP_SIBLING, numberOfRecords);
                break;
            case "work_extended_editorial_board":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_WORK_EXT_EDITORIAL_BOARD, numberOfRecords);
                break;
        }
        List<Map<?, ?>> randomWorkExtendedEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomWorkExtendedEPRIds.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());
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

    public void getWorkType(String workId) {
        Log.info("We get the type from Work Stitching Extended Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_JSON_REC, workId);
        //Log.info(sql);
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
                        " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getwork_type() != null ||
                        (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype() != null)) {
                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() + " is missing/not found in Work_Stitching table",
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getwork_type(),
                            dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                }
                //journalElsComInd not passing in to the stitching json if the value is false, it seems like only true and null no false value holding in DB.
                String journalElsComInd_workExtTable;
                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getjournal_els_com_ind()) {
                    journalElsComInd_workExtTable = "true";
                } else {
                    //journalElsComInd_workExtTable = "false";
                    journalElsComInd_workExtTable = null;
                }
                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> journalElsComInd => " + journalElsComInd_workExtTable +
                        " Work_JSON -> journalElsComInd => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalElsComInd());

                if (journalElsComInd_workExtTable != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalElsComInd() != null)) {
                    Assert.assertEquals("The journalElsComInd is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            journalElsComInd_workExtTable,
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalElsComInd());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> journs_aims_scope => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope() +
                        " Work_JSON -> journs_aims_scope => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope() != null)) {
                    Assert.assertEquals("The journs_aims_scope is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> imageFileRef => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getimage_file_ref() +
                        " Work_JSON -> imageFileRef => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getimageFileRef());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getimage_file_ref() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getimageFileRef() != null)) {
                    Assert.assertEquals("The imageFileRef is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getimage_file_ref(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getimageFileRef());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> master_isbn => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn() +
                        " Work_JSON -> master_isbn => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN() != null)) {
                    Assert.assertEquals("The master_isbn is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> authorByLineText => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text() +
                        " Work_JSON -> authorByLineText => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText() != null)) {
                    Assert.assertEquals("The authorByLineText is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> bookSubBusinessUnit => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getbook_sub_business_unit() +
                        " Work_JSON -> bookSubBusinessUnit => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getbookSubBusinessUnit());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getbook_sub_business_unit() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getbookSubBusinessUnit() != null)) {
                    Assert.assertEquals("The bookSubBusinessUnit is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getbook_sub_business_unit(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getbookSubBusinessUnit());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> rfLvi => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_lvi() +
                        " Work_JSON -> rfLvi => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfLvi());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_lvi() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfLvi() != null)) {
                    Assert.assertEquals("The rfLvi is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_lvi(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfLvi());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> business_unit_desc => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getbusiness_unit_desc() +
                        " Work_JSON -> business_unit_desc => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getptsBusinessUnitDesc());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getbusiness_unit_desc() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getptsBusinessUnitDesc() != null)) {
                    Assert.assertEquals("The business_unit_desc is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getbusiness_unit_desc(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getptsBusinessUnitDesc());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> rfFvi => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_fvi() +
                        " Work_JSON -> rfFvi => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfFvi());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_fvi() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfFvi() != null)) {
                    Assert.assertEquals("The rfFvi is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_fvi(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfFvi());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> rfTotalPagesQty => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_total_pages_qty() +
                        " Work_JSON -> rfTotalPagesQty => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfTotalPagesQty());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_total_pages_qty() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfTotalPagesQty() != null)) {
                    Assert.assertEquals("The rfTotalPagesQty is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_total_pages_qty(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfTotalPagesQty());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> rf_issues_qty => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_issues_qty() +
                        " Work_JSON -> rf_issues_qty => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfIssuesQty());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_issues_qty() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfIssuesQty() != null)) {
                    Assert.assertEquals("The rf_issues_qty is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getrf_issues_qty(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getrfIssuesQty());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> catalogueVolumeTo => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volume_to() +
                        " Work_JSON -> catalogueVolumeTo => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumeTo());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volume_to() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumeTo() != null)) {
                    Assert.assertEquals("The catalogueVolumeTo is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volume_to(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumeTo());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> catalogueIssuesQty    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_issues_qty() +
                        " Work_JSON -> catalogueIssuesQty => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueIssuesQty());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_issues_qty() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueIssuesQty() != null)) {
                    Assert.assertEquals("The catalogueIssuesQty is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_issues_qty(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueIssuesQty());
                }
                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> catalogueVolumesQty    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volumes_qty() +
                        " Work_JSON -> catalogueVolumesQty => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumesQty());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volumes_qty() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumesQty() != null)) {
                    Assert.assertEquals("The catalogueVolumesQty is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getcatalogue_volumes_qty(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getcatalogueVolumesQty());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> issue_prod_type_code    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getissue_prod_type_code() +
                        " Work_JSON -> issue_prod_type_code => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getissueProdTypeCode());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getissue_prod_type_code() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getissueProdTypeCode() != null)) {
                    Assert.assertEquals("The issue_prod_type_code is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getissue_prod_type_code(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getissueProdTypeCode());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> primarySiteSupportLevel    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_support_level() +
                        " Work_JSON -> primarySiteSupportLevel => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSupportLevel());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_support_level() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSupportLevel() != null)) {
                    Assert.assertEquals("The primarySiteSupportLevel is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_support_level(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSupportLevel());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> primarySiteAcronym    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_acronym() +
                        " Work_JSON -> primarySiteAcronym => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteAcronym());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_acronym() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteAcronym() != null)) {
                    Assert.assertEquals("The primarySiteAcronym is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_acronym(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteAcronym());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> primarySiteSystem    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_system() +
                        " Work_JSON -> primarySiteSystem => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSystem());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_system() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSystem() != null)) {
                    Assert.assertEquals("The primarySiteSystem is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getprimary_site_system(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprimarySiteSystem());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> journalProdSiteCode    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getjournal_prod_site_code() +
                        " Work_JSON -> journalProdSiteCode => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalProdSiteCode());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getjournal_prod_site_code() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalProdSiteCode() != null)) {
                    Assert.assertEquals("The journalProdSiteCode is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getjournal_prod_site_code(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalProdSiteCode());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> journs_aims_scope    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope() +
                        " Work_JSON -> journs_aims_scope => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope() != null)) {
                    Assert.assertEquals("The journs_aims_scope is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> profitCentre    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getprofit_centre() +
                        " Work_JSON -> profitCentre => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprofitCentre());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getprofit_centre() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprofitCentre() != null)) {
                    Assert.assertEquals("The profitCentre is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getprofit_centre(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getprofitCentre());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> text_ref_trade    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade() +
                        " Work_JSON -> text_ref_trade => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade() != null)) {
                    Assert.assertEquals("The text_ref_trade is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> text_ref_trade    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade() +
                        " Work_JSON -> text_ref_trade => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade() != null)) {
                    Assert.assertEquals("The text_ref_trade is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).gettext_ref_trade(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettextRefTrade());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> internalElsDiv    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getinternal_els_div() +
                        " Work_JSON -> internalElsDiv => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getinternalElsDiv());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getinternal_els_div() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getinternalElsDiv() != null)) {
                    Assert.assertEquals("The internalElsDiv is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getinternal_els_div(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getinternalElsDiv());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> audienceText    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getaudience_text() +
                        " Work_JSON -> audienceText => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getaudienceText());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getaudience_text() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getaudienceText() != null)) {
                    Assert.assertEquals("The audienceText is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getaudience_text(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getaudienceText());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> tocShort    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_short() +
                        " Work_JSON -> tocShort => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocShort());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_short() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocShort() != null)) {
                    Assert.assertEquals("The tocShort is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_short(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocShort());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> tocLong    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_long() +
                        " Work_JSON -> tocLong => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocLong());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_long() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocLong() != null)) {
                    Assert.assertEquals("The tocLong is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).gettoc_long(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().gettocLong());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> productLongDesc    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getproduct_long_desc() +
                        " Work_JSON -> productLongDesc => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getproductLongDesc());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getproduct_long_desc() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getproductLongDesc() != null)) {
                    Assert.assertEquals("The productLongDesc is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getproduct_long_desc(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getproductLongDesc());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> keyFeatures    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getkey_features() +
                        " Work_JSON -> keyFeatures => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getkeyFeatures());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getkey_features() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getkeyFeatures() != null)) {
                    Assert.assertEquals("The keyFeatures is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getkey_features(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getkeyFeatures());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> authorByLineText    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text() +
                        " Work_JSON -> authorByLineText => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText() != null)) {
                    Assert.assertEquals("The authorByLineText is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getauthor_by_line_text(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getauthorByLineText());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> masterISBN    => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn() +
                        " Work_JSON -> masterISBN => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN());

                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN() != null)) {
                    Assert.assertEquals("The masterISBN is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getmaster_isbn(),
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getmasterISBN());
                }

            }

        }

    }

    @Then("^Get the records from work extended metric table$")
    public void getRecordsFromWorkExtMEtricTable() {
        Log.info("We get the records from work Extended metric Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_METRIC_REC, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityStitchContext.recordsFromWorkExtMetric = DBManager.getDBResultAsBeanList(sql, WorkExtAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare work Extended metric and work Extended Stitching Table$")
    public void compareWorkExtMetricAndStitchingWorkExt() {
        if (dataQualityStitchContext.recordsFromWorkExtMetric.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            //   List <Integer> ignore = new ArrayList();
            for (int i = 0; i < dataQualityStitchContext.recordsFromWorkExtMetric.size(); i++) {
                String workId = dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id();
                getWorkExtendedJSONRec(workId);
                getWorkType(workId);
                if (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtMetric() != null) {
                    WorkExtMetricJson[] extworkMetric_temp = dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtMetric().clone();
                    for (int j = 0; j < extworkMetric_temp.length; j++) {
                        //    if(ignore.contains(j)) continue;
                        String sourceCode = dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_code();
                        //String sourceEpr = dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id();
                        //String trgtEpr = dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id();
                        String trgetCode = String.valueOf(extworkMetric_temp[j].getExtendedMetric().getType().get("code"));
                        String sourceMetricURL = dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_url();
                        String trgetMetricURL = extworkMetric_temp[j].getExtendedMetric().getMetricURL();
                        String sourceMetricYr = dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_year();
                        String trgetMetricYr = extworkMetric_temp[j].getExtendedMetric().getMetricYear();
                        if (sourceMetricURL != null && sourceMetricYr == null) {
                            if (sourceCode.equals(trgetCode) && (sourceMetricURL.equals(trgetMetricURL))) {
                                Log.info("Work_Ext_Metric -> EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                                if (dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() != null ||
                                        (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkStitching.getId());
                                }

                                Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> work_type => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getwork_type() +
                                        " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                if (dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getwork_type() != null ||
                                        (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getwork_type(),
                                            dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                }
                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> Metric_Code => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_code() +
                                        " Work_JSON -> Metric_Code => " + extworkMetric_temp[j].getExtendedMetric().getType().get("code"));
                                Assert.assertEquals("The Metric_Code is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_code(),
                                        extworkMetric_temp[j].getExtendedMetric().getType().get("code"));

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> Metric_Name => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_name() +
                                        " Work_JSON -> Metric_Name => " + extworkMetric_temp[j].getExtendedMetric().getType().get("name"));
                                Assert.assertEquals("The Metric_Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_name(),
                                        extworkMetric_temp[j].getExtendedMetric().getType().get("name"));

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> metric => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric() +
                                        " Work_JSON -> metric => " + extworkMetric_temp[j].getExtendedMetric().getMetric());
                                Assert.assertEquals("The metric is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric(),
                                        extworkMetric_temp[j].getExtendedMetric().getMetric());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> metricURL => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_url() +
                                        " Work_JSON -> metricURL => " + extworkMetric_temp[j].getExtendedMetric().getMetricURL());
                                Assert.assertEquals("The metricURL is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_url(),
                                        extworkMetric_temp[j].getExtendedMetric().getMetricURL());
                                j = 0;
                                //  ignore.add(j);
                                break;
                            } else {
                                j = j++;
                            }
                            //}

                        } else if (sourceMetricYr != null && sourceMetricURL == null) {
                            if (sourceCode.equals(trgetCode) && (sourceMetricYr.equals(trgetMetricYr))) {
                                Log.info("Work_Ext_Metric -> EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                                if (dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() != null ||
                                        (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkStitching.getId());
                                }

                                Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> work_type => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getwork_type() +
                                        " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                if (dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getwork_type() != null ||
                                        (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getwork_type(),
                                            dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                }

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> Metric_Code => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_code() +
                                        " Work_JSON -> Metric_Code => " + extworkMetric_temp[j].getExtendedMetric().getType().get("code"));
                                Assert.assertEquals("The Metric_Code is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_code(),
                                        extworkMetric_temp[j].getExtendedMetric().getType().get("code"));

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> Metric_Name => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_name() +
                                        " Work_JSON -> Metric_Name => " + extworkMetric_temp[j].getExtendedMetric().getType().get("name"));
                                Assert.assertEquals("The Metric_Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_name(),
                                        extworkMetric_temp[j].getExtendedMetric().getType().get("name"));

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> metric => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric() +
                                        " Work_JSON -> metric => " + extworkMetric_temp[j].getExtendedMetric().getMetric());
                                Assert.assertEquals("The metric is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric(),
                                        extworkMetric_temp[j].getExtendedMetric().getMetric());
                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> metricYear => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_year() +
                                        " Work_JSON -> metricYear => " + extworkMetric_temp[j].getExtendedMetric().getMetricYear());
                                Assert.assertEquals("The metricYear is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_year(),
                                        extworkMetric_temp[j].getExtendedMetric().getMetricYear());
                                j = 0;
                                //   ignore.add(j);
                                break;
                            } else {
                                j = j++;
                            }

                        } else {
                            if (sourceCode.equals(trgetCode)) {
                                Log.info("Work_Ext_Metric -> EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                                if (dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() != null ||
                                        (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkStitching.getId());
                                }

                                Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> work_type => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getwork_type() +
                                        " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                if (dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getwork_type() != null ||
                                        (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getwork_type(),
                                            dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                }
                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> Metric_Code => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_code() +
                                        " Work_JSON -> Metric_Code => " + extworkMetric_temp[j].getExtendedMetric().getType().get("code"));
                                Assert.assertEquals("The Metric_Code is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_code(),
                                        extworkMetric_temp[j].getExtendedMetric().getType().get("code"));

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> Metric_Name => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_name() +
                                        " Work_JSON -> Metric_Name => " + extworkMetric_temp[j].getExtendedMetric().getType().get("name"));
                                Assert.assertEquals("The Metric_Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_name(),
                                        extworkMetric_temp[j].getExtendedMetric().getType().get("name"));

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> metric => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric() +
                                        " Work_JSON -> metric => " + extworkMetric_temp[j].getExtendedMetric().getMetric());
                                Assert.assertEquals("The metric is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric(),
                                        extworkMetric_temp[j].getExtendedMetric().getMetric());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> metricURL => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_url() +
                                        " Work_JSON -> metricURL => " + extworkMetric_temp[j].getExtendedMetric().getMetricURL());
                                Assert.assertEquals("The metricURL is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_url(),
                                        extworkMetric_temp[j].getExtendedMetric().getMetricURL());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id() +
                                        " Work_Ext_Metric -> metricYear => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_year() +
                                        " Work_JSON -> metricYear => " + extworkMetric_temp[j].getExtendedMetric().getMetricYear());
                                Assert.assertEquals("The metricYear is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtMetric.get(i).getmetric_year(),
                                        extworkMetric_temp[j].getExtendedMetric().getMetricYear());
                                j = 0;
                                //  ignore.add(j);
                                break;
                            } else {
                                j = j++;
                            }
                        }
                    }
                }
            }
        }
    }

    @Then("^Get the records from work extended url table$")
    public void getRecordsFromWorkExtUrltTable() {
        Log.info("We get the records from work Extended url Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_URL_REC, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityStitchContext.recordsFromWorkExtUrl = DBManager.getDBResultAsBeanList(sql, WorkExtAccessObject.class, Constants.AWS_URL);
    }


    @And("^Compare work Extended url and work Extended Stitching Table$")
    public void compareWorkExtUrlAndStitchingWorkExt() {
        if (dataQualityStitchContext.recordsFromWorkExtUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            //   List <Integer> ignore = new ArrayList();
            for (int i = 0; i < dataQualityStitchContext.recordsFromWorkExtUrl.size(); i++) {
                String workId = dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id();
                getWorkExtendedJSONRec(workId);
                getWorkType(workId);
                if (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedUrls() != null) {
                    WorkExtUrlJson[] extworkUrl_temp = dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedUrls().clone();
                    for (int j = 0; j < extworkUrl_temp.length; j++) {
                        //    if(ignore.contains(j)) continue;
                        String sourceUrl = dataQualityStitchContext.recordsFromWorkExtUrl.get(i).geturl();
                        String trgetUrl = extworkUrl_temp[j].getExtendedUrl().getUrl();
                        String sourceCode = dataQualityStitchContext.recordsFromWorkExtUrl.get(i).geturl_type_code();
                        String trgetCode = String.valueOf(extworkUrl_temp[j].getExtendedUrl().getType().get("code"));
                        //  if(sourceMetricURL!=null && sourceMetricYr==null){
                        if (sourceUrl.equals(trgetUrl) && sourceCode.equals(trgetCode)) {
                            Log.info("Work_Ext_URL -> EPR => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id() +
                                    " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                            if (dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id() != null ||
                                    (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                                Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                        dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkStitching.getId());
                            }

                            Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id() +
                                    " Work_Ext_URL -> work_type => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getwork_type() +
                                    " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                            if (dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getwork_type() != null ||
                                    (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype() != null)) {
                                Assert.assertEquals("The worktype => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                        dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getwork_type(),
                                        dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                            }
                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id() +
                                    " Work_Ext_URL -> url_Code => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).geturl_type_code() +
                                    " Work_JSON -> UrlCode => " + extworkUrl_temp[j].getExtendedUrl().getType().get("code"));
                            Assert.assertEquals("The url_Code is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtUrl.get(i).geturl_type_code(),
                                    extworkUrl_temp[j].getExtendedUrl().getType().get("code"));

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id() +
                                    " Work_Ext_URL -> url_name => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).geturl_type_name() +
                                    " Work_JSON -> UrlName => " + extworkUrl_temp[j].getExtendedUrl().getType().get("name"));
                            Assert.assertEquals("The url_Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtUrl.get(i).geturl_type_name(),
                                    extworkUrl_temp[j].getExtendedUrl().getType().get("name"));

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id() +
                                    " Work_Ext_URL -> url => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).geturl() +
                                    " Work_JSON -> URL => " + extworkUrl_temp[j].getExtendedUrl().getUrl());
                            Assert.assertEquals("The url is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtUrl.get(i).geturl(),
                                    extworkUrl_temp[j].getExtendedUrl().getUrl());

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id() +
                                    " Work_Ext_URL -> urlTitle => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).geturl_title() +
                                    " Work_JSON -> URLtitle => " + extworkUrl_temp[j].getExtendedUrl().getUrlTitle());
                            Assert.assertEquals("The urlTitle is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtUrl.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtUrl.get(i).geturl_title(),
                                    extworkUrl_temp[j].getExtendedUrl().getUrlTitle());

                            j = 0;
                            //  ignore.add(j);
                            break;
                        } else {
                            j = j++;
                        }
                    }
                }
            }
        }
    }

    @Then("^Get the records from work extended subj area table$")
    public void getRecordsFromWorkExtSubAreaTable() {
        Log.info("We get the records from work Extended Subject Area Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_SUB_AREA_REC, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityStitchContext.recordsFromWorkExtSubjArea = DBManager.getDBResultAsBeanList(sql, WorkExtAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare work Extended subj area and work Extended Stitching Table$")
    public void compareWorkExtSubAreaAndStitchingWorkExt() {
        if (dataQualityStitchContext.recordsFromWorkExtSubjArea.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            //   List <Integer> ignore = new ArrayList();
            for (int i = 0; i < dataQualityStitchContext.recordsFromWorkExtSubjArea.size(); i++) {
                String workId = dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id();
                getWorkExtendedJSONRec(workId);
                getWorkType(workId);
                if (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedSubjectAreas() != null) {
                    WorkExtSubAreaJson[] extworkSubj_temp = dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedSubjectAreas().clone();
                    for (int j = 0; j < extworkSubj_temp.length; j++) {
                        //    if(ignore.contains(j)) continue;
                        String sourceCode = dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getcode();
                        String trgetCode = extworkSubj_temp[j].getExtendedSubArea().getCode();
                        //  if(sourceMetricURL!=null && sourceMetricYr==null){
                        if (sourceCode.equals(trgetCode)) {
                            Log.info("Work_Ext_Sub_Area -> EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id() +
                                    " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                            if (dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id() != null ||
                                    (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                                Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                        dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkStitching.getId());
                            }

                            Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id() +
                                    " Work_Ext_Sub_Area -> work_type => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getwork_type() +
                                    " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                            if (dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getwork_type() != null ||
                                    (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype() != null)) {
                                Assert.assertEquals("The worktype => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                        dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getwork_type(),
                                        dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                            }
                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id() +
                                    " Work_Ext_Sub_Area -> Code => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getcode() +
                                    " Work_JSON -> Code => " + extworkSubj_temp[j].getExtendedSubArea().getCode());
                            Assert.assertEquals("The Code is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getcode(),
                                    extworkSubj_temp[j].getExtendedSubArea().getCode());

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id() +
                                    " Work_Ext_Sub_Area -> name => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getname() +
                                    " Work_JSON -> Name => " + extworkSubj_temp[j].getExtendedSubArea().getName());
                            Assert.assertEquals("The Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getname(),
                                    extworkSubj_temp[j].getExtendedSubArea().getName());

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id() +
                                    " Work_Ext_Sub_Area -> TypeCode => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).gettype_code() +
                                    " Work_JSON -> TypeCode => " + extworkSubj_temp[j].getExtendedSubArea().getType().get("code"));
                            Assert.assertEquals("The TypeCode is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).gettype_code(),
                                    extworkSubj_temp[j].getExtendedSubArea().getType().get("code"));

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id() +
                                    " Work_Ext_Sub_Area -> TypeName => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).gettype_name() +
                                    " Work_JSON -> TypeName => " + extworkSubj_temp[j].getExtendedSubArea().getType().get("name"));
                            Assert.assertEquals("The TypeName is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).gettype_name(),
                                    extworkSubj_temp[j].getExtendedSubArea().getType().get("name"));

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id() +
                                    " Work_Ext_Sub_Area -> Priority => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getpriority() +
                                    " Work_JSON -> Priority => " + extworkSubj_temp[j].getExtendedSubArea().getPriority());
                            Assert.assertEquals("The Priority is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtSubjArea.get(i).getpriority(),
                                    extworkSubj_temp[j].getExtendedSubArea().getPriority());

                            j = 0;
                            //  ignore.add(j);
                            break;
                        } else {
                            j = j++;
                        }
                    }
                }
            }
        }
    }

    @Then("^Get the records from work extended person role table$")
    public void getRecordsFromWorkExtPersRoleTable() {
        Log.info("We get the records from work Extended Person Role Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_PERS_ROLE_REC, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityStitchContext.recordsFromWorkExtPersRole = DBManager.getDBResultAsBeanList(sql, WorkExtAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare work Extended person role and work Extended Stitching Table$")
    public void compareWorkExtPersRoleAndStitchingWorkExt() {
        if (dataQualityStitchContext.recordsFromWorkExtPersRole.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            //   List <Integer> ignore = new ArrayList();
            for (int i = 0; i < dataQualityStitchContext.recordsFromWorkExtPersRole.size(); i++) {
                String workId = dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id();
                getWorkExtendedJSONRec(workId);
                getWorkType(workId);
                if (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedPersons() != null) {
                    WorkExtPersonRoleJson[] extworkPerson_temp = dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedPersons().clone();
                    for (int j = 0; j < extworkPerson_temp.length; j++) {
                        //    if(ignore.contains(j)) continue;
                        String sourceCode = dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_code();
                        String trgetCode = extworkPerson_temp[j].getExtendedRole().getCode();
                        String sourceFirstName = dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getfirst_name();
                        String trgtFirstName = extworkPerson_temp[j].getExtendedPerson().getFirstName();
                        String sourceLastName = dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getlast_name();
                        String trgtLastName = extworkPerson_temp[j].getExtendedPerson().getLastName();
                        String sourceSeqNum = dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getsequence_number();
                        String trgtSeqNum = extworkPerson_temp[j].getSequenceNumber();

                        if (sourceFirstName != null && sourceSeqNum == null) {
                            if (sourceCode.equals(trgetCode) && sourceLastName.equals(trgtLastName) && sourceFirstName.equals(trgtFirstName)) {
                                Log.info("Work_Ext_perso_role -> EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                                if (dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() != null ||
                                        (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkStitching.getId());
                                }

                                Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> work_type => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getwork_type() +
                                        " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                if (dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getwork_type() != null ||
                                        (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype() != null)) {
                                    Assert.assertEquals("The worktype => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getwork_type(),
                                            dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                }
                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Role_Code => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_code() +
                                        " Work_JSON -> Role_Code => " + extworkPerson_temp[j].getExtendedRole().getCode());
                                Assert.assertEquals("The Code is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_code(),
                                        extworkPerson_temp[j].getExtendedRole().getCode());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Role_Name => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_name() +
                                        " Work_JSON -> Role_Name => " + extworkPerson_temp[j].getExtendedRole().getName());
                                Assert.assertEquals("The Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_name(),
                                        extworkPerson_temp[j].getExtendedRole().getName());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> firstName => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getfirst_name() +
                                        " Work_JSON -> firstName => " + extworkPerson_temp[j].getExtendedPerson().getFirstName());
                                Assert.assertEquals("The Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getfirst_name(),
                                        extworkPerson_temp[j].getExtendedPerson().getFirstName());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> lastName => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getlast_name() +
                                        " Work_JSON -> lastName => " + extworkPerson_temp[j].getExtendedPerson().getLastName());
                                Assert.assertEquals("The Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getlast_name(),
                                        extworkPerson_temp[j].getExtendedPerson().getLastName());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> email => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getemail() +
                                        " Work_JSON -> email => " + extworkPerson_temp[j].getExtendedPerson().getEmail());
                                Assert.assertEquals("The email is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getemail(),
                                        extworkPerson_temp[j].getExtendedPerson().getEmail());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> imageurl => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getimage_url() +
                                        " Work_JSON -> imageurl => " + extworkPerson_temp[j].getExtendedPerson().getImageUrl());
                                Assert.assertEquals("The imageurl is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getimage_url(),
                                        extworkPerson_temp[j].getExtendedPerson().getImageUrl());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Affiliation => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getaffiliation() +
                                        " Work_JSON -> Affiliation => " + extworkPerson_temp[j].getExtendedPerson().getAffiliation());
                                Assert.assertEquals("The Affiliation is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getaffiliation(),
                                        extworkPerson_temp[j].getExtendedPerson().getAffiliation());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Honours => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).gethonours() +
                                        " Work_JSON -> Honours => " + extworkPerson_temp[j].getExtendedPerson().getHonours());
                                Assert.assertEquals("The Honours is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).gethonours(),
                                        extworkPerson_temp[j].getExtendedPerson().getHonours());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> CoreWorkPersonRoleId => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getcore_work_person_role_id() +
                                        " Work_JSON -> CoreWorkPersonRoleId => " + extworkPerson_temp[j].getCoreWorkPersonRoleId());
                                Assert.assertEquals("The CoreWorkPersonRoleId is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getcore_work_person_role_id(),
                                        extworkPerson_temp[j].getCoreWorkPersonRoleId());

                                j = 0;
                                //  ignore.add(j);
                                break;
                            } else {
                                j = j++;
                            }
                        } else if (sourceFirstName == null && sourceSeqNum != null) {
                            if (sourceCode.equals(trgetCode) && sourceLastName.equals(trgtLastName) && sourceSeqNum.equals(trgtSeqNum)) {
                                Log.info("Work_Ext_perso_role -> EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                                if (dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() != null ||
                                        (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkStitching.getId());
                                }

                                Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> work_type => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getwork_type() +
                                        " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                if (dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getwork_type() != null ||
                                        (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype() != null)) {
                                    Assert.assertEquals("The worktype => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getwork_type(),
                                            dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                }
                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Role_Code => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_code() +
                                        " Work_JSON -> Role_Code => " + extworkPerson_temp[j].getExtendedRole().getCode());
                                Assert.assertEquals("The Code is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_code(),
                                        extworkPerson_temp[j].getExtendedRole().getCode());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Role_Name => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_name() +
                                        " Work_JSON -> Role_Name => " + extworkPerson_temp[j].getExtendedRole().getName());
                                Assert.assertEquals("The Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_name(),
                                        extworkPerson_temp[j].getExtendedRole().getName());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> lastName => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getlast_name() +
                                        " Work_JSON -> lastName => " + extworkPerson_temp[j].getExtendedPerson().getLastName());
                                Assert.assertEquals("The Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getlast_name(),
                                        extworkPerson_temp[j].getExtendedPerson().getLastName());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> email => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getemail() +
                                        " Work_JSON -> email => " + extworkPerson_temp[j].getExtendedPerson().getEmail());
                                Assert.assertEquals("The email is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getemail(),
                                        extworkPerson_temp[j].getExtendedPerson().getEmail());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> imageurl => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getimage_url() +
                                        " Work_JSON -> imageurl => " + extworkPerson_temp[j].getExtendedPerson().getImageUrl());
                                Assert.assertEquals("The imageurl is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getimage_url(),
                                        extworkPerson_temp[j].getExtendedPerson().getImageUrl());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Affiliation => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getaffiliation() +
                                        " Work_JSON -> Affiliation => " + extworkPerson_temp[j].getExtendedPerson().getAffiliation());
                                Assert.assertEquals("The Affiliation is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getaffiliation(),
                                        extworkPerson_temp[j].getExtendedPerson().getAffiliation());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Honours => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).gethonours() +
                                        " Work_JSON -> Honours => " + extworkPerson_temp[j].getExtendedPerson().getHonours());
                                Assert.assertEquals("The Honours is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).gethonours(),
                                        extworkPerson_temp[j].getExtendedPerson().getHonours());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> SequenceNumber => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getsequence_number() +
                                        " Work_JSON -> SequenceNumber => " + extworkPerson_temp[j].getSequenceNumber());
                                Assert.assertEquals("The SequenceNumber is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getsequence_number(),
                                        extworkPerson_temp[j].getSequenceNumber());
                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> CoreWorkPersonRoleId => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getcore_work_person_role_id() +
                                        " Work_JSON -> CoreWorkPersonRoleId => " + extworkPerson_temp[j].getCoreWorkPersonRoleId());
                                Assert.assertEquals("The CoreWorkPersonRoleId is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getcore_work_person_role_id(),
                                        extworkPerson_temp[j].getCoreWorkPersonRoleId());

                                j = 0;
                                //  ignore.add(j);
                                break;
                            } else {
                                j = j++;
                            }
                        } else {
                            if (sourceCode.equals(trgetCode) && sourceLastName.equals(trgtLastName) && sourceFirstName.equals(trgtFirstName) && sourceSeqNum.equals(trgtSeqNum)) {
                                Log.info("Work_Ext_perso_role -> EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                                if (dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() != null ||
                                        (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkStitching.getId());
                                }

                                Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> work_type => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getwork_type() +
                                        " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                if (dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getwork_type() != null ||
                                        (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype() != null)) {
                                    Assert.assertEquals("The worktype => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                            dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getwork_type(),
                                            dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                }
                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Role_Code => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_code() +
                                        " Work_JSON -> Role_Code => " + extworkPerson_temp[j].getExtendedRole().getCode());
                                Assert.assertEquals("The Code is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_code(),
                                        extworkPerson_temp[j].getExtendedRole().getCode());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Role_Name => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_name() +
                                        " Work_JSON -> Role_Name => " + extworkPerson_temp[j].getExtendedRole().getName());
                                Assert.assertEquals("The Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getrole_name(),
                                        extworkPerson_temp[j].getExtendedRole().getName());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> lastName => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getfirst_name() +
                                        " Work_JSON -> lastName => " + extworkPerson_temp[j].getExtendedPerson().getLastName());
                                Assert.assertEquals("The Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getfirst_name(),
                                        extworkPerson_temp[j].getExtendedPerson().getFirstName());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> lastName => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getlast_name() +
                                        " Work_JSON -> lastName => " + extworkPerson_temp[j].getExtendedPerson().getLastName());
                                Assert.assertEquals("The Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getlast_name(),
                                        extworkPerson_temp[j].getExtendedPerson().getLastName());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> email => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getemail() +
                                        " Work_JSON -> email => " + extworkPerson_temp[j].getExtendedPerson().getEmail());
                                Assert.assertEquals("The email is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getemail(),
                                        extworkPerson_temp[j].getExtendedPerson().getEmail());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> imageurl => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getimage_url() +
                                        " Work_JSON -> imageurl => " + extworkPerson_temp[j].getExtendedPerson().getImageUrl());
                                Assert.assertEquals("The imageurl is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getimage_url(),
                                        extworkPerson_temp[j].getExtendedPerson().getImageUrl());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Affiliation => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getaffiliation() +
                                        " Work_JSON -> Affiliation => " + extworkPerson_temp[j].getExtendedPerson().getAffiliation());
                                Assert.assertEquals("The Affiliation is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getaffiliation(),
                                        extworkPerson_temp[j].getExtendedPerson().getAffiliation());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> Honours => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).gethonours() +
                                        " Work_JSON -> Honours => " + extworkPerson_temp[j].getExtendedPerson().getHonours());
                                Assert.assertEquals("The Honours is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).gethonours(),
                                        extworkPerson_temp[j].getExtendedPerson().getHonours());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> SequenceNumber => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getsequence_number() +
                                        " Work_JSON -> SequenceNumber => " + extworkPerson_temp[j].getSequenceNumber());
                                Assert.assertEquals("The SequenceNumber is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getsequence_number(),
                                        extworkPerson_temp[j].getSequenceNumber());
                                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id() +
                                        " Work_Ext_perso_role -> CoreWorkPersonRoleId => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getcore_work_person_role_id() +
                                        " Work_JSON -> CoreWorkPersonRoleId => " + extworkPerson_temp[j].getCoreWorkPersonRoleId());
                                Assert.assertEquals("The CoreWorkPersonRoleId is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkExtPersRole.get(i).getcore_work_person_role_id(),
                                        extworkPerson_temp[j].getCoreWorkPersonRoleId());

                                j = 0;
                                //  ignore.add(j);
                                break;
                            } else {
                                j = j++;
                            }
                        }
                    }
                }
            }
        }
    }

    @Then("^Get the records from work extended relationship sibling table$")
    public void getRecordsFromWorkExtRelationshipSiblings() {
        Log.info("We get the records from work Extended relationship siblings Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_RELATION_SIBLINGS_REC, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityStitchContext.recordsFromWorkExtRelationSiblings = DBManager.getDBResultAsBeanList(sql, WorkExtAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare work Extended Relationships and work Extended Stitching Table$")
    public void compareWorkExtRelatSiblingsAndStitchingWorkExt() {
        if (dataQualityStitchContext.recordsFromWorkExtRelationSiblings.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
              List <Integer> ignore = new ArrayList();
            for (int i = 0; i < dataQualityStitchContext.recordsFromWorkExtRelationSiblings.size(); i++) {
                String workId = dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id();
                getWorkExtendedJSONRec(workId);
                getWorkType(workId);
                if (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedRelationships() != null) {
                    //ArrayList<WorkExtRelationshipsJson.ExtendedSiblings> extworkRelation_temp = new ArrayList(Arrays.asList(dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended()));
                    WorkExtRelationshipsJson.ExtendedSiblings[] extworkRelation_temp = dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedRelationships().getExtendedSiblings().clone();
                    for (int j = 0; j < extworkRelation_temp.length; j++) {
                        if(ignore.contains(j)) continue;

                        String sourceRelatId = dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_epr_id();
                        String trgetRelateId = extworkRelation_temp[j].getId();

                        //  if(sourceMetricURL!=null && sourceMetricYr==null){
                        if (sourceRelatId.equals(trgetRelateId)) {
                            Log.info("Work_Relation -> EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                            if (dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() != null ||
                                    (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                                Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                        dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromWorkStitching.getId());
                            }

                            Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_Relation -> work_type => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getwork_type() +
                                    " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                            if (dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getwork_type() != null ||
                                    (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype() != null)) {
                                Assert.assertEquals("The worktype => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                        dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getwork_type(),
                                        dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                            }
                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_Relation -> RelationId => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_epr_id() +
                                    " Work_JSON -> RelationId => " + extworkRelation_temp[j].getId());
                            Assert.assertEquals("The RelationId is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_epr_id(),
                                    extworkRelation_temp[j].getId());

                           Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_Relation -> RelationTitle => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_title() +
                                    " Work_JSON -> RelationTitle => " + extworkRelation_temp[j].getWorkExtendedSummary().getTitle());
                            Assert.assertEquals("The Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_title(),
                                    extworkRelation_temp[j].getWorkExtendedSummary().getTitle());

                          Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_Relation -> RelatedCode => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_type_code() +
                                    " Work_JSON -> RelatedCode => " + extworkRelation_temp[j].getWorkExtendedSummary().getType().get("code"));
                            Assert.assertEquals("The RelatedCode is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_type_code(),
                                    extworkRelation_temp[j].getWorkExtendedSummary().getType().get("code"));

                             Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_Relation -> RelatedTypeName => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_type_name() +
                                    " Work_JSON -> RelatedTypeName => " + extworkRelation_temp[j].getWorkExtendedSummary().getType().get("name"));
                            Assert.assertEquals("The RelatedTypeName is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_type_name(),
                                    extworkRelation_temp[j].getWorkExtendedSummary().getType().get("name"));

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_Relation -> RelatedRollUp => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_type_roll_up() +
                                    " Work_JSON -> RelatedRollUp => " + extworkRelation_temp[j].getWorkExtendedSummary().getType().get("typeRollUp"));
                            Assert.assertEquals("The RelatedRollUp is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_type_roll_up(),
                                    extworkRelation_temp[j].getWorkExtendedSummary().getType().get("typeRollUp"));

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_Relation -> RelatedStatusCode => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_status_code() +
                                    " Work_JSON -> RelatedStatusCode => " + extworkRelation_temp[j].getWorkExtendedSummary().getExtendedStatus().get("code"));
                            Assert.assertEquals("The RelatedStatusCode is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_status_code(),
                                    extworkRelation_temp[j].getWorkExtendedSummary().getExtendedStatus().get("code"));

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_Relation -> RelatedStatusName => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_status_name() +
                                    " Work_JSON -> RelatedStatusName => " + extworkRelation_temp[j].getWorkExtendedSummary().getExtendedStatus().get("code"));
                            Assert.assertEquals("The RelatedStatusName is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_status_name(),
                                    extworkRelation_temp[j].getWorkExtendedSummary().getExtendedStatus().get("name"));

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_Relation -> RelatedStatusRollUp => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_status_roll_up() +
                                    " Work_JSON -> RelatedStatusRollUp => " + extworkRelation_temp[j].getWorkExtendedSummary().getExtendedStatus().get("statusRollUp"));
                            Assert.assertEquals("The RelatedStatusRollUp is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelated_status_roll_up(),
                                    extworkRelation_temp[j].getWorkExtendedSummary().getExtendedStatus().get("statusRollUp"));

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_Relation -> RelationCode => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelationship_code() +
                                    " Work_JSON -> RelationCode => " + extworkRelation_temp[j].getType().get("code"));
                            Assert.assertEquals("The RelationCode is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelationship_code(),
                                    extworkRelation_temp[j].getType().get("code"));

                            Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id() +
                                    " Work_Relation -> RelationNames => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelationship_name() +
                                    " Work_JSON -> RelationNames => " + extworkRelation_temp[j].getType().get("name"));
                            Assert.assertEquals("The RelationNames is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getepr_id(),
                                    dataQualityStitchContext.recordsFromWorkExtRelationSiblings.get(i).getrelationship_name(),
                                    extworkRelation_temp[j].getType().get("name"));
                            j = 0;
                             ignore.add(j);
                            break;
                        } else {
                            j = j++;
                        }
                    }
                }
            }
        }
    }

    @Then("^Get the records from work extended editorial board table$")
    public void getRecordsFromWorkExtEditorialBoard() {
        Log.info("We get the records from work Extended editorial board Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_EDITORIAL_BOARD_REC, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityStitchContext.recordsFromWorkExtEditorial = DBManager.getDBResultAsBeanList(sql, WorkExtAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare work Extended editorial and work Extended Stitching Table$")
    public void compareWorkExtEditorialAndStitchingWorkExt() {
        if (dataQualityStitchContext.recordsFromWorkExtEditorial.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            List <Integer> ignore = new ArrayList();
            for (int i = 0; i < dataQualityStitchContext.recordsFromWorkExtEditorial.size(); i++) {
                String workId = dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id();
                getWorkExtendedJSONRec(workId);
                getWorkType(workId);
                if (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedEditorialBoard() != null) {
                    //ArrayList<WorkExtRelationshipsJson.ExtendedSiblings> extworkRelation_temp = new ArrayList(Arrays.asList(dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended()));
                    WorkExtEditorialBoardJson[] extworkEditorial_temp = dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getWorkExtendedEditorialBoard().clone();
                    for (int j = 0; j < extworkEditorial_temp.length; j++) {
                       // if(ignore.contains(j)) continue;
                        try {
                            String sourceFirstName = dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getfirst_name();
                            String trgetFirstName = extworkEditorial_temp[j].getExtendedBoardMember().getFirstName();
                            String sourceLastName = dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getlast_name();
                            String trgetLastName = extworkEditorial_temp[j].getExtendedBoardMember().getLastName();
                            String sourceGrpNumber = dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getgroup_number();
                            String trgetGrpNumber = extworkEditorial_temp[j].getGroupNumber();
                            String sourceSeqNumber = dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getsequence_number();
                            String trgetSeqNumber = extworkEditorial_temp[j].getSequenceNumber();
                            String sourceAff = dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getaffiliation();
                            String trgetSeqAff = extworkEditorial_temp[j].getExtendedBoardMember().getAffiliation();


                            if (sourceFirstName != null && sourceLastName != null && sourceGrpNumber != null) {
                                if (sourceFirstName.equals(trgetFirstName) && sourceLastName.equals(trgetLastName)
                                        && sourceGrpNumber.equals(trgetGrpNumber) && sourceAff.equals(trgetSeqAff)) {
                                    Log.info("Work_Editorial -> EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() +
                                            " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                                    if (dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() != null ||
                                            (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                                        Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                                dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id(),
                                                dataQualityStitchContext.recordsFromWorkStitching.getId());
                                    }

                                    Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() +
                                            " Work_Editorial -> work_type => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getwork_type() +
                                            " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                    if (dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getwork_type() != null ||
                                            (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype() != null)) {
                                        Assert.assertEquals("The worktype => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() + " is missing/not found in Work_Stiching table",
                                                dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getwork_type(),
                                                dataQualityStitchContext.recFromWorkTypeStitchExtended.get(0).gettype());
                                    }
                                    Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() +
                                            " Work_Editorial -> firstName => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getfirst_name() +
                                            " Work_JSON -> firstName => " + extworkEditorial_temp[j].getExtendedBoardMember().getFirstName());
                                    Assert.assertEquals("The firstName is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getfirst_name(),
                                            extworkEditorial_temp[j].getExtendedBoardMember().getFirstName());

                                    Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() +
                                            " Work_Editorial -> lastName => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getlast_name() +
                                            " Work_JSON -> lastName => " + extworkEditorial_temp[j].getExtendedBoardMember().getLastName());
                                    Assert.assertEquals("The lastName is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getlast_name(),
                                            extworkEditorial_temp[j].getExtendedBoardMember().getLastName());

                                    Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() +
                                            " Work_Editorial -> title => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).gettitle() +
                                            " Work_JSON -> title => " + extworkEditorial_temp[j].getExtendedBoardMember().getTitle());
                                    Assert.assertEquals("The Title is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).gettitle(),
                                            extworkEditorial_temp[j].getExtendedBoardMember().getTitle());

                                    Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() +
                                            " Work_Editorial -> Affiliation => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getaffiliation() +
                                            " Work_JSON -> Affiliation => " + extworkEditorial_temp[j].getExtendedBoardMember().getAffiliation());
                                    Assert.assertEquals("The Affiliation is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getaffiliation(),
                                            extworkEditorial_temp[j].getExtendedBoardMember().getAffiliation());

                                    Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() +
                                            " Work_Editorial -> imageUrl => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getimage_url() +
                                            " Work_JSON -> imageUrl => " + extworkEditorial_temp[j].getExtendedBoardMember().getImageUrl());
                                    Assert.assertEquals("The imageUrl is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getimage_url(),
                                            extworkEditorial_temp[j].getExtendedBoardMember().getImageUrl());

                                    Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() +
                                            " Work_Editorial -> notes => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getnotes_txt() +
                                            " Work_JSON -> notes => " + extworkEditorial_temp[j].getExtendedBoardMember().getNotes());
                                    Assert.assertEquals("The notes is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getnotes_txt(),
                                            extworkEditorial_temp[j].getExtendedBoardMember().getNotes());

                                    Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() +
                                            " Work_Editorial -> GroupNumber => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getgroup_number() +
                                            " Work_JSON -> GroupNumber => " + extworkEditorial_temp[j].getGroupNumber());
                                    Assert.assertEquals("The GroupNumber     is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getgroup_number(),
                                            extworkEditorial_temp[j].getGroupNumber());

                                    Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() +
                                            " Work_Editorial -> SequenceNumber => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getsequence_number() +
                                            " Work_JSON -> SequenceNumber => " + extworkEditorial_temp[j].getSequenceNumber());
                                    Assert.assertEquals("The notes is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getsequence_number(),
                                            extworkEditorial_temp[j].getSequenceNumber());

                                    Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id() +
                                            " Work_Editorial -> Role_Code => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getrole_code() +
                                            " Work_JSON -> Role_Code => " + extworkEditorial_temp[j].getExtendedBoardRole().getCode());
                                    Assert.assertEquals("The Role_Code is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromWorkExtEditorial.get(i).getrole_code(),
                                            extworkEditorial_temp[j].getExtendedBoardRole().getCode());
                                    j = 0;
                                    // ignore.add(j);
                                    break;
                                } else {
                                    j = j++;
                                }

                            }
                        }catch (NullPointerException e){
                            System.out.println(e);
                        }
                    }
                }
            }
        }
    }
}

