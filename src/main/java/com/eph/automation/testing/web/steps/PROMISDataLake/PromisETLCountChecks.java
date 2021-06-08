package com.eph.automation.testing.web.steps.PROMISDataLake;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.PROMISDataLakeSQL.PromisETLCountChecksSQL;
import cucumber.api.java.en.*;
import org.junit.*;

import java.util.*;

public class PromisETLCountChecks {
    private static String PromisSQL;
    private static int Promis_InboundCount;
    private static int Promis_CurrentCount;
    private static int Promis_DeltaQueryCount;
    private static int Promis_DeltaCount;
    private static int Promis_HistExclQueryCount;
    private static int PromisHistExclCount;
    private static int Promis_LatestQueryCount;
    private static int Promis_LatestCount;
    private static int Promis_All_sourceCount;
    private static int Promis_TransformMappingCount;
    private static int Promis_TransformHistoryCount;




//Current count checks
    @Given("We know the number of Promis (.*) data in inbound")
    public void getPromisInboundCount(String InboundtableName) {
        switch(InboundtableName) {
            case "promis_prmautpubt_part" :
            PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_prmautpubt_part, InboundtableName, InboundtableName);
            break;
            case "promis_prmclscodt_part" :
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_prmclscodt_part, InboundtableName, InboundtableName);
                break;
            case "promis_prmclst_part" :
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_prmclst_part, InboundtableName, InboundtableName);
                break;
            case "promis_prm_londes_2_html_part" :
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_prmlondest_part, InboundtableName, InboundtableName);
                break;
            case "promis_prmpricest_part" :
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_prmpricest_part, InboundtableName, InboundtableName);
                break;
            case "promis_prmpubinft_part" :
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_prmpubinft_part, InboundtableName, InboundtableName);
                break;
            case "promis_prmpubrelt_part" :
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_prmpubrelt_part, InboundtableName, InboundtableName, InboundtableName);
                break;
            case "promis_prmincpmct_part" :
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_prmincpmct_part, InboundtableName, InboundtableName, InboundtableName);
                break;
            case "promis_prmpmccodt_part" :
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_prmpmccodt_part, InboundtableName, InboundtableName, InboundtableName);
                break;
        }
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisInboundTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_InboundCount = ((Long) PromisInboundTableCount.get(0).get("Total_Count")).intValue();
        Log.info(InboundtableName + " table in Promis Inbound has the Count: " + Promis_InboundCount);
    }

    @Then("^Get the count for Promis (.*) current$")
    public void getPromisCurrentCount(String Currenttablename) {
        PromisSQL= String.format(PromisETLCountChecksSQL.GET_Promis_Current, Currenttablename);
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisCurrentTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_CurrentCount = ((Long) PromisCurrentTableCount.get(0).get("Total_Count")).intValue();
        Log.info(Currenttablename + " table in Promis Current has the Count: " + Promis_CurrentCount);
    }

    @And("^Compare the Promis count for (.*) table between inbound and current$")
    public void PromiscomparetoInbound(String InboundtableName) {
        Log.info("The count for table" + InboundtableName + " in Promis Inbound: " + Promis_InboundCount + " and Current: " + Promis_CurrentCount);
        Assert.assertEquals("The counts for table " + InboundtableName + " is not equal", Promis_InboundCount, Promis_CurrentCount);
    }

//Current to Transform History Counts
    @Given("^We know the number of Promis (.*) data for the current$")
    public void getCurrentCount(String PromisCurrentTable) {
        PromisSQL= String.format(PromisETLCountChecksSQL.GET_Promis_CurrentCounts, PromisCurrentTable);
        Log.info(PromisSQL);
        List<Map<String,Object>> Promis_DeltaQueryTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_CurrentCount = ((Long) Promis_DeltaQueryTableCount.get(0).get("Total_Count")).intValue();
        Log.info(PromisCurrentTable + " table in Promis Transform History has the Count: " + Promis_CurrentCount);
    }

    @Then("^Get the count for Promis (.*) Transform Hisory with the latest timestamp$")
    public void getPromisTransformHistoryCount(String Deltatablename) {
        PromisSQL= String.format(PromisETLCountChecksSQL.GET_Promis_TransformHistory_Count, Deltatablename, Deltatablename);
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisDeltaTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_TransformHistoryCount = ((Long) PromisDeltaTableCount.get(0).get("Total_Count")).intValue();
        Log.info(Deltatablename + " table in Promis Transform History with latest timestamp has the Count: " + Promis_TransformHistoryCount);
    }

    @And("^Compare the Promis count for (.*) table between Current and Transform Hisory with the latest timestamp$")
    public void PromiscompareCurrenttoHistory(String TransformHistoryTable) {
        Log.info("The count for table" + TransformHistoryTable + " in Promis Current: " + Promis_CurrentCount + " and Transform History with latest timestamp: " + Promis_TransformHistoryCount);
        Assert.assertEquals("The counts for Current and Transform history with latest timestamp is not equal", Promis_CurrentCount, Promis_TransformHistoryCount);
    }

//    Delta Count checks
    @Given("^We know the number of Promis (.*) data for the Delta Query$")
    public void getDeltaQueryCount(String DeltaQueryTable) {
        switch (DeltaQueryTable) {
            case "promis_transform_file_history_subject_areas_part":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Subject_Areas_DeltaQuery,
                        DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable
                );
                break;
            case "promis_transform_file_history_pricing_part":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Pricing_DeltaQuery,
                        DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable);
                break;
            case "promis_transform_file_history_person_roles_part":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Person_Roles_DeltaQuery,
                        DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable);
                break;
            case "promis_transform_file_history_works_part":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Works_DeltaQuery,
                        DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable);
                break;
            case "promis_transform_file_history_metrics_part":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Metrics_DeltaQuery,
                        DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable);
                break;
            case "promis_transform_file_history_urls_part":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Urls_DeltaQuery,
                        DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable);
                break;
            case "promis_transform_file_history_work_rels_part":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Work_Rels_DeltaQuery,
                        DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable);
                break;
        }
        Log.info(PromisSQL);
        List<Map<String,Object>> Promis_DeltaQueryTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_DeltaQueryCount = ((Long) Promis_DeltaQueryTableCount.get(0).get("Total_Count")).intValue();
        Log.info(DeltaQueryTable + " table in Promis Current minus Previous has the Count: " + Promis_DeltaQueryCount);
    }

    @Then("^Get the count for Promis (.*) Delta$")
    public void getPromisDeltaCount(String Deltatablename) {
        PromisSQL= String.format(PromisETLCountChecksSQL.GET_Promis_Delta, Deltatablename, Deltatablename);
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisDeltaTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_DeltaCount = ((Long) PromisDeltaTableCount.get(0).get("Total_Count")).intValue();
        Log.info(Deltatablename + " table in Promis Delta has the Count: " + Promis_DeltaCount);
    }

    @And("^Compare the Promis count for (.*) table between Current minus Previous and Delta$")
    public void PromiscomparetoDelta(String Deltatablename) {
        Log.info("The count for table" + Deltatablename + " in Promis Delta: " + Promis_DeltaCount + " and table query: " + Promis_DeltaQueryCount);
        Assert.assertEquals("The counts for table " + Deltatablename + " is not equal", Promis_DeltaQueryCount, Promis_DeltaCount);
    }

//    History excl Count checks
    @Given("^We know the number of Promis (.*) data for History excluding query$")
    public void getHistoryExclQueryCount(String tablename) {
        switch (tablename) {
            case "subject_areas":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Subject_Areas_HistExclQuery);
                break;
            case "pricing":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Pricing_HistExclQuery);
                break;
            case "person_roles":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Person_Roles_HistExclQuery);
                break;
            case "works":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Works_HistExclQuery);
                break;
            case "metrics":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Metrics_HistExclQuery);
                break;
            case "urls":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Urls_HistExclQuery);
                break;
            case "work_rels":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Work_Rels_HistExclQuery);
                break;
        }
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisHistExclQueryTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_HistExclQueryCount = ((Long) PromisHistExclQueryTableCount.get(0).get("Total_Count")).intValue();
        Log.info(tablename + " table in Promis History Excluding Query has the Count: " + Promis_HistExclQueryCount);
    }

    @Then("^Get the count for Promis (.*) History excluding$")
    public void getPromisHistoryExclCount(String HistExcltablename ) {
        PromisSQL= String.format(PromisETLCountChecksSQL.GET_Promis_History_Excluding, HistExcltablename);
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisHistExclTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        PromisHistExclCount = ((Long) PromisHistExclTableCount.get(0).get("Total_Count")).intValue();
        Log.info(HistExcltablename  + " table in Promis History Excluding has the Count: " + PromisHistExclCount);
    }

    @And("^Compare the Promis count for (.*) table between History excluding query and History excluding$")
    public void PromiscomparetoHistoryExcl(String HistExcltablename) {
        Log.info("The count for table " + HistExcltablename + " in Promis HistExcl: " + PromisHistExclCount + " and table query: " + Promis_HistExclQueryCount);
        Assert.assertEquals("The counts for table " + HistExcltablename + " is not equal", PromisHistExclCount, Promis_HistExclQueryCount);
    }

    //    Latest Count checks
    @Given("^We know the number of Promis (.*) data for latest query$")
    public void getLatestQueryCount(String tablename) {
        switch (tablename) {
            case "subject_areas":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Subject_Areas_LatestQuery);
                break;
            case "pricing":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Pricing_LatestQuery);
                break;
            case "person_roles":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Person_Roles_LatestQuery);
                break;
            case "works":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Works_LatestQuery);
                break;
            case "metrics":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Metrics_LatestQuery);
                break;
            case "urls":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Urls_LatestQuery);
                break;
            case "work_rels":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Work_Rels_LatestQuery);
                break;
        }
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisLatestQueryTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_LatestQueryCount = ((Long) PromisLatestQueryTableCount.get(0).get("Total_Count")).intValue();
        Log.info(tablename + " table in Promis Latest Query has the Count: " + Promis_LatestQueryCount);
    }

    @Then("^Get the count for Promis (.*) latest$")
    public void getPromisLatestCount(String Latesttablename ) {
        PromisSQL= String.format(PromisETLCountChecksSQL.GET_Promis_Latest_Excluding, Latesttablename);
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisLatestTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_LatestCount = ((Long) PromisLatestTableCount.get(0).get("Total_Count")).intValue();
        Log.info(Latesttablename  + " table in Promis Latest has the Count: " + Promis_LatestCount);
    }

    @And("^Compare the Promis count for (.*) table between Latest query and Latest$")
    public void PromiscomparetoLatest(String Latesttablename) {
        Log.info("The count for table " + Latesttablename + " in Promis Latest: " + Promis_LatestCount + " and table query: " + Promis_LatestQueryCount);
        Assert.assertEquals("The counts for table " + Latesttablename + " is not equal", Promis_LatestCount, Promis_LatestQueryCount);
    }

    //    Transform Mapping Count checks
    @Given("^We know the number of Promis (.*) data for Transform mapping$")
    public void getTransformMappingCount(String tablename) {
        switch (tablename) {
            case "subject_areas":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Subject_Areas_TransformMapping);
                break;
            case "pricing":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Pricing_TransformMapping);
                break;
            case "person_roles":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Person_Roles_TransformMapping);
                break;
            case "works":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Works_TransformMapping);
                break;
            case "metrics":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Metrics_TransformMapping);
                break;
            case "urls":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Urls_TransformMapping);
                break;
            case "work_rels":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Work_Rels_TransformMapping);
                break;
        }
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisTransformMappingTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_TransformMappingCount = ((Long) PromisTransformMappingTableCount.get(0).get("Total_Count")).intValue();
        Log.info(tablename + " table in Promis Transform Mapping has the Count: " + Promis_TransformMappingCount);
    }

    @Then("^Get the count for Promis (.*) Current$")
    public void getPromisTransformCurrentCount(String Currenttablename ) {
        PromisSQL= String.format(PromisETLCountChecksSQL.GET_Promis_Transform_Current, Currenttablename);
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisCurrentTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_CurrentCount = ((Long) PromisCurrentTableCount.get(0).get("Total_Count")).intValue();
        Log.info(Currenttablename  + " table in Promis Latest has the Count: " + Promis_CurrentCount);
    }

    @And("^Compare the Promis count for (.*) table between Transform Mapping and Current$")
    public void PromiscompareTransformMappingtoCurrent(String Currenttablename) {
        Log.info("The count for table " + Currenttablename + " in Promis Transform mapping: " + Promis_TransformMappingCount + " and Current: " + Promis_CurrentCount);
        Assert.assertEquals("The counts for table " + Currenttablename + " is not equal", Promis_TransformMappingCount, Promis_CurrentCount);
    }

    //    All Source Count checks
    @Given("^We know the number of Promis (.*) data for Latest$")
    public void getLatestCount(String latesttablename) {
        switch(latesttablename){
            case "promis_transform_latest_pricing":
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_pricing_Latest, latesttablename);
            break;
            default:
                PromisSQL = String.format(PromisETLCountChecksSQL.GET_Promis_Latest, latesttablename);
                break;
        }
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisLatestTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_LatestCount = ((Long) PromisLatestTableCount.get(0).get("Total_Count")).intValue();
        Log.info(latesttablename + " table in Promis Latest has the Count: " + Promis_LatestCount);
    }

    @Then("^Get the count for Promis (.*) Promis All_source$")
    public void getPromisAllSourceTableNameCount(String AllSourceTable ) {
        PromisSQL= String.format(PromisETLCountChecksSQL.GET_Promis_AllSource, AllSourceTable);
        Log.info(PromisSQL);
        List<Map<String,Object>> PromisAll_sourceTableCount = DBManager.getDLResultMap(PromisSQL,Constants.AWS_URL);
        Promis_All_sourceCount = ((Long) PromisAll_sourceTableCount.get(0).get("Total_Count")).intValue();
        Log.info(AllSourceTable  + " table in Promis All_source has the Count: " + Promis_All_sourceCount);
    }

    @And("^Compare the Promis count for (.*) table between Latest and All_source$")
    public void PromiscompareLatesttoAll_SourceTable(String All_SourceTable) {
        Log.info("The count for table " + All_SourceTable + " in Promis Transform mapping: " + Promis_LatestCount + " and All_Source: " + Promis_All_sourceCount);
        Assert.assertEquals("The counts for table " + All_SourceTable + " is not equal", Promis_LatestCount, Promis_All_sourceCount);
    }

}
