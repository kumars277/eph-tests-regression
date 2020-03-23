package com.eph.automation.testing.web.steps.JMDataLake;
// Created by Thomas Kruck on 20/03/20


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.JMDataLakeSQL.JMtoDataLakeCountChecksSQL;
import cucumber.api.java.en.*;
import org.junit.*;

import java.util.*;



public class JMtoDatalakeTableCountChecksSteps {

    private static String sqlDL;
    private static String sqlJM;
    private static int JMCount;
    private static int DLJMCount;

    @Given("^We know the number of JM (.*) data in EPH")
    public void getJMEPHCount(String tableName) {
        switch (tableName){
            case "jmf_allocation_change":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_allocation_change_Count;
                break;
            case "jmf_application_properties":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_application_properties_Count;
                break;
            case "jmf_approval_attachment":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_approval_attachment_Count;
                break;
            case "jmf_approval_request":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_approval_request_Count;
                break;
            case "jmf_chronicle_scenario":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_chronicle_scenario_Count;
                break;
            case "jmf_chronicle_status":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_chronicle_status_Count;
                break;
            case "jmf_family_resource_details":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_family_resource_details_Count;
                break;
                case "jmf_financial_information":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_financial_information_Count;
                break;
                case "jmf_legal_information":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_legal_information_Count;
                break;
                case "jmf_manifestation_electronic_details":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_manifestation_electronic_details_Count;
                break;
                case "jmf_manifestation_print_details":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_manifestation_print_details_Count;
                break;
                case "jmf_party_in_product":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_party_in_product_Count;
                break;
                case "jmf_product_availability":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_availability_Count;
                break;
                case "jmf_product_chronicle":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_chronicle_Count;
                break;
                case "jmf_product_family":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_family_Count;
                break;
                case "jmf_product_manifestation":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_manifestation_Count;
                break;
            case "jmf_product_subject_area":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_subject_area_Count;
                break;
                case "jmf_product_work":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_work_Count;
                break;
            case "jmf_production_information":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_production_information_Count;
                break;
            case "jmf_review_comment":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_review_comment_Count;
                break;
        }
        Log.info(sqlJM);
        List<Map<String, Object>> JMTableCount = DBManager.getDBResultMap(sqlJM, Constants.MYSQL_JM_URL);
        JMCount = ((Long) JMTableCount.get(0).get("count")).intValue();
        Log.info(tableName + " in JM Count: " + JMCount);
    }

    @When("^The JM (.*) data is in the DL$")
    public void getJMDLLCount(String tableName) {
        switch (tableName){
            case "jmf_allocation_change":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_allocation_change_Count;
                break;
            case "jmf_application_properties":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_application_properties_Count;
                break;
            case "jmf_approval_attachment":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_approval_attachment_Count;
                break;
            case "jmf_approval_request":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_approval_request_Count;
                break;
            case "jmf_chronicle_scenario":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_chronicle_scenario_Count;
                break;
            case "jmf_chronicle_status":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_chronicle_status_Count;
                break;
            case "jmf_family_resource_details":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_family_resource_details_Count;
                break;
                case "jmf_financial_information":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_financial_information_Count;
                break;
                case "jmf_legal_information":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_legal_information_Count;
                break;
                case "jmf_manifestation_electronic_details":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_manifestation_electronic_details_Count;
                break;
                case "jmf_manifestation_print_details":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_manifestation_print_details_Count;
                break;
                case "jmf_party_in_product":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_party_in_product_Count;
                break;
                case "jmf_product_availability":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_availability_Count;
                break;
                case "jmf_product_chronicle":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_chronicle_Count;
                break;
                case "jmf_product_family":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_family_Count;
                break;
                case "jmf_product_manifestation":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_manifestation_Count;
                break;
            case "jmf_product_subject_area":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_subject_area_Count;
                break;
                case "jmf_product_work":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_work_Count;
                break;
                case "jmf_production_information":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_production_information_Count;
                break;
                case "jmf_review_comment":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_review_comment_Count;
                break;

        }
        Log.info(sqlDL);
        List<Map<String, Object>> DPPCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLJMCount = ((Long) DPPCountDL.get(0).get("count")).intValue();
        Log.info(tableName + " data in DL: " + DLJMCount);
    }

    @Then("^The JM count for (.*) table between EPH and DL are identical$")
    public void JMcompareEPHtoDL(String tableName) {
        Assert.assertEquals("The counts between " + tableName + " is not equal", JMCount, DLJMCount);
        Log.info("The count for " + tableName + " table between EPH: " + JMCount + " and DL: " + DLJMCount + " is equal");
    }
}
