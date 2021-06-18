package com.eph.automation.testing.steps.jm;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.JMDataLakeSQL.JM_ETLCoreCountChecksSQL;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class JM_ETLCoreCountChecksSteps {

    private static String sqlDL;
    private static String sqlJM;
    private static int JMCount;
    private static int DLJMCount;


    @Given("^We know the total count of JM ETL Core (.*) data")
    public void getJMMYSQLCount(String ETLtable) {
        switch (ETLtable){
            case "etl_accountable_product_dq_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_ACCOUNTABLE_PRODUCT;
                break;

            case "etl_wwork_dq_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_WORK;
                break;

            case "etl_work_identifier_dq_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_WORK_IDENTIFIER;
                break;
            case "etl_work_subject_area_dq_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_WORK_SUBJECT_AREA;
                break;

            case "etl_work_person_role_dq_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_WORK_PERSON_ROLE;
                break;
            case "etl_manifestation_updates1_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_MANIFESTATION_UPDATES;
                break;

            case "etl_manifestation_dq_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_MANIFESTATION;
                break;

            case "etl_manifestation_identifier_dq_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_MANIFESTATION_IDENTIFIER;
                break;
            case "etl_product_part1_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_PRODUCT_PART;
                break;
            case "etl_product_inserts_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_PRODUCT_INSERT;
                break;
            case "etl_product_updates_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_PRODUCT_UPDATES;
                break;
            case "etl_product_dq_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_PRODUCT;
                break;
            case "etl_product_person_role_dq_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_PRODUCT_PERSON_ROLE;
                break;
            case "jm_imprint_data_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_IMPRINT_DATA;
                break;
            case "sd_subject_areas_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_SD_SUBJECT_AREAS;
                break;
            case "works_attrs_roles_people_v":
                sqlJM = JM_ETLCoreCountChecksSQL.GET_JMF_WORKS_ATTRS_ROLES_PEOPLE;
                break;
        }
        Log.info(sqlJM);
        List<Map<String, Object>> JMTableCount = DBManager.getDBResultMap(sqlJM, Constants.AWS_URL);
        JMCount = ((Long) JMTableCount.get(0).get("count")).intValue();
        Log.info(ETLtable + " in JM Count: " + JMCount);
    }

    @When("^Get the total count of JM (.*) data is in the JM Staging$")
    public void getJMDLLCount(String ETLtable) {
        switch (ETLtable){
            case "etl_accountable_product_dq_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_ACCOUNTABLE_PRODUCT;
                break;
            case "etl_wwork_dq_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_WORK;
                break;
            case "etl_work_identifier_dq_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_WORK_IDENTIFIER;
                break;
            case "etl_work_subject_area_dq_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_WORK_SUBJECT_AREA;
                break;
            case "etl_work_person_role_dq_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_WORK_PERSON_ROLE_Count;
                break;
            case "etl_manifestation_updates1_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_MANIFESTATION_UPDATES;
                break;
            case "etl_manifestation_dq_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_MANIFESTATION;
                break;
            case "etl_manifestation_identifier_dq_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_MANIFESTATION_IDENTIFIER;
                break;
            case "etl_product_part1_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_PRODUCT_PART;
                break;
            case "etl_product_inserts_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_PRODUCT_INSERT;
                break;
            case "etl_product_updates_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_PRODUCT_UPDATES;
                break;
            case "etl_product_dq_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_PRODUCT;
                break;
            case "etl_product_person_role_dq_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_PRODUCT_PERSON_ROLE;
                break;
            case "jm_imprint_data_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_IMPRINT_DATA;
                break;
            case "sd_subject_areas_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_SD_SUBJECT_AREAS;
                break;
            case "works_attrs_roles_people_v":
                sqlDL = JM_ETLCoreCountChecksSQL.GET_JMF_STAGING_WORKS_ATTRS_ROLES_PEOPLE;
                break;
        }
        Log.info(sqlDL);
        List<Map<String, Object>> DPPCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLJMCount = ((Long) DPPCountDL.get(0).get("count")).intValue();
        Log.info(ETLtable + " data in DL: " + DLJMCount);
    }

    @Then("^Compare count of the JM count for (.*) table between JM ETL Core and DL are identical$")
    public void JMcompareMYSQLtoDL(String ETLtable) {
        Assert.assertEquals("The counts between " + ETLtable + " is not equal", JMCount, DLJMCount);
        Log.info("The count for " + ETLtable + " table between MYSQL: " + JMCount + " and DL: " + DLJMCount + " is equal");
    }

}