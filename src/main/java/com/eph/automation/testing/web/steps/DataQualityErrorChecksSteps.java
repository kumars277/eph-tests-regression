package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityErrorContext;
import com.eph.automation.testing.models.dao.DQErrorChecksDataObject;
import com.eph.automation.testing.services.db.sql.DQErrorChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataQualityErrorChecksSteps {
    public DataQualityErrorContext DQErrorContext;
    public String sql;

    private String numberOfRecords;
    private static List<String> ids;
    private static List<String> allFailedIds;
    private static List<String> allFailedManifestations;
    private static List<String> allFailedProducts;
    private static List<String> manifestationSourceId;
    private static List<String> ephId;
    private static List<String> ephIdProducts;

    @Given("^We have failed data in Work DQ table$")
    public void getFailedIds() {

        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber") != null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        } else {
            numberOfRecords = "10";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = DQErrorChecksSQL.GET_RANDOM_RECORDS_FAILED_WORKS_DQ.replace("PARAM1", numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomIds.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Random ids of failed works : " + ids.toString());

        sql = DQErrorChecksSQL.GET_All_FAILED_WORKS_DQ;
        Log.info(sql);
        List<Map<?, ?>> failedWorks = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        allFailedIds = failedWorks.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Ids of all failed works : " + ids.toString());

        sql = String.format(DQErrorChecksSQL.GET_EPH_ID, Joiner.on("','").join(allFailedIds));
        Log.info("Get eph ids : " + sql);
        List<Map<?, ?>> randomWorkID = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ephId = randomWorkID.stream().map(m -> (String) m.get("id")).collect(Collectors.toList());
        Log.info("EPH IDs : " + ephId.toString());
    }

    @When("^We get the data from all child entities and dq tables by work id$")
    public void getChildData() {
        sql = String.format(DQErrorChecksSQL.GET_EPH_DATA_CHILD_ENTITIES, Joiner.on("','").join(ids));
        Log.info(sql);
        DQErrorContext.eph_data = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);

        sql = String.format(DQErrorChecksSQL.GET_MANIFESTATION_DQ, Joiner.on("','").join(ids));
        Log.info("Select all child manifestations of failed works : " + sql);
        DQErrorContext.manifestation_result = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);

        sql = String.format(DQErrorChecksSQL.GET_Product_DQ_Linked_work, Joiner.on("','").join(ids));
        Log.info("Select all child products of failed works : " + sql);
        DQErrorContext.product_result = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);


    }

    @Then("^The data has not reached EPH$")
    public void checkChildEntities() {
        for (int i = 0; i < DQErrorContext.manifestation_result.size(); i++) {
            if (DQErrorContext.manifestation_result.get(i).getDq_err().equalsIgnoreCase("N")) {
                Assert.fail("Manifestation " + DQErrorContext.manifestation_result.get(i).getPmx_source() + " linked to failed work did not fail!");
            } else {
                Log.info("Manifestation " + DQErrorContext.manifestation_result.get(i).getPmx_source() + " has failed as expected!");
            }
        }

        for (int i = 0; i < DQErrorContext.product_result.size(); i++) {
            if (DQErrorContext.product_result.get(i).getDq_err().equalsIgnoreCase("N")) {
                Assert.fail("Product " + DQErrorContext.product_result.get(i).getPmx_source() + " linked to failed work did not fail!");
            } else {
                Log.info("Product " + DQErrorContext.product_result.get(i).getPmx_source() + " has failed as expected!");
            }
        }

/*        if(DQErrorContext.eph_data.isEmpty()){
            Log.info("There is no failed work data in EPH");
        }else{
            for (int i=0;i<DQErrorContext.eph_data.size();i++) {
                Log.info("There is child data for id " + DQErrorContext.eph_data.get(i).eph_id);
            }
            Assert.fail("There is child data for failed records!");
        }*/
    }

    @And("^There is no EPH id created$")
    public void checkEPHId() {
        if (ephId.isEmpty()) {
            Log.info("There was no EPH id created!");
        } else {
            Log.info("EPH id was created for failed record!");
        }
    }

    @Given("^We have failed data in Manifestation DQ table$")
    public void getFailedManifestationIds() {

        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber") != null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        } else {
            numberOfRecords = "20";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = DQErrorChecksSQL.GET_RANDOM_RECORDS_FAILED_MANIFESTATIONS_DQ.replace("PARAM1", numberOfRecords);
        Log.info("Select random failed manifestation records : " + sql);
        List<Map<?, ?>> randomISBNIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        manifestationSourceId = randomISBNIds.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Ids of randomly selected failed manifestations: " + manifestationSourceId.toString());

        sql = DQErrorChecksSQL.GET_All_FAILED_MANIFESTATIONS_DQ;
        Log.info("Select all failed manifestations : " + sql);
        List<Map<?, ?>> failedManifestations = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        allFailedManifestations = failedManifestations.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Ids of all failed manifestations : " + allFailedManifestations.toString());

        sql = String.format(DQErrorChecksSQL.GET_EPH_Manifestation_ID, Joiner.on("','").join(allFailedManifestations));
        Log.info(sql);
        List<Map<?, ?>> randomManifestationId = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ephId = randomManifestationId.stream().map(m -> (String) m.get("manifestation_id")).collect(Collectors.toList());
        Log.info("" + ephId.toString());
    }

    @When("^We get the data from product dq table$")
    public void getProductDQData() {
        sql = String.format(DQErrorChecksSQL.GET_Product_DQ_Linked_manifestation, Joiner.on("','").join(manifestationSourceId));
        Log.info(sql);
        DQErrorContext.product_result_by_manifestation = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);
    }

    @Then("^The product data has failed$")
    public void checkProductDQ() {
        for (int i = 0; i < DQErrorContext.product_result_by_manifestation.size(); i++) {
            if (DQErrorContext.product_result_by_manifestation.get(i).getDq_err().equalsIgnoreCase("N")) {
                Assert.fail("Product " + DQErrorContext.product_result_by_manifestation.get(i).getPmx_id() + " linked to failed manifestation did not fail!");
            } else {
                Log.info("Product " + DQErrorContext.product_result_by_manifestation.get(i).getPmx_id() + " has failed as expected!");
            }
        }

        sql = DQErrorChecksSQL.GET_All_FAILED_Products;
        Log.info(sql);
        List<Map<?, ?>> randomProducts = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        allFailedProducts = randomProducts.stream().map(m -> (String) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info(allFailedProducts.toString());

        sql = String.format(DQErrorChecksSQL.GET_EPH_ID, Joiner.on("','").join(allFailedProducts));
        // Removed .replace method, replacing WORK with PRODUCT (was unused)
        Log.info(sql);
        List<Map<?, ?>> productIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ephIdProducts = productIds.stream().map(m -> (String) m.get("id")).collect(Collectors.toList());
        // changed "work_id" to "id"
        Log.info(ephIdProducts.toString());

        Assert.assertTrue("There are failed products with EPH ID", ephIdProducts.isEmpty());
        Log.info("None of the failed products has an EPH ID!");

    }

    @Given("^We have a person with missing last name$")
    public void getFailedPersons() {

        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber") != null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        } else {
            numberOfRecords = "10";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = DQErrorChecksSQL.GET_RANDOM_DATA_FAILED_PERSONS_DQ.replace("PARAM1", numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>>  randomFailedRecords = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomFailedRecords.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Person source ref ids of failed persons" + ids.toString());

    }

//    @When("^We get the data from the person dq table$")
//    public void getDqDataFailedPersonRecords() {
//        sql = String.format(DQErrorChecksSQL.GET_DQ_STATUS_PERSONS, Joiner.on("','").join(ids));
//        Log.info(sql);
//        DQErrorContext.dqFailedRecordsData = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);
//
//
//    }

//    @Then("^There is a dq error for this person$")
//    public void checkPersonData() {
//        for (int i = 0; i < DQErrorContext.dqFailedRecordsData.size(); i++) {
//            if (DQErrorContext.dqFailedRecordsData.get(i).getDq_err().equalsIgnoreCase("N")) {
//                Assert.fail("Person " + DQErrorContext.dqFailedRecordsData.get(i).getPerson_source_ref() + " did not fail!");
//            } else {
//                Log.info("Person " + DQErrorContext.dqFailedRecordsData.get(i).getPerson_source_ref() + " has failed as expected!");
//
//            }
//        }
//    }

//    @And("^There is a record in dq error table for this person")
//    public void checkDQErrorRecordTable() {
//        Log.info("Get the id of the last etl run ...");
//        sql = DQErrorChecksSQL.GET_LAST_ETL_ID;
//        Log.info(sql);
//
//        List<Map<String, Object>> listEtlIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
//        String etlId = (String) listEtlIds.get(0).get("etl_id");
//
//
//        for (int i = 0; i < DQErrorContext.dqFailedRecordsData.size(); i++) {
//            String personSourceRef = DQErrorContext.dqFailedRecordsData.get(i).getPerson_source_ref();
//            sql = String.format(DQErrorChecksSQL.GET_DQ_FAIL_RECORDS, personSourceRef, etlId);
//            Log.info(sql);
//
//            DQErrorContext.dqFailedRecordsData = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);
//            Log.info("Check there is a record added in dq fail record table for person_with id " + personSourceRef);
//            Assert.assertTrue(DQErrorContext.dqFailedRecordsData.size() == 1);
//
//        }
//    }

    @And("^There is no EPH data for the person$")
    public void checkEPHSAPerson() {
        for (int i = 0; i<DQErrorContext.dqFailedRecordsData.size(); i++){
            sql = DQErrorChecksSQL.GET_EPH_ID_PERSONS.replace("PARAM1","PERSON-"+
                    DQErrorContext.dqFailedRecordsData.get(i).getPerson_source_ref());
            Log.info(sql);

            DQErrorContext.person_eph_id = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);
            Assert.assertTrue("There is eph id for failed person", DQErrorContext.person_eph_id.isEmpty());

        Log.info("Check that failed persons are not going to SA table ..");
        sql = String.format(DQErrorChecksSQL.GET_PERSONS_SA, Joiner.on("','").join(ids));
        Log.info("Get persons from SA : " + sql);

        DQErrorContext.saPersons = DBManager
                .getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);

        Assert.assertTrue("Found failed persons in SA", DQErrorContext.saPersons.isEmpty());

        }
    }

    @Given("^We have a work with missing type$")
    public void getFailedWorksMissingType() {

        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber") != null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        } else {
            numberOfRecords = "10";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

//        Re-written below line to specify "wwork" and "f_status" was originally more dynamic with a String of column
        sql = String.format(DQErrorChecksSQL.GET_RANDOM_DATA_FAILED_RECORDS ,"wwork" ,"f_status" ,numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomFailedRecords = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomFailedRecords.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("PMX source reference ids of failed works" + ids.toString());

        if (ids.isEmpty())
                Log.info("No failed records found in dq");

    }


    @Given("^We have an (.*) which (.*) is unknown$")
    public void getFailedRecordsDQ(String entity, String column) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber") != null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        } else {
            numberOfRecords = "10";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        if(column.equals("work_title"))
            sql = String.format(DQErrorChecksSQL.GET_RANDOM_DATA_FAILED_WORKS_DQ_NO_TITLE, entity, column ,numberOfRecords);

        sql = String.format(DQErrorChecksSQL.GET_RANDOM_DATA_FAILED_RECORDS, entity, column ,numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomFailedRecords = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomFailedRecords.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("PMX source reference ids of failed " + entity + " : " + ids.toString());

        if (ids.isEmpty())
            Log.info("No failed records found in dq");

    }

    @When("^We get the data from the (.*) dq table$")
    public void getDqDataFailedRecords(String entity) {
        if (!ids.isEmpty()) {
            if (entity.equals("person")) {
                sql = String.format(DQErrorChecksSQL.GET_DQ_STATUS_PERSONS, Joiner.on("','").join(ids));

//            Added else if >>
            } else if (entity.equals("work") || entity.equals("wwork")){
                sql = String.format(DQErrorChecksSQL.GET_DQ_STATUS_WORKS, Joiner.on("','").join(ids));
        } else
            sql = String.format(DQErrorChecksSQL.GET_DQ_STATUS, entity, Joiner.on("','").join(ids));
            Log.info(sql);
            DQErrorContext.dqFailedRecordsData = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);
        }
    }

    @Then("^There is a dq error for this (.*)$")
    public void checkDqError(String entity) {
        if (!ids.isEmpty()) {
            for (int i = 0; i < DQErrorContext.dqFailedRecordsData.size(); i++) {
                /*if (DQErrorContext.dqFailedRecordsData.get(i).getDq_err().equalsIgnoreCase("N")) {
                    Assert.fail(entity + " " + DQErrorContext.dqFailedRecordsData.get(i).getPmx_id() + " did not fail!");
                } else {*/
                if (DQErrorContext.dqFailedRecordsData.get(i).getDq_err().equalsIgnoreCase("Y")) {
                    Log.info(entity + " " + DQErrorContext.dqFailedRecordsData.get(i).getPmx_id() + " has failed as expected!");
                }
            }
        }
    }


    @And("^There is a record in dq error table for this (.*)")
    public void checkDqErrorRecordTable(String entity) {
        if (!ids.isEmpty()) {

            Log.info("Get the id of the last etl run ...");
            sql = DQErrorChecksSQL.GET_LAST_ETL_ID;
            Log.info(sql);

            List<Map<String, Object>> listEtlIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            String etlId = (String) listEtlIds.get(0).get("etl_id");


            for (int i = 0; i < DQErrorContext.dqFailedRecordsData.size(); i++) {
                String id = DQErrorContext.dqFailedRecordsData.get(i).getPmx_id();
                sql = String.format(DQErrorChecksSQL.GET_DQ_FAIL_RECORDS, id, etlId);
                Log.info(sql);

                DQErrorContext.dqFailedRecordsData = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);
                Log.info("Check there is a record added in dq fail record table for " + entity +" with id " + id);
                Assert.assertTrue(DQErrorContext.dqFailedRecordsData.size() == 1);

            }
        }
    }


    @And("^There is no EPH data for this (.*)$")
    public void checkEPHSAWork(String ref_type) {
        if (!ids.isEmpty()) {

            for (int i = 0; i < DQErrorContext.dqFailedRecordsData.size(); i++) {
                sql = String.format(DQErrorChecksSQL.GET_EPH_ID, ref_type, DQErrorContext.dqFailedRecordsData.get(i).getPmx_id());
                Log.info(sql);
                DQErrorContext.dqFailedRecordsData = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);
                Assert.assertTrue("There is eph id for failed " + ref_type, DQErrorContext.dqFailedRecordsData.isEmpty());
        }
        }
    }

    @Given("^We have a work with missing title$")
    public void getFailedWorksMissingTitle() {

        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber") != null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        } else {
            numberOfRecords = "10";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = DQErrorChecksSQL.GET_RANDOM_DATA_FAILED_WORKS_DQ_NO_TITLE.replace("PARAM1", numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>>  randomFailedRecords = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomFailedRecords.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Ids of failed work" + ids.toString());

    }

    @Given("^We have an (.*) which record end date is after July 8th 2019$")
    public void getFailedRecordsDQRecEndDate(String entity) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber") != null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        } else {
            numberOfRecords = "10";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        if(entity.equals("product")) {
            sql = String.format(DQErrorChecksSQL.GET_RANDOM_PRODUCTS_FAILED_REC_END_DATE, numberOfRecords);
            Log.info(sql);
        }
        else if(entity.equals("wwork")) {
            sql = String.format(DQErrorChecksSQL.GET_RANDOM_WORKS_FAILED_REC_END_DATE, numberOfRecords);
            Log.info(sql);
        }
        else if(entity.equals("manifestation")) {
            sql = String.format(DQErrorChecksSQL.GET_RANDOM_MANIFESTATIONS_FAILED_REC_END_DATE, numberOfRecords);
            Log.info(sql);
        }


        List<Map<?, ?>> randomFailedRecords = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomFailedRecords.stream().map(m ->  m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info(entity + " ids of failed " + entity + " : " + ids.toString());

        if (ids.isEmpty())
            Log.info("No failed records found in dq");

    }
}
