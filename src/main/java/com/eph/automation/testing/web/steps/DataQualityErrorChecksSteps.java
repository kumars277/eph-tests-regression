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
import cucumber.api.java.en_scouse.An;
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
    public void getFailedIds(){

        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber")!=null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        }else {
            numberOfRecords = "10";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = DQErrorChecksSQL.GET_FAILED_DATA.replace("PARAM1", numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomISBNIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomISBNIds.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());

        sql = DQErrorChecksSQL.GET_All_FAILED_DATA;
        Log.info(sql);
        List<Map<?, ?>> failedWorks = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        allFailedIds = failedWorks.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());

        sql = String.format(DQErrorChecksSQL.GET_EPH_ID, Joiner.on("','").join(allFailedIds));
        Log.info(sql);
        List<Map<?, ?>> randomWorkID = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ephId = randomWorkID.stream().map(m -> (String) m.get("work_id")).collect(Collectors.toList());
        Log.info(ephId.toString());
    }

    @When("^We get the data from all child entities and dq tables by work id$")
    public void getChildData(){/*
        sql = String.format(DQErrorChecksSQL.GET_EPH_DATA_CHILD_ENTITIES, Joiner.on("','").join(workid));
        Log.info(sql);
        DQErrorContext.eph_data = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);*/

        sql = String.format(DQErrorChecksSQL.GET_MANIFESTATION_DQ, Joiner.on("','").join(ids));
        Log.info(sql);
        DQErrorContext.manifestation_result = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);

        sql = String.format(DQErrorChecksSQL.GET_Product_DQ_Linked_work, Joiner.on("','").join(ids));
        Log.info(sql);
        DQErrorContext.product_result = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);


    }

    @Then("^The data has not reached EPH$")
    public void checkChildEntities(){
        for (int i=0;i<DQErrorContext.manifestation_result.size();i++){
            if (DQErrorContext.manifestation_result.get(i).dq_err.equalsIgnoreCase("N")){
                Assert.fail("Manifestation "+ DQErrorContext.manifestation_result.get(i).pmx_source +" linked to failed work did not fail!");
            }else{
                Log.info("Manifestation "+ DQErrorContext.manifestation_result.get(i).pmx_source +" has failed as expected!");
            }
        }

        for (int i=0;i<DQErrorContext.product_result.size();i++){
            if (DQErrorContext.product_result.get(i).dq_err.equalsIgnoreCase("N")){
                Assert.fail("Product "+ DQErrorContext.product_result.get(i).pmx_source +" linked to failed work did not fail!");
            }else{
                Log.info("Product "+ DQErrorContext.product_result.get(i).pmx_source +" has failed as expected!");
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
    public void checkEPHId(){
        if(ephId.isEmpty()){
            Log.info("There was no EPH id created!");
        }else{
            Assert.fail("EPH id was created for failed record!");
        }
    }

    @Given("^We have failed data in Manifestation DQ table$")
    public void getFailedManifestationIds(){

        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber")!=null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        }else {
            numberOfRecords = "20";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = DQErrorChecksSQL.GET_FAILED_Manifestation_DATA.replace("PARAM1", numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomISBNIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        manifestationSourceId = randomISBNIds.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info(manifestationSourceId.toString());

        sql = DQErrorChecksSQL.GET_All_FAILED_Manifestations;
        Log.info(sql);
        List<Map<?, ?>> failedManifestations = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        allFailedManifestations = failedManifestations.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());

        sql = String.format(DQErrorChecksSQL.GET_EPH_Manifestation_ID, Joiner.on("','").join(allFailedManifestations));
        Log.info(sql);
        List<Map<?, ?>> randomManifestationId = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ephId = randomManifestationId.stream().map(m -> (String) m.get("manifestation_id")).collect(Collectors.toList());
        Log.info(ephId.toString());
    }

    @When("^We get the data from product dq table$")
    public void getProductDQData(){
        sql = String.format(DQErrorChecksSQL.GET_Product_DQ_Linked_manifestation, Joiner.on("','").join(manifestationSourceId));
        Log.info(sql);
        DQErrorContext.product_result_by_manifestation = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);
    }

    @Then("^The product data has failed$")
    public void checkProductDQ(){
        for (int i=0;i<DQErrorContext.product_result_by_manifestation.size();i++){
            if (DQErrorContext.product_result_by_manifestation.get(i).dq_err.equalsIgnoreCase("N")){
                Assert.fail("Product "+ DQErrorContext.product_result_by_manifestation.get(i).pmx_source +" linked to failed manifestation did not fail!");
            }else{
                Log.info("Product "+ DQErrorContext.product_result_by_manifestation.get(i).pmx_source +" has failed as expected!");
            }
        }

        sql = DQErrorChecksSQL.GET_All_FAILED_Products;
        Log.info(sql);
        List<Map<?, ?>> randomProducts = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        allFailedProducts = randomProducts.stream().map(m -> (String) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info(allFailedProducts.toString());

        sql = String.format(DQErrorChecksSQL.GET_EPH_ID.replace("WORK","PRODUCT"), Joiner.on("','").join(allFailedProducts));
        Log.info(sql);
        List<Map<?, ?>> productIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ephIdProducts = productIds.stream().map(m -> (String) m.get("work_id")).collect(Collectors.toList());
        Log.info(ephIdProducts.toString());

        Assert.assertTrue("There are failed products with EPH ID", ephIdProducts.isEmpty());
        Log.info("None of the failed products has an EPH ID!");

    }

    @Given("^We have a person with missing last name$")
    public void getFailedPersons(){

        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber")!=null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        }else {
            numberOfRecords = "10";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = DQErrorChecksSQL.GET_FAILED_Person_DATA.replace("PARAM1", numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomISBNIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomISBNIds.stream().map(m -> (BigDecimal) m.get("pmx_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());

    }

    @When("^We get the data from the dq table$")
    public void getDqData(){
        sql = String.format(DQErrorChecksSQL.GET_DQ_Status_Person, Joiner.on("','").join(ids));
        Log.info(sql);
        DQErrorContext.person_data = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);
    }

    @Then("^There is a dq error for this person$")
    public void checkPersonData(){
        for (int i=0;i<DQErrorContext.person_data.size();i++){
            if (DQErrorContext.person_data.get(i).dq_err.equalsIgnoreCase("N")){
                Assert.fail("Person "+ DQErrorContext.person_data.get(i).pmx_source +" did not fail!");
            }else{
                Log.info("Person "+ DQErrorContext.person_data.get(i).pmx_source +" has failed as expected!");
            }
        }
    }

    @And("^There is no EPH data for this person$")
    public void checkEPHIdPerson(){
        for (int i=0;i<DQErrorContext.person_data.size();i++){
            sql = DQErrorChecksSQL.GET_Person_EPH_ID.replace("PARAM1","PERSON-"+
                    DQErrorContext.person_data.get(i).pmx_source);
            Log.info(sql);
            DQErrorContext.person_eph_id = DBManager.getDBResultAsBeanList(sql, DQErrorChecksDataObject.class, Constants.EPH_URL);
            Assert.assertTrue("There is eph id for failed person", DQErrorContext.person_eph_id.isEmpty());
        }
    }
}
