package com.eph.automation.testing.web.steps.EPHDatalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityEPHDLContext;
import com.eph.automation.testing.models.dao.EPHDataLake.PersonDataDLObject;
import com.eph.automation.testing.services.db.EPHDataLakeSql.GetDLDBUser;
import com.eph.automation.testing.services.db.EPHDataLakeSql.PersonGDGHTablesDataChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class PersonGDGHTablesDataCheckSteps {

    @StaticInjection
    public DataQualityEPHDLContext dataQualityEPHDLContext;
    private static String sql;
    private static List<String> Ids;
    private PersonGDGHTablesDataChecksSQL personObj = new PersonGDGHTablesDataChecksSQL();


    @Given("^We get (.*) random person ids of (.*)")
    public void getRandomPersonIds(String numberOfRecords, String tableName) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random records ..");

        //Get property when running with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);

        switch (tableName){

            case "gd_person":
                sql = String.format(PersonGDGHTablesDataChecksSQL.GET_RANDOM_PERSON_ID, numberOfRecords);
                List<Map<?, ?>> randomPersonIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomPersonIds.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_person":
                sql = String.format(PersonGDGHTablesDataChecksSQL.GET_RANDOM_GH_PERSON_ID, numberOfRecords);
                List<Map<?, ?>> randomGHPersonIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomGHPersonIds.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_product_person_role":
                sql = String.format(PersonGDGHTablesDataChecksSQL.GET_RANDOM_GD_PRODUCT_PERSON_ID, numberOfRecords);
                List<Map<?, ?>> randomGDProductPersonIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomGDProductPersonIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_PERSON_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_product_person_role":
                sql = String.format(PersonGDGHTablesDataChecksSQL.GET_RANDOM_GH_PRODUCT_PERSON_ID, numberOfRecords);
                List<Map<?, ?>> randomGHProductPersonIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomGHProductPersonIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_PERSON_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_work_person_role":
                sql = String.format(PersonGDGHTablesDataChecksSQL.GET_RANDOM_GD_WORK_PERSON_ID, numberOfRecords);
                List<Map<?, ?>> randomGDWorkPersonIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomGDWorkPersonIds.stream().map(m -> (BigDecimal) m.get("WORK_PERSON_ROLE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_work_person_role":
                sql = String.format(PersonGDGHTablesDataChecksSQL.GET_RANDOM_GH_WORK_PERSON_ID, numberOfRecords);
                List<Map<?, ?>> randomGHWorkPersonIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomGHWorkPersonIds.stream().map(m -> (BigDecimal) m.get("WORK_PERSON_ROLE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }



    @When("^We get the person records from EPH of (.*)$")
    public void getPersonEPH(String tableName) {
        Log.info("We get the person records from EPH..");
        sql = String.format(personObj.personBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbPersonDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the person records from DL of (.*)$")
    public void getPersonDL(String tableName) {
        Log.info("We get the person records from DL..");
        sql = String.format(personObj.personBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbPersonDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare person records in EPH and DL of (.*)$")
    public void comparePersonDataEPHtoDL(String tableName) {
        if (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the GD person records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getPERSON_ID));
                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getPERSON_ID));
                if(tableName.equals("gh_person")){
                    dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getPERSON_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PERSON_ID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getPERSON_ID());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " B_CLASSNAME => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_person")){
                    Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }else{
                    Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " B_UPDDATE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());


                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " GIVEN_NAME => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getGIVEN_NAME()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getGIVEN_NAME());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getGIVEN_NAME() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getGIVEN_NAME()!= null)) {
                    Assert.assertEquals("The GIVEN_NAME is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getGIVEN_NAME(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getGIVEN_NAME());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " S_GIVEN_NAME => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getS_GIVEN_NAME()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getS_GIVEN_NAME());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getS_GIVEN_NAME() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getS_GIVEN_NAME()!= null)) {
                    Assert.assertEquals("The S_GIVEN_NAME is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getS_GIVEN_NAME(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getS_GIVEN_NAME());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " FAMILY_NAME => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getFAMILY_NAME()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getFAMILY_NAME());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getFAMILY_NAME() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getFAMILY_NAME()!= null)) {
                    Assert.assertEquals("The FAMILY_NAME is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getFAMILY_NAME(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getFAMILY_NAME());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " S_FAMILY_NAME => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getS_FAMILY_NAME()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getS_FAMILY_NAME());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getS_FAMILY_NAME() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getS_FAMILY_NAME()!= null)) {
                    Assert.assertEquals("The S_FAMILY_NAME is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getS_FAMILY_NAME(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getS_FAMILY_NAME());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " PEOPLEHUB_ID => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPEOPLEHUB_ID()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getPEOPLEHUB_ID());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPEOPLEHUB_ID() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getPEOPLEHUB_ID()!= null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPEOPLEHUB_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getPEOPLEHUB_ID());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " EMAIL => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEMAIL()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEMAIL());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEMAIL() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEMAIL()!= null)) {
                    Assert.assertEquals("The EMAIL is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEMAIL(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEMAIL());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " S_EMAIL => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getS_EMAIL()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getS_EMAIL());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getS_EMAIL() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getS_EMAIL()!= null)) {
                    Assert.assertEquals("The S_EMAIL is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getS_EMAIL(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getS_EMAIL());
                }
            }
        }
    }

    @When("^We get the product person records from EPH of (.*)$")
    public void getProductPersonEPH(String tableName) {
        Log.info("We get the product person records from EPH..");
        sql = String.format(personObj.productPersonBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbPersonDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.EPH_URL);
    }

    @When("^We get the product person records from DL of (.*)$")
    public void getProductPersonDL(String tableName) {
        Log.info("We get the product person records from EPH..");
        sql = String.format(personObj.productPersonBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbPersonDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.AWS_URL);
    }

    @And ("^Compare product person records in EPH and DL of (.*)$")
    public void compareProductPersonDataEPHtoDL(String tableName) {
        if (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the product person records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getPRODUCT_PERSON_ROLE_ID));
                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getPRODUCT_PERSON_ROLE_ID));
                if(tableName.equals("gh_product_person_role")){
                    dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getPRODUCT_PERSON_ROLE_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_PERSON_ROLE_ID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getPRODUCT_PERSON_ROLE_ID());
                }

                Log.info("ID => " + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID() +
                        " B_CLASSNAME => EPH=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_product_person_role")){
                    Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " B_UPDDATE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " EFFECTIVE_START_DATE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " EFFECTIVE_START_DATE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " F_ROLE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE()!= null)) {
                    Assert.assertEquals("The F_ROLE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " F_PRODUCT => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PRODUCT()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_PRODUCT());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PRODUCT() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_PRODUCT()!= null)) {
                    Assert.assertEquals("The F_PRODUCT is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PRODUCT(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_PRODUCT());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " F_PERSON => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON()!= null)) {
                    Assert.assertEquals("The F_PERSON is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " F_EVENT => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT()!= null)) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

    @When("^We get the work person role records from EPH of (.*)$")
    public void getWorkPersonRoleEPH(String tableName) {
        Log.info("We get the work person role records from EPH..");
        sql = String.format(personObj.personWorkBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbPersonDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the work person role records from DL of (.*)$")
    public void getWorkPersonRoleDL(String tableName) {
        Log.info("We get the work person records from DL..");
        sql = String.format(personObj.personWorkBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbPersonDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.AWS_URL);
    }

    @And ("^Compare work person role records in EPH and DL of (.*)$")
    public void compareGDWorkPersonDataEPHtoDL(String tableName) {
        if (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the GD work person records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getWORK_PERSON_ROLE_ID));
                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getWORK_PERSON_ROLE_ID));

                if(tableName.equals("gh_work_person_role")){
                    dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getWORK_PERSON_ROLE_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The WORK_PERSON_ROLE_ID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getWORK_PERSON_ROLE_ID());
                }

                Log.info("ID => " + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID() +
                        " B_CLASSNAME => EPH=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_work_person_role")){
                    Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " B_UPDDATE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " EFFECTIVE_START_DATE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " EFFECTIVE_START_DATE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " F_ROLE => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE()!= null)) {
                    Assert.assertEquals("The F_ROLE is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " F_WWORK => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_WWORK()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_WWORK());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_WWORK() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_WWORK()!= null)) {
                    Assert.assertEquals("The F_WWORK is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_WWORK(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_WWORK());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " F_PERSON => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON()!= null)) {
                    Assert.assertEquals("The F_PERSON is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " F_EVENT => EPH="+ dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+ dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT()!= null)) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityEPHDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT());
                }

            }
        }
    }

}





