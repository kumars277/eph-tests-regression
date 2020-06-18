package com.eph.automation.testing.web.steps.datalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityDLContext;
import com.eph.automation.testing.models.dao.datalake.PersonDataDLObject;
import com.eph.automation.testing.services.db.DataLakeSql.GetDLDBUser;
import com.eph.automation.testing.services.db.DataLakeSql.PersonGDGHTablesDataChecksSQL;
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
    public DataQualityDLContext dataQualityDLContext;
    private static String sql;
    private static List<String> Ids;
    private PersonGDGHTablesDataChecksSQL personObj = new PersonGDGHTablesDataChecksSQL();


    @Given("^We get (.*) random person ids of (.*)")
    public void getRandomPersonIds(String numberOfRecords, String tableName) {
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
        dataQualityDLContext.tbPersonDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the person records from DL of (.*)$")
    public void getPersonDL(String tableName) {
        Log.info("We get the person records from DL..");
        sql = String.format(personObj.personBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbPersonDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare person records in EPH and DL of (.*)$")
    public void comparePersonDataEPHtoDL(String tableName) {
        if (dataQualityDLContext.tbPersonDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the GD person records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbPersonDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getPERSON_ID));
                dataQualityDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getPERSON_ID));
                if(tableName.equals("gh_person")){
                    dataQualityDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPERSON_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PERSON_ID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPERSON_ID());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " B_CLASSNAME => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_person")){
                    Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                            " B_FROMBATCHID => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                                dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                            " B_TOBATCHID => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                                dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }else{
                    Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                            " B_BATCHID => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                                dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " B_UPDDATE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());


                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " EXTERNAL_REFERENCE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " GIVEN_NAME => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getGIVEN_NAME()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getGIVEN_NAME());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getGIVEN_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getGIVEN_NAME().equals("null"))) {
                    Assert.assertEquals("The GIVEN_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getGIVEN_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getGIVEN_NAME());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " S_GIVEN_NAME => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_GIVEN_NAME()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_GIVEN_NAME());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_GIVEN_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_GIVEN_NAME().equals("null"))) {
                    Assert.assertEquals("The S_GIVEN_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_GIVEN_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_GIVEN_NAME());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " FAMILY_NAME => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getFAMILY_NAME()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getFAMILY_NAME());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getFAMILY_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getFAMILY_NAME().equals("null"))) {
                    Assert.assertEquals("The FAMILY_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getFAMILY_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getFAMILY_NAME());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " S_FAMILY_NAME => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_FAMILY_NAME()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_FAMILY_NAME());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_FAMILY_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_FAMILY_NAME().equals("null"))) {
                    Assert.assertEquals("The S_FAMILY_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_FAMILY_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_FAMILY_NAME());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " PEOPLEHUB_ID => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPEOPLEHUB_ID()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPEOPLEHUB_ID());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPEOPLEHUB_ID() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPEOPLEHUB_ID().equals("null"))) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPEOPLEHUB_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPEOPLEHUB_ID());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " EMAIL => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEMAIL()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEMAIL());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEMAIL() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEMAIL().equals("null"))) {
                    Assert.assertEquals("The EMAIL is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEMAIL(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEMAIL());
                }
                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID()+
                        " S_EMAIL => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_EMAIL()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_EMAIL());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_EMAIL() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_EMAIL().equals("null"))) {
                    Assert.assertEquals("The S_EMAIL is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_EMAIL(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_EMAIL());
                }
            }
        }
    }

    @When("^We get the product person records from EPH of (.*)$")
    public void getProductPersonEPH(String tableName) {
        Log.info("We get the product person records from EPH..");
        sql = String.format(personObj.productPersonBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbPersonDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.EPH_URL);
    }

    @When("^We get the product person records from DL of (.*)$")
    public void getProductPersonDL(String tableName) {
        Log.info("We get the product person records from EPH..");
        sql = String.format(personObj.productPersonBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbPersonDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.AWS_URL);
    }

    @And ("^Compare product person records in EPH and DL of (.*)$")
    public void compareProductPersonDataEPHtoDL(String tableName) {
        if (dataQualityDLContext.tbPersonDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the product person records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i <dataQualityDLContext.tbPersonDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getPRODUCT_PERSON_ROLE_ID));
                dataQualityDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getPRODUCT_PERSON_ROLE_ID));
                if(tableName.equals("gh_product_person_role")){
                    dataQualityDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPRODUCT_PERSON_ROLE_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_PERSON_ROLE_ID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPRODUCT_PERSON_ROLE_ID());
                }

                Log.info("ID => " + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID() +
                        " B_CLASSNAME => EPH=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_product_person_role")){
                    Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                            " B_FROMBATCHID => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                                dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                            " B_TOBATCHID => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                                dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                            " B_BATCHID => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                                dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " B_UPDDATE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " EXTERNAL_REFERENCE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " EFFECTIVE_START_DATE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " EFFECTIVE_START_DATE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " F_ROLE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE().equals("null"))) {
                    Assert.assertEquals("The F_ROLE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " F_PRODUCT => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PRODUCT()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_PRODUCT());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PRODUCT() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_PRODUCT().equals("null"))) {
                    Assert.assertEquals("The F_PRODUCT is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PRODUCT(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_PRODUCT());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " F_PERSON => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON().equals("null"))) {
                    Assert.assertEquals("The F_PERSON is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " F_EVENT => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

    @When("^We get the work person role records from EPH of (.*)$")
    public void getWorkPersonRoleEPH(String tableName) {
        Log.info("We get the work person role records from EPH..");
        sql = String.format(personObj.personWorkBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbPersonDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the work person role records from DL of (.*)$")
    public void getWorkPersonRoleDL(String tableName) {
        Log.info("We get the work person records from DL..");
        sql = String.format(personObj.personWorkBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbPersonDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.AWS_URL);
    }

    @And ("^Compare work person role records in EPH and DL of (.*)$")
    public void compareGDWorkPersonDataEPHtoDL(String tableName) {
        if (dataQualityDLContext.tbPersonDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the GD work person records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i <dataQualityDLContext.tbPersonDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getWORK_PERSON_ROLE_ID));
                dataQualityDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getWORK_PERSON_ROLE_ID));

                if(tableName.equals("gh_work_person_role")){
                    dataQualityDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getWORK_PERSON_ROLE_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The WORK_PERSON_ROLE_ID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getWORK_PERSON_ROLE_ID());
                }

                Log.info("ID => " + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID() +
                        " B_CLASSNAME => EPH=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_work_person_role")){
                    Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                            " B_FROMBATCHID => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                                dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                            " B_TOBATCHID => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                                dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                            " B_BATCHID => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                                dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " B_UPDDATE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " EXTERNAL_REFERENCE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " EFFECTIVE_START_DATE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPRODUCT_PERSON_ROLE_ID()+
                        " EFFECTIVE_START_DATE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " F_ROLE => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE().equals("null"))) {
                    Assert.assertEquals("The F_ROLE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_ROLE(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_ROLE());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " F_WWORK => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_WWORK()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_WWORK());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_WWORK() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_WWORK().equals("null"))) {
                    Assert.assertEquals("The F_WWORK is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_WWORK(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_WWORK());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " F_PERSON => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON().equals("null"))) {
                    Assert.assertEquals("The F_PERSON is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_PERSON(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_PERSON());
                }

                Log.info("ID => "+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID()+
                        " F_EVENT => EPH="+dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getWORK_PERSON_ROLE_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getF_EVENT());
                }

            }
        }
    }

}





