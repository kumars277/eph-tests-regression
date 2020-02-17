package com.eph.automation.testing.web.steps.datalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityDLContext;
import com.eph.automation.testing.models.dao.datalake.PersonDataDLObject;
import com.eph.automation.testing.services.db.DataLakeSql.PersonDataChecksSQL;
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


public class PersonDataCheckSteps {

    @StaticInjection
    public DataQualityDLContext dataQualityDLContext;
    private static String sql;
    private static List<String> Ids;


    @Given("^We get (.*) random person ids of (.*)")
    public void getRandomPersonIds(String numberOfRecords, String tableName) {
        Log.info("Get random records ..");

        //Get property when running with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);
        if (tableName.contentEquals("gd_person")) {
            sql = String.format(PersonDataChecksSQL.GET_RANDOM_PERSON_ID, numberOfRecords);
            Log.info(sql);
            List<Map<?, ?>> randomPersonIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            Ids = randomPersonIds.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());
        }else{
            sql = String.format(PersonDataChecksSQL.GET_RANDOM_GH_PERSON_ID, numberOfRecords);
            Log.info(sql);
            List<Map<?, ?>> randomPersonIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            Ids = randomPersonIds.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());
        }
        Log.info(Ids.toString());
    }

    @When("^We get the gd person records from EPH$")
    public void getPersonEPH() {
        Log.info("We get the person records from EPH..");
        sql = String.format(PersonDataChecksSQL.GET_DATA_PERSON_EPH, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbPersonDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the gd person records from DL$")
    public void getPersonDL() {
        Log.info("We get the product records from DL..");
        sql = String.format(PersonDataChecksSQL.GET_DATA_PERSON_DL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbPersonDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare gd person records in EPH and DL$")
    public void compareGDPersonDataEPHtoDL() {
        if (dataQualityDLContext.tbPersonDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the person records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbPersonDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getPERSON_ID));
                dataQualityDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getPERSON_ID));

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPERSON_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PERSON_ID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPERSON_ID());

                }
                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                    Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_BATCHID(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_BATCHID());
                }
                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getGIVEN_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getGIVEN_NAME().equals("null"))) {
                    Assert.assertEquals("The GIVEN_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getGIVEN_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getGIVEN_NAME());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_GIVEN_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_GIVEN_NAME().equals("null"))) {
                    Assert.assertEquals("The S_GIVEN_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_GIVEN_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_GIVEN_NAME());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getFAMILY_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getFAMILY_NAME().equals("null"))) {
                    Assert.assertEquals("The FAMILY_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getFAMILY_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getFAMILY_NAME());

                }
                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_FAMILY_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_FAMILY_NAME().equals("null"))) {
                    Assert.assertEquals("The S_FAMILY_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_FAMILY_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_FAMILY_NAME());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPEOPLEHUB_ID() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPEOPLEHUB_ID().equals("null"))) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPEOPLEHUB_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPEOPLEHUB_ID());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEMAIL() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEMAIL().equals("null"))) {
                    Assert.assertEquals("The EMAIL is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEMAIL(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEMAIL());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_EMAIL() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_EMAIL().equals("null"))) {
                    Assert.assertEquals("The S_EMAIL is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_EMAIL(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_EMAIL());
                }

            }

        }
    }

    @When("^We get the gh person records from EPH$")
    public void getGHPersonEPH() {
        Log.info("We get the GH person records from EPH..");
        sql = String.format(PersonDataChecksSQL.GET_DATA_GH_PERSON_EPH, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbPersonDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.EPH_URL);
        Log.info("Size EPH =>"+ dataQualityDLContext.tbPersonDataObjectsFromEPH.size());
    }


    @Then("^We get the gh person records from DL$")
    public void getGHProductDL() {
        Log.info("We get the GH person records from DL..");
        sql = String.format(PersonDataChecksSQL.GET_DATA_GH_PERSON_DL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbPersonDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PersonDataDLObject.class, Constants.AWS_URL);
        Log.info("Size DL =>"+ dataQualityDLContext.tbPersonDataObjectsFromDL.size());
    }

    @And("^Compare gh person records in EPH and DL$")
    public void compareGHPersonDataEPHtoDL() {
        if (dataQualityDLContext.tbPersonDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the person records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i <dataQualityDLContext.tbPersonDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbPersonDataObjectsFromEPH.sort(Comparator.comparing(PersonDataDLObject::getPERSON_ID));
                dataQualityDLContext.tbPersonDataObjectsFromDL.sort(Comparator.comparing(PersonDataDLObject::getPERSON_ID));

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPERSON_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PERSON_ID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPERSON_ID());

                }
               if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                    Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_FROMBATCHID());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                    Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_TOBATCHID());
                }


                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_CREATOR());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getGIVEN_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getGIVEN_NAME().equals("null"))) {
                    Assert.assertEquals("The GIVEN_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getGIVEN_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getGIVEN_NAME());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_GIVEN_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_GIVEN_NAME().equals("null"))) {
                    Assert.assertEquals("The S_GIVEN_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_GIVEN_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_GIVEN_NAME());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getFAMILY_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getFAMILY_NAME().equals("null"))) {
                    Assert.assertEquals("The FAMILY_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getFAMILY_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getFAMILY_NAME());

                }
                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_FAMILY_NAME() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_FAMILY_NAME().equals("null"))) {
                    Assert.assertEquals("The S_FAMILY_NAME is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_FAMILY_NAME(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_FAMILY_NAME());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPEOPLEHUB_ID() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPEOPLEHUB_ID().equals("null"))) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPEOPLEHUB_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getPEOPLEHUB_ID());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEMAIL() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEMAIL().equals("null"))) {
                    Assert.assertEquals("The EMAIL is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getEMAIL(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getEMAIL());
                }

                if (dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_EMAIL() != null ||
                        (!dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_EMAIL().equals("null"))) {
                    Assert.assertEquals("The S_EMAIL is incorrect for id=" + dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getPERSON_ID(),
                            dataQualityDLContext.tbPersonDataObjectsFromEPH.get(i).getS_EMAIL(),
                            dataQualityDLContext.tbPersonDataObjectsFromDL.get(i).getS_EMAIL());
                }
            }

        }}


}





