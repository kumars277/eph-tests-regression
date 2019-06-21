package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.PersonDataObject;
import com.eph.automation.testing.services.db.sql.PersonDataSQL;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Created by Bistra Drazheva on 13/03/2019
 */
public class PersonDataQualityCheckSteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;

    private String sql;
    private static int countPersonsPMX;
    private static int countPersonsEPHSTG;
    private static int countPersonsEPHDQ;
    private static int countPersonsEPHSA;
    private static int countPersonsEPHGD;
    private static List<String> ids;
    private static List<String> personId;
    private static List<String> idsSourceRef;


    @Given("^Get the count of records for persons in PMX$")
    public void getCountPersonsPmx() {
        Log.info("When We get the count of persons records in PMX STG ..");
        sql = PersonDataSQL.GET_COUNT_PERSONS_PMX;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.PMX_URL);
        countPersonsPMX = ((BigDecimal) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons in PMX is: " + countPersonsPMX);
    }


    @When("^Get the count of records for persons in EPH Staging$")
    public void getCountPersonsEPHSTG() {
        Log.info("When We get the count of persons records in PMX STG ..");

        sql = PersonDataSQL.GET_COUNT_PERSONS_EPHSTG;
        Log.info(sql);


        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsEPHSTG = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons in EPH STG is: " + countPersonsEPHSTG);
    }

    @When("^Get the count of records for persons in EPH Staging going to DQ$")
    public void getCountPersonsEPHSTGGoingToDQ() {
        Log.info("When We get the count of persons records in PMX STG going to dq  ..");


        if (System.getProperty("LOAD") == null || System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
            sql = PersonDataSQL.GET_COUNT_PERSONS_EPHSTG;
            Log.info(sql);
        } else {
            sql = WorkCountSQL.GET_REFRESH_DATE;
            Log.info(sql);

            List<Map<String, Object>> refreshDateNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            String refreshDate = (String) refreshDateNumber.get(1).get("refresh_timestamp");

            sql = String.format( PersonDataSQL.GET_COUNT_PERSONS_EPHSTG_DELTA, refreshDate );
            Log.info(sql);
        }

        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsEPHSTG = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons in EPH STG is: " + countPersonsEPHSTG);
    }

    @When("^Get the count of records for persons in EPH DQ$")
    public void getCountPersonsEPHDQ() {
        Log.info("When We get the count of persons records in EPH DQ..");
        sql = PersonDataSQL.GET_COUNT_PERSONS_EPHDQ;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsEPHDQ = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons in EPH DQ is: " + countPersonsEPHDQ);
    }

    @When("^Get the count of records for persons in EPH DQ going to SA$")
    public void getCountPersonsEPHDQtoSA() {
        Log.info("When We get the count of persons records in EPH DQ going to sa..");
        sql = PersonDataSQL.GET_COUNT_PERSONS_EPHDQ_TO_SA;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsEPHDQ = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons in EPH DQ going to sa is: " + countPersonsEPHDQ);
    }

    @Then("^Compare the count on records for persons in PMX and EPH Staging$")
    public void verifyCountOfPersonsInPMXAndEPHSTGIsEqual() {
        Assert.assertEquals("\nPersons count in PMX and EPH STG is not equal", countPersonsPMX, countPersonsEPHSTG);
    }

    @Then("^Compare the count on records for persons in EPH Staging and EPH DQ$")
    public void verifyCountOfPersonsInEPHSTGAndEPHDQIsEqual() {
        Assert.assertEquals("\nPersons count in EPH STG and EPH DQ is not equal", countPersonsEPHSTG, countPersonsEPHDQ);
    }


    @When("^Get the count of records for persons in EPH SA$")
    public void getCountPersonsEPHSA() {
        Log.info("When We get the count of persons records in PMX STG ..");
        sql = PersonDataSQL.GET_COUNT_PERSONS_EPHSA;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsEPHSA = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons in EPH SA is: " + countPersonsEPHSA);
    }


    @Then("^Compare the count on records for persons in EPH DQ and EPH SA$")
    public void verifyCountOfPersonsInEPHDQAndEPHSAIsEqual() {
        Assert.assertEquals("\nPersons count in EPH DQ and EPH SA is not equal", countPersonsEPHDQ, countPersonsEPHSA);
    }

    @When("^Get the count of records for persons in EPH GD$")
    public void getCountPersonsEPHGD() {
        Log.info("When We get the count of persons records in PMX STG ..");
        sql = PersonDataSQL.GET_COUNT_PERSONS_EPHGD;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsEPHGD = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons in EPH GD is: " + countPersonsEPHGD);
    }

    @Then("^Compare the count on records for persons in EPH SA and EPH GD$")
    public void verifyCountOfPersonsInEPHSAndEPHGDIsEqual() {
        Assert.assertEquals("\nPersons count in EPH SA and EPH GD is not equal", countPersonsEPHSA, countPersonsEPHGD);
    }


    @Given("^We get (.*) random ids of persons$")
    public void getRandomPersonIds(String numberOfRecords) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);


        sql = String.format(PersonDataSQL.GET_RANDOM_PERSON_IDS, numberOfRecords);
        Log.info(sql);


        List<Map<?, ?>> randomPersons = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomPersons.stream().map(m -> (BigDecimal) m.get("PERSON_SOURCE_REF")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());
    }


    @When("^We get the person records from PMX$")
    public void getPersonsPMX() {
        Log.info("We get the person records from PMX..");
        sql = String.format(PersonDataSQL.GET_DATA_PERSONS_PMX, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.personDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, PersonDataObject.class, Constants.PMX_URL);
    }


    @Then("^We get the person records from EPH STG$")
    public void getProductsDataFromEPHSTG() {
        Log.info("In Then method");
        sql = String.format(PersonDataSQL.GET_DATA_PERSONS_EPHSTG, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.personDataObjectsFromEPHSTG = DBManager
                .getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
    }

    @Then("^We get the person records from EPH DQ$")
    public void getProductsDataFromEPHDQ() {
        Log.info("In Then method");
        sql = String.format(PersonDataSQL.GET_DATA_PERSONS_EPHDQ, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.personDataObjectsFromEPHDQ = DBManager
                .getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
    }

    @And("^Compare person records in PMX and EPH STG$")
    public void comparePersonRecordsInPMXAndEPHSTG() {
        Log.info("And compare the person records in PMX and EPH STG ..");//sort data in the lists
        dataQualityContext.personDataObjectsFromPMX.sort(Comparator.comparing(PersonDataObject::getPERSON_SOURCE_REF));
        dataQualityContext.personDataObjectsFromEPHSTG.sort(Comparator.comparing(PersonDataObject::getPERSON_SOURCE_REF));


        IntStream.range(0, dataQualityContext.personDataObjectsFromPMX.size()).forEach(i -> {

            //PERSON_SOURCE_REF
            Log.info("PERSON_SOURCE_REF in PMX : " + dataQualityContext.personDataObjectsFromPMX.get(i).getPERSON_SOURCE_REF());
            Log.info("PERSON_SOURCE_REF in EPH STG: " + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_SOURCE_REF());

            assertEquals("Expecting the PERSON_SOURCE_REF in PMX and EPH STG are consistent ",
                    dataQualityContext.personDataObjectsFromPMX.get(i).getPERSON_SOURCE_REF(),
                    dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_SOURCE_REF());

            //PERSON_FIRST_NAME
            Log.info("PERSON_FIRST_NAME in PMX : " + dataQualityContext.personDataObjectsFromPMX.get(i).getPERSON_FIRST_NAME());
            Log.info("PERSON_FIRST_NAME in EPH STG: " + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FIRST_NAME());

            assertEquals("Expecting the PERSON_FIRST_NAME in PMX and EPH STG are consistent ",
                    dataQualityContext.personDataObjectsFromPMX.get(i).getPERSON_FIRST_NAME(),
                    dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FIRST_NAME());

            //PERSON_FAMILY_NAME
            Log.info("PERSON_FAMILY_NAME in PMX : " + dataQualityContext.personDataObjectsFromPMX.get(i).getPERSON_FAMILY_NAME());
            Log.info("PERSON_FAMILY_NAME in EPH STG: " + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FAMILY_NAME());

            assertEquals("Expecting the PERSON_FAMILY_NAME in PMX and EPH STG are consistent ",
                    dataQualityContext.personDataObjectsFromPMX.get(i).getPERSON_FAMILY_NAME(),
                    dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FAMILY_NAME());

            //PEOPLEHUB_ID
            Log.info("PEOPLEHUB_ID in PMX : " + dataQualityContext.personDataObjectsFromPMX.get(i).getPEOPLEHUB_ID());
            Log.info("PEOPLEHUB_ID in EPH STG: " + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPEOPLEHUB_ID());

            assertEquals("Expecting the PEOPLEHUB_ID in PMX and EPH STG are consistent ",
                    dataQualityContext.personDataObjectsFromPMX.get(i).getPEOPLEHUB_ID(),
                    dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPEOPLEHUB_ID());

            //UPDATED
            Log.info("UPDATED in PMX: " + dataQualityContext.personDataObjectsFromPMX.get(i).getUPDATED());
            Log.info("UPDATED in EPH Staging: " + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getUPDATED());

            Log.info("Expecting UPDATED in PMX and EPH Staging are consistent for ");


         try {
                Date pmxUpdatedDate = new SimpleDateFormat("dd-MMM-yy HH.mm.ss.SSSSSS").parse(dataQualityContext.personDataObjectsFromPMX.get(i).getUPDATED());
                Date ephUpdatedDate = new SimpleDateFormat("dd-MMM-yy hh.mm.ss.SSSSSS aaa").parse(dataQualityContext.personDataObjectsFromEPHSTG.get(i).getUPDATED());

                assertEquals("UPDATED in PMX and EPH STG is not equal ", pmxUpdatedDate, ephUpdatedDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }


        });

    }

    @And("^Compare person records in EPH STG and EPH DQ$")
    public void comparePersonRecordsInEPHSTGAndEPHDQ() {
        Log.info("And compare the person records in EPH STG and EPH DQ ..");//sort data in the lists
        dataQualityContext.personDataObjectsFromEPHSTG.sort(Comparator.comparing(PersonDataObject::getPERSON_SOURCE_REF));
        dataQualityContext.personDataObjectsFromEPHDQ.sort(Comparator.comparing(PersonDataObject::getPERSON_SOURCE_REF));


        IntStream.range(0, dataQualityContext.personDataObjectsFromEPHSTG.size()).forEach(i -> {

            //PERSON_SOURCE_REF
            Log.info("PERSON_SOURCE_REF in EPH STG: " + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_SOURCE_REF());
            Log.info("PERSON_SOURCE_REF in EPH DQ: " + dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPERSON_SOURCE_REF());


            assertEquals("Expecting the PERSON_SOURCE_REF in EPH STG and EPH DQ are consistent ",
                    dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_SOURCE_REF(),
                    dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPERSON_SOURCE_REF());

            //PERSON_FIRST_NAME
            Log.info("PERSON_FIRST_NAME in EPH STG: " + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FIRST_NAME());
            Log.info("PERSON_FIRST_NAME in EPH DQ: " + dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPERSON_FIRST_NAME());

            assertEquals("Expecting the PERSON_FIRST_NAME in EPH STG and EPH DQ are consistent ",
                    dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FIRST_NAME(),
                    dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPERSON_FIRST_NAME());

            //PERSON_FAMILY_NAME
            Log.info("PERSON_FAMILY_NAME in EPH STG: " + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FAMILY_NAME());
            Log.info("PERSON_FAMILY_NAME in EPH DQ: " + dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPERSON_FAMILY_NAME());

            assertEquals("Expecting the PERSON_FAMILY_NAME in EPH STG and EPH DQ are consistent ",
                    dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FAMILY_NAME(),
                    dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPERSON_FAMILY_NAME());

            //PEOPLEHUB_ID
            Log.info("PEOPLEHUB_ID in EPH STG: " + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPEOPLEHUB_ID());
            Log.info("PEOPLEHUB_ID in EPH DQ: " + dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPEOPLEHUB_ID());


            assertEquals("Expecting the PEOPLEHUB_ID in EPH STG and EPH DQ are consistent ",
                    dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPEOPLEHUB_ID(),
                    dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPEOPLEHUB_ID());

            //DQ_ERR
            Log.info("DQ_ERR in EPH DQ: " + dataQualityContext.personDataObjectsFromEPHDQ.get(i).getDQ_ERR());
            assertEquals( "N", dataQualityContext.personDataObjectsFromEPHDQ.get(i).getDQ_ERR());
        });

    }




    @Then("^We get the person records from EPH SA$")
    public void getPersonDataFromEPHSA() {
        Log.info("In Then method");

        //get person ids
        sql = String.format(PersonDataSQL.GET_PERSON_IDS_FROM_SA, Joiner.on("','").join(ids));
        Log.info(sql);

        List<Map<?, ?>> results = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        personId = results.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());

        sql = String.format(PersonDataSQL.GET_DATA_PERSONS_EPHSA, Joiner.on("','").join(personId));

        Log.info(sql);

        dataQualityContext.personDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
    }



    @And("^Compare person records in EPH DQ and EPH SA$")
    public void comparePersonRecordsInEPHDQAndEPHSA() {
        Log.info("And compare the person records in EPH DQ and EPH SA ..");

        if (dataQualityContext.personDataObjectsFromEPHDQ.isEmpty()&& System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Person data");
        } else {
            IntStream.range(0, dataQualityContext.personDataObjectsFromEPHDQ.size()).forEach(i -> {

                //get data from SA for the current record from DQ
                sql = String.format(PersonDataSQL.GET_PERSON_IDS_FROM_SA, dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPERSON_SOURCE_REF());
                Log.info(sql);

                List<Map<?, ?>> results = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                personId = results.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());

                sql = String.format(PersonDataSQL.GET_DATA_PERSONS_EPHSA, Joiner.on("','").join(personId));
                Log.info(sql);

                dataQualityContext.personDataObjectsFromEPHSA = DBManager
                        .getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);


                //B_CLASSNAME
                Log.info("B_CLASSNAME in EPH SA: " + dataQualityContext.personDataObjectsFromEPHSA.get(0).getB_CLASSNAME());
                assertEquals("Person", dataQualityContext.personDataObjectsFromEPHSA.get(0).getB_CLASSNAME());

                //FIRST_NAME
                Log.info("FIRST_NAME in EPH DQ : " + dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPERSON_FIRST_NAME());
                Log.info("FIRST_NAME in EPH SA: " + dataQualityContext.personDataObjectsFromEPHSA.get(0).getPERSON_FIRST_NAME());

                Log.info("Expecting FIRST_NAME in EPH DQ and EPH SA is consistent");

                assertEquals(dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPERSON_FIRST_NAME(), dataQualityContext.personDataObjectsFromEPHSA.get(0).getPERSON_FIRST_NAME());

                //FAMILY_NAME
                Log.info("FAMILY_NAME in EPH DQ : " + dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPERSON_FAMILY_NAME());
                Log.info("FAMILY_NAME in EPH SA: " + dataQualityContext.personDataObjectsFromEPHSA.get(0).getPERSON_FAMILY_NAME());

                Log.info("Expecting FAMILY_NAME in EPH DQ and EPH SA is consistent");

                assertEquals(dataQualityContext.personDataObjectsFromEPHDQ.get(i).getPERSON_FAMILY_NAME(), dataQualityContext.personDataObjectsFromEPHSA.get(0).getPERSON_FAMILY_NAME());

            });
        }
    }

    @Then("^We get the person records from EPH GD$")
    public void getPersonDataFromEPHGD() {
        Log.info("In Then method");
        sql = String.format(PersonDataSQL.GET_DATA_PERSONS_EPHGD, Joiner.on("','").join(personId));
        Log.info(sql);

        dataQualityContext.personDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
    }


    @And("^Compare person records in EPH SA and EPH GD$")
    public void comparePersonRecordsInEPHSAndEPHGD() {
        Log.info("And compare the person records in EPH SA and EPH GD ..");

        IntStream.range(0, dataQualityContext.personDataObjectsFromEPHSA.size()).forEach(i -> {


            //B_CLASSNAME
            Log.info("B_CLASSNAME in EPH SA: " + dataQualityContext.personDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
            Log.info("B_CLASSNAME in EPH GD: " + dataQualityContext.personDataObjectsFromEPHGD.get(i).getB_CLASSNAME());

            assertEquals("Person", dataQualityContext.personDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
            assertEquals(dataQualityContext.personDataObjectsFromEPHSA.get(i).getB_CLASSNAME(), dataQualityContext.personDataObjectsFromEPHGD.get(i).getB_CLASSNAME());


            //FIRST_NAME
            Log.info("FIRST_NAME in EPH SA: " + dataQualityContext.personDataObjectsFromEPHSA.get(i).getPERSON_FIRST_NAME());
            Log.info("FIRST_NAME in EPH GD: " + dataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FIRST_NAME());

            Log.info("Expecting FIRST_NAME in EPH SA  and EPH GD is consistent");

            assertEquals(dataQualityContext.personDataObjectsFromEPHSA.get(i).getPERSON_FIRST_NAME(), dataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FIRST_NAME());

            //FAMILY_NAME
            Log.info("FAMILY_NAME in EPH SA : " + dataQualityContext.personDataObjectsFromEPHSA.get(i).getPERSON_FAMILY_NAME());
            Log.info("FAMILY_NAME in EPH GD: " + dataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FIRST_NAME());

            Log.info("Expecting FAMILY_NAME in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.personDataObjectsFromEPHSA.get(i).getPERSON_FAMILY_NAME(), dataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FAMILY_NAME());

        });

    }
}
