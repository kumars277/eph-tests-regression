package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.PersonDataObject;
import com.eph.automation.testing.services.db.sql.PersonDataSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
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
    private static int countPersonsEPHSA;
    private static int countPersonsEPHGD;
    private static List<String> ids;
    private static List<String> idsLookup;
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

    @Then("^Compare the count on records for persons in PMX and EPH Staging$")
    public void verifyCountOfPersonsInPMXAndEPHSTGIsEqual() {
        Assert.assertEquals("\nPersons count in PMX and EPH STG is not equal", countPersonsPMX, countPersonsEPHSTG);
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


    @Then("^Compare the count on records for persons in EPH Staging and EPH SA$")
    public void verifyCountOfPersonsInEPHSTGAndEPHSAIsEqual() {
        Assert.assertEquals("\nPersons count in PMX and EPH STG is not equal", countPersonsEPHSTG, countPersonsEPHSA);
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

        });

    }


    @Then("^We get the ids of the records in EPH SA from the lookup table$")
    public void getIdsFromLookupTable() {
        Log.info("Get the ids of the records in EPH SA from the lookup table ..");
        idsSourceRef = new ArrayList<>(ids);


        IntStream.range(0, idsSourceRef.size()).forEach(i -> idsSourceRef.set(i, "PERSON-" + idsSourceRef.get(i)));
        sql = String.format(PersonDataSQL.GET_IDS_FROM_LOOKUP_TABLE, Joiner.on("','").join(idsSourceRef));
        Log.info(sql);


        List<Map<?, ?>> lookupResults = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        idsLookup = lookupResults.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info(idsLookup.toString());
    }

    @Then("^We get the person records from EPH SA$")
    public void getPersonDataFromEPHSA() {
        Log.info("In Then method");
        sql = String.format(PersonDataSQL.GET_DATA_PERSONS_EPHSA, Joiner.on("','").join(idsLookup));
        Log.info(sql);

        dataQualityContext.personDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
    }

    @And("^Compare person records in EPH STG and EPH SA$")
    public void comparePersonRecordsInEPHSTGAndEPHSA() {
        Log.info("And compare the person records in EPH STG and EPH SA ..");

        IntStream.range(0, dataQualityContext.personDataObjectsFromEPHSTG.size()).forEach(i -> {

            //get data from SA for the current record from STG
            String currentId = "PERSON-" + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_SOURCE_REF();
            sql = String.format(PersonDataSQL.GET_IDS_FROM_LOOKUP_TABLE, currentId);
            Log.info(sql);

            List<Map<?, ?>> lookupResults = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            idsLookup = lookupResults.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());

            sql = String.format(PersonDataSQL.GET_DATA_PERSONS_EPHSA, Joiner.on("','").join(idsLookup));
            Log.info(sql);

            dataQualityContext.personDataObjectsFromEPHSA = DBManager
                    .getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);


            //B_CLASSNAME
            Log.info("B_CLASSNAME in EPH SA: " + dataQualityContext.personDataObjectsFromEPHSA.get(0).getB_CLASSNAME());
            assertEquals("Person", dataQualityContext.personDataObjectsFromEPHSA.get(0).getB_CLASSNAME());

            //FIRST_NAME
            Log.info("FIRST_NAME in EPH STG : " + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FIRST_NAME());
            Log.info("FIRST_NAME in EPH SA: " + dataQualityContext.personDataObjectsFromEPHSA.get(0).getPERSON_FIRST_NAME());

            Log.info("Expecting FIRST_NAME in EPH STG  and EPH SA is consistent");

            assertEquals(dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FIRST_NAME(), dataQualityContext.personDataObjectsFromEPHSA.get(0).getPERSON_FIRST_NAME());

            //FAMILY_NAME
            Log.info("FAMILY_NAME in EPH STG : " + dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FAMILY_NAME());
            Log.info("FAMILY_NAME in EPH SA: " + dataQualityContext.personDataObjectsFromEPHSA.get(0).getPERSON_FIRST_NAME());

            Log.info("Expecting FAMILY_NAME in EPH STG and EPH SA is consistent");

            assertEquals(dataQualityContext.personDataObjectsFromEPHSTG.get(i).getPERSON_FAMILY_NAME(), dataQualityContext.personDataObjectsFromEPHSA.get(0).getPERSON_FAMILY_NAME());

        });

    }

    @Then("^We get the person records from EPH GD$")
    public void getPersonDataFromEPHGD() {
        Log.info("In Then method");
        sql = String.format(PersonDataSQL.GET_DATA_PERSONS_EPHGD, Joiner.on("','").join(idsLookup));
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
