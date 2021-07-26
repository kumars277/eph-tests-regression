package com.eph.automation.testing.steps.z_other;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.PersonProductRoleDataObject;
import com.eph.automation.testing.services.db.sql.PersonDataSQL;
import com.eph.automation.testing.services.db.sql.PersonProductRoleDataSQL;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Created by Bistra Drazheva on 14/03/2019
 */
public class PersonProductRoleDataQualityCheckSteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    private String sql;
    private static int countPersonsEPHSTG;
    private static int countPersonsEPHSTGGoingToSA;
    private static int countPersonsEPHSA;
    private static int countPersonsEPHGD;
    private static int countPersonsProductRoleEPHAE;
    private static List<String> ids;




    @Given("^Get the count of records for persons product role in EPH STG DQ")
    public void getCountPersonsProductRoleEPHSTGDQ() {
        Log.info("When We get the count of persons product role records in EPH STG Product DQ..");
        sql = PersonProductRoleDataSQL.GET_COUNT_PERSONS_PRODUCT_ROLE_EPH_STG_DQ;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsEPHSTGGoingToSA = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons product role in DQ is: " + countPersonsEPHSTGGoingToSA);
    }


    @When("^Get the count of records for persons product role in EPH Staging$")
    public void getCountPersonsProductRolePHSTG() {
        Log.info("When We get the count of persons product role records in EPH STG ..");

        sql = PersonProductRoleDataSQL.GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSTG;
        Log.info(sql);

        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsEPHSTG = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons product role in EPH STG is: " + countPersonsEPHSTG);
    }


    @When("^Get the count of records for persons product role in EPH Staging going to SA$")
    public void getCountPersonsProductRolePHSTGGoingToSA() {
        Log.info("When We get the count of persons product role records in EPH STG going to SA..");


        if (System.getProperty("LOAD") == null || System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
            sql = PersonProductRoleDataSQL.GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSTG_GOING_TO_SA;
            Log.info(sql);
        } else {
            sql = WorkCountSQL.GET_REFRESH_DATE;
            Log.info(sql);
            List<Map<String, Object>> refreshDateNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            String refreshDate = (String) refreshDateNumber.get(1).get("refresh_timestamp");
            sql = String.format(PersonProductRoleDataSQL.GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSTG_GOING_TO_SA_DELTA, refreshDate );
            Log.info(sql);

        }

        List<Map<String, Object>> personsNumberDQ = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsEPHSTGGoingToSA = ((Long) personsNumberDQ.get(0).get("count")).intValue();
        Log.info("Count of persons product role in EPH STG going to SA is: " + countPersonsEPHSTGGoingToSA);
    }

    @Then("^Compare the count on records for persons product role in EPH STG DQ and EPH Staging$")
    public void verifyCountOfPersonsProductRoleInPMXAndEPHSTGIsEqual() {
        Assert.assertEquals("\nPersons product role count in DQ and STG is not equal", countPersonsEPHSTGGoingToSA, countPersonsEPHSTG);
    }


    @When("^Get the count of records for persons product role in EPH SA$")
    public void getCountPersonsProductRoleEPHSA() {
        Log.info("When We get the count of persons product role records in PMX STG ..");
        sql = PersonProductRoleDataSQL.GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSA;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsEPHSA = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons product role in EPH SA is: " + countPersonsEPHSA);
    }

    @Given("^Get the count of records for persons product role in EPH AE$")
    public void getCountPersonsProductRoleEPHAE() {
        Log.info("When We get the count of persons records in PMX STG ..");
        sql = PersonProductRoleDataSQL.GET_COUNT_PERSONS_PRODUCT_ROLE_EPHAE;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsProductRoleEPHAE = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons product role in EPH AE is: " + countPersonsProductRoleEPHAE);
    }

    @Then("^Verify sum of records for persons product role in EPH GD and EPH AE is equal to number of records in EPH SA$")
    public void verifyCountOfPersonsProductRoleInEPHGDAndEPHAEIsEqualToEPHSA() {
        int sumOFRecords = countPersonsProductRoleEPHAE + countPersonsEPHGD;
        Assert.assertEquals("\nSum of the records for persons product role in EPH GD and EPH AE is NOT equal to number of records in EPH SA", sumOFRecords, countPersonsEPHSA);
    }

    @Then("^Compare the count on records for persons product role in EPH Staging with DQ and EPH SA$")
    public void verifyCountOfPersonsProductRoleInEPHSTGAndEPHSAIsEqual() {
        Assert.assertEquals("\nPersons product role count in EPH STG and EPH SA is not equal", countPersonsEPHSTGGoingToSA, countPersonsEPHSA);
    }

    @When("^Get the count of records for persons product role in EPH GD$")
    public void getCountPersonsProductRoleEPHGD() {
        Log.info("When We get the count of persons product role records in PMX STG ..");
        sql = PersonProductRoleDataSQL.GET_COUNT_PERSONS_PRODUCT_ROLE_EPHGD;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsEPHGD = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons product role in EPH GD is: " + countPersonsEPHGD);
    }

    @Then("^Compare the count on records for persons product role in EPH SA and EPH GD$")
    public void verifyCountOfPersonsProductRoleInEPHSAndEPHGDIsEqual() {
        Assert.assertEquals("\nPersons product role count in EPH SA and EPH GD is not equal", countPersonsEPHSA, countPersonsEPHGD);
    }

    @Given("^We get (.*) random ids of persons product role$")
    public void getRandomIds(String numberOfRecords) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);

        if (System.getProperty("LOAD") == null || System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
        sql = String.format(PersonProductRoleDataSQL.GET_RANDOM_PERSON_PRODUCT_ROLE_IDS, numberOfRecords);
            Log.info(sql);
        } else {
            sql = WorkCountSQL.GET_REFRESH_DATE;
            Log.info(sql);
            List<Map<String, Object>> refreshDateNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            String refreshDate = (String) refreshDateNumber.get(1).get("refresh_timestamp");
            sql = String.format(PersonProductRoleDataSQL.GET_RANDOM_PERSON_PRODUCT_ROLE_IDS_DELTA, refreshDate, numberOfRecords );
            Log.info(sql);

        }



        List<Map<?, ?>> randomPersons = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomPersons.stream().map(m -> (String) m.get("PROD_PER_ROLE_SOURCE_REF")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());
    }

//    @Given("^We get (.*) random ids of persons product role from SA$")
//    public void getRandomIdsSA(String numberOfRecords) {
//        Log.info("Get random records ..");
//
//        //Get property when run with jenkins
////        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
//        Log.info("numberOfRecords = " + numberOfRecords);
//
//
//        sql = String.format(PersonProductRoleDataSQL.GET_RANDOM_PERSON_PRODUCT_ROLE_IDS_FROM_SA, numberOfRecords);
//        Log.info(sql);
//
//
//        List<Map<?, ?>> randomPersons = DBManager.getDBResultMap(sql, Constants.EPH_URL);
//
//        ids = randomPersons.stream().map(m -> (BigDecimal) m.get("PRODUCT_PERSON_ROLE_ID")).map(String::valueOf).collect(Collectors.toList());
//        Log.info(ids.toString());
//    }

    @When("^We get the person product role records from EPH DQ$")
    public void getPersonProductRoleRecordsEphDQ() {
        Log.info("Get the person product role records from EPH DQ ..");
        sql = String.format(PersonProductRoleDataSQL.GET_DATA_PERSONS_PRODUCT_ROLE_EPH_DQ, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ = DBManager
                .getDBResultAsBeanList(sql, PersonProductRoleDataObject.class, Constants.EPH_URL);
    }

    @Then("^We get the person product role records from EPH STG$")
    public void getPersonProductRoleRecordsEphStg() {
        Log.info("Get the person product role records from EPH STG  ..");
        sql = String.format(PersonProductRoleDataSQL.GET_DATA_PERSONS_PRODUCT_ROLE_EPHSTG, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.personProductRoleDataObjectsFromEPHSTG = DBManager
                .getDBResultAsBeanList(sql, PersonProductRoleDataObject.class, Constants.EPH_URL);
    }

    @And("^Compare person product role records in EPH STG DQ and EPH STG$")
    public void comparePersonProductRolesRecordsInSTGAndDQ() {
        Log.info("And compare product role records in EPH STG DQ and EPH STG ..");

        //sort the lists before comparison
        dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.sort(Comparator.comparing(PersonProductRoleDataObject::getPROD_PER_ROLE_SOURCE_REF));
        dataQualityContext.personProductRoleDataObjectsFromEPHSTG.sort(Comparator.comparing(PersonProductRoleDataObject::getPROD_PER_ROLE_SOURCE_REF));

        IntStream.range(0, dataQualityContext.personProductRoleDataObjectsFromEPHSTG.size()).forEach(i -> {


            //PROD_PER_ROLE_SOURCE_REF
            Log.info("PROD_PER_ROLE_SOURCE_REF in EPH STG: " + dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getPROD_PER_ROLE_SOURCE_REF());

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getPROD_PER_ROLE_SOURCE_REF(), dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getPROD_PER_ROLE_SOURCE_REF());


            //PRODUCT_SOURCE_REF
            Log.info("PRODUCT_SOURCE_REF in EPH STG DQ : " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_SOURCE_REF());
            Log.info("PRODUCT_SOURCE_REF in EPH STG: " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_SOURCE_REF());

            Log.info("Expecting PRODUCT_SOURCE_REF in EPH STG DQ and EPH STG is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getPRODUCT_SOURCE_REF(), dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getPRODUCT_SOURCE_REF());

            //PERSON_SOURCE_REF
            Log.info("PERSON_SOURCE_REF in EPH STG DQ : " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getPERSON_SOURCE_REF());
            Log.info("PERSON_SOURCE_REF in EPH STG: " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getPERSON_SOURCE_REF());

            Log.info("Expecting PERSON_SOURCE_REF in EPH STG DQ and EPH STG is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getPERSON_SOURCE_REF(), dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getPERSON_SOURCE_REF());

            //F_ROLE
            Log.info("F_ROLE in EPH STG: " + dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getF_ROLE());

            assertEquals("PO", dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getF_ROLE());

            //WORK_ROLE
            Log.info("WORK_ROLE in EPH STG DQ : " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getWORK_ROLE());
            Log.info("WORK_ROLE in EPH STG: " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getWORK_ROLE());

            Log.info("Expecting WORK_ROLE in EPH STG DQ and EPH STG is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getWORK_ROLE(), dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getWORK_ROLE());


            //START_DATE
            Log.info("START_DATE in EPH STG DQ : " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getSTART_DATE());
            Log.info("START_DATE in EPH STG: " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getSTART_DATE());

            Log.info("Expecting START_DATE in EPH STG DQ and EPH STG is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getSTART_DATE(), dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getSTART_DATE());


            //END_DATE
            Log.info("END_DATE in EPH STG DQ : " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getEND_DATE());
            Log.info("END_DATE in EPH STG: " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getEND_DATE());

            Log.info("Expecting END_DATE in EPH STG DQ and EPH STG is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getEND_DATE(), dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getEND_DATE());


            //UPDATED
            Log.info("UPDATED in EPH STG DQ : " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getUPDATED());
            Log.info("UPDATED in EPH STG: " + dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getUPDATED());

            Log.info("Expecting UPDATED in EPH STG DQ and EPH STG is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSTGDQ.get(i).getUPDATED(), dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getUPDATED());

        });

    }

//old    @Then("^We get the ids of the person product role records in EPH SA from the lookup table$")
//    public void getIdsFromLookupTable() {
//        Log.info("Get the ids of the records in EPH SA from the lookup table ..");
//        idsSourceRef = new ArrayList<>();
//
//        IntStream.range(0, dataQualityContext.personProductRoleDataObjectsFromEPHSTG.size()).forEach(i -> idsSourceRef.add(i, "PPR-" + dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getPROD_PER_ROLE_SOURCE_REF()));
//
//
//        sql = String.format(PersonDataSQL.GET_IDS_FROM_LOOKUP_TABLE, Joiner.on("','").join(idsSourceRef));
//        Log.info(sql);
//
//
//        List<Map<?, ?>> lookupResults = DBManager.getDBResultMap(sql, Constants.EPH_URL);
//
//        idsLookup = lookupResults.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());
//        Log.info(idsLookup.toString());
//    }


    @Then("^We get the person product role records from EPH SA$")
    public void getPersonProductRoleRecordsEPHSA() {
        Log.info("Get the person product role records from EPH SA  ..");
        sql = String.format(PersonProductRoleDataSQL.GET_DATA_PERSONS_PRODUCT_ROLE_EPHSA, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.personProductRoleDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, PersonProductRoleDataObject.class, Constants.EPH_URL);
        sql.length();
    }


    @Then("^We get the random person product role records from EPH SA$")
    public void geRandomPersonProductRoleRecordsEPHSA() {
        Log.info("Get the person product role records from EPH SA  ..");
        sql = String.format(PersonProductRoleDataSQL.GET_DATA_PERSONS_PRODUCT_ROLE_EPHSA, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.personProductRoleDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, PersonProductRoleDataObject.class, Constants.EPH_URL);
        sql.length();
    }


    @And("^Check the mandatory columns are populated$")
    public void checkMandatoryColumnsForPersonsInSAArePopulated() {
        Log.info("We check that mandatory columns are populated ...");

        if ( CollectionUtils.isEmpty(dataQualityContext.personProductRoleDataObjectsFromEPHSA)&& System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Person Product Role data");
        } else {
            IntStream.range(0, dataQualityContext.personProductRoleDataObjectsFromEPHSA.size()).forEach(i -> {
                //verify F_EVENT is not null
                assertNotNull(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_EVENT());
                //verify PRODUCT_PERSON_ROLE_ID is not null
                assertNotNull(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getPRODUCT_PERSON_ROLE_ID());
                //verify F_ROLE
                assertNotNull(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_ROLE());
                //verify F_PRODUCT
                assertNotNull(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_PRODUCT());
                //verify F_PERSON
                assertNotNull(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_PERSON());


            });
        }
    }


    @And("^Compare person product role records in EPH STG and EPH SA$")
    public void comparePersonProductRolesRecordsInSTGAndSA() {
        Log.info("And compare product role records in EPH STG and EPH SA ..");

        dataQualityContext.personProductRoleDataObjectsFromEPHSTG.sort(Comparator.comparing(PersonProductRoleDataObject::getPROD_PER_ROLE_SOURCE_REF));
        dataQualityContext.personProductRoleDataObjectsFromEPHSA.sort(Comparator.comparing(PersonProductRoleDataObject::getEXTERNAL_REFERENCE));

        if ( CollectionUtils.isEmpty(dataQualityContext.personProductRoleDataObjectsFromEPHSA) && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Person Product Role data");
        } else {
            IntStream.range(0, dataQualityContext.personProductRoleDataObjectsFromEPHSTG.size()).forEach(i -> {

//            //get data from SA for the current record from STG
//            String currentId = "PPR-" + dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getPROD_PER_ROLE_SOURCE_REF();
//            sql = String.format(PersonDataSQL.GET_IDS_FROM_LOOKUP_TABLE, currentId);
//            Log.info(sql);
//
//            List<Map<?, ?>> lookupResults = DBManager.getDBResultMap(sql, Constants.EPH_URL);
//            idsLookup = lookupResults.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());
//
//            Log.info("The id's are : " +idsLookup);
//
//            sql = String.format(PersonProductRoleDataSQL.GET_DATA_PERSONS_PRODUCT_ROLE_EPHSA, Joiner.on("','").join(idsLookup));
//            Log.info(sql);
//
//            dataQualityContext.personProductRoleDataObjectsFromEPHSA = DBManager
//                    .getDBResultAsBeanList(sql, PersonProductRoleDataObject.class, Constants.EPH_URL);


                //B_CLASSNAME
                Log.info("B_CLASSNAME in EPH SA: " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
                assertEquals("ProductPersonRole", dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getB_CLASSNAME());


                //EFFECTIVE_START_DATE
                Log.info("EFFECTIVE_START_DATE in EPH STG : " + dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getSTART_DATE());
                Log.info("EFFECTIVE_START_DATE in EPH SA: " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE());

                Log.info("Expecting EFFECTIVE_START_DATE in EPH STG and EPH SA to be consistent");

                assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getSTART_DATE(), dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE());


                //EFFECTIVE_END_DATE
                Log.info("EFFECTIVE_END_DATE in EPH STG : " + dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getEND_DATE());
                Log.info("EFFECTIVE_END_DATE in EPH SA: " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE());

                Log.info("Expecting EFFECTIVE_END_DATE in EPH STG and EPH SA to be consistent");

                assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getEND_DATE(), dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE());


                //F_ROLE
                Log.info("F_ROLE in EPH STG : " + dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getF_ROLE());
                Log.info("F_ROLE in EPH SA: " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_ROLE());

                Log.info("Expecting F_ROLE in EPH STG and EPH SA is consistent");

                assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getF_ROLE(), dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_ROLE());

                //F_PRODUCT
                String currentF_PRODUCT = dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_PRODUCT();
                Log.info("F_PRODUCT in EPH SA: " + currentF_PRODUCT);


                String idL = dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getPRODUCT_SOURCE_REF();
                sql = String.format(PersonDataSQL.GET_IDS_FROM_LOOKUP_TABLE_EPH, "PRODUCT", idL);
                Log.info(sql);


                String expectedF_PRODUCT;


                List<Map<String, Object>> results = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                expectedF_PRODUCT = ((String) results.get(0).get("PERSON_ID"));

                assertEquals(expectedF_PRODUCT, currentF_PRODUCT);


                //F_PERSON

                Log.info("F_PERSON in EPH STG : " + dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getF_PERSON());
                Log.info("F_PERSON in EPH SA: " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_PERSON());

                Log.info("Expecting F_PERSON in EPH STG and EPH SA is consistent");

                assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSTG.get(i).getF_PERSON(), dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_PERSON());


            });
        }

    }

    @Then("^We get the person product role records from EPH GD$")
    public void getPersonProductRoleRecordsEPHGD() {
        Log.info("Get the person product role records from EPH GD  ..");
        sql = String.format(PersonProductRoleDataSQL.GET_DATA_PERSONS_PRODUCT_ROLE_EPHGD, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.personProductRoleDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, PersonProductRoleDataObject.class, Constants.EPH_URL);
        sql.length();
    }


    @And("^Compare person product role records in EPH SA and EPH GD$")
    public void comparePersonProductRolesRecordsInSAAndGD() {
        Log.info("And compare product role records in EPH SA and EPH GD ..");

        dataQualityContext.personProductRoleDataObjectsFromEPHSA.sort(Comparator.comparing(PersonProductRoleDataObject::getPRODUCT_PERSON_ROLE_ID));
        dataQualityContext.personProductRoleDataObjectsFromEPHGD.sort(Comparator.comparing(PersonProductRoleDataObject::getPRODUCT_PERSON_ROLE_ID));

        IntStream.range(0, dataQualityContext.personProductRoleDataObjectsFromEPHSA.size()).forEach(i -> {


            //F_EVENT
            Log.info("F_EVENT in EPH SA : " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_EVENT());
            Log.info("F_EVENT in EPH GD: " + dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getF_EVENT());

            Log.info("Expecting F_EVENT in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_EVENT(), dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getF_EVENT());


            //B_CLASSNAME
            Log.info("B_CLASSNAME in EPH SA : " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
            Log.info("B_CLASSNAME in EPH GD: " + dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getB_CLASSNAME());

            Log.info("Expecting B_CLASSNAME in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getB_CLASSNAME(), dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getB_CLASSNAME());


            //PRODUCT_PERSON_ROLE_ID
            Log.info("PRODUCT_PERSON_ROLE_ID in EPH SA : " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getPRODUCT_PERSON_ROLE_ID());
            Log.info("PRODUCT_PERSON_ROLE_ID in EPH GD: " + dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getPRODUCT_PERSON_ROLE_ID());

            Log.info("Expecting PRODUCT_PERSON_ROLE_ID in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getPRODUCT_PERSON_ROLE_ID(), dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getPRODUCT_PERSON_ROLE_ID());


            //EFFECTIVE_START_DATE
            Log.info("EFFECTIVE_START_DATE in EPH SA : " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE());
            Log.info("EFFECTIVE_START_DATE in EPH GD: " + dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getEFFECTIVE_START_DATE());

            Log.info("Expecting EFFECTIVE_START_DATE in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE(), dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getEFFECTIVE_START_DATE());


            //EFFECTIVE_END_DATE
            Log.info("EFFECTIVE_END_DATE in EPH SA : " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE());
            Log.info("EFFECTIVE_END_DATE in EPH GD: " + dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getEFFECTIVE_END_DATE());

            Log.info("Expecting EFFECTIVE_END_DATE in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE(), dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getEFFECTIVE_END_DATE());


            //F_ROLE
            Log.info("F_ROLE in EPH SA : " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_ROLE());
            Log.info("F_ROLE in EPH GD: " + dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getF_ROLE());

            Log.info("Expecting F_ROLE in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_ROLE(), dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getF_ROLE());


            //F_PRODUCT
            Log.info("F_PRODUCT in EPH SA : " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_PRODUCT());
            Log.info("F_PRODUCT in EPH GD: " + dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getF_PRODUCT());

            Log.info("Expecting F_PRODUCT in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_PRODUCT(), dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getF_PRODUCT());

            //F_PERSON
            Log.info("F_PERSON in EPH SA : " + dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_PERSON());
            Log.info("F_PERSON in EPH GD: " + dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getF_PERSON());

            Log.info("Expecting F_PERSON in EPH SA and EPH GD is consistent");

            assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHSA.get(i).getF_PERSON(), dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(i).getF_PERSON());

        });

    }

    }


