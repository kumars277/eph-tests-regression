package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.PersonWorkRoleDataObject;
import com.eph.automation.testing.services.db.sql.PersonDataSQL;
import com.eph.automation.testing.services.db.sql.PersonWorkRoleDataSQL;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Bistra Drazheva on 14/03/2019
 */
public class PersonWorkRoleDataQualityCheckSteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    private String sql;
    private static int countPersonsWorkRolePMX;
    private static int countPersonsWorkRoleEPHSTG;
    private static int countPersonsWorkRoleEPHSTGGoingToSA;
    private static int countPersonsWorkRoleEPHSA;
    private static int countPersonsWorkRoleEPHAE;
    private static int countPersonsWorkRoleEPHGD;
    private List<Map<?, ?>> personIds;
    private static List<String> ids;
    private static List<String> idsPMX;
    private static List<String> idsSourceRef;
    private static List<String> idsSA;
    private static List<String> idsEPHstg;


    @Given("^Get the count of records for persons work role in PMX$")
    public void getCountPersonsProductRolePmx() {
        Log.info("When We get the count of persons work role records in PMX STG ..");
        sql = PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_PMX;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.PMX_URL);
        countPersonsWorkRolePMX = ((BigDecimal) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons work role in PMX is: " + countPersonsWorkRolePMX);
    }


    @When("^Get the count of records for persons work role in EPH Staging$")
    public void getCountPersonsProductRolePHSTG() {
        Log.info("When We get the count of persons work role records in EPH STG ..");

        sql = PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHSTG;
        Log.info(sql);

        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsWorkRoleEPHSTG = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons work role in EPH STG is: " + countPersonsWorkRoleEPHSTG);
    }

    @When("^Get the count of records for persons work role in EPH Staging going to SA$")
    public void getCountPersonsProductRoleEPHSTGGoingTOSA() {
        Log.info("When We get the count of persons work role records in EPH STG going to SA ..");

        if (System.getProperty("LOAD") == null || System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
            sql = PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHSTGGoingToSA;
            Log.info(sql);
        } else {
            sql = WorkCountSQL.GET_REFRESH_DATE;
            List<Map<String, Object>> refreshDateNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            String refreshDate = (String) refreshDateNumber.get(1).get("refresh_timestamp");
            sql = String.format(PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHSTG_DELTA, refreshDate);
            Log.info(sql);
        }

        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsWorkRoleEPHSTGGoingToSA = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons work role in EPH STG going to SA is: " + countPersonsWorkRoleEPHSTGGoingToSA);
    }


    @Then("^Compare the count on records for persons work role in PMX and EPH Staging$")
    public void verifyCountOfPersonsProductRoleInPMXAndEPHSTGIsEqual() {
        Assert.assertEquals("\nPersons work role count in PMX and EPH STG is not equal", countPersonsWorkRolePMX, countPersonsWorkRoleEPHSTG);
    }


    @When("^Get the count of records for persons work role in EPH SA$")
    public void getCountPersonsProductRoleEPHSA() {
        Log.info("When We get the count of persons records in EPH SA..");
        sql = PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHSA;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsWorkRoleEPHSA = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons work role in EPH SA is: " + countPersonsWorkRoleEPHSA);
    }

    @When("^Get the count of records for persons work role in EPH SA going to GD$")
    public void getCountPersonsProductRoleEPHSAGoingToGD() {
        Log.info("When We get the count of persons records in EPH SA ..");
        sql = PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHSATOGD;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsWorkRoleEPHSA = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons work role in EPH SA is: " + countPersonsWorkRoleEPHSA);
    }

    @Given("^Get the count of records for persons work role in EPH AE$")
    public void getCountPersonsWorkRoleEPHAE() {
        Log.info("When We get the count of persons records in PMX STG ..");
        sql = PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHAE;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsWorkRoleEPHAE = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons work role in EPH AE is: " + countPersonsWorkRoleEPHAE);
    }


    @Then("^Compare the count on records for persons work role in EPH Staging and EPH SA$")
    public void verifyCountOfPersonsProductRoleInEPHSTGAndEPHSAIsEqual() {
        Assert.assertEquals("\nPersons work role count in EPH STG and EPH SA is not equal", countPersonsWorkRoleEPHSTGGoingToSA, countPersonsWorkRoleEPHSA);
    }

    @When("^Get the count of records for persons work role in EPH GD$")
    public void getCountPersonsProductRoleEPHGD() {
        Log.info("When We get the count of persons records in PMX STG ..");
        sql = PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHGD;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsWorkRoleEPHGD = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons work role in EPH GD is: " + countPersonsWorkRoleEPHGD);
    }

    @Then("^Compare the count on records for persons work role in EPH SA and EPH GD$")
    public void verifyCountOfPersonsProductRoleInEPHSAndEPHGDIsEqual() {
        Assert.assertEquals("\nPersons work role count in EPH SA and EPH GD is not equal", countPersonsWorkRoleEPHSA, countPersonsWorkRoleEPHGD);
    }

    @Then("^Verify sum of records for persons work role in EPH GD and EPH AE is equal to number of records in EPH SA$")
    public void verifyCountOfPersonsProductRoleInEPHGDAndEPHAEIsEqualToEPHSA() {
        int sumOFRecords = countPersonsWorkRoleEPHAE + countPersonsWorkRoleEPHGD;
        Assert.assertEquals("\nSum of the records for persons work role in EPH GD and EPH AE is NOT equal to number of records in EPH SA", sumOFRecords, countPersonsWorkRoleEPHSA);
    }


    @Given("^We get (.*) random ids of persons work role with (.*)$")
    public void getRandomIds(String numberOfRecords, String type) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);

        if (System.getProperty("LOAD") == null || System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
            sql = String.format(PersonWorkRoleDataSQL.GET_RANDOM_PERSON_WORK_ROLE_IDS_FROM_STG_WITH_NO_ERROR, type, numberOfRecords);
        } else {
            sql = WorkCountSQL.GET_REFRESH_DATE;
            List<Map<String, Object>> refreshDateNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            String refreshDate = (String) refreshDateNumber.get(1).get("refresh_timestamp");
//            sql = String.format(PersonWorkRoleDataSQL.GET_RANDOM_DELTA, type, refreshDate, numberOfRecords);  -- uncomment when hardcoded value for refresh date is removed
            sql = String.format(PersonWorkRoleDataSQL.GET_RANDOM_DELTA, type, numberOfRecords);
            Log.info(sql);
        }


        List<Map<?, ?>> randomPersons = DBManager.getDBResultMap(sql, Constants.EPH_URL);

//      old  if (randomPersons.isEmpty()&& System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
//            Log.info("There are no records found for Person Work Role");
//        } else {

//        ids = randomPersons.stream().map(m -> (String) m.get("WORK_PERSON_ROLE_SOURCE_REF")).map(String::valueOf).collect(Collectors.toList());
//            idsSA = randomPersons.stream().map(m -> (BigDecimal) m.get("WORK_PERSON_ROLE_ID")).map(String::valueOf).collect(Collectors.toList());
//            Log.info(idsSA.toString());
//
//            //get ids (WORK_PERSON_SOURCE_REF)
//            Log.info("Get the ids of the records in EPH STG from the lookup table ..");
//            idsSourceRef = new ArrayList<>(idsSA);
//
//            sql = String.format(PersonWorkRoleDataSQL.GET_IDS_FROM_STG_FOR_GIVEN_IDS_IN_SA, Joiner.on("','").join(idsSA));
//            Log.info(sql);
//
//
//            List<Map<?, ?>> lookupResults = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomPersons.stream().map(m -> (String) m.get("WORK_PERSON_ROLE_SOURCE_REF")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());

//        if (ids.isEmpty()&& System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
//            Log.info("There are no records found for Person Work Role");
//        }
    }


    @When("^We get the person work role records with (.*) from PMX$")
    public void getPersonWorkRoleRecordsPMX(String type) {
        Log.info("Get the person work role records with type from PMX ..");

        //prepare ids
        idsPMX = new ArrayList<>(ids);
//        IntStream.range(0, idsPMX.size()).forEach(i -> idsPMX.set(i, idsPMX.get(i).substring(idsPMX.get(i).indexOf("-")+1, idsPMX.get(i).lastIndexOf("-"))));

        IntStream.range(0, idsPMX.size()).forEach(i -> idsPMX.set(i, idsPMX.get(i).replace("-" + type, "")));


        if (type.equals("PD")) {
            sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_PMX_PD, Joiner.on("','").join(idsPMX));
            Log.info(sql);
        }
        if (type.equals("AU")) {
            sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_PMX_AU, Joiner.on("','").join(idsPMX));
            Log.info(sql);
        }
        if (type.equals("PU")) {
            sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_PMX_PU, Joiner.on("','").join(idsPMX));
            Log.info(sql);
        }
        if (type.equals("AE")) {
            sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_PMX_AE, Joiner.on("','").join(idsPMX));
            Log.info(sql);
        }

        dataQualityContext.personWorkRoleDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.PMX_URL);

    }

    @Then("^We get the person work role records from EPH STG$")
    public void getPersonWorkRoleRecordsEphStg() {
        Log.info("Get the person work role records from EPH STG  ..");

//        idsEPHstg = new ArrayList<>(ids);
//        IntStream.range(0,idsEPHstg.size()).forEach(i -> idsEPHstg.set(i,idsEPHstg.get(i).substring(idsEPHstg.get(i).indexOf("-")+1)));

        sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHSTG, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.personWorkRoleDataObjectsFromEPHSTG = DBManager
                .getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);

    }


    @And("^Compare person work role records in PMX and EPH STG for (.*)$")
    public void comparePersonWorkRolesRecordsInPMXAndEPHSTG(String type) {
        Log.info("And compare work role records in PMX and EPH STG ..");
        if (dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.isEmpty() && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Person Work Role");
        } else {
            //sort the lists before comparison
            dataQualityContext.personWorkRoleDataObjectsFromPMX.sort(Comparator.comparing(PersonWorkRoleDataObject::getWORK_PERSON_ROLE_SOURCE_REF));
            dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.sort(Comparator.comparing(PersonWorkRoleDataObject::getWORK_PERSON_ROLE_SOURCE_REF));

            IntStream.range(0, dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.size()).forEach(i -> {


                //WORK_PERSON_ROLE_SOURCE_REF
                Log.info("WORK_PERSON_ROLE_SOURCE_REF in PMX: " + dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getWORK_PERSON_ROLE_SOURCE_REF());
                Log.info("WORK_PERSON_ROLE_SOURCE_REF in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getWORK_PERSON_ROLE_SOURCE_REF());

                Log.info("Expecting WORK_PERSON_ROLE_SOURCE_REF in PMX and EPH STG is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getWORK_PERSON_ROLE_SOURCE_REF(), dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getWORK_PERSON_ROLE_SOURCE_REF());


                //PMX_PARTY_SOURCE_REF
                Log.info("PMX_PARTY_SOURCE_REF in PMX : " + dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getPMX_PARTY_SOURCE_REF());
                Log.info("PMX_PARTY_SOURCE_REF in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getPMX_PARTY_SOURCE_REF());

                Log.info("Expecting PMX_PARTY_SOURCE_REF in PMX and EPH STG is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getPMX_PARTY_SOURCE_REF(), dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getPMX_PARTY_SOURCE_REF());

                //PMX_WORK_SOURCE_REF
                Log.info("PMX_WORK_SOURCE_REF in PMX : " + dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getPMX_WORK_SOURCE_REF());
                Log.info("PMX_WORK_SOURCE_REF in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getPMX_WORK_SOURCE_REF());

                Log.info("Expecting PMX_WORK_SOURCE_REF in PMX and EPH STG is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getPMX_WORK_SOURCE_REF(), dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getPMX_WORK_SOURCE_REF());


                //F_ROLE
                Log.info("F_ROLE in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getF_ROLE());

                assertEquals(type, dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getF_ROLE());


                //START_DATE
                Log.info("START_DATE in PMX : " + dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getSTART_DATE());
                Log.info("START_DATE in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getSTART_DATE());

                if(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getSTART_DATE() !=null &&  dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getSTART_DATE() != null) {
                    LocalDate pmxStartDate = LocalDate.parse(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getSTART_DATE(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"));

                    LocalDate ephStartDate = LocalDate.parse(dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getSTART_DATE(), DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                    Log.info("Expecting START_DATE in PMX and EPH STG is consistent");

                    if (!type.equals("PD"))
                        assertEquals(pmxStartDate, ephStartDate);
                }

//                //END_DATE
//                Log.info("END_DATE in PMX : " + dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getEND_DATE());
//                LocalDate pmxEndDate = LocalDate.parse(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getEND_DATE(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"));
//
//                Log.info("END_DATE in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getEND_DATE());
//                LocalDate ephEndDate = LocalDate.parse(dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getEND_DATE(), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
//
//                Log.info("Expecting END_DATE in PMX and EPH STG is consistent");
//
//                assertEquals(pmxEndDate, ephEndDate);


                //UPDATED
                Log.info("UPDATED in PMX : " + dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getUPDATED());
                Log.info("UPDATED in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getUPDATED());

                Log.info("Expecting UPDATED in PMX and EPH STG is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getUPDATED(), dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getUPDATED());


            });
        }
    }

//    @Then("^We get the ids of the person work role records in EPH SA from the lookup table$")
//    public void getIdsFromLookupTable() {
//        Log.info("Get the ids of the records in EPH SA from the lookup table ..");
//        idsSourceRef = new ArrayList<>(ids);
//
//
//        IntStream.range(0, idsSourceRef.size()).forEach(i -> idsSourceRef.set(i, "WPR-" + idsSourceRef.get(i)));
//        sql = String.format(PersonDataSQL.GET_IDS_FROM_LOOKUP_TABLE, Joiner.on("','").join(idsSourceRef));
//        Log.info(sql);
//
//
//        List<Map<?, ?>> lookupResults = DBManager.getDBResultMap(sql, Constants.EPH_URL);
//
//        idsSA = lookupResults.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());
//        Log.info(idsSA.toString());
//    }


    @Then("^We get the person work role records from EPH SA$")
    public void getPersonWorkRoleRecordsEPHSA() {
        Log.info("Get the person product role records from EPH SA  ..");
        sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHSA, Joiner.on("','").join(ids));
//        sql = PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHSA;
        Log.info(sql);

        dataQualityContext.personWorkRoleDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);
        sql.length();

    }

    @And("^Check the mandatory columns are populated for persons work role$")
    public void checkMandatoryColumnsForPersonsWorkRoleInSAArePopulated() {
        Log.info("We check that mandatory columns are populated ...");
        if (dataQualityContext.personWorkRoleDataObjectsFromEPHSA.isEmpty() && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There are no records found for Person Work Role");
        } else {
            IntStream.range(0, dataQualityContext.personWorkRoleDataObjectsFromEPHSA.size()).forEach(i -> {
                //verify F_EVENT is not null
                assertNotNull(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_EVENT());
                //verify WORK_PERSON_ROLE_ID is not null
                assertNotNull(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getWORK_PERSON_ROLE_ID());
                //verify F_ROLE
                assertNotNull(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_ROLE());
                //verify F_WWORK
                assertNotNull(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_WWORK());
                //verify F_PERSON
                assertNotNull(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_PERSON());


            });
        }
    }

    @And("^Compare person work role records in EPH STG and EPH SA$")
    public void comparePersonWorkRolesRecordsInSTGAndSA() {
        Log.info("And compare work role records in EPH STG and EPH SA ..");

        dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.sort(Comparator.comparing(PersonWorkRoleDataObject::getWORK_PERSON_ROLE_SOURCE_REF));
        dataQualityContext.personWorkRoleDataObjectsFromEPHSA.sort(Comparator.comparing(PersonWorkRoleDataObject::getWORK_PERSON_ROLE_SOURCE_REF));
        IntStream.range(0, dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.size()).forEach(i -> {

            //get data from SA for the current record from STG
//                String currentId = "WPR-" + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getWORK_PERSON_ROLE_SOURCE_REF();
//                sql = String.format(PersonDataSQL.GET_IDS_FROM_LOOKUP_TABLE, currentId);
//                Log.info(sql);
//
//                List<Map<?, ?>> lookupResults = DBManager.getDBResultMap(sql, Constants.EPH_URL);
//                idsSA = lookupResults.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());
//
//                sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHSA, Joiner.on("','").join(idsSA));
//                Log.info(sql);
//
//                if (dataQualityContext.personWorkRoleDataObjectsFromEPHSA.isEmpty()&& System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
//                    Log.info("There are no records found for Person Work Role");
//                } else {
//                    dataQualityContext.personWorkRoleDataObjectsFromEPHSA = DBManager
//                            .getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);


            //B_CLASSNAME
            Log.info("B_CLASSNAME in EPH SA: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
            assertEquals("WorkPersonRole", dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getB_CLASSNAME());


            //EFFECTIVE_START_DATE
            Log.info("EFFECTIVE_START_DATE in EPH STG : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getEFFECTIVE_START_DATE());
            Log.info("EFFECTIVE_START_DATE in EPH SA: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE());

            Log.info("Expecting EFFECTIVE_START_DATE in EPH STG and EPH SA to be consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getSTART_DATE(), dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE());


            //EFFECTIVE_END_DATE
            Log.info("EFFECTIVE_END_DATE in EPH STG : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getEFFECTIVE_END_DATE());
            Log.info("EFFECTIVE_END_DATE in EPH SA: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE());

            Log.info("Expecting EFFECTIVE_END_DATE in EPH STG and EPH SA to be consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getEND_DATE(), dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE());


            //F_ROLE
            Log.info("F_ROLE in EPH STG : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getF_ROLE());
            Log.info("F_ROLE in EPH SA: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_ROLE());

            Log.info("Expecting F_ROLE in EPH STG and EPH SA to be consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getF_ROLE(), dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_ROLE());

            //F_WWORK
            String currentF_WWORK = dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_WWORK();
            Log.info("F_WWORK in EPH SA: " + currentF_WWORK);


            String idL = dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getPMX_WORK_SOURCE_REF();
            sql = String.format(PersonDataSQL.GET_IDS_FROM_LOOKUP_TABLE_EPH, "WORK", idL);
            Log.info(sql);


            String expectedF_WWORK;


            List<Map<String, Object>> results = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            expectedF_WWORK = ((String) results.get(0).get("PERSON_ID"));

            assertEquals("Expecting F_WWORK in EPH STG and EPH SA to be consistent", expectedF_WWORK, currentF_WWORK);


            //F_PERSON

            Log.info("F_PERSON in EPH STG : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getF_PERSON());
            Log.info("F_PERSON in EPH SA: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_PERSON());

            Log.info("Expecting F_PERSON in EPH STG and EPH SA is consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getF_PERSON(), dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getF_PERSON());

        });

    }


    @Then("^We get the person work role records from EPH GD with (.*)$")
    public void getPersonWorkRoleRecordsEPHGD(String type) {
        Log.info("Get the person work role records from EPH GD  ..");
//        idsPMX = new ArrayList<>(ids);
//        IntStream.range(0, idsPMX.size()).forEach(i -> idsPMX.set(i, idsPMX.get(i).substring(idsPMX.get(i).indexOf("-")+1, idsPMX.get(i).lastIndexOf("-"))));

//        IntStream.range(0, idsPMX.size()).forEach(i -> idsPMX.set(i, idsPMX.get(i).replace("-" + type, "")));
        if (ids.isEmpty()) {
            sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHGD, '0');
        } else
            sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHGD, Joiner.on("','").join(ids));

        Log.info(sql);

        dataQualityContext.personWorkRoleDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);
        sql.length();

    }


    @And("^Compare person work role records in EPH SA and EPH GD$")
    public void comparePersonWorkRolesRecordsInSAAndGD() {
        Log.info("And compare work role records in EPH SA and EPH GD ..");
        if (dataQualityContext.personWorkRoleDataObjectsFromEPHSA.isEmpty() && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There are no records found for Person Work Role");
        } else {
            dataQualityContext.personWorkRoleDataObjectsFromEPHSA.sort(Comparator.comparing(PersonWorkRoleDataObject::getWORK_PERSON_ROLE_ID));
            dataQualityContext.personWorkRoleDataObjectsFromEPHGD.sort(Comparator.comparing(PersonWorkRoleDataObject::getWORK_PERSON_ROLE_ID));

            IntStream.range(0, dataQualityContext.personWorkRoleDataObjectsFromEPHSA.size()).forEach(i -> {


                //F_EVENT
                Log.info("F_EVENT in EPH SA : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_EVENT());
                Log.info("F_EVENT in EPH GD: " + dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getF_EVENT());

                Log.info("Expecting F_EVENT in EPH SA and EPH GD is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_EVENT(), dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getF_EVENT());


                //B_CLASSNAME
                Log.info("B_CLASSNAME in EPH SA : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
                Log.info("B_CLASSNAME in EPH GD: " + dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getB_CLASSNAME());

                Log.info("Expecting B_CLASSNAME in EPH SA and EPH GD is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getB_CLASSNAME(), dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getB_CLASSNAME());


                //WORK_PERSON_ROLE_ID
                Log.info("WORK_PERSON_ROLE_ID in EPH SA : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getWORK_PERSON_ROLE_ID());
                Log.info("WORK_PERSON_ROLE_ID in EPH GD: " + dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getWORK_PERSON_ROLE_ID());

                Log.info("Expecting WORK_PERSON_ROLE_ID in EPH SA and EPH GD is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getWORK_PERSON_ROLE_ID(), dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getWORK_PERSON_ROLE_ID());


                //EFFECTIVE_START_DATE
                Log.info("EFFECTIVE_START_DATE in EPH SA : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE());
                Log.info("EFFECTIVE_START_DATE in EPH GD: " + dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getEFFECTIVE_START_DATE());

                Log.info("Expecting EFFECTIVE_START_DATE in EPH SA and EPH GD is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE(), dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getEFFECTIVE_START_DATE());


                //EFFECTIVE_END_DATE
                Log.info("EFFECTIVE_END_DATE in EPH SA : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE());
                Log.info("EFFECTIVE_END_DATE in EPH GD: " + dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getEFFECTIVE_END_DATE());

                Log.info("Expecting EFFECTIVE_END_DATE in EPH SA and EPH GD is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE(), dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getEFFECTIVE_END_DATE());


                //F_ROLE
                Log.info("F_ROLE in EPH SA : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_ROLE());
                Log.info("F_ROLE in EPH GD: " + dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getF_ROLE());

                Log.info("Expecting F_ROLE in EPH SA and EPH GD is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_ROLE(), dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getF_ROLE());


                //F_WWORK
                Log.info("F_WWORK in EPH SA : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_WWORK());
                Log.info("F_WWORK in EPH GD: " + dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getF_WWORK());

                Log.info("Expecting F_WWORK in EPH SA and EPH GD is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_WWORK(), dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getF_WWORK());

                //F_PERSON
                Log.info("F_PERSON in EPH SA : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_PERSON());
                Log.info("F_PERSON in EPH GD: " + dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getF_PERSON());

                Log.info("Expecting F_PERSON in EPH SA and EPH GD is consistent");

                assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(i).getF_PERSON(), dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(i).getF_PERSON());

            });

        }
    }

}
