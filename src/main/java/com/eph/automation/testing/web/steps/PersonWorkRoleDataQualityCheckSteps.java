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
    private static int countPersonsWorkRoleEPHSTGDQ;
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

        if (System.getProperty("LOAD") != null) {
            if (System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                sql = PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHSTG;
                Log.info(sql);
            } else {
                sql = WorkCountSQL.GET_REFRESH_DATE;
                List<Map<String, Object>> refreshDateNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                String refreshDate = (String) refreshDateNumber.get(0).get("refresh_timestamp");
                sql = String.format( PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHSTG_DELTA, refreshDate );
            }
        }



        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsWorkRoleEPHSTG = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons work role in EPH STG is: " + countPersonsWorkRoleEPHSTG);
    }

    @When("^Get the count of records for persons work role in EPH Staging with DQ$")
    public void getCountPersonsProductRoleEPHSTG_DQ() {
        Log.info("When We get the count of persons work role records in EPH STG with DQ ..");
        sql = PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHSTGDQ;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsWorkRoleEPHSTGDQ = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons work role in EPH STG with DQ is: " + countPersonsWorkRoleEPHSTGDQ);
    }


    @Then("^Compare the count on records for persons work role in PMX and EPH Staging$")
    public void verifyCountOfPersonsProductRoleInPMXAndEPHSTGIsEqual() {
        Assert.assertEquals("\nPersons work role count in PMX and EPH STG is not equal", countPersonsWorkRolePMX, countPersonsWorkRoleEPHSTG);
    }


    @When("^Get the count of records for persons work role in EPH SA$")
    public void getCountPersonsProductRoleEPHSA() {
        Log.info("When We get the count of persons records in PMX STG ..");
        sql = PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHSA;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsWorkRoleEPHSA = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons work role in EPH SA is: " + countPersonsWorkRoleEPHSA);
    }

    @Given("^Get the count of records for persons work role in EPH AE$")
    public void getCountPersonsProductRoleEPHAE() {
        Log.info("When We get the count of persons records in PMX STG ..");
        sql = PersonWorkRoleDataSQL.GET_COUNT_PERSONS_WORK_ROLE_EPHAE;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countPersonsWorkRoleEPHAE = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of persons work role in EPH AE is: " + countPersonsWorkRoleEPHAE);
    }


    @Then("^Compare the count on records for persons work role in EPH Staging with DQ and EPH SA$")
    public void verifyCountOfPersonsProductRoleInEPHSTGAndEPHSAIsEqual() {
        Assert.assertEquals("\nPersons work role count in EPH STG with DQ and EPH SA is not equal", countPersonsWorkRoleEPHSTGDQ, countPersonsWorkRoleEPHSA);
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
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);

        switch(type) {
            case "PD":
//                sql = String.format(PersonWorkRoleDataSQL.GET_RANDOM_PERSON_WORK_ROLE_IDS, type, numberOfRecords);
                sql = String.format(PersonWorkRoleDataSQL.GET_RANDOM_PERSON_WORK_ROLE_IDS_FROM_SA_WITH_NO_ERROR, type, numberOfRecords);
                Log.info(sql);
                break;
            case "AU":
//                sql = String.format(PersonWorkRoleDataSQL.GET_RANDOM_PERSON_WORK_ROLE_IDS, type, numberOfRecords);
                sql = String.format(PersonWorkRoleDataSQL.GET_RANDOM_PERSON_WORK_ROLE_IDS_FROM_SA_WITH_NO_ERROR, type, numberOfRecords);
                Log.info(sql);
                break;
            case "PU":
//                sql = String.format(PersonWorkRoleDataSQL.GET_RANDOM_PERSON_WORK_ROLE_IDS, type, numberOfRecords);
                sql = String.format(PersonWorkRoleDataSQL.GET_RANDOM_PERSON_WORK_ROLE_IDS_FROM_SA_WITH_NO_ERROR, type, numberOfRecords);
                Log.info(sql);
                break;
            default:
                break;
        }

        List<Map<?, ?>> randomPersons = DBManager.getDBResultMap(sql, Constants.EPH_URL);

//        ids = randomPersons.stream().map(m -> (String) m.get("WORK_PERSON_ROLE_SOURCE_REF")).map(String::valueOf).collect(Collectors.toList());
        idsSA = randomPersons.stream().map(m -> (BigDecimal) m.get("WORK_PERSON_ROLE_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info(idsSA.toString());

        //get ids (WORK_PERSON_SOURCE_REF)
        Log.info("Get the ids of the records in EPH STG from the lookup table ..");
        idsSourceRef = new ArrayList<>(idsSA);


//        IntStream.range(0, idsSourceRef.size()).forEach(i -> idsSourceRef.set(i, "WPR-" + idsSourceRef.get(i)));
        sql = String.format(PersonWorkRoleDataSQL.GET_IDS_FROM_LOOKUP_TABLE, Joiner.on("','").join(idsSA));
        Log.info(sql);


        List<Map<?, ?>> lookupResults = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = lookupResults.stream().map(m -> (String) m.get("WORK_PERSON_ROLE_SOURCE_REF")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());
    }


    @When("^We get the person work role records with (.*) from PMX$")
    public void getPersonWorkRoleRecordsPMX(String type) {
        Log.info("Get the person work role records with type from PMX ..");

        //prepare ids
        idsPMX = new ArrayList<>(ids);
        IntStream.range(0, idsPMX.size()).forEach(i -> idsPMX.set(i, idsPMX.get(i).substring(idsPMX.get(i).indexOf("-")+1, idsPMX.get(i).lastIndexOf("-"))));


        if(type.equals("PD")) {
            sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_PMX_PD, Joiner.on("','").join(idsPMX));
            Log.info(sql);
        } if(type.equals("AU")) {
            sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_PMX_AU, Joiner.on("','").join(idsPMX));
            Log.info(sql);
        } if(type.equals("PU")) {
            sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_PMX_PU, Joiner.on("','").join(idsPMX));
            Log.info(sql);
        }

        dataQualityContext.personWorkRoleDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.PMX_URL);
    }

    @Then("^We get the person work role records from EPH STG$")
    public void getPersonWorkRoleRecordsEphStg() {
        Log.info("Get the person work role records from EPH STG  ..");

        idsEPHstg = new ArrayList<>(ids);
        IntStream.range(0,idsEPHstg.size()).forEach(i -> idsEPHstg.set(i,idsEPHstg.get(i).substring(idsEPHstg.get(i).indexOf("-")+1)));

        sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHSTG, Joiner.on("','").join(idsEPHstg));
        Log.info(sql);

        dataQualityContext.personWorkRoleDataObjectsFromEPHSTG = DBManager
                .getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);
    }


    @And("^Compare person work role records in PMX and EPH STG for (.*)$")
    public void comparePersonWorkRolesRecordsInPMXAndEPHSTG(String type) {
        Log.info("And compare work role records in PMX and EPH STG ..");

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
            Log.info("PMX_PARTY_SOURCE_REF in PMX : " + dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getWORK_PERSON_ROLE_SOURCE_REF());
            Log.info("PMX_PARTY_SOURCE_REF in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getWORK_PERSON_ROLE_SOURCE_REF());

            Log.info("Expecting PMX_PARTY_SOURCE_REF in PMX and EPH STG is consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getWORK_PERSON_ROLE_SOURCE_REF(), dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getWORK_PERSON_ROLE_SOURCE_REF());

            //PMX_WORK_SOURCE_REF
            Log.info("PMX_WORK_SOURCE_REF in PMX : " + dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getPMX_WORK_SOURCE_REF());
            Log.info("PMX_WORK_SOURCE_REF in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getPMX_WORK_SOURCE_REF());

            Log.info("Expecting PMX_WORK_SOURCE_REF in PMX and EPH STG is consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getPMX_WORK_SOURCE_REF(), dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getPMX_WORK_SOURCE_REF());


            //F_ROLE
            Log.info("F_ROLE in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getF_ROLE());

            assertEquals(type , dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getF_ROLE());


            //START_DATE
            Log.info("START_DATE in PMX : " + dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getSTART_DATE());
            Log.info("START_DATE in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getSTART_DATE());

            Log.info("Expecting START_DATE in PMX and EPH STG is consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getSTART_DATE(), dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getSTART_DATE());

            //END_DATE
            Log.info("END_DATE in PMX : " + dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getEND_DATE());
            Log.info("END_DATE in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getEND_DATE());

            Log.info("Expecting END_DATE in PMX and EPH STG is consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getEND_DATE(), dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getEND_DATE());


            //UPDATED
            Log.info("UPDATED in PMX : " + dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getUPDATED());
            Log.info("UPDATED in EPH STG: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getUPDATED());

            Log.info("Expecting UPDATED in PMX and EPH STG is consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromPMX.get(i).getUPDATED(), dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getUPDATED());


        });

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
        sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHSA, Joiner.on("','").join(idsSA));
//        sql = PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHSA;
        Log.info(sql);

        dataQualityContext.personWorkRoleDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);
        sql.length();
    }

    @And("^Check the mandatory columns are populated for persons work role$")
    public void checkMandatoryColumnsForPersonsWorkRoleInSAArePopulated() {
        Log.info("We check that mandatory columns are populated ...");

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

    @And("^Compare person work role records in EPH STG and EPH SA$")
    public void comparePersonWorkRolesRecordsInSTGAndSA() {
        Log.info("And compare work role records in EPH STG and EPH SA ..");


        IntStream.range(0, dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.size()).forEach(i -> {

            //get data from SA for the current record from STG
            String currentId = "WPR-" + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getWORK_PERSON_ROLE_SOURCE_REF();
            sql = String.format(PersonDataSQL.GET_IDS_FROM_LOOKUP_TABLE, currentId);
            Log.info(sql);

            List<Map<?, ?>> lookupResults = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            idsSA = lookupResults.stream().map(m -> (BigDecimal) m.get("PERSON_ID")).map(String::valueOf).collect(Collectors.toList());

            sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHSA, Joiner.on("','").join(idsSA));
            Log.info(sql);

            dataQualityContext.personWorkRoleDataObjectsFromEPHSA = DBManager
                    .getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);


            //B_CLASSNAME
            Log.info("B_CLASSNAME in EPH SA: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(0).getB_CLASSNAME());
            assertEquals("WorkPersonRole", dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(0).getB_CLASSNAME());


            //EFFECTIVE_START_DATE
            Log.info("EFFECTIVE_START_DATE in EPH STG : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getEFFECTIVE_START_DATE());
            Log.info("EFFECTIVE_START_DATE in EPH SA: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(0).getEFFECTIVE_START_DATE());

            Log.info("Expecting EFFECTIVE_START_DATE in EPH STG and EPH SA to be consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getEFFECTIVE_START_DATE(), dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(0).getEFFECTIVE_START_DATE());


            //EFFECTIVE_END_DATE
            Log.info("EFFECTIVE_END_DATE in EPH STG : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getEFFECTIVE_END_DATE());
            Log.info("EFFECTIVE_END_DATE in EPH SA: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(0).getEFFECTIVE_END_DATE());

            Log.info("Expecting EFFECTIVE_END_DATE in EPH STG and EPH SA to be consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getEFFECTIVE_END_DATE(), dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(0).getEFFECTIVE_END_DATE());


            //F_ROLE
            Log.info("F_ROLE in EPH STG : " + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getF_ROLE());
            Log.info("F_ROLE in EPH SA: " + dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(0).getF_ROLE());

            Log.info("Expecting F_ROLE in EPH STG and EPH SA to be consistent");

            assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getF_ROLE(), dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(0).getF_ROLE());

            //F_WWORK
            String currentF_WWORK = dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(0).getF_WWORK();
            Log.info("F_WWORK in EPH SA: " + currentF_WWORK);


            String idL = dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getPMX_WORK_SOURCE_REF();
            sql = String.format(PersonDataSQL.GET_IDS_FROM_LOOKUP_TABLE_EPH, "WORK", idL);
            Log.info(sql);


            String expectedF_WWORK;


            List<Map<String, Object>> results = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            expectedF_WWORK = ((String) results.get(0).get("PERSON_ID"));

            assertEquals("Expecting F_WWORK in EPH STG and EPH SA to be consistent", expectedF_WWORK, currentF_WWORK);


            //F_PERSON

            String currentF_PERSON = dataQualityContext.personWorkRoleDataObjectsFromEPHSA.get(0).getF_PERSON();



            idL = "PERSON-" + dataQualityContext.personWorkRoleDataObjectsFromEPHSTG.get(i).getPMX_PARTY_SOURCE_REF();
            sql = String.format(PersonDataSQL.GET_IDS_FROM_LOOKUP_TABLE, idL);
            Log.info(sql);

            String expectedF_PERSON;
            List<Map<String,Object>> p_results = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            expectedF_PERSON = (p_results.get(0).get("PERSON_ID")).toString();

            Log.info("F_PERSON in EPH STG: " + expectedF_PERSON);
            Log.info("F_PERSON in EPH SA: " + currentF_PERSON);

            assertEquals("Expecting F_PERSON in EPH STG and EPH SA to be consistent",currentF_PERSON, expectedF_PERSON);

        });

    }


    @Then("^We get the person work role records from EPH GD$")
    public void getPersonWorkRoleRecordsEPHGD() {
        Log.info("Get the person work role records from EPH GD  ..");
        sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHGD, Joiner.on("','").join(idsSA));
//        sql = PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHGD;

        Log.info(sql);

        dataQualityContext.personWorkRoleDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);
        sql.length();
    }


    @And("^Compare person work role records in EPH SA and EPH GD$")
    public void comparePersonWorkRolesRecordsInSAAndGD() {
        Log.info("And compare work role records in EPH SA and EPH GD ..");

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
