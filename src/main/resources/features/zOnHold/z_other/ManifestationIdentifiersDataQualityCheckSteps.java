package features.zOnHold.z_other;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import features.zOnHold.znotused.znotusedobjects.ManifestationIdentifierObject;
import features.zOnHold.znotused.WorkCountSQL;
import features.zOnHold.znotused.WorkExtractSQL;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by Bistra Drazheva on 23/01/2019
 */


public class ManifestationIdentifiersDataQualityCheckSteps {
    private String sql;
    private static int countIdentifiersSTGPMX;
    private static int countIdentifiersSA;
    private static int countIdentifiersGD;
    private static int countManifestationIdentifiersEPHAE;
    private static List<String> ids;
    private static List<Map<String, String>> manifestationIdentifiersDataObjects;


    @StaticInjection
    public DataQualityContext dataQualityContext;


    @Given("We get the count of records with (.*) in STG_PMX_MANIFESTATION$")
    public void getCountOfRecordsWithISBNInSTGPMX(String identifier) {
        if (System.getProperty("LOAD") == null || System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
            sql = String.format(WorkExtractSQL.COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_PMX_MANIFESTATION_TABLE, identifier, identifier, identifier);
            Log.info(sql);
        } else {
        sql = WorkCountSQL.GET_REFRESH_DATE;
        Log.info(sql);
        List<Map<String, Object>> refreshDateNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String refreshDate = (String) refreshDateNumber.get(1).get("refresh_timestamp");
        Log.info("refreshDate" + refreshDate);
        if(identifier.equals("ISBN"))
            sql = String.format(WorkExtractSQL.COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_PMX_MANIFESTATION_DELTA, refreshDate);
        else
            sql = String.format(WorkExtractSQL.COUNT_OF_RECORDS_WITH_ISSN_IN_EPH_STG_PMX_MANIFESTATION_DELTA, refreshDate);


        Log.info(sql);
        }

        List<Map<String, Object>> numberOfISBNs = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countIdentifiersSTGPMX = ((Long) numberOfISBNs.get(0).get("count")).intValue();
        Log.info("Count of of records in STG_PMX_MANIFESTATION table is: " + countIdentifiersSTGPMX);

    }

    @When("^We get the count of records with (.*) in SA_MANIFESTATION_IDENTIFIER$")
    public void getCountOfRecordsInEPHSA(String identifier) {
        if(identifier != null)
            sql = String.format(WorkExtractSQL.COUNT_OF_RECORDS_IN_EPH_SA_MANIFESTATION_TABLE, identifier);
        else
            sql = WorkExtractSQL.COUNT_OF_ALL_RECORDS_IN_EPH_SA_MANIFESTATION_TABLE;
        Log.info(sql);

        List<Map<String, Object>> numberOfISBNs = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countIdentifiersSA = ((Long) numberOfISBNs.get(0).get("count")).intValue();
        Log.info("Count of of records in SA_MANIFESTATION_IDENTIFIER table is: " + countIdentifiersSA);
    }

//    @When("^We get the count of all manifestation idenfieirs in SA_MANIFESTATION_IDENTIFIER$")
//    public void getCountOfAllRecordsInEPHSA() {
//
//        sql = String.format(WorkExtractSQL.COUNT_OF_RECORDS_IN_EPH_SA_MANIFESTATION_TABLE);
//        Log.info(sql);
//
//        List<Map<String, Object>> numberOfISBNs = DBManager.getDBResultMap(sql, Constants.EPH_URL);
//        countIdentifiersSA = ((Long) numberOfISBNs.get(0).get("count")).intValue();
//        Log.info("Count of of records in SA_MANIFESTATION_IDENTIFIER table is: " + countIdentifiersSA);
//    }


    @Then("^Check the count of the records in STG_PMX_MANIFESTATION and SA_MANIFESTATION_IDENTIFIER is equal for (.*)$")
    public void verifyCountOfRecordsIsEqualInSTGAndSA(String identifier) {
        assertEquals("\nThe number of records in STG_PMX_MANIFESTATION and SA_MANIFESTATION_IDENTIFIER is not equal for " + identifier, countIdentifiersSTGPMX, countIdentifiersSA);
    }

    @When("^We get the count of records with (.*) in GD_MANIFESTATION_IDENTIFIER$")
    public void getCountOfRecordsInEPHGD(String identifier) {
        if(identifier != null)
            sql = String.format(WorkExtractSQL.COUNT_OF_RECORDS_IN_EPH_GD_MANIFESTATION_TABLE, identifier);
        else
            sql = WorkExtractSQL.COUNT_OF_ALL_RECORDS_IN_EPH_GD_MANIFESTATION_TABLE;
        Log.info(sql);

        List<Map<String, Object>> numberOfISBNs = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countIdentifiersGD = ((Long) numberOfISBNs.get(0).get("count")).intValue();
        Log.info("Count of of records in GD_MANIFESTATION_IDENTIFIER table is: " + countIdentifiersGD);
    }

    @Given("^Get the count of records for manifestation identifiers in EPH AE$")
    public void getCountManifestationIdentifiersEPHAE() {
        Log.info("When We get the count of manifestation identifiers in PMX STG ..");
        sql = WorkExtractSQL.GET_COUNT_MANIFESTATIONS_IDENTIFIERS_EPHAE;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countManifestationIdentifiersEPHAE = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of manifestation identifiers in EPH AE is: " + countManifestationIdentifiersEPHAE);
    }

    @Then("^Verify sum of records for manifestation identifiers in EPH GD and EPH AE is equal to number of records in EPH SA$")
    public void verifyCountOfManifestationIdentifiersInEPHGDAndEPHAEIsEqualToEPHSA() {
        int sumOFRecords = countManifestationIdentifiersEPHAE + countIdentifiersGD;
        Assert.assertEquals("\nSum of the records for manifestation identifiers in EPH GD and EPH AE is NOT equal to number of records in EPH SA", sumOFRecords, countIdentifiersSA);
    }

    @Then("^Check the count of the records in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal for (.*)$")
    public void verifyCountOfRecordsWithISBNIsEqualInSAndGD(String identifier) {
        assertEquals("\nThe number of records in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is not equal for " + identifier, countIdentifiersSA, countIdentifiersGD);
    }

    @Given("^We get the manifestation ids of (.*) random records from STG_PMX_MANIFESTATION that have (.*) for (.*)$")
    public void getRandomRecords(String numberOfRecords, String identifier, String type) {
        //Get property when run with jenkins.
//        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);

        switch (type) {
            case "PHB":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISBNS_PHB, identifier, numberOfRecords);
                Log.info(sql);
                break;
            case "PSB":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISBNS_PSB, identifier, numberOfRecords);
                Log.info(sql);
                break;
            case "EBK":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISBNS_EBK, identifier, numberOfRecords);
                Log.info(sql);
                break;
            case "JPR":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISSNS_JPR_IDS, identifier, numberOfRecords);
                Log.info(sql);
                break;
            case "JEL":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISSNS_JEL_IDS, identifier, numberOfRecords);
                Log.info(sql);
                break;
            default:
                break;
        }

        List<Map<?, ?>> manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        if (identifier.equals("ISBN")) {
            ids = manifestationIds.stream().map(m -> (String) m.get("ISBN")).collect(Collectors.toList());
            Log.info("isbns : " + ids);

            sql = String.format(WorkExtractSQL.SELECT_RECORDS_STG_MANIF_IDENTIFIER_ISBN, Joiner.on("','").join(ids));
            Log.info(sql);

            dataQualityContext.manifestationIdentifiersDataObjectsFromSTG = DBManager
                    .getDBResultAsBeanList(sql, ManifestationIdentifierObject.class, Constants.EPH_URL);

            if ( CollectionUtils.isEmpty(dataQualityContext.manifestationIdentifiersDataObjectsFromSTG) && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
                Log.info("There are no records found for Manifestation identifiers");

            }
        }
        if (identifier.equals("ISSN")) {
            ids = manifestationIds.stream().map(m -> (String) m.get("ISSN")).collect(Collectors.toList());
            Log.info("issns : " + ids);

            sql = String.format(WorkExtractSQL.SELECT_RECORDS_STG_MANIF_IDENTIFIER_ISSN, Joiner.on("','").join(ids));
            Log.info(sql);

            dataQualityContext.manifestationIdentifiersDataObjectsFromSTG = DBManager
                    .getDBResultAsBeanList(sql, ManifestationIdentifierObject.class, Constants.EPH_URL);

            if ( CollectionUtils.isEmpty(dataQualityContext.manifestationIdentifiersDataObjectsFromSTG)&& System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD"))
                Log.info("There are no records found for Manifestation identifiers");

        }
    }


    @Given("^Get ISBN from STG and GD for end dated records in GD$")
    public void getISBNSForENDDATEDRecords() {

        sql = WorkExtractSQL.SELECT_ISBNS_FROM_STG_AND_SA_FOR_END_DATED_RECORDS;
        Log.info(sql);

        manifestationIdentifiersDataObjects = DBManager.getDBResultMap(sql, Constants.EPH_URL);

    }

    @Given("^Get ISSN from STG and GD for end dated records in GD$")
    public void getISSNSForENDDATEDRecords() {

        sql = WorkExtractSQL.SELECT_ISSNS_FROM_STG_AND_SA_FOR_END_DATED_RECORDS;
        Log.info(sql);

        manifestationIdentifiersDataObjects =  DBManager.getDBResultMap(sql, Constants.EPH_URL);

    }

//    @Given("^We get the manifestation ids of all records with set updated effective_end_date in SA for (.*)$")
//    public void getRecordsWithEndDate(String identifier) {
//
//        if (identifier.equals("ISBN")) {
//            sql = String.format(WorkExtractSQL.SELECT_MANIFESTATION_IDS_ISBN);
//            Log.info(sql);
//        } else if (identifier.equals("ISSN"))
//            sql = String.format(WorkExtractSQL.SELECT_MANIFESTATION_IDS_ISSN);
//        Log.info(sql);
//
//        List<Map<?, ?>> manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
//
//        ids = manifestationIds.stream().map(m -> (BigDecimal) m.get("MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());
//
//        Log.info("manifestation ids : " + ids);
//
//        if (identifier.equals("ISBN")) {
//            sql = String.format(WorkExtractSQL.SELECT_RECORDS_STG_MANIF_IDENTIFIER_ISBN_GIVEN_MANIF_ID, Joiner.on("','").join(ids));
//            Log.info(sql);
//        } else if (identifier.equals("ISSN"))
//            sql = String.format(WorkExtractSQL.SELECT_RECORDS_STG_MANIF_IDENTIFIER_ISSN_GIVEN_MAN_ID, Joiner.on("','").join(ids));
//
//
//        dataQualityContext.manifestationIdentifiersDataObjectsFromSTG = DBManager
//                .getDBResultAsBeanList(sql, ManifestationIdentifierObject.class, Constants.EPH_URL);
//
//    }

//    @Then("^Check the manifestation identifiers are updated for (.*)$")
//    public void checkIdentifiersAreUpdated(String identifier) {
//        IntStream.range(0, dataQualityContext.manifestationIdentifiersDataObjectsFromSTG.size()).forEach(i -> {
//            //get eph_id from ephsit_talend_owner.map_sourceref_2_ephid for current record
//            sql = String.format(WorkExtractSQL.GET_EPH_ID, dataQualityContext.manifestationIdentifiersDataObjectsFromSTG.get(i).getManif_identifier_id());
//            Log.info(sql);
//
//            List<Map<String, Object>> listEphIDs = DBManager.getDBResultMap(sql, Constants.EPH_URL);
//
//            String eph_id = listEphIDs.get(0).get("eph_id").toString();
//            Log.info(sql);
//
//            //get the ident_id
//            if (identifier.equals("ISBN"))
//                sql = String.format(WorkExtractSQL.GET_IDENT_ID, eph_id, dataQualityContext.manifestationIdentifiersDataObjectsFromSTG.get(i).getIdentifier());
//            else
//                sql = String.format(WorkExtractSQL.GET_IDENT_ID_ISSN, eph_id, dataQualityContext.manifestationIdentifiersDataObjectsFromSTG.get(i).getIdentifier());
//
//            Log.info(sql);
//
//            List<Map<String, Object>> listIdentIDs = DBManager.getDBResultMap(sql, Constants.EPH_URL);
//
//            String ident_id = listIdentIDs.get(0).get("ident_id").toString();
//            Log.info(sql);
//
//            //get data from GD for the current record from STG
//
//            sql = String.format(WorkExtractSQL.SELECT_RECORDS_GD_MANIFESTATION_IDENTIFIER_GIVEN_MANIF_ID, ident_id);
//            Log.info(sql);
//
//            List manifestationIdentifiersDataObjectsFromGD = DBManager
//                    .getDBResultAsBeanList(sql, ManifestationIdentifierObject.class, Constants.EPH_URL);
//
//
//            //assert identifier value is not equal
//            assertNotEquals(dataQualityContext.manifestationIdentifiersDataObjectsFromSTG.get(i).getIdentifier(), dataQualityContext.manifestationIdentifiersDataObjectsFromGD.get(0).getIdentifier());
//
//
//        });
//    }

    @Then("^Check the manifestation identifiers are updated properly$")
    public void checkIdentifiersAreUpdatedProperly() {

        if (manifestationIdentifiersDataObjects.size() != 0) {
            IntStream.range(0, manifestationIdentifiersDataObjects.size()).forEach(i -> {
                //assert identifier value is not equal

                String STG = manifestationIdentifiersDataObjects.get(i).get("gd_old_identifier");
                Log.info("ISBN in STG : " + STG);
                String GD = manifestationIdentifiersDataObjects.get(i).get("new_identifier");
                Log.info("ISBN in GD : " + GD);

                assertNotEquals("ISBNS are not updated!", STG, GD);

            });
        } else
            Log.info("No records with set end date in GD!");
    }


    @Then("^Check the manifestation identifiers are updated properly for ISSN$")
    public void checkIdentifiersAreUpdatedProperlyISSN() {

        if (manifestationIdentifiersDataObjects.size() != 0) {
            IntStream.range(0, manifestationIdentifiersDataObjects.size()).forEach(i -> {
                //assert identifier value is not equal

                String STG = manifestationIdentifiersDataObjects.get(i).get("gd_old_identifier");
                Log.info("ISSN in STG : " + STG);
                String GD = manifestationIdentifiersDataObjects.get(i).get("new_identifier");
                Log.info("ISSN in GD : " + GD);

                assertNotEquals("ISSN are not updated!", STG, GD);

            });
        } else
            Log.info("No records with set end date in GD!");
    }


    @When("^We get the records from SA_MANIFESTATION_IDENTIFIER$")
    public void getEPHStagingManifestationIdentifiersData() {

        sql = String.format(WorkExtractSQL.SELECT_RECORDS_SA_MANIFESTATION_IDENTIFIER, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.manifestationIdentifiersDataObjectsFromSA = DBManager
                .getDBResultAsBeanList(sql, ManifestationIdentifierObject.class, Constants.EPH_URL);

    }

    @Then("^Verify that data in SA_MANIFESTATION_IDENTIFIER is populated correctly for (.*)$")
    public void verifyDataIsPopulatedCorrectlyInSATable(String identifier) {

        Log.info("And the identifier data in STG and SA ..");

        dataQualityContext.manifestationIdentifiersDataObjectsFromSTG.sort(Comparator.comparing(ManifestationIdentifierObject::getExternal_reference));
        dataQualityContext.manifestationIdentifiersDataObjectsFromSA.sort(Comparator.comparing(ManifestationIdentifierObject::getExternal_reference));
        if ( CollectionUtils.isEmpty(dataQualityContext.manifestationIdentifiersDataObjectsFromSA) && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Manifestations");
        } else {
            IntStream.range(0, dataQualityContext.manifestationIdentifiersDataObjectsFromSA.size()).forEach(i -> {

                //b_classname
                assertEquals("ManifestationIdentifier", dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getB_classname());

                //f_type
                assertEquals(identifier, dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_type());


                //f_manifestation
                Log.info("F_MANIFESTATION  in EPH STG : " + dataQualityContext.manifestationIdentifiersDataObjectsFromSTG.get(i).getF_manifestation());
                Log.info("F_MANIFESTATION  in EPH SA : " + dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_manifestation());

                assertEquals(dataQualityContext.manifestationIdentifiersDataObjectsFromSTG.get(i).getF_manifestation(), dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_manifestation());

                //identifier
                Log.info("IDENTIFIER  in EPH STG : " + dataQualityContext.manifestationIdentifiersDataObjectsFromSTG.get(i).getIdentifier());
                Log.info("IDENTIFIER  in EPH SA: " + dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getIdentifier());

                assertEquals(dataQualityContext.manifestationIdentifiersDataObjectsFromSTG.get(i).getIdentifier(), dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getIdentifier());

            });
        }
    }

    @And("^We get the records from GD_MANIFESTATION_IDENTIFIER$")
    public void getEPHGDManifestationIdentifiersData() {
        sql = String.format(WorkExtractSQL.SELECT_RECORDS_GD_MANIFESTATION_IDENTIFIER, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.manifestationIdentifiersDataObjectsFromGD = DBManager
                .getDBResultAsBeanList(sql, ManifestationIdentifierObject.class, Constants.EPH_URL);

    }

    @Given("^We have identifiers in GD with set effective_end_date$")
    public void getEPHGDManifestationIdentifiersDataWithUpdatedEndDate() {
        sql = String.format(WorkExtractSQL.SELECT_RECORDS_GD_MANIFESTATION_IDENTIFIER, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.manifestationIdentifiersDataObjectsFromGD = DBManager
                .getDBResultAsBeanList(sql, ManifestationIdentifierObject.class, Constants.EPH_URL);
    }


    @And("^Verify the data in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal for (.*)$")
    public void verifyDataInSAAndGDIsEqual(String identifier) {
//        assertThat("\nData for manifestations in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal without order",  dataQualityContext.manifestationIdentifiersDataObjectsFromSA , containsInAnyOrder(DataQualityContext.manifestationIdentifiersDataObjectsFromGD.toArray()));

        //old logic
        //ids.sort(Comparator.reverseOrder());

        dataQualityContext.manifestationIdentifiersDataObjectsFromSA.sort(Comparator.comparing(ManifestationIdentifierObject::getManif_identifier_id));
        dataQualityContext.manifestationIdentifiersDataObjectsFromGD.sort(Comparator.comparing(ManifestationIdentifierObject::getManif_identifier_id));

        IntStream.range(0, dataQualityContext.manifestationIdentifiersDataObjectsFromSA.size()).forEach(i -> {

            //classname
            assertEquals(dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getB_classname(), dataQualityContext.manifestationIdentifiersDataObjectsFromGD.get(i).getB_classname());


            //f_type
            assertEquals(identifier, dataQualityContext.manifestationIdentifiersDataObjectsFromGD.get(i).getF_type());


            Log.info("F_MANIFESTATION  in EPH SA: " + dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_manifestation());
            Log.info("F_MANIFESTATION  in EPH GD : " + dataQualityContext.manifestationIdentifiersDataObjectsFromGD.get(i).getF_manifestation());

            //f_manifestation
            assertEquals(dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_manifestation(), dataQualityContext.manifestationIdentifiersDataObjectsFromGD.get(i).getF_manifestation());



        /* old logic
            //            assertEquals(ids.get(i) + "-" + dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_type(), DataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getManif_identifier_id());
            Objects.equals(dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_type(), dataQualityContext.manifestationIdentifiersDataObjectsFromGD.get(i).getF_type());
//            Objects.equals(dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getIdentifier(), dataQualityContext.manifestationIdentifiersDataObjectsFromGD.get(i).getIdentifier());
            //            assertEquals(ids.get(i), dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_manifestation());*/


        });


    }

}
