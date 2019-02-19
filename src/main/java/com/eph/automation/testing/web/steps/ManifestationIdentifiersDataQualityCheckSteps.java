package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ManifestationIdentifierObject;
import com.eph.automation.testing.services.db.sql.WorkExtractSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Created by Bistra Drazheva on 23/01/2019
 */


public class ManifestationIdentifiersDataQualityCheckSteps {
    private String sql;
    private static int countISBNSTGPMX;
    private static int countISBNSA;
    private static int countISBNGD;
    private static List<String> ids;


    @StaticInjection
    public DataQualityContext dataQualityContext;


    @Given("We get the count of records with (.*) in STG_PMX_MANIFESTATION$")
    public void getCountOfRecordsWithISBNInSTGPMX(String identifier) {
        sql = String.format(WorkExtractSQL.COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_PMX_MANIFESTATION_TABLE, identifier);
        List<Map<String, Object>> numberOfISBNs = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);
        countISBNSTGPMX = ((Long) numberOfISBNs.get(0).get("count")).intValue();
        System.out.println("\nCount of of records in STG_PMX_MANIFESTATION table is: " + countISBNSTGPMX);
    }

    @When("^We get the count of records with (.*) in SA_MANIFESTATION_IDENTIFIER$")
    public void getCountOfRecordsInEPHSA(String identifier) {
        sql = String.format(WorkExtractSQL.COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_SA_MANIFESTATION_TABLE, identifier);
        List<Map<String, Object>> numberOfISBNs = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);
        countISBNSA = ((Long) numberOfISBNs.get(0).get("count")).intValue();
        System.out.println("\nCount of of records in SA_MANIFESTATION_IDENTIFIER table is: " + countISBNSA);
    }

    @Then("^Check the count of the records in STG_PMX_MANIFESTATION and SA_MANIFESTATION_IDENTIFIER is equal for (.*)$")
    public void verifyCountOfRecordsIsEqualInSTGAndSA(String identifier) {
        assertEquals("\nThe number of records in STG_PMX_MANIFESTATION and SA_MANIFESTATION_IDENTIFIER is equal for " + identifier, countISBNSTGPMX, countISBNSA);
    }

    @When("^We get the count of records with (.*) in GD_MANIFESTATION_IDENTIFIER$")
    public void getCountOfRecordsInEPHGD(String identifier) {
        sql = String.format(WorkExtractSQL.COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_GD_MANIFESTATION_TABLE, identifier);
        List<Map<String, Object>> numberOfISBNs = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);
        countISBNGD = ((Long) numberOfISBNs.get(0).get("count")).intValue();
        System.out.println("\nCount of of records in GD_MANIFESTATION_IDENTIFIER table is: " + countISBNGD);
    }

    @Then("^Check the count of the records in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal for (.*)$")
    public void verifyCountOfRecordsWithISBNIsEqualInSAndGD(String identifier) {
        assertEquals("\nThe number of records in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal for " + identifier, countISBNSA, countISBNGD);
    }

    @Given("^We get the manifestation ids of (.*) random records from STG_PMX_MANIFESTATION that have (.*) for (.*)$")
    public void getRandomRecords(String numberOfRecords, String identifier, String type) {
        numberOfRecords = System.getProperty("dbRecordsNumber");
        switch (type) {
            case "PHB":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISBNS_PHB, identifier, numberOfRecords);
                break;
            case "PSB":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISBNS_PSB, identifier, numberOfRecords);
                break;
            case "EBK":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISBNS_EBK, identifier, numberOfRecords);
                break;
            case "JPR":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISSNS_JPR_IDS, numberOfRecords);
                break;
            case "JEL":
                sql = String.format(WorkExtractSQL.SELECT_RANDOM_ISSNS_JEL_IDS, numberOfRecords);
                break;
            default:
                break;
        }

        List<Map<?, ?>> manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);

        if (identifier.equals("ISBN")) {
            ids = manifestationIds.stream().map(m -> (String) m.get("ISBN")).collect(Collectors.toList());
        }
        if (identifier.equals("ISSN")) {
            ids = manifestationIds.stream().map(m -> (String) m.get("ISSN")).collect(Collectors.toList());
        }
    }

    @When("^We get the records from SA_MANIFESTATION_IDENTIFIER$")
    public void getEPHStagingManifestationIdentifiersData() {

        sql = String.format(WorkExtractSQL.SELECT_RECORDS_SA_MANIFESTATION_IDENTIFIER, Joiner.on("','").join(ids));

        dataQualityContext.manifestationIdentifiersDataObjectsFromSA = DBManager
                .getDBResultAsBeanList(sql, ManifestationIdentifierObject.class, Constants.EPH_SIT_URL);
        sql.length();

    }

    @Then("^Verify that data in SA_MANIFESTATION_IDENTIFIER is populated correctly for (.*)$")
    public void verifyDataIsPopulatedCorrectlyInSATable(String identifier) {

        IntStream.range(0, dataQualityContext.manifestationIdentifiersDataObjectsFromSA.size()).forEach(i -> {

            assertEquals("ManifestationIdentifier", dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getB_classname());
//            assertEquals(ids.get(i) + "-" + dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_type(), DataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getManif_identifier_id());
            assertEquals(identifier, dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_type());
//            assertEquals(ids.get(i), dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_manifestation());
        });
    }

    @And("^We get the records from GD_MANIFESTATION_IDENTIFIER$")
    public void getEPHGDManifestationIdentifiersData() {
        sql = String.format(WorkExtractSQL.SELECT_RECORDS_GD_MANIFESTATION_IDENTIFIER, Joiner.on("','").join(ids));

        dataQualityContext.manifestationIdentifiersDataObjectsFromGD = DBManager
                .getDBResultAsBeanList(sql, ManifestationIdentifierObject.class, Constants.EPH_SIT_URL);
    }

    @And("^Verify the data in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal$")
    public void verifyDataInSAAndGDIsEqual() {
//        assertThat("\nData for manifestations in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal without order",  dataQualityContext.manifestationIdentifiersDataObjectsFromSA , containsInAnyOrder(DataQualityContext.manifestationIdentifiersDataObjectsFromGD.toArray()));
        ids.sort(Comparator.reverseOrder());


        IntStream.range(0, dataQualityContext.manifestationIdentifiersDataObjectsFromSA.size()).forEach(i -> {

            assertEquals(dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getB_classname(), dataQualityContext.manifestationIdentifiersDataObjectsFromGD.get(i).getB_classname());
            //            assertEquals(ids.get(i) + "-" + dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_type(), DataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getManif_identifier_id());
            Objects.equals(dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_type(), dataQualityContext.manifestationIdentifiersDataObjectsFromGD.get(i).getF_type());
//            Objects.equals(dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getIdentifier(), dataQualityContext.manifestationIdentifiersDataObjectsFromGD.get(i).getIdentifier());
            //            assertEquals(ids.get(i), dataQualityContext.manifestationIdentifiersDataObjectsFromSA.get(i).getF_manifestation());
        });


    }

}
