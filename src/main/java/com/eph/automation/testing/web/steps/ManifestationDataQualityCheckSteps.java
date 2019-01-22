package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.services.db.sql.ProductExtractSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static database.DataAccessUnitChecks.dataQualityContext;
import static org.junit.Assert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;


public class ManifestationDataQualityCheckSteps {
    private String sql;
    private static int countManifestationsEPH;
    private static int countManifestationsPMX;
    private static int countManifestationsSTGPMX;
    private static int countManifestationsEPHGD;
    private List<Map<?, ?>> randomISBNIds;
    private List<Map<?, ?>> manifestationIds;
    private static List<String> ids;
    private static List<String> isbns;
    private static List<String> idsManifestationsToSelect;


    @Given("We get the count of the manifestations records in PMX$")
    public void getCountManifestationsPmxGdProductManifestation() {
        sql = ProductExtractSQL.COUNT_MANIFESTATIONS_IN_PMX_GD_PRODUCT_MANIFESTATION_TABLE;
        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.PMX_SIT_URL);
        countManifestationsPMX = ((BigDecimal) manifestationsNumber.get(0).get("count")).intValue();
        System.out.println("\nCount of manifestations in PMX.GD_PRODUCT_MANIFESTATION table is: " + countManifestationsPMX);
    }

    @When("We get the count of the manifestations records in PMX STG$")
    public void getCountManifestationsPmxStg() {
        sql = ProductExtractSQL.COUNT_MANIFESTATIONS_IN_EPH_STG_PMX_MANIFESTATION_TABLE;
        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);
        countManifestationsSTGPMX = ((Long) manifestationsNumber.get(0).get("count")).intValue();
        System.out.println("\nCount of manifestations in STG_PMX_MANIFESTATION table is: " + countManifestationsSTGPMX);

    }

    @Then("^The number of the records in PMX and EPH staging table is equal$")
    public void verifyCountOfManifestationDataInPMXAndEPHIsEqual() {
        Assert.assertEquals("\nThe number of manifestations in PMX GD_PRODUCT_MANIFESTATION and STG_PMX_MANIFESTATION is equal", countManifestationsPMX, countManifestationsSTGPMX);
    }

    @When("^The manifestations are transferred to EPH$")
    public void getCountSAManifestation() {
        sql = ProductExtractSQL.COUNT_MANIFESTATIONS_IN_SA_MANIFESTATION_TABLE;
        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);
        countManifestationsEPH = ((Long) manifestationsNumber.get(0).get("count")).intValue();
        System.out.println("\nCount of manifestations in STG_PMX_MANIFESTATION table is: " + countManifestationsEPH);

    }

    @Then("^The number of the records in EPH staging table and SA_MANIFESTATION is equal$")
    public void verifyCountOfManifestationDataInSTGEPHandSAManifestationIsEqual() {
        Assert.assertEquals("\nThe number of manifestations in PMX_STG and SA_MANIFESTATION is equal", countManifestationsSTGPMX, countManifestationsEPH);
    }

    @Then("^The manifestations are transferred to the golden data table$")
    public void getCountGDManifestation() {
        sql = ProductExtractSQL.COUNT_MANIFESTATIONS_IN_GD_MANIFESTATION_TABLE;
        List<Map<String, Object>> manifestationsNumber = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);
        countManifestationsEPHGD = ((Long) manifestationsNumber.get(0).get("count")).intValue();
        System.out.println("\nCount of manifestations in GD_MANIFESTATION table is: " + countManifestationsEPHGD);
    }

    @Then("^The number of the records in EPH staging table and GD_MANIFESTATION is equal$")
    public void verifyCountOfManifestationsInStagingAndGoldenDataTableIsEqual() {
        Assert.assertEquals("\nThe number of manifestations in EPH_STG and GD_MANIFESTATION is equal", countManifestationsEPH, countManifestationsEPHGD);

    }

    @Given("^We get (.*) random records for (.*)$")
    public void getRandomData(String numberOfRecords, String journalType) {
        switch (journalType) {
            case "JPR":
                sql = String.format(ProductExtractSQL.SELECT_RANDOM_MANIFESTATION_IDS_JPR, numberOfRecords);
                break;
            case "JEL":
                sql = String.format(ProductExtractSQL.SELECT_RANDOM_MANIFESTATION_IDS_JEL, numberOfRecords);
                break;
        }
        manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);
        ids = manifestationIds.stream().map(m -> (String) m.get("manifestation_id")).collect(Collectors.toList());
    }


    @Given("^We get (.*) random ISBNs for (.*)$")
    public void getRandomISBNs(String numberOfRecords, String bookType) {
        switch (bookType) {
            case "PHB":
                sql = String.format(ProductExtractSQL.SELECT_RANDOM_ISBN_IDS_PHB, numberOfRecords);
                break;
            case "PSB":
                sql = ProductExtractSQL.SELECT_RANDOM_ISBN_IDS_PSB;
                break;
            case "EBK":
                sql = ProductExtractSQL.SELECT_RANDOM_ISBN_IDS_EBK;
                break;
            default:
                break;
        }

        randomISBNIds = DBManager.getDBResultMap(sql, Constants.EPH_SIT_URL);

        isbns = randomISBNIds.stream().map(m -> (String) m.get("isbn")).collect(Collectors.toList());
    }

    @When("^We get the manifestation ids for these books$")
    public void getManifestationsIds() {
        //  Get manifestations ids for isbns which are in PMX STG
        String sqlSelectManifestationIDS = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_IDS_FOR_SPECIFIC_ISBN, Joiner.on("','").join(isbns));

        manifestationIds = DBManager.getDBResultMap(sqlSelectManifestationIDS, Constants.EPH_SIT_URL);
        ids = manifestationIds.stream().map(m -> (String) m.get("manifestation_id")).collect(Collectors.toList());
    }

    @Then("^We get the manifestations records from PMX$")
    public void getPMXManifestationData() {
        sql = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX, Joiner.on("','").join(ids));

        dataQualityContext.manifestationDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.PMX_SIT_URL);
        sql.length();

    }

    @Then("^We have the manifestations in PMX STG$")
    public void getEPHStagingManifestationData() {
        sql = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX_STG, Joiner.on("','").join(ids));

        dataQualityContext.manifestationDataObjectsFromEPH = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_SIT_URL);
        sql.length();

    }


    @And("^The data for manifestations in PMX and PMX STG is equal$")
    public void compareManifestationDataBetweenPMXAndEPH() {
        assertThat("Data for manifestations in PMX and EPH staging is equal without order", dataQualityContext.manifestationDataObjectsFromPMX, containsInAnyOrder(dataQualityContext.manifestationDataObjectsFromEPH.toArray()));
    }


    @And("^We get the manifestations in EPH$")
    public void getManifestationsEPHSA() {
//        //  Get manifestations ids for isbns which are in PMX STG
//        String sqlSelectManifestationIDS = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_IDS_FOR_SPECIFIC_ISBN, Joiner.on("','").join(ids));
//
//        manifestationIds = DBManager.getDBResultMap(sqlSelectManifestationIDS, Constants.EPH_SIT_URL);
//        List<String> idsManifestationsToSelect = manifestationIds.stream().map(m -> (String) m.get("manifestation_id")).collect(Collectors.toList());

        // Get manifestations data from EPH SA_MANIFESTATION
        sql = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX_SA, Joiner.on("','").join(ids));

        dataQualityContext.manifestationDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_SIT_URL);
        sql.length();
    }

    @And("^We compare the manifestations in PMX STG and EPH$")
    public void compareDataBetweenStagingAndEPHSA() {
        assertThat("Data for manifestations in EPH Staging and EPH SA is equal without order", dataQualityContext.manifestationDataObjectsFromEPH, containsInAnyOrder(dataQualityContext.manifestationDataObjectsFromEPHSA.toArray()));

    }


    @And("^We get the manifestations in EPH golden data$")
    public void getManifestationsEPHGD() {
//        //  Get manifestations ids for isbns which are in PMX STG
//        String sqlSelectManifestationIDS = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_IDS_FOR_SPECIFIC_ISBN, Joiner.on("','").join(ids));
//
//        manifestationIds = DBManager.getDBResultMap(sqlSelectManifestationIDS, Constants.EPH_SIT_URL);
//        List<String> idsManifestationsToSelect = manifestationIds.stream().map(m -> (String) m.get("manifestation_id")).collect(Collectors.toList());

        // Get manifestations data from EPH SA_MANIFESTATION
        sql = String.format(ProductExtractSQL.SELECT_MANIFESTATIONS_DATA_IN_PMX_GD, Joiner.on("','").join(ids));

        dataQualityContext.manifestationDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_SIT_URL);
        sql.length();
    }

    @And("^We compare the manifestations in EPH and EPH golden data$")
    public void compareDataBetweenEPHAndEPHGD() {
        assertThat("Data for manifestations in EPH and EPH GD is equal without order", dataQualityContext.manifestationDataObjectsFromEPH, containsInAnyOrder(dataQualityContext.manifestationDataObjectsFromEPHGD.toArray()));

    }
}
