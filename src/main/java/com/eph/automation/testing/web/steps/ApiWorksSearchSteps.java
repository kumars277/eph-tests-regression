package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.api.*;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.*;
import com.eph.automation.testing.services.api.APIService;
import com.eph.automation.testing.services.api.AzureOauthTokenFetchingException;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import static com.eph.automation.testing.models.contexts.DataQualityContext.*;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import cucumber.api.PendingException;
import cucumber.api.java.en.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.Assert;

/*
 * Created by GVLAYKOV on 11/02/2019
 * updated by Nishant @ 20 April 2020 for data model changes
 */

public class ApiWorksSearchSteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    private APIService apiService=new APIService();
    private String sql;
    //private static List<String> ids;
    private static List<String> manifestaionids;
    private WorkApiObject workApi_response;
    private static List<WorkDataObject> workIdentifiers;
    private ApiProductsSearchSteps apiProductsSearchSteps;

    String EndPoint;
    public ApiWorksSearchSteps() {}

    @Given("^We get (.*) random search ids for works")
    public void getRandomWorkIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_IDS_FOR_SEARCH, numberOfRecords);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
        //added by Nishant @ 27 Dec for debugging failures
        ids.clear();ids.add("EPR-W-102S7C");Log.info("hard coded work ids are : " + ids);
        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }

    @Given("^We get id for work search (.*)$")
    public void getProductById(String workID) {
        sql = String.format(APIDataSQL.SELECT_WORK_BY_ID_FOR_SEARCH, workID);
//        Log.info(sql);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        DataQualityContext.ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("work ids  : " + ids);
    }

    private void getRandomWorkIdWithProducts() {
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_IDS_WITH_PRODUCT);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids with product is : " + ids);
        Assert.assertFalse("verify list with random id is not empty", ids.isEmpty());
    }

    @Given("^We get (.*) random search ids for Extended works")
    public void getRandomExtendedWorkIds(String numberOfRecords) {
        //created by Nishant @ 01 Jul 2020
        sql = String.format(APIDataSQL.SELECT_RANDOM_EXTENDED_WORK_IDS_SIT, numberOfRecords);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
        //added by Nishant @ 27 Dec for debugging failures
        // ids.clear(); ids.add("EPR-W-108TJK");  Log.info("hard coded work ids are : " + ids);
        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }

    @Given("^We get (.*) random search ids for Extended manifestation")
    public void getRandomExtendedManifestationIds(String numberOfRecords) {
        //created by Nishant @ 01 Jul 2020
        sql = String.format(APIDataSQL.SELECT_RANDOM_EXTENDED_MANIFESTATION_IDS_SIT, numberOfRecords);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        manifestaionids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random extended manifestation ids  : " + manifestaionids);
        Assert.assertFalse("Verify That list with random ids is not empty.", manifestaionids.isEmpty());
    }

    @Given("^We get (.*) random journal ids for search")
    public void getRandomJournalIds(String numberOfRecords) {//created by Nishant @ 25 Jun 2020
        sql = String.format(APIDataSQL.SELECT_RANDOM_JOURNAL_IDS_FOR_SEARCH, numberOfRecords);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random Journal ids  : " + ids);
        //for debugging failure
       // ids.clear(); ids.add("EPR-W-102SN4");  Log.info("hard coded work ids are : " + ids); //EPR-W-108VK7, EPR-W-108RJG   , EPR-W-108V6K
        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }

    @Given("^We set specific journal ids for search")
    public void setspecificJournalIds() {//created by Nishant @ 04 Aug 2020
        ids=new ArrayList<>();
        //production random work ids
        ids.add("EPR-W-102RGY");
        ids.add("EPR-W-102RB6");
        ids.add("EPR-W-102NHD");
        ids.add("EPR-W-102RRG");
        ids.add("EPR-W-102VF4");

        Log.info("hard coded work ids are : " + ids);

    }

    @Given("^We get (.*) random search ids for person roles")
    public void getRandomPersonRolesIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_PERSON_ROLES_FOR_SEARCH, numberOfRecords);
        List<Map<?, ?>> randomPersonSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomPersonSearchIds.stream().map(m -> (BigDecimal) m.get("f_person")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random person ids  : " + ids);
      //  ids.clear(); ids.add("10077793");  Log.info("hard coded work ids are : " + ids);
        Assert.assertFalse("Verify That list with random person roles is not empty.", ids.isEmpty());
    }

    @And("^We get the work search data from EPH GD$")
    public void getWorksDataFromEPHGD() {
        Log.info("We get the work data from EPH GD ...");
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(DataQualityContext.ids));
        dataQualityContext.workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        dataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));
        Assert.assertFalse("Verify that list with work objects from DB is not empty", dataQualityContext.workDataObjectsFromEPHGD.isEmpty());
    }

    @Given("We get the work data from EPH GD for (.*)")
    public void getSpecificWorkDataFromEPHGD(String WorkId) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, WorkId);
        DataQualityContext.workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        DataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));
    }


    //Updated by Nishant for data model changes in Apr 2020
    @When("^the work details are retrieved and compared$")
    public void compareWorkSearchResultsWithDB() throws AzureOauthTokenFetchingException {
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            Log.info("#################################################################################");
            Log.info("Verifying " + dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            workApi_response = apiService.searchForWorkByIDResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            workApi_response.compareWithDB();
        }
    }

    @Then("^the work details are retrieved by accountableProduct and compared$")
    public void compareWorkByAccountableProductWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            String accountableProduct_SegmentCode = apiProductsSearchSteps.getSegmentCode(dataQualityContext.workDataObjectsFromEPHGD.get(i).getF_accountable_product());
            Log.info("acountableProduct to be tested..." + accountableProduct_SegmentCode);
            returnedWorks = apiService.searchForWorksByaccountableProduct(accountableProduct_SegmentCode);
            Log.info("API total count matched..." + returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByAccountableProduct(accountableProduct_SegmentCode));
        }
    }

    @Then("^the work details are retrieved by workStatus and compared$")
    public void compareWorksByWorkStatusWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
            String workStatus = dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_STATUS();
            Log.info("searchKeyword and workStatus: " + searchKeyword + " and " + workStatus);

            returnedWorks = apiService.searchForWorksByWorkStatus(searchKeyword, workStatus);
            Log.info("API total count matched..." + returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByWorkStatus(searchKeyword, workStatus));
        }
    }

    @Then("^the work details are retrieved by workType and compared$")
    public void compareWorksByWorkTypeWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
            String workType = dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TYPE();
            Log.info("searchKeyword and workType: " + searchKeyword + " and " + workType);

            returnedWorks = apiService.searchForWorksByWorkType(searchKeyword, workType);
            Log.info("API total count matched..." + returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByWorkType(searchKeyword, workType));
        }
    }

    @Then("^the work details are retrieved by manifestationType and compared$")
    public void compareWorksByManifestationTypeWithDB() {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
            String ManifestationType = dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getF_TYPE();
            Log.info("searchKeyword and ManifestationType: " + searchKeyword + " and " + ManifestationType);
            try {
                returnedWorks = apiService.searchForWorksByManifestationType(searchKeyword, ManifestationType);
                Log.info("API total count matched..." + returnedWorks.getTotalMatchCount());
                returnedWorks.verifyWorksAreReturned();
                returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByManifestationType(searchKeyword, ManifestationType));
            } catch (Exception e) {
                System.out.println(e.getMessage());
                Assert.fail();

            }
        }
    }

    @Then("^the work details are retrieved by search with PMC code and compared$")
    public void compareWorkBySearchWithPMCCodeWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0];

        for (int i = 0; i < bound; i++) {
            Log.info("search keyword '" + searchKeyword + "' and pmcCode '" + dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC() + "'");
            returnedWorks = apiService.searchForWorksBySearchWithPMCCode(searchKeyword, dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
            Log.info("API total count matched..." + returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksBySearchWithPMCCode(searchKeyword, dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC()));

        }
    }

    @Then("^the work details are retrieved by search with PMG code and compared$")
    public void compareWorkBySearchWithPMGCodeWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
        for (int i = 0; i < bound; i++) {
            String PMGCode = getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
            Log.info("search keyword '" + searchKeyword + "' and pmgCode '" + PMGCode + "'");

            returnedWorks = apiService.searchForWorksBySearchWithPMGCode(searchKeyword, PMGCode);
            Log.info("API total matched count..." + returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksBySearchWithPMGCode(searchKeyword, PMGCode));
        }
    }

    @When("^the work details are retrieved by title (.*) and compared$")
    public void compareWorkSearchByTitleResultsWithDB(String titleType) throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            Log.info("###########-----getWorkByTitle - " + titleType);
            Log.info("#######################################################");
            Assert.assertTrue("Verify that the searched work exists and is accessible trough the API", apiService.checkWorkExists(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID()));
            switch (titleType) {
                case "WORK_TITLE":
                    returnedWorks = apiService.searchForWorkByTitleResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
                    break;
                //implemented by Nishant @ 20 Apr 2020
                case "WORK_MANIFESTATION_TITLE":
                    getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    returnedWorks = apiService.searchForWorkByTitleResult(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_KEY_TITLE());
                    break;
                //implemented by Nishant @ 22 Apr 2020
                case "WORK_PRODUCT_SUMMARY_NAME":
                    //not all works has products hence need separate query
                    getRandomWorkIdWithProducts();
                    getWorksDataFromEPHGD();
                    getProductDetailByWorkId(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    returnedWorks = apiService.searchForWorkByTitleResult(dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_NAME());
                    break;
                //implemented by Nishant @ 21 Apr 2020
                case "WORK_MANIFESTATION_PRODUCT_SUMMARY_NAME":
                    getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    getProductDetailByManifestationId(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
                    returnedWorks = apiService.searchForWorkByTitleResult(dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_NAME());
                    break;
            }
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
        }
    }

    @When("^the works search by identifier (.*) details are retrieved and compared$")
    public void compareWorkSearchByIdentifierResultsWithDB(String identifierType) throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {

            Log.info("###########-----getWorkByIdentifier - " + identifierType);
            Log.info("#######################################################");

            switch (identifierType) {
                case "WORK_IDENTIFIER":
                    getWorkIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    returnedWorks = apiService.searchForWorksByIdentifierResult(workIdentifiers.get(i).getIDENTIFIER());
                    break;
                case "WORK_MANIFESTATION_IDENTIFIER":
                    List<Map<String, Object>> manifIdentifiers = getEPHGDManifestationIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    returnedWorks = apiService.searchForWorksByIdentifierResult(manifIdentifiers.get(i).get("identifier").toString());
                    break;
                case "WORK_ID":
                    returnedWorks = apiService.searchForWorksByIdentifierResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    break;
                case "WORK_MANIFESTATION_ID":
                    getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    returnedWorks = apiService.searchForWorksByIdentifierResult(dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_ID());
                    break;
            }
            assert returnedWorks != null;
            Log.info(returnedWorks.toString());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());

        }
    }

    @When("^the work search by identifier (.*) and type details are retrieved and compared$")
    public void compareSearchByIdentifierAndTypeResultsWithDB(String identifierType) throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            Log.info("###########-----getWorkByIdentifierAndType - " + identifierType);
            Log.info("#######################################################\n");
            if (identifierType.equals("WORK_IDENTIFIER")) {
                getWorkIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                returnedWorks = apiService.searchForWorksByIdentifierAndTypeResult(workIdentifiers.get(i).getIDENTIFIER(), workIdentifiers.get(i).getF_TYPE());
            } else if (identifierType.equals("WORK_MANIFESTATION_IDENTIFIER")) {
                List<Map<String, Object>> manifIdentifiers = getEPHGDManifestationIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                returnedWorks = apiService.searchForWorksByIdentifierAndTypeResult(manifIdentifiers.get(i).get("identifier").toString(), manifIdentifiers.get(i).get("f_type").toString());
            }
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
        }
    }

    @When("^the works retrieved by search (.*) details are retrieved and compared$")
    public void compareWorksRetrievdBySearchOptionWithDB(String searchType) throws AzureOauthTokenFetchingException {
        //completed by Nishant @ 23-24 Apr 2020
        WorksMatchedApiObject returnedWorks = null;
        Log.info("searching by..." + searchType);
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            String workId = dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID();
            switch (searchType) {
                case "WORK_IDENTIFIER":
                    getWorkIdentifiersByWorkID(workId);
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(workIdentifiers.get(i).getIDENTIFIER());
                    break;

                case "JOURNAL_ACRONYM":
                    getWorkIdentifiersByWorkID(workId);
                    String jacronymValue = "";
                    for (WorkDataObject workIdentifier : workIdentifiers) {
                        if (workIdentifier.getF_TYPE().equalsIgnoreCase("JOURNAL ACRONYM"))
                            jacronymValue = workIdentifier.getIDENTIFIER();
                        break;
                    }
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(jacronymValue);
                    break;

                case "JOURNAL_NUMBER":
                    getWorkIdentifiersByWorkID(workId);
                    String jNumber = "";
                    for (WorkDataObject workIdentifier : workIdentifiers) {
                        if (workIdentifier.getF_TYPE().equalsIgnoreCase("ELSEVIER JOURNAL NUMBER"))
                            jNumber = workIdentifier.getIDENTIFIER();
                        break;
                    }
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(jNumber);
                    break;

                case "ISSN":
                    getWorkIdentifiersByWorkID(workId);
                    String issn = "";
                    for (WorkDataObject workIdentifier : workIdentifiers) {
                        if (workIdentifier.getF_TYPE().equalsIgnoreCase("ISSN-L"))
                            issn = workIdentifier.getIDENTIFIER();
                        break;
                    }
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(issn);
                    break;

                case "WORK_ID":
                case "EPR_ID":
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(workId);
                    break;

                case "WORK_MANIFESTATION_ID":
                    getManifestationsByWorkID(workId);
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
                    break;

                case "WORK_MANIFESTATION_IDENTIFIER":
                    List<Map<String, Object>> manifIdentifiers = getEPHGDManifestationIdentifiersByWorkID(workId);
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(manifIdentifiers.get(0).get("identifier").toString());
                    break;

                case "WORK_PRODUCT_ID":
                    getRandomWorkIdWithProducts();
                    getWorksDataFromEPHGD();
                    getProductDetailByWorkId(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID());
                    break;

                case "WORK_MANIFESTATION_PRODUCT_ID":
                    getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    getProductDetailByManifestationId(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID());
                    break;

                case "WORK_TITLE":
                case "TITLE":
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
                    break;

                case "WORK_MANIFESTATION_TITLE":
                    getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE());
                    break;

                //implemented by Nishant @ 24 Apr 2020
                case "WORK_PERSONS_FULLNAME":
                        /*
                        created by Nishant @24 Apr 2020
                        getWorkByPerson returns sometimes 70000+ records and most probable intended work id does not appear in first 20 records
                        hence we need to send API request with size 5000 and check if intended workID is returned
                        if not, send request again for next 5000 records
                        */
                    boolean found = false;
                    int from = 0;
                    int size = 5000;
                    getWorkPersonRoleByWorkId(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                    getPersonDataByPersonId(dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON());
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " +
                            dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);

                    Log.info("Total work found... - " + returnedWorks.getTotalMatchCount());
                    while (!returnedWorks.verifyWorkWithIdIsReturnedOnly(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())
                            && from + size < returnedWorks.getTotalMatchCount()) {
                        from += size;
                        Log.info("scanned workID from " + (from - size) + " to " + from + " records...");
                        returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " +
                                dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);
                    }
                    break;
                case "WORK_PRODUCT_SUMMARY_NAME":
                    getRandomWorkIdWithProducts();
                    getWorksDataFromEPHGD();
                    getProductDetailByWorkId(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
                    break;
                case "WORK_MANIFESTATION_PRODUCT_SUMMARY_NAME":
                    getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    getProductDetailByManifestationId(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
                    break;
            }
            assert returnedWorks != null;
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
        }
    }

    @When("^the journal by search (.*) details are retrieved and compared$")
    public void compareWorksRetrievdByJournalSearchOptionWithDB(String searchType) throws AzureOauthTokenFetchingException {
        //created by Nishant @ 24-25 Apr 2020
        WorksMatchedApiObject returnedWorks = null;
        Log.info("searching by..." + searchType);
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            String workId = dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID();
            switch (searchType) {
                case "EPR_ID":
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(workId);
                    break;

                case "TITLE":
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
                    break;

                case "JOURNAL_ACRONYM":
                    getWorkIdentifiersByWorkID(workId);
                    String jacronymValue = "";
                    for (WorkDataObject workIdentifier : workIdentifiers) {
                        if (workIdentifier.getF_TYPE().equalsIgnoreCase("JOURNAL ACRONYM")) {
                            jacronymValue = workIdentifier.getIDENTIFIER();
                            break;
                        }
                    }
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(jacronymValue);
                    break;

                case "JOURNAL_NUMBER":
                    getWorkIdentifiersByWorkID(workId);
                    String jNumber = "";
                    for (WorkDataObject workIdentifier : workIdentifiers) {
                        if (workIdentifier.getF_TYPE().equalsIgnoreCase("ELSEVIER JOURNAL NUMBER")) {
                            jNumber = workIdentifier.getIDENTIFIER();
                            break;
                        }
                    }
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(jNumber);
                    break;

                case "ISSN":
                    getWorkIdentifiersByWorkID(workId);
                    String issn = "";
                    for (WorkDataObject workIdentifier : workIdentifiers) {
                        if (workIdentifier.getF_TYPE().equalsIgnoreCase("ISSN-L")) {
                            issn = workIdentifier.getIDENTIFIER();
                            break;
                        }
                    }
                    returnedWorks = apiService.searchForWorksBySearchOptionResult(issn);
                    break;
            }
            assert returnedWorks != null;
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
        }
    }

    @When("^the work details are retrieved by PMC Code and compared$")
    public void compareWorkSearchByPMCResultsWithDB() throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            Log.info("pmc to be tested..." + dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
            returnedWorks = apiService.searchForWorkByPMCResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
            returnedWorks.verifyWorksAreReturned();
            Log.info("API total count matched..." + returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC()));
        }
    }

    @When("^the work details are retrieved by PMG Code and compared$")
    public void compareWorkSearchByPMGResultsWithDB() throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            String pmgCode = getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
            returnedWorks = apiService.searchForWorkByPMGResult(pmgCode);
            returnedWorks.verifyWorksAreReturned();
            Log.info("API total count matched..." + returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByPMG(pmgCode));
        }
    }

    @When("^the work response count is compared with the count in the DB for Person Id$")
    public void compareSearchResultCountForPersonIdresponse() throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks;
        int bound = ids.size();
        for (String id : ids) {
            Log.info("personId to be tested..." + id);
            returnedWorks = apiService.searchForWorksByPersonIDResult(id);
            returnedWorks.verifyWorksAreReturned();
            Log.info("Total API count matched..." + returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByPersonIDs(id));
        }
    }

    @When("^work response is compared with the DB for (.*)$")
    public void compareSearchResultCountForPersonOptions(String personSearchOption) throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = ids.size();
        for (int i = 0; i < bound; i++) {
            getWorkPersonRoleByWorkId(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            getPersonDataByPersonId(dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON());
            Log.info("personId to be tested..." + personDataObjectsFromEPHGD.get(0).getPERSON_ID());

            int from = 0;
            int size = 5000;
            switch (personSearchOption) {
                case "PERSON_NAME":
                        /*created by Nishant @24 Apr 2020
                        getWorkByPerson returns sometimes 70000+ records and most probable intended work id does not appear in first 20 records
                        hence we need to send API request with size 5000 and check if intended workID is returned
                        if not, send request again for next 5000 records*/

                    returnedWorks = apiService.searchForWorksByQueryTypeResult("personName", dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " +
                            dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);

                    while (!returnedWorks.verifyWorkWithIdIsReturnedOnly(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())
                            && from + size < returnedWorks.getTotalMatchCount()) {
                        from += size;
                        Log.info("scanned workID from " + (from - size) + " to " + from + " records...");
                        returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " +
                                dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);
                    }
                    break;

                case "personFullNameCurrent":
                    returnedWorks = apiService.searchForWorksByQueryTypeResult("personFullNameCurrent", dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " +
                            dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);

                    while (!returnedWorks.verifyWorkWithIdIsReturnedOnly(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())
                            && from + size < returnedWorks.getTotalMatchCount()) {
                        from += size;
                        Log.info("scanned workID from " + (from - size) + " to " + from + " records...");
                        returnedWorks = apiService.searchForWorksBySearchOptionResult(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " +
                                dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);
                    }
                    break;


                case "PEOPLE_HUB_ID":
                    returnedWorks = apiService.searchForWorksByPersonIDResult(personDataObjectsFromEPHGD.get(0).getPEOPLEHUB_ID() + "&from=" + from + "&size=" + size);
                    while (!returnedWorks.verifyWorkWithIdIsReturnedOnly(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())
                            && from + size < returnedWorks.getTotalMatchCount()) {
                        from += size;
                        Log.info("scanned workID from " + (from - size) + " to " + from + " records...");
                        returnedWorks = apiService.searchForWorksByPersonIDResult(personDataObjectsFromEPHGD.get(0).getPEOPLEHUB_ID() + "&from=" + from + "&size=" + size);
                    }
                    break;

                case "PERSON_ID":
                    returnedWorks = apiService.searchForWorksByPersonIDResult(personDataObjectsFromEPHGD.get(0).getPERSON_ID() + "&from=" + from + "&size=" + size);
                    while (!returnedWorks.verifyWorkWithIdIsReturnedOnly(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())
                            && from + size < returnedWorks.getTotalMatchCount()) {
                        from += size;
                        Log.info("scanned workID from " + (from - size) + " to " + from + " records...");
                        returnedWorks = apiService.searchForWorksByPersonIDResult(personDataObjectsFromEPHGD.get(0).getPERSON_ID() + "&from=" + from + "&size=" + size);
                    }
                    break;

                case "personIdCurrent":
                    returnedWorks = apiService.searchForWorksByQueryTypeResult("personIdCurrent", personDataObjectsFromEPHGD.get(0).getPERSON_ID() + "&from=" + from + "&size=" + size);
                    while (!returnedWorks.verifyWorkWithIdIsReturnedOnly(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())
                            && from + size < returnedWorks.getTotalMatchCount()) {
                        from += size;
                        Log.info("scanned workID from " + (from - size) + " to " + from + " records...");
                        returnedWorks = apiService.searchForWorksByPersonIDResult(personDataObjectsFromEPHGD.get(0).getPERSON_ID() + "&from=" + from + "&size=" + size);
                    }
                    break;
            }

            assert returnedWorks != null;
            returnedWorks.verifyWorksAreReturned();

            Log.info("Total API count matched..." + returnedWorks.getTotalMatchCount());
            int dbCount = getNumberOfWorksByPersonIDs(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_ID());
            //  returnedWorks.verifyWorksReturnedCount(dbCount);

            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            Log.info("verified work present in search result...");
        }
    }


    WorksMatchedApiObject callAPI_workByOption(String personSearchOption, String queryValue) throws AzureOauthTokenFetchingException {
        //created by Nishant @ 10 Jul 2020
        //verify Journal Finder search result with API
       Log.info("calling workAPI for... "+personSearchOption);
        WorksMatchedApiObject returnedWorks;
        returnedWorks = apiService.searchForWorksByQueryTypeResult(personSearchOption, queryValue);
        return returnedWorks;
    }

    @And("^get work by manifestation$")
    public void getWorkByManifestation() {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH_BY_MANIFESTATIONID,
                Joiner.on("','").join(manifestaionids));
        List<Map<?, ?>> workIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = workIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
    }

    private int getNumberOfWorksByAccountableProduct(String accountableProduct) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_ACCOUNTABLE_PRODUCT, accountableProduct);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH work count..." + count);
        return count;
    }

    private int getNumberOfWorksByWorkStatus(String searchKeyword, String workStatus) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_WORKSTATUS, searchKeyword, workStatus);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH work count..." + count);
        return count;
    }

    private int getNumberOfWorksByWorkType(String searchKeyword, String workType) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_WORKTYPE, searchKeyword, workType);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH work count..." + count);
        return count;
    }

    private int getNumberOfWorksByManifestationType(String searchKeyword, String manifestationType) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_MANIFESTATIONTYPE, searchKeyword, manifestationType);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH count..." + count);
        return count;
    }

    private int getNumberOfWorksBySearchWithPMCCode(String searchKeyword, String PMCCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_PMCCODE, searchKeyword, PMCCode);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH count..." + count);
        return count;
    }

    private int getNumberOfWorksBySearchWithPMGCode(String searchKeyword, String PMCCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_PMGCODE, searchKeyword, PMCCode);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH count..." + count);
        return count;
    }

    private int getNumberOfWorksByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_AMOUNT_BYPMC, pmcCode);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH work count... " + count);
        return count;
    }

    private int getNumberOfWorksByPersonIDs(String personID) {
        sql = String.format(APIDataSQL.SELECT_COUNT_PERSONID_FOR_WORKS, personID);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH work count..." + count);
        return count;
    }

    private int getNumberOfWorksByPMG(String pmgCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_AMOUNT_BYPMG, pmgCode);
        List<Map<String, Object>> subjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) subjectAreaNumber.get(0).get("count")).intValue();
        Log.info("EPH work count..." + count);
        return count;
    }

    String getPMGcodeByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.EPH_GD_PMG_CODE_EXTRACT_BYPMC, pmcCode);
         Log.info(sql);
        List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String pmgCode = ((String) getPMG.get(0).get("f_pmg"));
        Log.info("pmg code..." + pmgCode);
        return pmgCode;
    }

    List<String> getManifestationIdsForWorkID(String workID) {
        //  Log.info("Get manifestationApiObject ids ..");

        sql = String.format(APIDataSQL.SELECT_MANIFESTATION_IDS_BY_WORKID, workID);
        //   Log.info(sql);

        List<Map<?, ?>> manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        List<String> ids = manifestationIds.stream().map(m -> (String) m.get("manifestation_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Manifestation ids for the work: " + ids);
        Assert.assertFalse("Verify that manifestation ids can be successfully extracted from db by work ids", ids.isEmpty());
        return ids;
    }

    //created by Nishant @ 22 Apr 2020
    void getProductDetailByManifestationId(String manifestationId) {
        Log.info("get product by manifestation id of the work...");
        sql = String.format(APIDataSQL.SelectProductByManifestationId, manifestationId);
        productDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
        Assert.assertFalse("Verify that product by manifestation id is extracted successfully from the DB", productDataObjectsFromEPHGD.isEmpty());
    }

    void getProductDetailByWorkId(String workId) {
        Log.info("get product by work id...");
        sql = String.format(APIDataSQL.SelectProductByWorkId, workId);
        productDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
        Assert.assertFalse("Verify that product by work id is extracted successfully from the DB", productDataObjectsFromEPHGD.isEmpty());
    }

    //created by Nishant @ 24 Apr 2020
    private void getWorkPersonRoleByWorkId(String workId) {
        sql = String.format(APIDataSQL.selectWorkPersonByworkId, workId);
        personWorkRoleDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);
        Assert.assertFalse("Verify person role by work id successfully extracted from EPH DB", personWorkRoleDataObjectsFromEPHGD.isEmpty());
    }

    private List getEPHGDManifestationIdentifiersByWorkID(String workID) {
        sql = String.format(APIDataSQL.SELECT_GD_MANIFESTATION_IDENTIFIER_BY_MANIFESTATION_ID,
                Joiner.on("','").join(getManifestationIdsForWorkID(workID)));
        //   Log.info(sql);
        return DBManager.getDBResultMap(sql, Constants.EPH_URL);
    }

    private void getWorkIdentifiersByWorkID(String workID) {
        sql = APIDataSQL.getWorkIdentifiersDataFromGD.replace("PARAM1", workID);
        workIdentifiers = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        //added by Nishant @ 28 Dec 2019 to handle works without identifiers
        if (workIdentifiers.size() == 0) {
            Log.info("Error - random work do not have any identifier hence scenario failure");
        }

    }

    private void getManifestationsByWorkID(String workID) {
        Log.info("get manifestations for the work in EPH GD ..");
        sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_WORKID, workID);
        manifestationDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
        Assert.assertFalse("Verify that manifestaion by work id is extracted successfully from the DB", manifestationDataObjectsFromEPHGD.isEmpty());
    }

    private void getPersonDataByPersonId(String personId) {
        sql = String.format(APIDataSQL.SelectPersonDataByPersonId, personId);
        personDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
        Assert.assertFalse("verify person Data by person id extracted from EPH DB", personDataObjectsFromEPHGD.isEmpty());
    }

    public void getJsonToObject(String workId) {
        //created by Nishant @ 19 Jun 2020 to verify extended json value with APIv3
        sql = "SELECT \"json\" FROM ephsit_extended_data_stitch.stch_work_ext_json where epr_id='" + workId + "'";
        List<Map<String, String>> jsonValue = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        DataQualityContext.workExtendedTestClass = new Gson().fromJson(jsonValue.get(0).get("json"), WorkExtendedTestClass.class);
    }


    @And("^get person data from EPH DB$")
    public void getPersonDataByPersonId() {
        Log.info("We get the work data from EPH GD ...");
        sql = String.format(APIDataSQL.SelectPersonDataByPersonId, Joiner.on("','").join(ids));
        dataQualityContext.personDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
        dataQualityContext.personDataObjectsFromEPHGD.sort(Comparator.comparing(PersonDataObject::getPERSON_ID));
        Assert.assertFalse("Verify that list with person objects from DB is not empty", dataQualityContext.personDataObjectsFromEPHGD.isEmpty());
    }
}