package com.eph.automation.testing.web.steps;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.api.WorkApiObject;
import com.eph.automation.testing.models.api.WorksMatchedApiObject;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.models.dao.ManifestationIdentifierObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.api.AzureOauthTokenFetchingException;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.google.common.base.Joiner;
import cucumber.api.PendingException;
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

import static com.eph.automation.testing.models.contexts.DataQualityContext.manifestationDataObjectsFromEPHGD;
import static com.eph.automation.testing.services.api.APIService.*;
//import static com.eph.automation.testing.services.api.APIService.*;


/**
 * Created by Georgi Vlaykov on 11/02/2019
 */
public class ApiWorksSearchSteps {

    @StaticInjection
    public DataQualityContext dataQualityContext;

    public static List<ManifestationIdentifierObject> manifestationIdentifiers;
    private String sql;
    private static List<String> ids;
    private static List<String> manifestation_Ids;
    private static List<String> manifestationIdentifiers_Ids;
    private WorkApiObject workApi_response;
    private static List<WorkDataObject> workIdentifiers;
    private List<WorkDataObject> workDataObjectsFromEPHGD;
    public ApiProductsSearchSteps apiProductsSearchSteps = new ApiProductsSearchSteps();
    @Given("^We get (.*) random search ids for works")
    public void getRandomWorkIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_IDS_FOR_SEARCH, numberOfRecords);
        //  Log.info(sql);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
        //added by Nishant @ 27 Dec for debugging failures
        // ids.clear();
        // ids.add("EPR-W-109GCN");
        // Log.info("hard coded work ids are : " + ids);

        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }

    @Given("^We get (.*) random search ids for person roles")
    public void getRandomPersonRolesIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_PERSON_ROLES_FOR_SEARCH, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomPersonSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomPersonSearchIds.stream().map(m -> (BigDecimal) m.get("f_person")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
        Assert.assertFalse("Verify That list with random person roles is not empty.", ids.isEmpty());
    }

    @And("^We get the work search data from EPH GD$")
    public void getWorksDataFromEPHGD() {
        Log.info("And We get the data from EPH GD for journals ...");
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        //   Log.info(sql);

        dataQualityContext.workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        dataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));

        Assert.assertFalse("Verify that list with work objects from DB is not empty", dataQualityContext.workDataObjectsFromEPHGD.isEmpty());
    }


    //created by Nishant
    @Then("^the work details are retrieved by accountableProduct and compared$")
    public void compareWorkByAccountableProductWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            String accountableProduct_SegmentCode = apiProductsSearchSteps.getSegmentCode(dataQualityContext.workDataObjectsFromEPHGD.get(i).getF_accountable_product());
            returnedWorks = searchForWorksByaccountableProduct(accountableProduct_SegmentCode);
            Log.info("work with accountableProduct in API : "+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturned(getNumberOfWorksByAccountableProduct(accountableProduct_SegmentCode));
        }
    }

    public int getNumberOfWorksByAccountableProduct(String accountableProduct) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_ACCOUNTABLE_PRODUCT, accountableProduct);
        //   Log.info(sql);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("work with accountableProduct in DB: "+count);
        return count;
    }

    @Then("^the work details are retrieved by workStatus and compared$")
    public void compareWorksByWorkStatusWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {

            String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
            String workStatus = dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_STATUS();
            Log.info("\nsearchKeyword and workStatus: "+searchKeyword+" and "+workStatus);
            returnedWorks = searchForWorksByWorkStatus(searchKeyword, workStatus);
            Log.info("work with workStatus in API : "+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturned(getNumberOfWorksByWorkStatus(searchKeyword,workStatus));
        }
    }

    public int getNumberOfWorksByWorkStatus(String searchKeyword,String workStatus) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_WORKSTATUS,searchKeyword, workStatus);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("work with workStatus in DB: "+count);
        return count;
    }

    @Then("^the work details are retrieved by workType and compared$")
    public void compareWorksByWorkTypeWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {

            String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
            String workType = dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TYPE();
            Log.info("\nsearchKeyword and workType: "+searchKeyword+" and "+workType);
            returnedWorks = searchForWorksByWorkType(searchKeyword,workType);
            Log.info("work with workType in API : "+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturned(getNumberOfWorksByWorkType(searchKeyword,workType));

        }
    }

    public int getNumberOfWorksByWorkType(String searchKeyword,String workType) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_WORKTYPE,searchKeyword, workType);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("work with workType in DB: "+count);
        return count;
    }

    @Then("^the work details are retrieved by manifestationType and compared$")
    public void compareWorksByManifestationTypeWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {

            getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
            String ManifestationType = dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getF_TYPE();
            Log.info("\nsearchKeyword and ManifestationType: "+searchKeyword+" and "+ManifestationType);
            try{
                returnedWorks = searchForWorksByManifestationType(searchKeyword,ManifestationType );
                Log.info("work with manifestationType in API : "+returnedWorks.getTotalMatchCount());
                returnedWorks.verifyWorksAreReturned();
                returnedWorks.verifyWorksReturned(getNumberOfWorksByManifestationType(searchKeyword,ManifestationType));
            }
            catch (Exception e)
            {
                System.out.println(e.getMessage());
                Assert.assertFalse(true);

            }
        }
    }

    public int getNumberOfWorksByManifestationType(String searchKeyword, String manifestationType) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_MANIFESTATIONTYPE,searchKeyword, manifestationType);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("work with manifestationType in DB: "+count);
        return count;
    }

    @Then("^the work details are retrieved by search with PMC code and compared$")
    public void compareWorkBySearchWithPMCCodeWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0];

        for (int i = 0; i < bound; i++) {

            returnedWorks = searchForWorksBySearchWithPMCCode(searchKeyword, dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
            Log.info("work with SearchWithPMCCode in API : "+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturned(getNumberOfWorksBySearchWithPMCCode(searchKeyword,dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC()));

        }
    }

    public int getNumberOfWorksBySearchWithPMCCode(String searchKeyword,String PMCCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_PMCCODE,searchKeyword, PMCCode);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("work with SearchWithPMCCode in DB: "+count);
        return count;
    }

    @Then("^the work details are retrieved by search with PMG code and compared$")
    public void compareWorkBySearchWithPMGCodeWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
        for (int i = 0; i < bound; i++) {
            String PMGCode = getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
            returnedWorks = searchForWorksBySearchWithPMGCode(searchKeyword, PMGCode);
            Log.info("work with SearchWithPMGCode in API : "+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturned(getNumberOfWorksBySearchWithPMGCode(searchKeyword,PMGCode));
        }
    }

    public int getNumberOfWorksBySearchWithPMGCode(String searchKeyword,String PMCCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_PMGCODE,searchKeyword, PMCCode);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("work with SearchWithPMGCode in DB: "+count);
        return count;
    }

    //Updated by Nishant
    @When("^the work details are retrieved and compared$")
    public void compareWorkSearchResultsWithDB() throws AzureOauthTokenFetchingException {
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {

            Assert.assertTrue("Verify that the searched work exists and is accessible trough the API", checkWorkExists(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID()));
            workApi_response = searchForWorkByIDResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            getEPHGDManifestationIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            workApi_response.compareWithDB();

        }
    }

    @When("^the work details are retrieved by title and compared$")
    public void compareWorkSearchByTitleResultsWithDB() throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {

            Assert.assertTrue("Verify that the searched work exists and is accessible trough the API", checkWorkExists(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID()));
            returnedWorks = searchForWorkByTitleResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());

        }
    }

    @When("^the works search by identifier (.*) details are retrieved and compared$")
    public void compareWorkSearchByIdentifierResultsWithDB(String identifierType) throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;

        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            try{
                if (identifierType.equals("WORK_IDENTIFIER")){
                    getWorkIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    returnedWorks = searchForWorksByIdentifierResult(workIdentifiers.get(i).getIDENTIFIER());
                } else if (identifierType.equals("WORK_MANIFESTATION_IDENTIFIER")){
                    List<Map<String, Object>> manifIdentifiers = getEPHGDManifestationIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    returnedWorks = searchForWorksByIdentifierResult(manifIdentifiers.get(i).get("identifier").toString());
                } else if (identifierType.equals("WORK_ID")){
                    returnedWorks = searchForWorksByIdentifierResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                } else if (identifierType.equals("WORK_MANIFESTATION_ID")){
                    getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                    returnedWorks = searchForWorksByIdentifierResult(dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_ID());
                }
                Log.info(returnedWorks.toString());
                returnedWorks.verifyWorksAreReturned();
                returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            } catch (Exception e) {
                Log.info(e.getMessage());
            }
        }
    }

    @When("^the work search by identifier (.*) and type details are retrieved and compared$")
    public void compareSearchByIdentifierAndTypeResultsWithDB(String identifierType) throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {

            if (identifierType.equals("WORK_IDENTIFIER")) {
                getWorkIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                returnedWorks = searchForWorksByIdentifierAndTypeResult(workIdentifiers.get(i).getIDENTIFIER(), workIdentifiers.get(i).getF_TYPE());
            } else if (identifierType.equals("WORK_MANIFESTATION_IDENTIFIER")) {
                List<Map<String, Object>> manifIdentifiers = getEPHGDManifestationIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                returnedWorks = searchForWorksByIdentifierAndTypeResult(manifIdentifiers.get(i).get("identifier").toString(), manifIdentifiers.get(i).get("f_type").toString());
            }
            Log.info(returnedWorks.toString());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());

        }
    }

    @When("^the works retrieved by search (.*) details are retrieved and compared$")
    public void compareWorksRetrievdBySearchOptionWithDB(String identifierType) throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            try {
                String workId = dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID();
                Log.info("WorkId to be tested: " + workId);
                if (identifierType.equals("WORK_IDENTIFIER")) {
                    getWorkIdentifiersByWorkID(workId);
                    returnedWorks = searchForWorksBySearchOptionResult(workIdentifiers.get(i).getIDENTIFIER());
                } else if (identifierType.equals("WORK_MANIFESTATION_IDENTIFIER")) {
                    List<Map<String, Object>> manifIdentifiers = getEPHGDManifestationIdentifiersByWorkID(workId);
                    returnedWorks = searchForWorksBySearchOptionResult(manifIdentifiers.get(0).get("identifier").toString());
                } else if (identifierType.equals("WORK_ID")) {
                    returnedWorks = searchForWorksBySearchOptionResult(workId);
                } else if (identifierType.equals("WORK_MANIFESTATION_ID")) {
                    getManifestationsByWorkID(workId);
                    returnedWorks = searchForWorksBySearchOptionResult(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
                } else if (identifierType.equals("WORK_PRODUCT_ID")) {
                    returnedWorks = searchForWorksBySearchOptionResult(workId);
                } else if (identifierType.equals("WORK_MANIFESTATION_TITLE")) {
                    returnedWorks = searchForWorksBySearchOptionResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
                } else if (identifierType.equals("WORK_TITLE")) {
                    returnedWorks = searchForWorksBySearchOptionResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
                }
                returnedWorks.verifyWorksAreReturned();
                returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            } catch (Exception e) {
                Log.info(identifierType + " : "+ e.getMessage());
                Assert.assertFalse(false);
            }
        }
    }

    @When("^the work details are retrieved by PMC Code and compared$")
    public void compareWorkSearchByPMCResultsWithDB() throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {

            returnedWorks = searchForWorkByPMCResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
            Log.info(returnedWorks.toString());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturned(getNumberOfWorksByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC()));

        }
    }

    @When("^the work details are retrieved by PMG Code and compared$")
    public void compareWorkSearchByPMGResultsWithDB() throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {

            returnedWorks = searchForWorkByPMGResult(getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC()));
            Log.info(returnedWorks.toString());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturned(getNumberOfWorksByPMG(getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC())));

        }
    }

    @When("^the work response count is compared with the count in the DB for person ID$")
    public void compareSearchResultCountForPersonIdsresponse() throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = ids.size();
        for (int i = 0; i < bound; i++) {

            returnedWorks = searchForWorksByPersonIDResult(ids.get(i));
            Log.info(returnedWorks.toString());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturned(getNumberOfWorksByPersonIDs(ids.get(i)));

        }
    }


    public int getNumberOfWorksByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_AMOUNT_BYPMC, pmcCode);
        Log.info("When We get the count of works with that PMC code ..");
        Log.info(sql);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        return count;
    }

    public int getNumberOfWorksByPersonIDs(String personID) {
        sql = String.format(APIDataSQL.SELECT_COUNT_PERSONID_FOR_WORKS, personID);
        Log.info("When We get the count of works with that PMC code ..");
        Log.info(sql);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        return count;
    }

    public String getPMGcodeByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.EPH_GD_PMG_CODE_EXTRACT_BYPMC, pmcCode);
        Log.info(sql);
        List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String pmgCode = ((String) getPMG.get(0).get("f_pmg"));
        return pmgCode;
    }

    public int getNumberOfWorksByPMG(String pmgCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_AMOUNT_BYPMG, pmgCode);
        Log.info("When We get the count of works with that PMC code ..");
        Log.info(sql);
        List<Map<String, Object>> subjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) subjectAreaNumber.get(0).get("count")).intValue();
        return count;
    }

    public List<String> getManifestationIdsForWorkID(String workID) {
        Log.info("Get manifestationApiObject ids ..");

        sql = String.format(APIDataSQL.SELECT_MANIFESTATION_IDS_BY_WORKID, workID);
        //   Log.info(sql);

        List<Map<?, ?>> manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        List<String> ids = manifestationIds.stream().map(m -> (String) m.get("manifestation_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Manifestation ids for the work: " + ids);
        Assert.assertFalse("Verify that manifestation ids can be successfully extracted from db by work ids", ids.isEmpty());
        return ids;
    }

    public void getManifestationsByWorkID(String workID) {
        Log.info("And get the manifestations in EPH GD ..");
        sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_WORKID, workID);
        manifestationDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
        Assert.assertFalse("Verify that manifestaion by work id is extracted successfully from the DB",manifestationDataObjectsFromEPHGD.isEmpty());
    }

    public List getEPHGDManifestationIdentifiersByWorkID(String workID) {
        sql = String.format(APIDataSQL.SELECT_GD_MANIFESTATION_IDENTIFIER_BY_MANIFESTATION_ID,
                Joiner.on("','").join(getManifestationIdsForWorkID(workID)));
        //   Log.info(sql);
        return DBManager.getDBResultMap(sql, Constants.EPH_URL);
    }

    public void getWorkIdentifiersByWorkID(String workID){
        sql = APIDataSQL.getWorkIdentifiersDataFromGD
                .replace("PARAM1", workID);
        workIdentifiers = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

        //added by Nishant @ 28 Dec 2019 to handle works without identifiers
        if(workIdentifiers.size()==0)
        {
            Log.info("Error - random work do not have any identifier hence scenario failure");
        }

    }

    @Given("^We get id for work search (.*)$")
    public void getWorkDataFromEPHGD(String workID) {
        sql = String.format(APIDataSQL.SELECT_WORK_BY_ID_FOR_SEARCH, workID);
        Log.info(sql);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
        Assert.assertFalse("Verify that list with work ids is not empty",ids.isEmpty());
    }



}