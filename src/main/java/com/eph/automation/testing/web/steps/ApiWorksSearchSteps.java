package com.eph.automation.testing.web.steps;
/*
 * Created by GVLAYKOV
 * updated by Nishant @ 20 April 2020 for data model changes
 */
import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.api.WorkApiObject;
import com.eph.automation.testing.models.api.WorksMatchedApiObject;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.*;
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

import static com.eph.automation.testing.models.contexts.DataQualityContext.*;
import static com.eph.automation.testing.services.api.APIService.*;


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
    private ApiProductsSearchSteps apiProductsSearchSteps = new ApiProductsSearchSteps();
    @Given("^We get (.*) random search ids for works")
    public void getRandomWorkIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_IDS_FOR_SEARCH, numberOfRecords);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
        //added by Nishant @ 27 Dec for debugging failures
         ids.clear(); ids.add("EPR-W-1135VY");  Log.info("hard coded work ids are : " + ids);
        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }

    private void getRandomWorkIdWithProducts(){
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_IDS_WITH_PRODUCT);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids with product is : " + ids);
   Assert.assertFalse("verify list with random id is not empty",ids.isEmpty());
    }

    @Given("^We get (.*) random search ids for person roles")
    public void getRandomPersonRolesIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_PERSON_ROLES_FOR_SEARCH, numberOfRecords);
        List<Map<?, ?>> randomPersonSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomPersonSearchIds.stream().map(m -> (BigDecimal) m.get("f_person")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random person ids  : " + ids);
        Assert.assertFalse("Verify That list with random person roles is not empty.", ids.isEmpty());
    }

    @And("^We get the work search data from EPH GD$")
    public void getWorksDataFromEPHGD() {
        Log.info("And We get the data from EPH GD for journals ...");
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        dataQualityContext.workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        dataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));
        Assert.assertFalse("Verify that list with work objects from DB is not empty", dataQualityContext.workDataObjectsFromEPHGD.isEmpty());
    }

    //created by Nishant
    @Then("^the work details are retrieved by accountableProduct and compared$")
    public void compareWorkByAccountableProductWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            String accountableProduct_SegmentCode = apiProductsSearchSteps.getSegmentCode(dataQualityContext.workDataObjectsFromEPHGD.get(i).getF_accountable_product());
            Log.info("acountableProduct to be tested..."+accountableProduct_SegmentCode);
            returnedWorks = searchForWorksByaccountableProduct(accountableProduct_SegmentCode);
            Log.info("API total count matched..."+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByAccountableProduct(accountableProduct_SegmentCode));
        }
    }

    private int getNumberOfWorksByAccountableProduct(String accountableProduct) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_ACCOUNTABLE_PRODUCT, accountableProduct);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH work count..."+count);
        return count;
    }

    @Then("^the work details are retrieved by workStatus and compared$")
    public void compareWorksByWorkStatusWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
            String workStatus = dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_STATUS();
            Log.info("searchKeyword and workStatus: "+searchKeyword+" and "+workStatus);

            returnedWorks = searchForWorksByWorkStatus(searchKeyword, workStatus);
            Log.info("API total count matched..."+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByWorkStatus(searchKeyword,workStatus));
        }
    }

    private int getNumberOfWorksByWorkStatus(String searchKeyword,String workStatus) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_WORKSTATUS,searchKeyword, workStatus);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH work count..."+count);
        return count;
    }

    @Then("^the work details are retrieved by workType and compared$")
    public void compareWorksByWorkTypeWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
            String workType = dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TYPE();
            Log.info("searchKeyword and workType: "+searchKeyword+" and "+workType);

            returnedWorks = searchForWorksByWorkType(searchKeyword,workType);
            Log.info("API total count matched..."+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByWorkType(searchKeyword,workType));
        }
    }

    private int getNumberOfWorksByWorkType(String searchKeyword,String workType) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_WORKTYPE,searchKeyword, workType);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH work count..."+count);
        return count;
    }

    @Then("^the work details are retrieved by manifestationType and compared$")
    public void compareWorksByManifestationTypeWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
            String ManifestationType = dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getF_TYPE();
            Log.info("searchKeyword and ManifestationType: "+searchKeyword+" and "+ManifestationType);
            try{
                returnedWorks = searchForWorksByManifestationType(searchKeyword,ManifestationType );
                Log.info("API total count matched..."+returnedWorks.getTotalMatchCount());
                returnedWorks.verifyWorksAreReturned();
                returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByManifestationType(searchKeyword,ManifestationType));
            }
            catch (Exception e)
            {
                System.out.println(e.getMessage());
                Assert.assertFalse(true);

            }
        }
    }

    private int getNumberOfWorksByManifestationType(String searchKeyword, String manifestationType) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_MANIFESTATIONTYPE,searchKeyword, manifestationType);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH count..."+count);
        return count;
    }

    @Then("^the work details are retrieved by search with PMC code and compared$")
    public void compareWorkBySearchWithPMCCodeWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0];

        for (int i = 0; i < bound; i++) {
            Log.info("search keyword '"+searchKeyword+"' and pmcCode '"+dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC()+"'");
            returnedWorks = searchForWorksBySearchWithPMCCode(searchKeyword, dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
            Log.info("API total count matched..."+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksBySearchWithPMCCode(searchKeyword,dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC()));

        }
    }

    private int getNumberOfWorksBySearchWithPMCCode(String searchKeyword,String PMCCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_PMCCODE,searchKeyword, PMCCode);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH count..."+count);
        return count;
    }

    @Then("^the work details are retrieved by search with PMG code and compared$")
    public void compareWorkBySearchWithPMGCodeWithDB() throws Throwable {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        String searchKeyword = dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().split(" ")[0].toUpperCase();
        for (int i = 0; i < bound; i++) {
            String PMGCode = getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
            Log.info("search keyword '"+searchKeyword+"' and pmgCode '"+PMGCode+"'");

            returnedWorks = searchForWorksBySearchWithPMGCode(searchKeyword, PMGCode);
            Log.info("API total matched count..."+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksBySearchWithPMGCode(searchKeyword,PMGCode));
        }
    }

    private int getNumberOfWorksBySearchWithPMGCode(String searchKeyword,String PMCCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_PMGCODE,searchKeyword, PMCCode);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH count..."+count);
        return count;
    }

    //Updated by Nishant for data model changes in Apr 2020
    @When("^the work details are retrieved and compared$")
    public void compareWorkSearchResultsWithDB() throws AzureOauthTokenFetchingException {
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            workApi_response = searchForWorkByIDResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            workApi_response.compareWithDB();
        }
    }

    @When("^the work details are retrieved by title (.*) and compared$")
    public void compareWorkSearchByTitleResultsWithDB(String titleType) throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            Log.info("###########-----getWorkByTitle - "+titleType);
            Log.info("#######################################################");
            Assert.assertTrue("Verify that the searched work exists and is accessible trough the API", checkWorkExists(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID()));
           switch(titleType) {
               case "WORK_TITLE":
                   returnedWorks = searchForWorkByTitleResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
                   break;
                   //implemented by Nishant @ 20 Apr 2020
               case "WORK_MANIFESTATION_TITLE":
                   getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                   returnedWorks = searchForWorkByTitleResult(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_KEY_TITLE());
                   break;
               //implemented by Nishant @ 22 Apr 2020
               case "WORK_PRODUCT_SUMMARY_NAME":
                   //not all works has products hence need separate query
                   getRandomWorkIdWithProducts();
                   getWorksDataFromEPHGD();
                   getProductDetailByWorkId(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                   returnedWorks=searchForWorkByTitleResult(dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_NAME());
                   break;
               //implemented by Nishant @ 21 Apr 2020
               case "WORK_MANIFESTATION_PRODUCT_SUMMARY_NAME":
                   getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                   getProductDetailByManifestationId(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
                   returnedWorks=searchForWorkByTitleResult(dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_NAME());
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

        Log.info("###########-----getWorkByIdentifier - "+identifierType);
        Log.info("#######################################################");

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

        }
    }

    @When("^the work search by identifier (.*) and type details are retrieved and compared$")
    public void compareSearchByIdentifierAndTypeResultsWithDB(String identifierType) throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            Log.info("###########-----getWorkByIdentifierAndType - "+identifierType);
            Log.info("#######################################################\n");
            if (identifierType.equals("WORK_IDENTIFIER")) {
                getWorkIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                returnedWorks = searchForWorksByIdentifierAndTypeResult(workIdentifiers.get(i).getIDENTIFIER(), workIdentifiers.get(i).getF_TYPE());
            }
            else if (identifierType.equals("WORK_MANIFESTATION_IDENTIFIER")) {
                List<Map<String, Object>> manifIdentifiers = getEPHGDManifestationIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                returnedWorks = searchForWorksByIdentifierAndTypeResult(manifIdentifiers.get(i).get("identifier").toString(), manifIdentifiers.get(i).get("f_type").toString());
            }
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
        }
    }

    @When("^the works retrieved by search (.*) details are retrieved and compared$")
    public void compareWorksRetrievdBySearchOptionWithDB(String identifierType) throws AzureOauthTokenFetchingException {
        //completed by Nishant @ 23-24 Apr 2020
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
                String workId = dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID();
                switch(identifierType)
                {
                    case "WORK_IDENTIFIER": getWorkIdentifiersByWorkID(workId);
                        returnedWorks = searchForWorksBySearchOptionResult(workIdentifiers.get(i).getIDENTIFIER());
                        break;

                    case "WORK_ID": returnedWorks = searchForWorksBySearchOptionResult(workId);
                        break;

                    case "WORK_MANIFESTATION_ID":  getManifestationsByWorkID(workId);
                        returnedWorks = searchForWorksBySearchOptionResult(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
                        break;

                    case "WORK_MANIFESTATION_IDENTIFIER":List<Map<String, Object>> manifIdentifiers = getEPHGDManifestationIdentifiersByWorkID(workId);
                        returnedWorks = searchForWorksBySearchOptionResult(manifIdentifiers.get(0).get("identifier").toString());
                        break;

                    case "WORK_PRODUCT_ID":
                        getRandomWorkIdWithProducts();
                        getWorksDataFromEPHGD();
                        getProductDetailByWorkId(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                        returnedWorks=searchForWorksBySearchOptionResult(dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID());
                        break;

                    case "WORK_MANIFESTATION_PRODUCT_ID":
                        getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                        getProductDetailByManifestationId(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
                        returnedWorks=searchForWorksBySearchOptionResult(dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID());
                        break;

                    case "WORK_TITLE":returnedWorks = searchForWorksBySearchOptionResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
                        break;

                    case "WORK_MANIFESTATION_TITLE":
                        getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                        returnedWorks=searchForWorksBySearchOptionResult(dataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE());
                        break;

                        //implemented by Nishant @ 24 Apr 2020
                    case "WORK_PERSONS_FULLNAME":
                        /*
                        created by Nishant @24 Apr 2020
                        getWorkByPerson returns sometimes 70000+ records and most probable intended work id does not appear in first 20 records
                        hence we need to send API request with size 10000 and check if intended workID is returned
                        if not, send request again for next 10000 records
                        */
                        boolean found=false; int from=0; int size=5000;
                        getWorkPersonRoleByWorkId(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                        getPersonDataByPersonId(dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON());
                        returnedWorks=searchForWorksBySearchOptionResult(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME()+" "+
                                        dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME()+"&from="+from+"&size="+size);

                        Log.info("Total work found... - "+returnedWorks.getTotalMatchCount());
                        while(!returnedWorks.verifyWorkWithIdIsReturnedOnly(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID())
                                &&from+size<returnedWorks.getTotalMatchCount())
                        {
                            from+=size;
                            Log.info("scanned workID from "+(from-size)+" to "+from+" records...");
                            returnedWorks=searchForWorksBySearchOptionResult(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME()+" "+
                                            dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME()+"&from="+from+"&size="+size);
                        }
                        break;
                    case "WORK_PRODUCT_SUMMARY_NAME":
                        getRandomWorkIdWithProducts();
                        getWorksDataFromEPHGD();
                        getProductDetailByWorkId(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                        returnedWorks=searchForWorksBySearchOptionResult(dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
                        break;
                    case "WORK_MANIFESTATION_PRODUCT_SUMMARY_NAME":
                        getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                        getProductDetailByManifestationId(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
                        returnedWorks=searchForWorksBySearchOptionResult(dataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
                        break;
                }
                returnedWorks.verifyWorksAreReturned();
                returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
        }
    }

    @When("^the work details are retrieved by PMC Code and compared$")
    public void compareWorkSearchByPMCResultsWithDB() throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            Log.info("pmc to be tested..."+dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
            returnedWorks = searchForWorkByPMCResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
            returnedWorks.verifyWorksAreReturned();
            Log.info("API total count matched..."+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC()));
        }
    }

    @When("^the work details are retrieved by PMG Code and compared$")
    public void compareWorkSearchByPMGResultsWithDB() throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            String pmgCode = getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
            returnedWorks = searchForWorkByPMGResult(pmgCode);
            returnedWorks.verifyWorksAreReturned();
            Log.info("API total count matched..."+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByPMG(pmgCode));
        }
    }

    @When("^the work response count is compared with the count in the DB for person ID$")
    public void compareSearchResultCountForPersonIdsresponse() throws AzureOauthTokenFetchingException {
        WorksMatchedApiObject returnedWorks = null;
        int bound = ids.size();
        for (int i = 0; i < bound; i++) {
            Log.info("personId to be tested..."+ids.get(i));
            returnedWorks = searchForWorksByPersonIDResult(ids.get(i));
            returnedWorks.verifyWorksAreReturned();
            Log.info("Total API count matched..."+returnedWorks.getTotalMatchCount());
            returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByPersonIDs(ids.get(i)));
        }
    }

    private int getNumberOfWorksByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_AMOUNT_BYPMC, pmcCode);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH work count... "+count);
        return count;
    }

    private int getNumberOfWorksByPersonIDs(String personID) {
        sql = String.format(APIDataSQL.SELECT_COUNT_PERSONID_FOR_WORKS, personID);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("EPH work count..."+count);
        return count;
    }

    private String getPMGcodeByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.EPH_GD_PMG_CODE_EXTRACT_BYPMC, pmcCode);
       // Log.info(sql);
        List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String pmgCode = ((String) getPMG.get(0).get("f_pmg"));
        Log.info("pmg code..."+pmgCode);
        return pmgCode;
    }

    private int getNumberOfWorksByPMG(String pmgCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_AMOUNT_BYPMG, pmgCode);
        List<Map<String, Object>> subjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) subjectAreaNumber.get(0).get("count")).intValue();
        Log.info("EPH work count..."+count);
        return count;
    }


    private List<String> getManifestationIdsForWorkID(String workID) {
      //  Log.info("Get manifestationApiObject ids ..");

        sql = String.format(APIDataSQL.SELECT_MANIFESTATION_IDS_BY_WORKID, workID);
        //   Log.info(sql);

        List<Map<?, ?>> manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        List<String> ids = manifestationIds.stream().map(m -> (String) m.get("manifestation_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Manifestation ids for the work: " + ids);
        Assert.assertFalse("Verify that manifestation ids can be successfully extracted from db by work ids", ids.isEmpty());
        return ids;
    }

    private void getManifestationsByWorkID(String workID) {
        Log.info("get manifestations for the work in EPH GD ..");
        sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_WORKID, workID);
        manifestationDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
        Assert.assertFalse("Verify that manifestaion by work id is extracted successfully from the DB",manifestationDataObjectsFromEPHGD.isEmpty());
    }

    //created by Nishant @ 22 Apr 2020
    private void getProductDetailByManifestationId(String manifestationId){
        Log.info("get product by manifestation id of the work...");
        sql=String.format(APIDataSQL.SelectProductByManifestationId,manifestationId);
        productDataObjectsFromEPHGD=DBManager.getDBResultAsBeanList(sql, ProductDataObject.class,Constants.EPH_URL);
        Assert.assertFalse("Verify that product by manifestation id is extracted successfully from the DB",productDataObjectsFromEPHGD.isEmpty());
    }

    public void getProductDetailByWorkId(String workId){
        Log.info("get product by work id...");
        sql=String.format(APIDataSQL.SelectProductByWorkId,workId);
        productDataObjectsFromEPHGD=DBManager.getDBResultAsBeanList(sql, ProductDataObject.class,Constants.EPH_URL);
        Assert.assertFalse("Verify that product by work id is extracted successfully from the DB",productDataObjectsFromEPHGD.isEmpty());
    }

    //created by Nishant @ 24 Apr 2020
    private void getWorkPersonRoleByWorkId(String workId)
    {
       sql = String.format(APIDataSQL.selectWorkPersonByworkId,workId);
        personWorkRoleDataObjectsFromEPHGD=DBManager.getDBResultAsBeanList(sql,PersonWorkRoleDataObject.class,Constants.EPH_URL);
        Assert.assertFalse("Verify person role by work id successfully extracted from EPH DB", personWorkRoleDataObjectsFromEPHGD.isEmpty());
    }

    private void getPersonDataByPersonId(String personId)
    {
        sql =String.format(APIDataSQL.SelectPersonDataByPersonId,personId);
        personDataObjectsFromEPHGD=DBManager.getDBResultAsBeanList(sql,PersonDataObject.class,Constants.EPH_URL);
        Assert.assertFalse("verify person Data by person id extracted from EPH DB",personDataObjectsFromEPHGD.isEmpty());
    }

    private List getEPHGDManifestationIdentifiersByWorkID(String workID) {
        sql = String.format(APIDataSQL.SELECT_GD_MANIFESTATION_IDENTIFIER_BY_MANIFESTATION_ID,
                Joiner.on("','").join(getManifestationIdsForWorkID(workID)));
        //   Log.info(sql);
        return DBManager.getDBResultMap(sql, Constants.EPH_URL);
    }

    private void getWorkIdentifiersByWorkID(String workID){
        sql = APIDataSQL.getWorkIdentifiersDataFromGD.replace("PARAM1", workID);
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
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
        Assert.assertFalse("Verify that list with work ids is not empty",ids.isEmpty());
    }



}