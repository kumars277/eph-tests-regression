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
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.eph.automation.testing.models.contexts.DataQualityContext.manifestationDataObjectsFromEPHGD;
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

    @Given("^We get (.*) random search ids for works")
    public void getRandomWorkIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_IDS_FOR_SEARCH, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
    }

    @Given("^We get (.*) random search ids for person roles")
    public void getRandomPersonRolesIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_PERSON_ROLES_FOR_SEARCH, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomPersonSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomPersonSearchIds.stream().map(m -> (BigDecimal) m.get("f_person")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
    }

    @And("^We get the work search data from EPH GD$")
    public void getWorksDataFromEPHGD() {
        Log.info("And We get the data from EPH GD for journals ...");
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.workDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        dataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));
    }


    @When("^the work details are retrieved and compared$")
    public void compareWorkSearchResultsWithDB() {
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        System.out.println("Missing entries:\n");
        for (int i = 0; i < bound; i++) {
            Assert.assertTrue("Verify that the searched work exists and is accessible trough the API", checkWorkExists(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID()));

            workApi_response = searchForWorkByIDResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            compareWorkApiResponseWithDB(i);
        }
    }

    @When("^the work details are retrieved by title and compared$")
    public void compareWorkSearchByTitleResultsWithDB() {
        WorksMatchedApiObject returnedWorks = null;
        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        System.out.println("Missing entries:\n");
        for (int i = 0; i < bound; i++) {
            Assert.assertTrue("Verify that the searched work exists and is accessible trough the API", checkWorkExists(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID()));
            returnedWorks = searchForWorkByTitleResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            returnedWorks.verifyWorksAreReturned();
        }
    }

    @When("^the work details are retrieved by PMC Code and compared$")
    public void compareWorkSearchByPMCResultsWithDB() {
        WorksMatchedApiObject returnedWorks = null;

        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {

            returnedWorks = searchForWorkByPMCResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());

            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturned(getNumberOfWorksByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC()));

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



    @When("^the work details are retrieved by PMG Code and compared$")
    public void compareWorkSearchByPMGResultsWithDB() {
        WorksMatchedApiObject returnedWorks = null;

        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {

            returnedWorks = searchForWorkByPMGResult(getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC()));

            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturned(getNumberOfWorksByPMG(getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC())));

        }
    }

    public int getNumberOfWorksByPMG(String pmgCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_AMOUNT_BYPMG, pmgCode);
        Log.info("When We get the count of works with that PMC code ..");
        Log.info(sql);
        List<Map<String, Object>> subjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) subjectAreaNumber.get(0).get("count")).intValue();
        return count;
    }

    @When("^the works search by identifier (.*) details are retrieved and compared$")
    public void compareWorkSearchByIdentifierResultsWithDB(String identifierType) {
        WorksMatchedApiObject returnedWorks = null;

        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
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
                returnedWorks = searchForWorksByIdentifierResult(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
            }
            
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());

        }
    }

    @When("^the works retrieved by search (.*) details are retrieved and compared$")
    public void compareWorksRetrievdBySearchOptionWithDB(String identifierType) {
        WorksMatchedApiObject returnedWorks = null;

        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            if (identifierType.equals("WORK_IDENTIFIER")){
                getWorkIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                returnedWorks = searchForWorksBySearchOptionResult(workIdentifiers.get(i).getIDENTIFIER());
            } else if (identifierType.equals("WORK_MANIFESTATION_IDENTIFIER")){
                List<Map<String, Object>> manifIdentifiers = getEPHGDManifestationIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                returnedWorks = searchForWorksBySearchOptionResult(manifIdentifiers.get(i).get("identifier").toString());
            } else if (identifierType.equals("WORK_ID")){
                returnedWorks = searchForWorksBySearchOptionResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            } else if (identifierType.equals("WORK_MANIFESTATION_ID")){
                getManifestationsByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                returnedWorks = searchForWorksBySearchOptionResult(dataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
            } else if (identifierType.equals("WORK_PRODUCT_ID")){
                returnedWorks = searchForWorksBySearchOptionResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            } else if (identifierType.equals("WORK_MANIFESTATION_TITLE")){
                returnedWorks = searchForWorksBySearchOptionResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
            } else if (identifierType.equals("WORK_TITLE")){
                returnedWorks = searchForWorksBySearchOptionResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
            }

            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
        }
    }

    @When("^the work search by identifier (.*) and type details are retrieved and compared$")
    public void compareSearchByIdentifierAndTypeResultsWithDB(String identifierType) {
        WorksMatchedApiObject returnedWorks = null;

        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        for (int i = 0; i < bound; i++) {
            if (identifierType.equals("WORK_IDENTIFIER")){
                getWorkIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                returnedWorks = searchForWorksByIdentifierAndTypeResult(workIdentifiers.get(i).getIDENTIFIER(), workIdentifiers.get(i).getF_TYPE());
            } else if (identifierType.equals("WORK_MANIFESTATION_IDENTIFIER")) {
                List<Map<String, Object>> manifIdentifiers = getEPHGDManifestationIdentifiersByWorkID(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
                returnedWorks = searchForWorksByIdentifierAndTypeResult(manifIdentifiers.get(i).get("identifier").toString(),manifIdentifiers.get(i).get("f_type").toString());
            }
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorkWithIdIsReturned(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
        }
    }

    @When("^the work response count is compared with the count in the DB for person ID$")
    public void compareSearchResultCountForPersonIdsresponse() {
        WorksMatchedApiObject returnedWorks = null;

        int bound = ids.size();
        for (int i = 0; i < bound; i++) {
            returnedWorks = searchForWorksByPersonIDResult(ids.get(i));
            returnedWorks.verifyWorksAreReturned();
            returnedWorks.verifyWorksReturned(getNumberOfWorksByPersonIDs(ids.get(i)));
        }
    }


    public void compareWorkApiResponseWithDB(int DataObjectNumber){
        Assert.assertEquals(workApi_response.getId(), dataQualityContext.workDataObjectsFromEPHGD.get(DataObjectNumber).getWORK_ID());
        Assert.assertEquals(workApi_response.getTitle(), dataQualityContext.workDataObjectsFromEPHGD.get(DataObjectNumber).getWORK_TITLE());
        Assert.assertTrue(workApi_response.getElectronicRightsInd().contains(
                dataQualityContext.workDataObjectsFromEPHGD.get(DataObjectNumber).getELECTRONIC_RIGHTS_IND()));
//        Assert.assertTrue(Integer.valueOf(workApi_response.getEditionNumber()).equals(dataQualityContext.workDataObjectsFromEPHGD.get(DataObjectNumber).getEDITION_NUMBER()));
//        Assert.assertTrue(Integer.valueOf(workApi_response.getVolume()).equals(dataQualityContext.workDataObjectsFromEPHGD.get(DataObjectNumber).getVOLUME()));
//        Assert.assertTrue(Integer.valueOf(workApi_response.getCopyrightYear()).equals(dataQualityContext.workDataObjectsFromEPHGD.get(DataObjectNumber).getCOPYRIGHT_YEAR()));

        Assert.assertEquals(workApi_response.getType().get("code"),dataQualityContext.workDataObjectsFromEPHGD.get(DataObjectNumber).getF_TYPE());
        Assert.assertEquals(workApi_response.getStatus().get("code"),dataQualityContext.workDataObjectsFromEPHGD.get(DataObjectNumber).getWORK_STATUS());
        Assert.assertEquals(workApi_response.getImprint().get("code"),dataQualityContext.workDataObjectsFromEPHGD.get(DataObjectNumber).getIMPRINT());
        Assert.assertEquals(workApi_response.getPmc().getCode(),dataQualityContext.workDataObjectsFromEPHGD.get(DataObjectNumber).getPMC());
        // TODO: PMG compare
//        Assert.assertEquals(workApi_response.getPmc().getPmg().get("pmgCode"),dataQualityContext.workDataObjectsFromEPHGD.get(DataObjectNumber).getPMG());
    }


    public List<String> getManifestationIdsForWorkID(String workID) {
        Log.info("Get manifestationApiObject ids ..");

        sql = String.format(APIDataSQL.SELECT_MANIFESTATION_IDS_BY_WORKID, workID);
        Log.info(sql);

        List<Map<?, ?>> manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        List<String> ids = manifestationIds.stream().map(m -> (String) m.get("manifestation_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("ids : " + ids);

        return ids;
    }

    public void getManifestationsByWorkID(String workID) {
        Log.info("And get the manifestations in EPH GD ..");
        sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID, Joiner.on("','").join(getManifestationIdsForWorkID(workID)));
        Log.info(sql);

        manifestationDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    }

    public List getEPHGDManifestationIdentifiersByWorkID(String workID) {
        sql = String.format(APIDataSQL.SELECT_GD_MANIFESTATION_IDENTIFIER_BY_MANIFESTATION_ID,
                Joiner.on("','").join(getManifestationIdsForWorkID(workID)));
        Log.info(sql);

        return DBManager.getDBResultMap(sql, Constants.EPH_URL);
    }

    public void getWorkIdentifiersByWorkID(String workID){
        sql = APIDataSQL.getWorkIdentifiersDataFromGD
                .replace("PARAM1", workID);
        workIdentifiers = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }



}