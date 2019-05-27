package com.eph.automation.testing.web.steps;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.api.ProductApiObject;
import com.eph.automation.testing.models.api.ProductsMatchedApiObject;
import com.eph.automation.testing.models.api.WorkApiObject;
import com.eph.automation.testing.models.api.WorksMatchedApiObject;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.eph.automation.testing.models.contexts.DataQualityContext.manifestationDataObjectsFromEPHGD;
import static com.eph.automation.testing.services.api.APIService.*;
import static com.eph.automation.testing.services.api.APIService.searchForProductsByIdentifierResult;


/**
 * Created by Georgi Vlaykov on 11/02/2019
 */
public class ApiProductsSearchSteps {

    @StaticInjection
    public DataQualityContext dataQualityContext;

    private String sql;
    private static List<String> ids;
    private static List<String> manifestation_Ids;
    private static List<String> manifestationIdentifiers_Ids;
    private ProductApiObject response;
    private WorkApiObject workApi_response;
    private static List<WorkDataObject> workIdentifiers;
    private static List<ProductDataObject> productDataObjects;

    List<ManifestationDataObject> manifestationDataObjects;

    @Given("^We get (.*) random search ids for products$")
    public void getRandomProductIds(String numberOfRecords) {
        Log.info("Get random ids ..");
        //Get property when run with jenkins
//        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_IDS_FOR_SEARCH_BOOKS, numberOfRecords);
        Log.info(sql);

        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomProductSearchIds.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product manifestationApiObject ids  : " + ids);
    }

    @Given("^We get (.*) search ids from the db for person roles of products$")
    public void getRandomPersonRolesIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_PERSON_ROLES_FOR_SEARCH, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomPersonSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomPersonSearchIds.stream().map(m -> (BigDecimal) m.get("f_person")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
    }

    @And("^We get the search data from EPH GD for products$")
    public void getProductsDataFromEPHGD() {
        Log.info("And We get the products data from EPH GD ...");
        sql = String.format(APIDataSQL.EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        Log.info(sql);

       productDataObjects = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
       productDataObjects.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
    }

    @When("^the product details are retrieved and compared$")
    public void compareSearchResultsWithDB() throws IOException {
        int bound =productDataObjects.size();
        System.out.println("Missing entries:\n");
        for (int i = 0; i < bound; i++) {
            boolean code = checkProductExists(productDataObjects.get(i).getPRODUCT_ID());
            if (code) {
                System.out.println(i + ") Checking: " +productDataObjects.get(i).getPRODUCT_ID());

                response = searchForProductResult(productDataObjects.get(i).getPRODUCT_ID());

            }
        }
    }

    @When("^the product details are retrieved when searched by (.*) and compared$")
    public void productSearchByTitleAndCompare(String title) throws IOException {
        ProductsMatchedApiObject returnedProducts = null;
        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            if (title.equals("PRODUCT_PRODUCT_TITLE")) {
                returnedProducts = searchForProductsByTitleResult(productDataObjects.get(i).getPRODUCT_NAME());
            } else if (title.equals("WORK_MANIFESTATION_TITLE")) {
                getManifestationByID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductsByTitleResult(manifestationDataObjects.get(0).getMANIFESTATION_KEY_TITLE());
            }
            else if (title.equals("PRODUCT_MANIFESTATION_WORK_TITLE")) {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductsByTitleResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
            }
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
        }
    }

    @When("^the product details are retrieved and compared when searched by (.*)$")
    public void productSearchByIdentifiersAndCompare(String identifierType) throws IOException {
        ProductsMatchedApiObject returnedProducts = null;
        int bound =productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            if (identifierType.equals("PRODUCT_ID")) {
                returnedProducts = searchForProductsByIdentifierResult(productDataObjects.get(i).getPRODUCT_ID());
            } else if (identifierType.equals("PRODUCT_WORK_IDENTIFIER")) {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                getWorkIdentifiers(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                returnedProducts = searchForProductsByIdentifierResult(workIdentifiers.get(0).getIDENTIFIER());
            } else if (identifierType.equals("PRODUCT_MANIFESTATION_IDENTIFIER")) {
                List<Map<String, Object>> manifestationIdentifiers = getManifestationIdentifierByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductsByIdentifierResult(manifestationIdentifiers.get(0).get("identifier").toString());
            } else if (identifierType.equals("PRODUCT_WORK_ID")) {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductsByIdentifierResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
            } else if (identifierType.equals("PRODUCT_MANIFESTATION_ID")) {
                returnedProducts = searchForProductsByIdentifierResult(String.valueOf(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP()));
            }
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
        }
    }

    @When("^the product details are retrieved and compared when search option is used with (.*)$")
    public void productSearchBySearchOptionAndCompare(String identifierType) throws IOException {
        ProductsMatchedApiObject returnedProducts = null;
        int bound =productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            if (identifierType.equals("PRODUCT_ID")) {
                returnedProducts = searchForProductsBySearchResult(productDataObjects.get(i).getPRODUCT_ID());
            } else if (identifierType.equals("PRODUCT_WORK_IDENTIFIER")) {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                getWorkIdentifiers(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                returnedProducts = searchForProductsBySearchResult(workIdentifiers.get(0).getIDENTIFIER());
            } else if (identifierType.equals("PRODUCT_MANIFESTATION_IDENTIFIER")) {
                List<Map<String, Object>> manifestationIdentifiers = getManifestationIdentifierByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductsBySearchResult(manifestationIdentifiers.get(0).get("identifier").toString());
            } else if (identifierType.equals("PRODUCT_WORK_ID")) {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductsBySearchResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
            } else if (identifierType.equals("PRODUCT_MANIFESTATION_ID")) {
                returnedProducts = searchForProductsBySearchResult(String.valueOf(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP()));
            } else  if (identifierType.equals("PRODUCT_PRODUCT_TITLE")) {
                returnedProducts = searchForProductsByTitleResult(productDataObjects.get(i).getPRODUCT_NAME());
            } else if (identifierType.equals("WORK_MANIFESTATION_TITLE")) {
                getManifestationByID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductsByTitleResult(manifestationDataObjects.get(0).getMANIFESTATION_KEY_TITLE());
            } else if (identifierType.equals("PRODUCT_MANIFESTATION_WORK_TITLE")) {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductsByTitleResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
            }
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
        }
    }

    @When("^the products detail are retrieved and compared when searched by type and (.*)$")
    public void productSearchByIdentifierWithTypeAndCompare(String identifierType) throws IOException {
        ProductsMatchedApiObject returnedProducts = null;
        int bound =productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            if (identifierType.equals("PRODUCT_WORK_IDENTIFIER")) {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                getWorkIdentifiers(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                returnedProducts = searchForProductssByIdentifierAndTypeResult(workIdentifiers.get(0).getIDENTIFIER(), workIdentifiers.get(0).getF_TYPE());
            } else if (identifierType.equals("PRODUCT_MANIFESTATION_IDENTIFIER")) {
                List<Map<String, Object>> manifestationIdentifiers = getManifestationIdentifierByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductssByIdentifierAndTypeResult(manifestationIdentifiers.get(0).get("identifier").toString(), manifestationIdentifiers.get(0).get("f_type").toString());
            }
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
        }
    }

    public List getManifestationIdentifierByManifestationID(String manifestationID) {
        List<String> manIds = new ArrayList<>();
        manIds.add(manifestationID);
        sql = String.format(APIDataSQL.SELECT_GD_MANIFESTATION_IDENTIFIER_BY_MANIFESTATION_ID,
                Joiner.on("','").join(manIds));
        Log.info(sql);

        return DBManager.getDBResultMap(sql, Constants.EPH_URL);
    }

    public void getWorkByManifestationID(String manifestationID) {
        List<String> manIds = new ArrayList<>();
        manIds.add(manifestationID);
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH_BY_MANIFESTATIONID,
                Joiner.on("','").join(manIds));
        Log.info(sql);

        dataQualityContext.workDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    public void getWorkIdentifiers(String workID){
        sql = APIDataSQL.getWorkIdentifiersDataFromGD
                .replace("PARAM1", workID);
        workIdentifiers = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    public void getManifestationByID(String manifestationID) {
        List<String> manIds = new ArrayList<>();
        manIds.add(manifestationID);
        Log.info("And get the manifestations in EPH GD ..");
        sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID, Joiner.on("','").join(manIds));
        Log.info(sql);

        manifestationDataObjects = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    }

    public String getPackageIdByProductID(String workID) {
        sql = String.format(APIDataSQL.EPH_GD_PMG_CODE_EXTRACT_BYPMC, workID);
        Log.info(sql);
        List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String pmgCode = ((String) getPMG.get(0).get("f_pmg"));
        return pmgCode;
    }

    @Given("^We get (.*) random search ids for products in packages")
    public void getRandomProductIdsInPackages(String numberOfRecords){
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_INPACKAGE_IDS_FOR_SEARCH, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomProductSearchIds.stream().map(m -> (String) m.get("f_component")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
    }


    @When("^the product response returned when searched by packages is verified$")
    public void compareProductsRetrievdByIsInPackagesOptionWithDB() {
        ProductsMatchedApiObject returnedProducts = null;

        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            returnedProducts = searchForProductsByPackageResult(getProductPackageByID(productDataObjects.get(i).getPRODUCT_ID()));
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfPackageComponents(getProductPackageByID(productDataObjects.get(i).getPRODUCT_ID())));
        }
    }

    @When("^the product response returned when searched by personID is verified$")
    public void compareProductsRetrievdByPersonWithDB() {
        ProductsMatchedApiObject returnedProducts = null;

        int bound = ids.size();
        for (int i = 0; i < bound; i++) {
            returnedProducts = searchForProductsByPersonIDResult(ids.get(i));
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfProductsByPersonIDs(ids.get(i)));
        }
    }

    public String getProductPackageByID(String id){
        sql = String.format(APIDataSQL.EPH_GD_PACKAGEID_EXTRACT_BY_PRODUCTID, id);
        Log.info(sql);
        List<Map<String, Object>> getPackage = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String packageID = ((String) getPackage.get(0).get("f_package_owner"));
        return packageID;
    }

    public int getNumberOfPackageComponents(String packageID) {
        sql = String.format(APIDataSQL.EPH_GD_PRODUCT_PACKAGE_COUNT_EXTRACT, packageID);
        Log.info(sql);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        return count;
    }

    public int getNumberOfProductsByPersonIDs(String personID) {
        sql = String.format(APIDataSQL.SELECT_COUNT_PERSONID_FOR_WORKS, personID);
        Log.info(sql);
        List<Map<String, Object>> getCountWorks = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int countWorks = ((Long) getCountWorks.get(0).get("count")).intValue();

        sql = String.format(APIDataSQL.SELECT_COUNT_PERSONID_FOR_PRODUCTS, personID);
        Log.info(sql);
        List<Map<String, Object>> getCountProd = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int countProd = ((Long) getCountProd.get(0).get("count")).intValue();

        int count = countProd + countWorks;
        return count;
    }
    @When("^the product details are retrieved by PMC Code and compared$")
    public void compareWorkSearchByPMCResultsWithDB() {
        ProductsMatchedApiObject returnedProducts = null;

        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {

            getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            returnedProducts = searchForProductsByPMCResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());

            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfProductsByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC()));
        }
    }

    public int getNumberOfProductsByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_AMOUNT_BYPMC, pmcCode);
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

    public int getNumberOfProductsByPMG(String pmgCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_AMOUNT_BYPMG, pmgCode);
        Log.info("When We get the count of works with that PMC code ..");
        Log.info(sql);
        List<Map<String, Object>> subjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) subjectAreaNumber.get(0).get("count")).intValue();
        return count;
    }

    @When("^the product details are retrieved by PMG Code and compared$")
    public void compareWorkSearchByPMGResultsWithDB() {
        ProductsMatchedApiObject returnedProducts = null;

        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {

            getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            returnedProducts = searchForProductsByPMGResult(getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC()));

            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfProductsByPMG(getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC())));
        }
    }
}