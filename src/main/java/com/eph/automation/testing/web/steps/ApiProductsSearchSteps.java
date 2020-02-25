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
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.models.dao.ProductDataObject;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.eph.automation.testing.services.api.APIService.*;
//import static com.eph.automation.testing.services.api.APIService.;

/**
 * Created by Georgi Vlaykov on 11/02/2019
 */
public class ApiProductsSearchSteps {

    @StaticInjection
    public DataQualityContext dataQualityContext;

    private String sql;
    private static List<String> packageIds;
    private static List<String> ids;
    private static List<String> manifestationIds;
    private static List<String> manifestationIdentifiers_Ids;
    private ProductApiObject response;
    private WorkApiObject workApi_response;
    private static List<WorkDataObject> workIdentifiers;
    private static List<ProductDataObject> productDataObjects;
    List<ManifestationDataObject> manifestationDataObjects;


    @Given("^We get (.*) random search ids for products$")
    public void getRandomProductIds(String numberOfRecords) {
        //Get property when run with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("Getting random ids for numberOfRecords = " + numberOfRecords);
        sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_IDS_FOR_SEARCH_BOOKS, numberOfRecords);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product ids are : " + ids);

        //added by Nishant @ 26 Dec for debugging failures
        //   ids.clear();
        //  ids.add("EPR-10Y62K");
        // Log.info("hard coded product ids are : " + ids);

        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }

    @Given("^We get (.*) search ids from the db for person roles of products$")
    public void getRandomPersonRolesIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_PERSON_ROLES_FOR_SEARCH, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomPersonSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomPersonSearchIds.stream().map(m -> (BigDecimal) m.get("f_person")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
        Assert.assertFalse("Verify That list with random person ids is not empty.", ids.isEmpty());
    }

    @And("^We get the search data from EPH GD for products$")
    public void getProductsDataFromEPHGD() {
        Log.info("And We get the products data from EPH GD ...");
        sql = String.format(APIDataSQL.EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        productDataObjects = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
        productDataObjects.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
        Assert.assertFalse("Verify That product objects list from DB is not empty.", productDataObjects.isEmpty());
    }

    //created by Nishant @ 29 Nov 2019
    @Given("^We get (.*) random package id")
    public void getRandomPackagesIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_PACKAGE_IDS_FOR_SEARCH, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomPackageIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        packageIds = randomPackageIds.stream().map(m -> (String) m.get("product_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random package ids  : " + packageIds);
        Assert.assertFalse("Verify That list with random ids is not empty.", packageIds.isEmpty());
    }

    @And("^We get 1 random search ids from package$")
    public void getRandomProductIdFromPackage() {
        sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_FROM_PACKAGE, Joiner.on("','").join(packageIds));
        Log.info(sql);
        List<Map<?, ?>> randomProductFromPackageIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductFromPackageIds.stream().map(m -> (String) m.get("f_component")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product is " + ids.toString() + " from package ids  : " + packageIds.toString());
        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }

    @When("^the product response returned when searched by packages is verified$")
    public void compareProductsRetrievedByIsInPackagesOptionWithDB() throws AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts = null;
        for (int i = 0; i < packageIds.size(); i++) {
            returnedProducts = searchForProductsByPackageResult(packageIds.get(i));
            Log.info("\n number of package component in API is : " + returnedProducts.getTotalMatchCount());
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfPackageComponents());
        }
    }

    public int getNumberOfPackageComponents() {
        sql = String.format(APIDataSQL.EPH_GD_PRODUCT_COUNT_BY_PACKAGE_EXTRACT, Joiner.on("','").join(packageIds));
        Log.info(sql);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("\n number of package component in DB is : " + count);
        return count;
    }

    @When("^the product response returned when searched by components is verified$")
    public void compareProductsRetrievedByhasComponentsOptionWithDB() throws AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts = null;
        for (int i = 0; i < ids.size(); i++) {
            returnedProducts = searchForProductsByComponentsResult(ids.get(i));
            Log.info("\n number of packages in API : " + returnedProducts.getTotalMatchCount() + " - for products " + ids.get(i));
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfHasComponents());
        }
    }

    public int getNumberOfHasComponents() {
        sql = String.format(APIDataSQL.EPH_GD_PACKAGE_COUNT_BY_PRODUCT_EXTRACT, Joiner.on("','").join(ids));
        Log.info(sql);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("\n number of packages in DB : " + count + " - for products " + ids.toString());
        return count;
    }

    @When("^the product details are retrieved by accountableProduct and compared$")
    public void compareProductsRetrievedByaccountableProductWithDB() throws AzureOauthTokenFetchingException{
        ProductsMatchedApiObject returnedProducts = null;
        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            String accountableProductSegmentCode = getSegmentCode(dataQualityContext.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
            Log.info("accountableProduct segmentcode is: "+accountableProductSegmentCode);
            returnedProducts = searchForProductsByaccountableProduct(accountableProductSegmentCode);
            Log.info("\n product count in API by accountableProduct is : " + returnedProducts.getTotalMatchCount());
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfProductByAccountableProducts(accountableProductSegmentCode));
        }

    }

    public String getSegmentCode(String accountableProduct) {
        if (accountableProduct != null) {
            sql = String.format(APIDataSQL.SELECT_PRODUCT_SEGMENT_CODE_FROM_WORK, accountableProduct);
            List<Map<String, Object>> list_accountableProduct = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            String segmentCode = list_accountableProduct.get(0).get("gl_product_segment_code").toString();

            return segmentCode;
        }
        else Log.info("accountableProduct id is null, proceeding with segmentCode J018393");
        return "J018393";
    }

    public int getNumberOfProductByAccountableProducts(String segmentCode) {
        sql = String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_SEGMENT_CODE, segmentCode,segmentCode);
        List<Map<String, Object>> countByAccountableProduct = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) countByAccountableProduct.get(0).get("count")).intValue();
        Log.info("products count by accontableProducts in DB is: "+count);
        return count;
    }

    public int getProductCountByProductStatus(String productStatus){//created by Nishant @ 5th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_PRODUCTSTATUS,productStatus);
        List<Map<String,Object>> countByProductStatus = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByProductStatus.get(0).get("count")).intValue();
        Log.info("products count by productStatus in DB is: "+count);
        return count;
    }

    public int getProductCountByProductType(String productType)    {//created by Nishant @ 9th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_PRODUCTTYPE,productType);
        List<Map<String,Object>> countByProductType = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByProductType.get(0).get("count")).intValue();
        Log.info("products count by productType in DB is: "+count);
        return count;
    }

    public int getProductCountByWorkType(String workType){//created by Nishant @ 9th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_WORKTYPE,workType);
        List<Map<String,Object>> countByWorkType = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByWorkType.get(0).get("count")).intValue();
        Log.info("products count by workType in DB is: "+count);
        return count;
    }

    public int getProductCountByManifestationType(String manifestationType){//created by Nishant @ 9th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_MANIFESTATIONTYPE,manifestationType);
        List<Map<String,Object>> countByManifestationType = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByManifestationType.get(0).get("count")).intValue();
        Log.info("products count by workType in DB is: "+count);
        return count;
    }

    public int getProductCountByPMCCode(String searchTerm,String pmcCode){//created by Nishant @ 9th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_PMC_WITHSEARCH,searchTerm,pmcCode);
        List<Map<String,Object>> countByPMC = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByPMC.get(0).get("count")).intValue();
        Log.info("products count by search with pmc in DB is: "+count);
        return count;
    }

    public int getProductCountByPMGCode(String searchTerm,String pmgCode){//created by Nishant @ 20th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_PMG_WITHSEARCH,searchTerm,pmgCode);
        List<Map<String,Object>> countByPMG = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByPMG.get(0).get("count")).intValue();
        Log.info("products count by search with pmg in DB is: "+count);
        return count;
    }

    //Updated by Nishant

    @Then("^the product details are retrieved and compared$")
    public void compareSearchResultsWithDB() throws IOException, AzureOauthTokenFetchingException {
        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            boolean code = checkProductExists(productDataObjects.get(i).getPRODUCT_ID());
            if (code) {
                response = searchForProductResult(productDataObjects.get(i).getPRODUCT_ID());
                response.compareWithDB();
            }
        }
    }

    @When("^the product details are retrieved when searched by (.*) and compared$")
    public void productSearchByTitleAndCompare(String title) throws IOException, AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts = null;
        Log.info("And we get the products data from the API ...");
        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            if (title.equals("PRODUCT_PRODUCT_TITLE")) {
                returnedProducts = searchForProductsByTitleResult(productDataObjects.get(i).getPRODUCT_NAME());
            } else if (title.equals("WORK_MANIFESTATION_TITLE")) {
                getManifestationByID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductsByTitleResult(manifestationDataObjects.get(0).getMANIFESTATION_KEY_TITLE());
            } else if (title.equals("PRODUCT_MANIFESTATION_WORK_TITLE")) {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductsByTitleResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
            }
            Log.info(returnedProducts.toString());
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
        }
    }

    @When("^the product details are retrieved and compared when searched by (.*)$")
    public void productSearchByIdentifiersAndCompare(String identifierType) throws IOException, AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts = null;
        Log.info("And we get the products data from the API ...");
        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            if (identifierType.equals("PRODUCT_ID")) {
                returnedProducts = searchForProductsByIdentifierResult(productDataObjects.get(i).getPRODUCT_ID());
            } else if (identifierType.equals("PRODUCT_WORK_IDENTIFIER")) {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                getWorkIdentifiers(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                returnedProducts = searchForProductsByIdentifierResult(workIdentifiers.get(0).getIDENTIFIER());
            } else if (identifierType.equals("PRODUCT_MANIFESTATION_IDENTIFIER")) {
                List<Map<String, Object>> manifestationIdentifiers = getManifestationIdentifierByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                if (manifestationIdentifiers.size() != 0)
                    returnedProducts = searchForProductsByIdentifierResult(manifestationIdentifiers.get(0).get("identifier").toString());
                Log.info("Manifestation identifier not found");
            } else if (identifierType.equals("PRODUCT_WORK_ID")) {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductsByIdentifierResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
            } else if (identifierType.equals("PRODUCT_MANIFESTATION_ID")) {
                returnedProducts = searchForProductsByIdentifierResult(String.valueOf(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP()));
            }
            //  Log.info(returnedProducts.toString());
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
        }
    }

    @When("^the products detail are retrieved and compared when searched by type and (.*)$")
    public void productSearchByIdentifierWithTypeAndCompare(String identifierType) throws IOException, AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts = null;
        Log.info("And we get the products data from the API ...");
        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            if (identifierType.equals("PRODUCT_WORK_IDENTIFIER")) {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                getWorkIdentifiers(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                returnedProducts = searchForProductssByIdentifierAndTypeResult(workIdentifiers.get(0).getIDENTIFIER(), workIdentifiers.get(0).getF_TYPE());
            } else if (identifierType.equals("PRODUCT_MANIFESTATION_IDENTIFIER")) {
                List<Map<String, Object>> manifestationIdentifiers = getManifestationIdentifierByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts = searchForProductssByIdentifierAndTypeResult(manifestationIdentifiers.get(0).get("identifier").toString(), manifestationIdentifiers.get(0).get("f_type").toString());
            }
            Log.info(returnedProducts.toString());
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
        }
    }

    @When("^the product details are retrieved and compared when search option is used with (.*)$")
    public void productSearchBySearchOptionAndCompare(String identifierType) throws IOException, AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts = null;
        Log.info("And we get the products data from the API ...");
        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            //   Thread.sleep(3000);
            try {

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
                } else if (identifierType.equals("PRODUCT_PRODUCT_TITLE")) {
                    returnedProducts = searchForProductsByTitleResult(productDataObjects.get(i).getPRODUCT_NAME());
                } else if (identifierType.equals("WORK_MANIFESTATION_TITLE")) {
                    getManifestationByID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                    returnedProducts = searchForProductsByTitleResult(manifestationDataObjects.get(0).getMANIFESTATION_KEY_TITLE());
                } else if (identifierType.equals("PRODUCT_MANIFESTATION_WORK_TITLE")) {
                    getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                    returnedProducts = searchForProductsByTitleResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
                }
                Log.info(returnedProducts.toString());
                returnedProducts.verifyProductsAreReturned();
                //returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
                boolean prd = returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
                System.out.println(prd+"=================");
                Assert.assertTrue(prd);

            }
            catch(Exception e)
            {
                Log.info("catch message: "+e.getMessage());
                Assert.assertTrue(false);
            }
        }
    }

    @When("^the product response returned when searched by personID is verified$")
    public void compareProductsRetrievdByPersonWithDB() throws AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts = null;
        int bound = ids.size();
        for (int i = 0; i < bound; i++) {
            returnedProducts = searchForProductsByPersonIDResult(ids.get(i));
            Log.info("PersonID chosen : "+ids.get(i));
            Log.info("Product count in API : "+returnedProducts.getTotalMatchCount());
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfProductsByPersonIDs(ids.get(i)));
        }
    }

    @When("^the product details are retrieved by PMC Code and compared$")
    public void compareProductSearchByPMCResultsWithDB() throws AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts = null;
        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
            String pmcCode = dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC();
            Log.info("pmcCode to be tested: "+pmcCode);
            returnedProducts = searchForProductsByPMCResult(pmcCode);

            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfProductsByPMC(pmcCode));
        }
    }

    @When("^the product details are retrieved by PMG Code and compared$")
    public void compareProductSearchByPMGResultsWithDB() throws AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts = null;
        int bound = productDataObjects.size();
        for (int i = 0; i < bound; i++) {
            try {
                getWorkByManifestationID(productDataObjects.get(i).getF_PRODUCT_MANIFESTATION_TYP());
                String pmgCode = getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
                Log.info("pmgCode to be tested: " + pmgCode);
                returnedProducts = searchForProductsByPMGResult(pmgCode);
                returnedProducts.verifyProductsAreReturned();
                returnedProducts.verifyProductsReturned(getProductsCountByPMGandPMC(dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC()));
            }
            catch (Exception e)
            {
                System.out.println("pmgCode to be tested: " + getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC()));
                System.out.println(e.getMessage());
                Assert.assertFalse(true);

            }
        }
    }

    @Given("^We get product by ID (.*)$")
    public void getProductById(String productID) {
        Log.info("Get random ids ..");
        //Get property when run with jenkins
//        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + productID);

        sql = String.format(APIDataSQL.SELECT_PRODUCT_BY_ID, productID);
        Log.info(sql);

        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomProductSearchIds.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product manifestationApiObject ids  : " + ids);
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
        //  Log.info(sql);

        dataQualityContext.workDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    public void getWorkIdentifiers(String workID) {
        sql = APIDataSQL.getWorkIdentifiersDataFromGD.replace("PARAM1", workID);
        workIdentifiers = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    public void getManifestationByID(String manifestationID) {
        List<String> manIds = new ArrayList<>();
        manIds.add(manifestationID);
        Log.info("And get the manifestations in EPH GD ..");
        sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID, Joiner.on("','").join(manIds));
        //  Log.info(sql);

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

    public String getProductPackageByID(String id) {
        sql = String.format(APIDataSQL.EPH_GD_PACKAGEID_EXTRACT_BY_PRODUCTID, id);
        Log.info(sql);
        List<Map<String, Object>> getPackage = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String packageID = ((String) getPackage.get(0).get("f_package_owner"));
        return packageID;
    }

    public int getNumberOfProductsByPersonIDs(String personID) {
        sql = String.format(APIDataSQL.SELECT_COUNT_PERSONID_FOR_PRODUCTS,personID, personID);
        //    Log.info(sql);
        List<Map<String, Object>> getCountProd = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int countProd = ((Long) getCountProd.get(0).get("count")).intValue();
        Log.info("Product count in DB : "+countProd);
        return countProd;
    }

    public int getNumberOfProductsByPMG(String pmgCode) {
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_AMOUNT_BYPMG, pmgCode);
        Log.info("When We get the count of works with that PMC code ..");
        Log.info(sql);
        List<Map<String, Object>> countPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) countPMG.get(0).get("count")).intValue();
        return count;
    }

    public void getWorksByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.SELECT_WORKS_BY_PMC_CODE, pmcCode);
        //   Log.info(sql);
        List<Map<?, ?>> workIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = workIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Assert.assertFalse("Verify That list with work ids by pmc is not empty.", ids.isEmpty());
    }

    public void getPMGWorksByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.EPH_GD_PMG_CODE_WORKS_EXTRACT_BY_PMC, pmcCode);
        //   Log.info(sql);
        List<Map<?, ?>> workIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = workIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Assert.assertFalse("Verify That list with work ids by pmc is not empty.", ids.isEmpty());
    }

    public void getManifestationsByWorks() {
        sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_IDS_BY_WORKS, Joiner.on("','").join(ids));
        // Log.info(sql);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        manifestationIds = randomProductSearchIds.stream().map(m -> (String) m.get("MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());
        Assert.assertFalse("Verify That list with manifestation ids by work ids is not empty.", ids.isEmpty());
    }

    public int getProductsCountByManifestations() {
        sql = String.format(APIDataSQL.SELECT_COUNT_PRODUCTS_BY_MANIFESTATIONS, Joiner.on("','").join(manifestationIds));
        //   Log.info(sql);
        List<Map<String, Object>> productsCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) productsCount.get(0).get("count")).intValue();
        return count;
    }

    public int getProductsCountByWork() {//created by Nishant @ 25 Nov 2019
        sql = String.format(APIDataSQL.SELECT_COUNT_PRODUCTS_BY_WORK, Joiner.on("','").join(ids));
        //   Log.info(sql);
        List<Map<String, Object>> productsCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) productsCount.get(0).get("count")).intValue();
        return count;
    }

    public int getNumberOfProductsByPMC(String pmcCode) {
        getWorksByPMC(pmcCode);
        getManifestationsByWorks();
        int count = getProductsCountByManifestations() +  getProductsCountByWork();
        Log.info("products count by workType in DB is: "+count);
        return count;
    }

    public int getProductsCountByPMGandPMC(String pmcCode) {
        getPMGWorksByPMC(pmcCode);
        getManifestationsByWorks();
        return getProductsCountByManifestations();// + getProductsCountByWork();
    }

    public String getPMGcodeByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.EPH_GD_PMG_CODE_EXTRACT_BYPMC, pmcCode);
        //  Log.info(sql);
        List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String pmgCode = ((String) getPMG.get(0).get("f_pmg"));
        return pmgCode;
    }


    @Then("^the product count are retrieved by (.*) compared$")
    public void theProductDetailsAreRetrievedByParamKeyAndCompared(String paramKey) throws Throwable {
        Log.info("Automation is in progress");
        ProductsMatchedApiObject returnedProducts = null;
        String searchTerm = (productDataObjects.get(0).getPRODUCT_NAME().split(" ")[0]).toUpperCase();
        switch (paramKey)
        {
            case "productStatus":
                returnedProducts =   searchForProductByParam("cell",paramKey,productDataObjects.get(0).getF_STATUS());
                Log.info("products count by productStatus in API is: "+returnedProducts.getTotalMatchCount());
                returnedProducts.verifyProductsAreReturned();
                returnedProducts.verifyProductsReturned(getProductCountByProductStatus(productDataObjects.get(0).getF_STATUS()));
                break;
            case "productType":
                returnedProducts =   searchForProductByParam("cell",paramKey,productDataObjects.get(0).getF_TYPE());
                Log.info("products count by productType in API is: "+returnedProducts.getTotalMatchCount());
                returnedProducts.verifyProductsAreReturned();
                returnedProducts.verifyProductsReturned(getProductCountByProductType(productDataObjects.get(0).getF_TYPE()));
                break;
            case "workType":
                getWorkByManifestationID(productDataObjects.get(0).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts =   searchForProductByParam("cell",paramKey,dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
                Log.info("products count by workType in API is: "+returnedProducts.getTotalMatchCount());
                returnedProducts.verifyProductsAreReturned();
                returnedProducts.verifyProductsReturned(getProductCountByWorkType(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE()));
                break;
            case "manifestationType":
                getManifestationByID(productDataObjects.get(0).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts =   searchForProductByParam("cell",paramKey,manifestationDataObjects.get(0).getF_TYPE());
                Log.info("products count by manifestationType in API is: "+returnedProducts.getTotalMatchCount());
                returnedProducts.verifyProductsAreReturned();
                returnedProducts.verifyProductsReturned(getProductCountByManifestationType(manifestationDataObjects.get(0).getF_TYPE()));

                break;
            case "pmcCode":
                getWorkByManifestationID(productDataObjects.get(0).getF_PRODUCT_MANIFESTATION_TYP());
                returnedProducts =   searchForProductByParam(searchTerm,paramKey,dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
                Log.info("products count by pmc in API is: "+returnedProducts.getTotalMatchCount());
                returnedProducts.verifyProductsAreReturned();
                returnedProducts.verifyProductsReturned(getProductCountByPMCCode(searchTerm,dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC()));
                break;
            case "pmgCode":
                getWorkByManifestationID(productDataObjects.get(0).getF_PRODUCT_MANIFESTATION_TYP());
                String pmgCode = getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
                returnedProducts =   searchForProductByParam(searchTerm,paramKey,pmgCode);
                Log.info("products count by pmgCode "+pmgCode+" in API is: "+returnedProducts.getTotalMatchCount());
                returnedProducts.verifyProductsAreReturned();
                returnedProducts.verifyProductsReturned(getProductCountByPMGCode(searchTerm,pmgCode));

                break;

        }

        {


        }
    }
}