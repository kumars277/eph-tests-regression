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
import com.eph.automation.testing.models.dao.*;
import com.eph.automation.testing.services.api.APIService;
import com.eph.automation.testing.services.api.AzureOauthTokenFetchingException;

import static com.eph.automation.testing.models.contexts.DataQualityContext.personWorkRoleDataObjectsFromEPHGD;
import static com.eph.automation.testing.services.api.APIService.*;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.eph.automation.testing.services.db.sql.PersonProductRoleDataSQL;
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
import com.eph.automation.testing.web.steps.ApiWorksSearchSteps;
import static com.eph.automation.testing.models.contexts.DataQualityContext.personDataObjectsFromEPHGD;
import static com.eph.automation.testing.models.contexts.DataQualityContext.personProductRoleDataObjectsFromEPHGD;




/**
 * Created by Georgi Vlaykov on 11/02/2019
 */
public class ApiProductsSearchSteps {

    @StaticInjection
    public DataQualityContext dataQualityContext;
    private APIService apiService=new APIService();
    private String sql;
    private static List<String> packageIds;
    private static List<String> ids;
    private static List<String> manifestationIds;
    private static List<String> manifestationIdentifiers_Ids;
    private ProductApiObject response;
    private WorkApiObject workApi_response;
    private static List<WorkDataObject> workIdentifiers;
    private static List<ProductDataObject> productIdentifiers;
    private static List<ProductDataObject> productDataObjects;
    private List<ManifestationDataObject> manifestationDataObjects;


    @Given("^We get (.*) random search ids for products (.*)$")
    public void getRandomProductIds(String numberOfRecords,String productProperty) {
        //updated by Nishant @ 25 may 2021 for EPHD-3122
        //Get property when run with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        switch(productProperty)
        {
            case "PRODUCT_WORK_IDENTIFIER":
            case "PRODUCT_WORK_ID":
            case "PRODUCT_WORK_ACCOUNTABLE_PRODUCT":
            case "PRODUCT_WORK_TITLE":
            case "PRODUCT_WORK_PERSONS_FULLNAME":
                sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_IDS_WITH_WORK); break;

            case"PRODUCT_PERSONS_FULLNAME":
                sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_IDS_WITH_PERSON);break;

            case"PRODUCT_IDENTIFIER":
                sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_IDS_WITH_IDENTIFIER);break;

            default:sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_IDS_FOR_SEARCH, numberOfRecords); break;
        }

         List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Environment used..."+System.getProperty("ENV"));
        Log.info("Selected random product ids are : " + ids);
        //added by Nishant @ 26 Dec for debugging failures
         // ids.clear(); ids.add("EPR-11CW7Y"); Log.info("hard coded product ids are : " + ids);//

        DataQualityContext.breadcrumbMessage += "->" + ids;
        Assert.assertFalse(DataQualityContext.breadcrumbMessage +" Verify That list with random ids is not empty.", ids.isEmpty());
    }

    @Given("^We get (.*) search ids from the db for person roles of products$")
    public void getRandomPersonRolesIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_PERSON_ROLES_FOR_SEARCH, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomPersonSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomPersonSearchIds.stream().map(m -> (BigDecimal) m.get("f_person")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random work ids  : " + ids);
        DataQualityContext.breadcrumbMessage += "->" + ids;
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" Verify That list with random person ids is not empty.", ids.isEmpty());
    }

    //created by Nishant @ 29 Nov 2019
    @Given("^We get (.*) random package id")
    public void getRandomPackagesIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_PACKAGE_IDS_FOR_SEARCH, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomPackageIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        packageIds = randomPackageIds.stream().map(m -> (String) m.get("product_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random package ids  : " + packageIds);
        DataQualityContext.breadcrumbMessage += "->" + ids;
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" Verify That list with random ids is not empty.", packageIds.isEmpty());
    }

    @And("^We get 1 random search ids from package$")
    public void getRandomProductIdFromPackage() {
        sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_FROM_PACKAGE, Joiner.on("','").join(packageIds));
        Log.info(sql);
        List<Map<?, ?>> randomProductFromPackageIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductFromPackageIds.stream().map(m -> (String) m.get("f_component")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product is " + ids.toString() + " from package ids  : " + packageIds.toString());
        DataQualityContext.breadcrumbMessage += "->" + ids;
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" Verify That list with random ids is not empty.", ids.isEmpty());
    }


/*
    @Given("We get (.*) random product id with work")
    public void getRandomProductIdWithWork(){//created by Nishant @ 7 May 2020
        sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_IDS_WITH_WORK);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product id with work is : " + ids);
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" verify list with random id is not empty",ids.isEmpty());
    }
    */
/*
    private void getRandomProductIdWithPerson(){//created by Nishant @ 8 May 2020
        sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_IDS_WITH_PERSON);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product id with person is : " + ids);
       // ids.clear(); ids.add("EPR-10W7JR"); Log.info("hard code value for debuggind failures ..."+ids.get(0));
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" verify list with random id is not empty",ids.isEmpty());
    }
*/
/*
    private void getRandomeProductIdWithIdentifier() //created by Nishant @ 8 Mar 2021
    {
        sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_IDS_WITH_IDENTIFIER);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomProductSearchIds.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product id with identifier is : " + ids);
        // ids.clear(); ids.add("EPR-10W7JR"); Log.info("hard code value for debuggind failures ..."+ids.get(0));
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" verify list with random id is not empty",ids.isEmpty());
    }
*/


    @And("^We get the search data from EPH GD for products$")
    public void getProductsDataFromEPHGD() {
        Log.info("get products data from EPH GD ...");
        sql = String.format(APIDataSQL.EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        productDataObjects = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
        productDataObjects.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" Verify That product objects list from DB is not empty.", productDataObjects.isEmpty());
    }

    private void getProductsDataFromEPHGD(String ProductId) {
        // created by Nishant @ 7 May 2020
        sql = String.format(APIDataSQL.EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH, ProductId);
        productDataObjects = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
        productDataObjects.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" Verify That product objects list from DB is not empty.", productDataObjects.isEmpty());
    }

    private void getWorksDataFromEPHGD(String workId) {
        Log.info("And We get the data from EPH GD for journals ...");
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, workId);
        dataQualityContext.workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        dataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" Verify that list with work objects from DB is not empty", dataQualityContext.workDataObjectsFromEPHGD.isEmpty());
    }

    @When("^the product response returned when searched by packages is verified$")
    public void compareProductsRetrievedByIsInPackagesOptionWithDB() throws AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts = null;
        for (String packageId : packageIds) {
            returnedProducts = searchForProductsByPackageResult(packageId);
            Log.info("\n number of package component in API is : " + returnedProducts.getTotalMatchCount());
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfPackageComponents());
        }
    }

    private int getNumberOfPackageComponents() {
        sql = String.format(APIDataSQL.EPH_GD_PRODUCT_COUNT_BY_PACKAGE_EXTRACT, Joiner.on("','").join(packageIds));
        Log.info(sql);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("\n number of package component in DB is : " + count);
        return count;
    }

    @When("^the product response returned when searched by components is verified$")
    public void compareProductsRetrievedByhasComponentsOptionWithDB() throws AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts;
        for (String id : ids) {
            returnedProducts = searchForProductsByComponentsResult(id);
            Log.info("\n number of packages in API : " + returnedProducts.getTotalMatchCount() + " - for products " + id);
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfHasComponents());
        }
    }

    private int getNumberOfHasComponents() {
        sql = String.format(APIDataSQL.EPH_GD_PACKAGE_COUNT_BY_PRODUCT_EXTRACT, Joiner.on("','").join(ids));
        Log.info(sql);
        List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) getCount.get(0).get("count")).intValue();
        Log.info("\n number of packages in DB : " + count + " - for products " + ids.toString());
        return count;
    }

    @When("^the product details are retrieved and compared by (.*)$")
    public void compareProductsRetrievedByaccountableProductWithDB(String accounableProductType) throws AzureOauthTokenFetchingException{
        //created by Nishant @ 13 May 2020
        ProductsMatchedApiObject returnedProducts = null;
        int bound = productDataObjects.size();
        for (ProductDataObject productDataObject : productDataObjects) {
            String accountableProductSegmentCode = "";
            switch (accounableProductType) {
                case "PRODUCT_ACCOUNTABLE_PRODUCT":
                    //on hold, gd_product table it has none record with accountable product.
                    //pending with Dev to clarify as of 13 May 2020
                    break;
                case "PRODUCT_WORK_ACCOUNTABLE_PRODUCT":
                //    getRandomProductIdWithWork();
                //    getProductsDataFromEPHGD();
                    getWorksDataFromEPHGD(productDataObject.getF_PRODUCT_WORK());
                    accountableProductSegmentCode = getSegmentCode(dataQualityContext.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
                    returnedProducts = searchForProductsByaccountableProduct(accountableProductSegmentCode);
                    break;
                case "PRODUCT_MANIFESTATION_WORK_ACCOUNTABLE_PRODUCT":
                    getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                    accountableProductSegmentCode = getSegmentCode(dataQualityContext.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
                    returnedProducts = searchForProductsByaccountableProduct(accountableProductSegmentCode);
                    break;
            }
            Log.info("accountableProduct segmentcode is: " + accountableProductSegmentCode);
            assert returnedProducts != null;
            Log.info("\n product count in API by accountableProduct is : " + returnedProducts.getTotalMatchCount());
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfProductByAccountableProducts(accountableProductSegmentCode));
        }

    }

    String getSegmentCode(String accountableProduct) {
        if (accountableProduct != null) {
            sql = String.format(APIDataSQL.SELECT_PRODUCT_SEGMENT_CODE_FROM_WORK, accountableProduct);
            List<Map<String, Object>> list_accountableProduct = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            return list_accountableProduct.get(0).get("gl_product_segment_code").toString();
        }
        else Log.info("accountableProduct id is null, proceeding with segmentCode J018393");
        return "J018393";
    }

    private int getNumberOfProductByAccountableProducts(String segmentCode) {
        sql = String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_SEGMENT_CODE, segmentCode,segmentCode);
        List<Map<String, Object>> countByAccountableProduct = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        int count = ((Long) countByAccountableProduct.get(0).get("count")).intValue();
        Log.info("products count by accontableProducts in DB is: "+count);
        return count;
    }

    private int getProductCountByProductStatus(String productStatus){//created by Nishant @ 5th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_PRODUCTSTATUS,productStatus);
        List<Map<String,Object>> countByProductStatus = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByProductStatus.get(0).get("count")).intValue();
        Log.info("products count by productStatus in DB is: "+count);
        return count;
    }

    private int getProductCountByProductType(String productType)    {//created by Nishant @ 9th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_PRODUCTTYPE,productType);
        List<Map<String,Object>> countByProductType = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByProductType.get(0).get("count")).intValue();
        Log.info("products count by productType in DB is: "+count);
        return count;
    }

    private int getProductCountByWorkType(String workType){//created by Nishant @ 9th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_WORKTYPE,workType);
        List<Map<String,Object>> countByWorkType = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByWorkType.get(0).get("count")).intValue();
        Log.info("products count by workType in DB is: "+count);
        return count;
    }

    private int getProductCountByManifestationType(String manifestationType){//created by Nishant @ 9th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_MANIFESTATIONTYPE,manifestationType);
        List<Map<String,Object>> countByManifestationType = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByManifestationType.get(0).get("count")).intValue();
        Log.info("products count by manifestationType in DB is: "+count);
        return count;
    }

    private int getProductCountByPMCCode(String searchTerm,String pmcCode){//created by Nishant @ 9th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_PMC_WITHSEARCH,searchTerm,pmcCode);
        List<Map<String,Object>> countByPMC = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByPMC.get(0).get("count")).intValue();
        Log.info("products count by search with pmc in DB is: "+count);
        return count;
    }

    private int getProductCountByPMGCode(String searchTerm,String pmgCode){//created by Nishant @ 20th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_PMG_WITHSEARCH,searchTerm,pmgCode);
        List<Map<String,Object>> countByPMG = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByPMG.get(0).get("count")).intValue();
        Log.info("products count by search with pmg in DB is: "+count);
        return count;
    }
    private int getProductCountByPMGCode(String pmgCode){//created by Nishant @ 20th Dec 2019
        sql=String.format(APIDataSQL.SELECT_PRODUCTCOUNT_BY_PMG,pmgCode);
        List<Map<String,Object>> countByPMG = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        int count=((Long)countByPMG.get(0).get("count")).intValue();
        Log.info("products count by pmg in DB is: "+count);
        return count;
    }

    //Updated by Nishant

    @Then("^the product details are retrieved and compared$")
    public void compareSearchResultsWithDB() throws AzureOauthTokenFetchingException {
        int bound = productDataObjects.size();
        for (ProductDataObject productDataObject : productDataObjects) {
            boolean code = apiService.checkProductExists(productDataObject.getPRODUCT_ID());
            if (code) {
                response = apiService.searchForProductResult(productDataObject.getPRODUCT_ID());
                response.compareWithDB();
            }
        }
    }

    @When("^the product details are retrieved when searched by (.*) and compared$")
    public void productSearchByTitleAndCompare(String title) throws AzureOauthTokenFetchingException {
        //updated by Nishant @ 4 May 2020
        ProductsMatchedApiObject returnedProducts = null;
        int bound = productDataObjects.size();
        for (ProductDataObject productDataObject : productDataObjects) {
            boolean found = false;
            int from;
            int size;

            switch (title) {
                case "PRODUCT_PRODUCT_TITLE":
                    from = 0;
                    size = 50;
                    returnedProducts = searchForProductsByTitleResult(productDataObject.getPRODUCT_NAME()+ "&from=" + from + "&size=" + size);

                    Log.info("Total product found for product title... - " + returnedProducts.getTotalMatchCount());
                    while (!returnedProducts.verifyProductWithIdIsReturnedOnly(productDataObjects.get(0).getPRODUCT_ID()) && from + size < returnedProducts.getTotalMatchCount()) {
                        from += size;
                        Log.info("scanned productID from " + (from - size) + " to " + from + " records...");
                        returnedProducts = searchForProductsByTitleResult(productDataObject.getPRODUCT_NAME()+ "&from=" + from + "&size=" + size);
                    }


                    break;

                case "WORK_MANIFESTATION_TITLE":
                    getManifestationByID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                    returnedProducts = searchForProductsByTitleResult(manifestationDataObjects.get(0).getMANIFESTATION_KEY_TITLE()+ "&from=0&size=100");
                    break;

                case "PRODUCT_MANIFESTATION_WORK_TITLE":
                    getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                    returnedProducts = searchForProductsByTitleResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE()+ "&from=0&size=100");
                    break;
            }
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductWithIdIsReturned(productDataObject.getPRODUCT_ID());
        }
    }

    @When("^the product details are retrieved and compared when searched by (.*)$")
    public void productSearchByIdentifiersAndCompare(String identifierType) throws AzureOauthTokenFetchingException {
        //updated by Nishant @ 5-6 May 2020
        ProductsMatchedApiObject returnedProducts = null;
            int bound = productDataObjects.size();
        for (ProductDataObject productDataObject : productDataObjects) {
            switch (identifierType) {
                case "PRODUCT_IDENTIFIER":
                    //hard coded by Nishant @ 7 may 2020 as there is only record in SIT gd_product_identifier table
                    //as per Mark reply on 7th May
                    //"This is kind of like a data issue we are having in SIT and UAT - all the identifiers are missing."
                 //   getProductsDataFromEPHGD(productDataObjects.get(0).getPRODUCT_ID());
                    returnedProducts = searchForProductsByIdentifierResult(productDataObjects.get(0).getIDENTIFIER());
                    break;
                case "PRODUCT_WORK_IDENTIFIER":
                 //   getRandomProductIdWithWork();
                 //   getProductsDataFromEPHGD();
                    getWorkIdentifiers(productDataObjects.get(0).getF_PRODUCT_WORK());
                    returnedProducts = searchForProductsByIdentifierResult(workIdentifiers.get(0).getIDENTIFIER());

                    break;

                case "PRODUCT_MANIFESTATION_IDENTIFIER":
                    List<Map<String, Object>> manifestationIdentifiers = getManifestationIdentifierByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                    returnedProducts = searchForProductsByIdentifierResult(manifestationIdentifiers.get(0).get("identifier").toString());
                    break;

                case "PRODUCT_MANIFESTATION_WORK_IDENTIFIER":
                    getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                    getWorkIdentifiers(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                    returnedProducts = searchForProductsByIdentifierResult(workIdentifiers.get(0).getIDENTIFIER());
                    break;

                case "PRODUCT_ID":
                    returnedProducts = searchForProductsByIdentifierResult(productDataObject.getPRODUCT_ID());
                    break;

                case "PRODUCT_WORK_ID":
                //    getRandomProductIdWithWork();
                //    getProductsDataFromEPHGD();
                    returnedProducts = searchForProductsByIdentifierResult(productDataObjects.get(0).getF_PRODUCT_WORK());
                    break;

                case "PRODUCT_MANIFESTATION_ID":
                    returnedProducts = searchForProductsByIdentifierResult(String.valueOf(productDataObject.getF_PRODUCT_MANIFESTATION_TYP()));
                    break;

                case "PRODUCT_MANIFESTATION_WORK_ID":
                    getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                    returnedProducts = searchForProductsByIdentifierResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                    break;
            }
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductWithIdIsReturned(productDataObject.getPRODUCT_ID());

        }
    }

    @When("^the products detail are retrieved and compared when searched by type and (.*)$")
    public void productSearchByIdentifierWithTypeAndCompare(String identifierType) throws AzureOauthTokenFetchingException {
        //updated by Nishant @ 7 May 2020
        ProductsMatchedApiObject returnedProducts = null;
        int bound = productDataObjects.size();
        for (ProductDataObject productDataObject : productDataObjects) {
            switch (identifierType) {
                case "PRODUCT_IDENTIFIER":
                    //hard coded by Nishant @ 7 may 2020 as there is only record in SIT gd_product_identifier table
                    getProductsDataFromEPHGD("EPR-10V1T5");
                    returnedProducts = searchForProductssByIdentifierAndTypeResult("1234-0707", "ISBN");
                    break;
                case "PRODUCT_WORK_IDENTIFIER":
                  //  getRandomProductIdWithWork();
                  //  getProductsDataFromEPHGD();
                    getWorkIdentifiers(productDataObjects.get(0).getF_PRODUCT_WORK());
                    returnedProducts = searchForProductssByIdentifierAndTypeResult(workIdentifiers.get(0).getIDENTIFIER(), workIdentifiers.get(0).getWORK_TYPE());
                    break;

                case "PRODUCT_MANIFESTATION_IDENTIFIER":
                    List<Map<String, Object>> manifestationIdentifiers = getManifestationIdentifierByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                    returnedProducts = searchForProductssByIdentifierAndTypeResult(manifestationIdentifiers.get(0).get("identifier").toString(), manifestationIdentifiers.get(0).get("f_type").toString());
                    break;

                case "PRODUCT_MANIFESTATION_WORK_IDENTIFIER":
                    getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                    getWorkIdentifiers(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                    returnedProducts = searchForProductssByIdentifierAndTypeResult(workIdentifiers.get(0).getIDENTIFIER(), workIdentifiers.get(0).getF_TYPE());
                    break;
            }
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductWithIdIsReturned(productDataObject.getPRODUCT_ID());
        }
    }

    @When("^the product details are retrieved and compared when search option is used with (.*)$")
    public void productSearchBySearchOptionAndCompare(String searchOption) throws AzureOauthTokenFetchingException {
        int bound = productDataObjects.size();
   try {
       for (ProductDataObject productDataObject : productDataObjects) {
           boolean found = false;
           int from;
           int size;
           from = 0;
           size = 50;
           ProductsMatchedApiObject returnedProducts;
           switch (searchOption) {
               case "PRODUCT_ID":
                   returnedProducts = searchForProductsBySearchResult(productDataObject.getPRODUCT_ID());
                   break;
               case "PRODUCT_TITLE":
                   returnedProducts = searchForProductsBySearchResult(productDataObject.getPRODUCT_NAME());
                   break;
               case "PRODUCT_IDENTIFIER":
                   //hard coded by Nishant @ 8 may 2020 as there is only few record in SIT gd_product_identifier table
                   //     getRandomeProductIdWithIdentifier();
                   //     getProductsDataFromEPHGD();
                   getProductIdentifiers(productDataObjects.get(0).getPRODUCT_ID());
                   returnedProducts = searchForProductsBySearchResult(productIdentifiers.get(0).getIDENTIFIER());
                   break;

               case "PRODUCT_WORK_ID":
                   returnedProducts = searchForProductsBySearchResult(productDataObjects.get(0).getF_PRODUCT_WORK());
                   break;
               case "PRODUCT_WORK_TITLE":
                   getWorksDataFromEPHGD(productDataObjects.get(0).getF_PRODUCT_WORK());
                   returnedProducts = searchForProductsBySearchResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE() + "&from=" + from + "&size=" + size);
                   Log.info("Total product found for workTitle search... - " + returnedProducts.getTotalMatchCount());
                   while (!returnedProducts.verifyProductWithIdIsReturnedOnly(productDataObjects.get(0).getPRODUCT_ID()) && from + size < returnedProducts.getTotalMatchCount()) {
                       from += size;
                       Log.info("scanned productID from " + (from - size) + " to " + from + " records...");
                       returnedProducts = searchForProductsBySearchResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE() + "&from=" + from + "&size=" + size);
                   }
                   break;
               case "PRODUCT_WORK_IDENTIFIER":
                   getWorkIdentifiers(productDataObjects.get(0).getF_PRODUCT_WORK());
                   returnedProducts = searchForProductsBySearchResult(workIdentifiers.get(0).getIDENTIFIER());
                   break;

               case "PRODUCT_MANIFESTATION_ID":
                   returnedProducts = searchForProductsBySearchResult(String.valueOf(productDataObject.getF_PRODUCT_MANIFESTATION_TYP()));
                   break;
               case "PRODUCT_MANIFESTATION_TITLE":
                   getManifestationByID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                   returnedProducts = searchForProductsBySearchResult(manifestationDataObjects.get(0).getMANIFESTATION_KEY_TITLE() + "&from=" + from + "&size=" + size);
                   Log.info("Total product found for manifestationTitle search... - " + returnedProducts.getTotalMatchCount());
                   while (!returnedProducts.verifyProductWithIdIsReturnedOnly(productDataObjects.get(0).getPRODUCT_ID()) && from + size < returnedProducts.getTotalMatchCount()) {
                       from += size;
                       Log.info("scanned productID from " + (from - size) + " to " + from + " records...");
                       returnedProducts = searchForProductsBySearchResult(manifestationDataObjects.get(0).getMANIFESTATION_KEY_TITLE() + "&from=" + from + "&size=" + size);
                   }
                   break;
               case "PRODUCT_MANIFESTATION_IDENTIFIER":
                   List<Map<String, Object>> manifestationIdentifiers = getManifestationIdentifierByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                   returnedProducts = searchForProductsBySearchResult(manifestationIdentifiers.get(0).get("identifier").toString());
                   break;

               case "PRODUCT_MANIFESTATION_WORK_ID":
                   getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                   returnedProducts = searchForProductsBySearchResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                   break;
               case "PRODUCT_MANIFESTATION_WORK_TITLE":
                   getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                   returnedProducts = searchForProductsBySearchResult(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
                   break;
               case "PRODUCT_MANIFESTATION_WORK_IDENTIFIER":
                   getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                   getWorkIdentifiers(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                   returnedProducts = searchForProductsBySearchResult(workIdentifiers.get(0).getIDENTIFIER());
                   break;

               case "PRODUCT_PERSONS_FULLNAME":
                     /*
                        created by Nishant @8 May 2020
                        getProuctByPerson returns sometimes 70000+ records and most probable intended product id does not appear in first 20 records
                        hence we need to send API request with size 5000 and check if intended workID is returned
                        if not, send request again for next 5000 records until product id found
                        */
                   getProductPersonRoleByProductId(productDataObjects.get(0).getPRODUCT_ID());
                   getPersonDataByPersonId(personProductRoleDataObjectsFromEPHGD.get(0).getF_PERSON());
                   //        from = 0;       size = 50;
                   returnedProducts = searchForProductsBySearchResult(personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " + personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);
                   Log.info("Total product found for product person full name... - " + returnedProducts.getTotalMatchCount());
                   while (!returnedProducts.verifyProductWithIdIsReturnedOnly(productDataObjects.get(0).getPRODUCT_ID()) && from + size < returnedProducts.getTotalMatchCount()) {
                       from += size;
                       Log.info("scanned productID from " + (from - size) + " to " + from + " records...");
                       returnedProducts = searchForProductsBySearchResult(personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " + personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);
                   }
                   break;
               case "PRODUCT_WORK_PERSONS_FULLNAME":
                   getWorkPersonRoleByWorkId(productDataObjects.get(0).getF_PRODUCT_WORK());
                   getPersonDataByPersonId(personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON());
                   //        from = 0;        size = 50;
                   returnedProducts = searchForProductsBySearchResult(personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " + personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);
                   Log.info("Total product found for product work person full name... - " + returnedProducts.getTotalMatchCount());
                   while (!returnedProducts.verifyProductWithIdIsReturnedOnly(productDataObjects.get(0).getPRODUCT_ID()) && from + size < returnedProducts.getTotalMatchCount()) {
                       from += size;
                       Log.info("scanned productID from " + (from - size) + " to " + from + " records...");
                       returnedProducts = searchForProductsBySearchResult(personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " + personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);
                   }
                   break;
               case "PRODUCT_MANIFESTATION_WORK_PERSONS_FULLNAME":
                   getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                   getWorkPersonRoleByWorkId(dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                   getPersonDataByPersonId(personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON());
                   //    from = 0; size = 50;
                   returnedProducts = searchForProductsBySearchResult(personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " + personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);
                   Log.info("Total product found for product manifestation work person full name... - " + returnedProducts.getTotalMatchCount());
                   while (!returnedProducts.verifyProductWithIdIsReturnedOnly(productDataObjects.get(0).getPRODUCT_ID()) && from + size < returnedProducts.getTotalMatchCount()) {
                       from += size;
                       Log.info("scanned productID from " + (from - size) + " to " + from + " records...");
                       returnedProducts = searchForProductsBySearchResult(personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " + personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() + "&from=" + from + "&size=" + size);
                   }
                   break;
               default:
                   throw new IllegalStateException("Unexpected value: " + searchOption);
           }

           returnedProducts.verifyProductsAreReturned();
           returnedProducts.verifyProductWithIdIsReturned(productDataObject.getPRODUCT_ID());
       }
   }
   catch(Exception e)
   {
       Assert.assertFalse(DataQualityContext.breadcrumbMessage+" "+e.getMessage(),true);
   }
    }

    @When("^the product response returned when searched by personID is verified$")
    public void compareProductsRetrievdByPersonWithDB() throws AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts;
        int bound = ids.size();
        for (String id : ids) {
            returnedProducts = searchForProductsByPersonIDResult(id);
            Log.info("PersonID chosen : " + id);
            Log.info("Product count in API : " + returnedProducts.getTotalMatchCount());
            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfProductsByPersonIDs(id));
        }
    }

    @When("^the product details are retrieved by PMC Code and compared$")
    public void compareProductSearchByPMCResultsWithDB() throws AzureOauthTokenFetchingException {
        ProductsMatchedApiObject returnedProducts;
        int bound = productDataObjects.size();
        for (ProductDataObject productDataObject : productDataObjects) {
            getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
            String pmcCode = dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC();
            Log.info("pmcCode to be tested: " + pmcCode);
            returnedProducts = searchForProductsByPMCResult(pmcCode);

            returnedProducts.verifyProductsAreReturned();
            returnedProducts.verifyProductsReturned(getNumberOfProductsByPMC(pmcCode));
        }
    }

    @When("^the product details are retrieved by PMG Code and compared$")
    public void compareProductSearchByPMGResultsWithDB() throws AzureOauthTokenFetchingException {
        //update by Nishant @ 14 May 2020
        ProductsMatchedApiObject returnedProducts ;
        int bound = productDataObjects.size();
        for (ProductDataObject productDataObject : productDataObjects) {
            getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
            String pmgCode = getPMGcodeByPMC(dataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
            // pmgCode="736";//for debugging
            Log.info("pmgCode to be tested: " + pmgCode);
            returnedProducts = searchForProductsByPMGResult(pmgCode);
            Log.info("Total count by API..." + returnedProducts.getTotalMatchCount());
            returnedProducts.verifyProductsAreReturned(); //non zero products
            returnedProducts.verifyProductsReturned(getProductsCountByPMG(pmgCode));
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

    private List getManifestationIdentifierByManifestationID(String manifestationID) {
        List<String> manIds = new ArrayList<>();
        manIds.add(manifestationID);
        sql = String.format(APIDataSQL.SELECT_GD_MANIFESTATION_IDENTIFIER_BY_MANIFESTATION_ID,Joiner.on("','").join(manIds));
        return DBManager.getDBResultMap(sql, Constants.EPH_URL);
    }

    private void getWorkByManifestationID(String manifestationID) {
        List<String> manIds = new ArrayList<>();
        manIds.add(manifestationID);
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH_BY_MANIFESTATIONID,
                Joiner.on("','").join(manIds));
        //  Log.info(sql);

        dataQualityContext.workDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    private void getWorkIdentifiers(String workID) {
        sql = APIDataSQL.getWorkIdentifiersDataFromGD.replace("PARAM1", workID);
        workIdentifiers = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    private void getProductIdentifiers(String productID) {
        sql = APIDataSQL.getProductIdentifiersDataFromGD.replace("PARAM1", productID);
        productIdentifiers = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    }

    private void getManifestationByID(String manifestationID) {
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
        return ((String) getPMG.get(0).get("f_pmg"));
    }

    public String getProductPackageByID(String id) {
        sql = String.format(APIDataSQL.EPH_GD_PACKAGEID_EXTRACT_BY_PRODUCTID, id);
        Log.info(sql);
        List<Map<String, Object>> getPackage = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        return ((String) getPackage.get(0).get("f_package_owner"));
    }

    private int getNumberOfProductsByPersonIDs(String personID) {
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
        return ((Long) countPMG.get(0).get("count")).intValue();
    }

    private void getWorksByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.SELECT_WORKS_BY_PMC_CODE, pmcCode);
        //   Log.info(sql);
        List<Map<?, ?>> workIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = workIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" Verify That list with work ids by pmc is not empty.", ids.isEmpty());
    }

    public void getPMGWorksByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.EPH_GD_PMG_CODE_WORKS_EXTRACT_BY_PMC, pmcCode);
        List<Map<?, ?>> workIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = workIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" Verify That list with work ids by pmc is not empty.", ids.isEmpty());
    }

    private void getManifestationsByWorks() {
        sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_IDS_BY_WORKS, Joiner.on("','").join(ids));
        // Log.info(sql);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        manifestationIds = randomProductSearchIds.stream().map(m -> (String) m.get("MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" Verify That list with manifestation ids by work ids is not empty.", ids.isEmpty());
    }

    private int getProductsCountByManifestations() {
        sql = String.format(APIDataSQL.SELECT_COUNT_PRODUCTS_BY_MANIFESTATIONS, Joiner.on("','").join(manifestationIds));
        //   Log.info(sql);
        List<Map<String, Object>> productsCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        return ((Long) productsCount.get(0).get("count")).intValue();
    }

    private int getProductsCountByWork() {//created by Nishant @ 25 Nov 2019
        sql = String.format(APIDataSQL.SELECT_COUNT_PRODUCTS_BY_WORK, Joiner.on("','").join(ids));
        //   Log.info(sql);
        List<Map<String, Object>> productsCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        return ((Long) productsCount.get(0).get("count")).intValue();
    }

    private int getNumberOfProductsByPMC(String pmcCode) {
        getWorksByPMC(pmcCode);
        getManifestationsByWorks();
        int count = getProductsCountByManifestations() +  getProductsCountByWork();
        Log.info("products count by workType in DB is: "+count);
        return count;
    }

    private int getProductsCountByPMG(String pmgCode) {return getProductCountByPMGCode(pmgCode);}

    private String getPMGcodeByPMC(String pmcCode) {
        sql = String.format(APIDataSQL.EPH_GD_PMG_CODE_EXTRACT_BYPMC, pmcCode);
        //  Log.info(sql);
        List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        return ((String) getPMG.get(0).get("f_pmg"));
    }

    //created by Nishant @ 24 Apr 2020
    private void getWorkPersonRoleByWorkId(String workId) {
        Log.info("get person role by Work id..."+workId);
        sql = String.format(APIDataSQL.selectWorkPersonByworkId,workId);
        personWorkRoleDataObjectsFromEPHGD=DBManager.getDBResultAsBeanList(sql,PersonWorkRoleDataObject.class,Constants.EPH_URL);
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" Verify person role by work id successfully extracted from EPH DB", personWorkRoleDataObjectsFromEPHGD.isEmpty());
    }

    //created by Nishant @ 8 May 2020
    private void getProductPersonRoleByProductId(String productId){
        Log.info("get person role by product id..."+productId);
        sql = String.format(PersonProductRoleDataSQL.selectProductPersonByproductId,productId);
        personProductRoleDataObjectsFromEPHGD=DBManager.getDBResultAsBeanList(sql,PersonProductRoleDataObject.class,Constants.EPH_URL);
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" Verify person role by product id successfully extracted from EPH DB", personProductRoleDataObjectsFromEPHGD.isEmpty());
    }

    private void getPersonDataByPersonId(String personId){
        Log.info("get person data by person id..."+personId);
        sql =String.format(APIDataSQL.SelectPersonDataByPersonId,personId);
        personDataObjectsFromEPHGD=DBManager.getDBResultAsBeanList(sql,PersonDataObject.class,Constants.EPH_URL);
        Assert.assertFalse(DataQualityContext.breadcrumbMessage+" verify person Data by person id extracted from EPH DB",personDataObjectsFromEPHGD.isEmpty());
    }


    @Then("^the product count are retrieved by (.*) compared$")
    public void theProductDetailsAreRetrievedByParamKeyAndCompared(String paramKey) throws Throwable {
        Log.info("Automation is in progress");
        ProductsMatchedApiObject returnedProducts;
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





    }
}