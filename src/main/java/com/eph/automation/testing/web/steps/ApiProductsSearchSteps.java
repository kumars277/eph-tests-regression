package com.eph.automation.testing.web.steps;

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
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;

import java.io.IOException;
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
    private static ProductsMatchedApiObject returnedProducts = null;
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
            int code = checkProductExists(productDataObjects.get(i).getPRODUCT_ID());
            if (code == 200) {
                System.out.println(i + ") Checking: " +productDataObjects.get(i).getPRODUCT_ID());

                response = searchForProductResult(productDataObjects.get(i).getPRODUCT_ID());

            }
        }
    }

    @When("^the product details are retrieved when searched by (.*) and compared$")
    public void productSearchByTitleAndCompare(String title) throws IOException {
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
            verifyResponseNotEmpty();
            returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
        }
    }

    @When("^the product details are retrieved and compared when searched by (.*)$")
    public void productSearchByIdentifiersAndCompare(String identifierType) throws IOException {
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
            verifyResponseNotEmpty();
            returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
        }
    }

    @When("^the product details are retrieved and compared when search option is used with (.*)$")
    public void productSearchBySearchOptionAndCompare(String identifierType) throws IOException {
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
            }
            verifyResponseNotEmpty();
            returnedProducts.verifyProductWithIdIsReturned(productDataObjects.get(i).getPRODUCT_ID());
        }
    }

    @When("^the products detail are retrieved and compared when searched by type and (.*)$")
    public void productSearchByIdentifierWithTypeAndCompare(String identifierType) throws IOException {
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
            verifyResponseNotEmpty();
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

    public void verifyResponseNotEmpty(){
        returnedProducts.verifyProductsAreReturned();
    }
}