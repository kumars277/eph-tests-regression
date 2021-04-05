package com.eph.automation.testing.services.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant in Apr-May 2020 for data model changes
 *
 */

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.RESTEndPoints;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.api.ProductApiObject;
import com.eph.automation.testing.models.api.ProductsMatchedApiObject;
import com.eph.automation.testing.models.api.WorkApiObject;
import com.eph.automation.testing.models.api.WorksMatchedApiObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;

import java.util.concurrent.ThreadLocalRandom;

import static com.eph.automation.testing.configuration.Constants.*;
import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;


public class APIService {
    private static String SearchAPI_EndPoint;

    public APIService()
    {
        if(TestContext.getValues().environment.equalsIgnoreCase("SIT"))this.SearchAPI_EndPoint=Constants.PRODUCT_SEARCH_END_POINT_SIT;
        else this.SearchAPI_EndPoint=Constants.PRODUCT_SEARCH_END_POINT_UAT;
    }

    private static RequestSpecification request;

    public static Response getEIPNotificationEndpointResponse() {return get(Constants.EIP_NOTIFICATION_WADL_END_POINT_SIT);}

    public static Response getPPM_ChangeHistory_APIResponse(String dateFrom, String dateTo) {
        return given()
                .header("content-type", "application/json")
                .header("Authorization", RESTEndPoints.Headers.Authorization.toString())
                .baseUri(RESTEndPoints.PPM_CDS_API.PROD.toString())
                .when()
                .get("/changeHistory/metalog/"+dateFrom+"&"+dateTo+"/page_limit=1500&p_from=0")
                .thenReturn();
    }

    public static int getRandomNumForGivenRange(int min,int max) {return ThreadLocalRandom.current().nextInt(min, max + 1);}

    public static Response getPPM_SearchProjectResponse(String projectNumber) {
        return given()
                .header("content-type", "application/json")
                .header("Authorization", RESTEndPoints.Headers.Authorization.toString())
                .baseUri(RESTEndPoints.PPM_CDS_API.UAT.toString())
                .when()
                .get("/search/projno/" + projectNumber)
                .thenReturn();
    }

    //by Nishant - updated for search API v2 - complete as of 27 Nov 2019
    //updated by Nishant @ 28 Apr 2020 for data model changes
    public static boolean checkProductExists(String productID) throws AzureOauthTokenFetchingException {
        int statusCode = given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-products/products/" + productID)
                .thenReturn().statusCode();

        if(statusCode==200){return true;} else {return false;}
    }

    public static ProductApiObject searchForProductResult(String productID) throws AzureOauthTokenFetchingException {
       //updated by Nishant @ 28 Apr 2020
        Response response=given()
               .baseUri(SearchAPI_EndPoint)
               .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
               .when()
               .get("/product-hub-products/products/" + productID);
       response.prettyPrint();
        return  response.thenReturn().as(ProductApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByTitleResult(String title) throws AzureOauthTokenFetchingException {
        //updated by Nishant @ 4 May 2020
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .param("queryType","name")
                .param("queryValue", title)
                .when()
                .get("/product-hub-products/products")
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByIdentifierResult(String identifier) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-products/products?queryType=identifier&queryValue="+identifier)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductssByIdentifierAndTypeResult(String identifier, String identifierType) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .param("identifierType", identifierType).when()
                .get("/product-hub-products/products?queryType=identifier&queryValue="+identifier)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByPackageResult(String packageID) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-products/products?queryType=isInPackages&queryValue="+packageID)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByPersonIDResult(String personId) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-products/products?queryType=personId&queryValue="+personId)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByPMCResult(String pmcCode) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-products/products?queryType=pmcCode&queryValue=" + pmcCode)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByPMGResult(String pmgCode) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-products/products?queryType=pmgCode&queryValue=" + pmgCode)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByComponentsResult(String componentID) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-products/products?queryType=HasComponents&queryValue="+componentID)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsBySearchResult(String searchOption) throws AzureOauthTokenFetchingException {
       Response response =given()
               .baseUri(SearchAPI_EndPoint)
               .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
               .when()
               .get("/product-hub-products/products?queryType=search&queryValue="+searchOption);
       response.prettyPrint();
       return response.thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByaccountableProduct(String accountableProduct) throws AzureOauthTokenFetchingException {
        //created by Nishant as per search API v2 changes
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-products/products?queryType=accountableProduct&queryValue=" + accountableProduct)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductByParam(String searchTerm, String ParamKey,String ParamValue) throws AzureOauthTokenFetchingException {
        /*
        //created by Nishant as per search API v2 changes
       function supports below
       query type*      : name, identifier, personId, isInPackages, hasComponents, pmgCode, pmcCode, accountableProduct, latest, search
       productStatus    : PAS,PNS or PPL etc
       productType      : JAS,JBS etc
       manifestationType: JEL,JPR or !PHB
       workType         : !RBK,BKS or JNL
       pmcCode          : 300,303 or 746
       pmgCode          : 030,090 or 077
       */
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .param("queryType","name")
                .param("queryValue",searchTerm)
                .param(ParamKey,ParamValue)
                .when()
                .get("/product-hub-products/products")
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    //getWorks APIs
    public static Boolean checkWorkExists(String workID) throws AzureOauthTokenFetchingException {
        int statusCode = given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works/" + workID)
                .thenReturn().statusCode();
        if (statusCode == 200) {return true;} else {return false;}
    }

    public static WorkApiObject searchForWorkByIDResult(String workID) throws AzureOauthTokenFetchingException {
        //updated by Nishant @ 30 Mar 2020 as per latest API changes
         Response getWorkResponse = given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works/" + workID);

        Assert.assertTrue("Verify that the searched work exists and is accessible trough the API",getWorkResponse.statusCode()==200);

        //Log.info("print response start#######################");
         getWorkResponse.prettyPrint();
        // Log.info("print response end ########################");

        return getWorkResponse.thenReturn().as(WorkApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorkByTitleResult(String title) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType=title&queryValue="+title)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByIdentifierResult(String identifier) throws AzureOauthTokenFetchingException {
       Response response=
         given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType=identifier&queryValue="+identifier);
       response.prettyPrint();
       return response.thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByIdentifierAndTypeResult(String identifier, String identifierType) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
                .param("identifierType", identifierType)
                .when()
                .get("/product-hub-works/works?queryType=identifier&queryValue=" + identifier+"&identifierType="+identifierType)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksBySearchOptionResult(String searchFor) throws AzureOauthTokenFetchingException {

     Response response = given()
             .baseUri(SearchAPI_EndPoint)
             .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
             .when()
             .get("/product-hub-works/works?queryType=search&queryValue="+searchFor);
        response.prettyPrint();
        return response.thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByQueryTypeResult(String queryType, String queryValue) throws AzureOauthTokenFetchingException {

        Response response = given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType="+queryType+"&queryValue="+queryValue);
         //response.prettyPrint();

        return response.thenReturn().as(WorksMatchedApiObject.class);
    }


    public static WorksMatchedApiObject searchForWorkByPMCResult(String pmcCode) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType=pmcCode&queryValue=" + pmcCode)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorkByPMGResult(String pmgCode) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType=pmgCode&queryValue=" + pmgCode)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByPersonIDResult(String identifier) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType=personId&queryValue="+identifier)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByaccountableProduct(String accountableProduct) throws AzureOauthTokenFetchingException {
        //created by Nishant as per search API v2 changes
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType=accountableProduct&queryValue=" + accountableProduct)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByWorkStatus(String searchKeyword, String workStatus) throws AzureOauthTokenFetchingException {
        //created by Nishant as per search API v2 changes
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType=title&queryValue="+searchKeyword+"&workStatus=" + workStatus)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByWorkType(String searchKeyword,String workType) throws AzureOauthTokenFetchingException {
        //created by Nishant as per search API v2 changes
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType=title&queryValue="+searchKeyword+"&workType=" + workType)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByManifestationType(String searchKeyword,String manifestationType) throws AzureOauthTokenFetchingException {
        //created by Nishant as per search API v2 changes
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType=search&queryValue="+searchKeyword+"&manifestationType=" + manifestationType)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksBySearchWithPMCCode(String searchKeyword,String pmcCode) throws AzureOauthTokenFetchingException {
        //created by Nishant as per search API v2 changes
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType=title&queryValue="+searchKeyword+"&pmcCode=" + pmcCode)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksBySearchWithPMGCode(String searchKeyword,String pmgCode) throws AzureOauthTokenFetchingException {
        //created by Nishant as per search API v2 changes
        return given()
                .baseUri(SearchAPI_EndPoint)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/product-hub-works/works?queryType=search&queryValue="+searchKeyword+"&pmgCode=" + pmgCode)
                .thenReturn().as(WorksMatchedApiObject.class);
    }


}
