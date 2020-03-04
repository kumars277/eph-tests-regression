package com.eph.automation.testing.services.api;
/**
 * Created by GVLAYKOV
 */

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.RESTEndPoints;
import com.eph.automation.testing.models.api.ProductApiObject;
import com.eph.automation.testing.models.api.ProductsMatchedApiObject;
import com.eph.automation.testing.models.api.WorkApiObject;
import com.eph.automation.testing.models.api.WorksMatchedApiObject;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;

import java.util.concurrent.ThreadLocalRandom;

import static com.eph.automation.testing.configuration.Constants.PRODUCT_SEARCH_END_POINT_SIT;
import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;


public class APIService {

    private static RequestSpecification request;

    public static Response getEIPNotificationEndpointResponse() {
        return get(Constants.EIP_NOTIFICATION_WADL_END_POINT_SIT);
    }

    public static Response getPPM_ChangeHistory_APIResponse(String dateFrom, String dateTo) {
        return given()
                .header("content-type", "application/json")
                .header("Authorization", RESTEndPoints.Headers.Authorization.toString())
                .baseUri(RESTEndPoints.PPM_CDS_API.PROD.toString())
                .when()
                .get("/changeHistory/metalog/"+dateFrom+"&"+dateTo+"/page_limit=1500&p_from=0")
                .thenReturn();
    }

    public static int getRandomNumForGivenRange(int min,int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

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
    public static boolean checkProductExists(String productID) throws AzureOauthTokenFetchingException {
        int statusCode = given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/products/" + productID)
                .thenReturn().statusCode();

        if(statusCode==200){return true;} else {return false;}
    }

    public static ProductApiObject searchForProductResult(String productID) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/products/" + productID)
                .thenReturn().as(ProductApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByIdentifierResult(String identifier) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/products?queryType=identifier&queryValue="+identifier)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductssByIdentifierAndTypeResult(String identifier, String identifierType) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .param("identifierType", identifierType)
                .when()
                .get("/products?queryType=identifier&queryValue="+identifier)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByPackageResult(String packageID) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/products?queryType=isInPackages&queryValue="+packageID)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByPersonIDResult(String personId) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/products?queryType=personId&queryValue="+personId)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByPMCResult(String pmcCode) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/products?queryType=pmcCode&queryValue=" + pmcCode)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByPMGResult(String pmgCode) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/products?queryType=pmgCode&queryValue=" + pmgCode)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByComponentsResult(String componentID) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/products?queryType=HasComponents&queryValue="+componentID)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    //getWorks APIs
    public static Boolean checkWorkExists(String workID) throws AzureOauthTokenFetchingException {
        int statusCode = given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works/" + workID)
                .thenReturn().statusCode();
        if (statusCode == 200) {
            return true;
        } else {
            return false;
        }
    }

    public static WorkApiObject searchForWorkByIDResult(String workID) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works/" + workID)
                .thenReturn().as(WorkApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorkByTitleResult(String title) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=title&queryValue="+title)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByIdentifierResult(String identifier) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=identifier&queryValue="+identifier)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByIdentifierAndTypeResult(String identifier, String identifierType) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
                .param("identifierType", identifierType)
                .when()
                .get("/works?queryType=identifier&queryValue=" + identifier).thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksBySearchOptionResult(String searchFor) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=search&queryValue="+searchFor)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorkByPMCResult(String pmcCode) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=pmcCode&queryValue=" + pmcCode)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorkByPMGResult(String pmgCode) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=pmgCode&queryValue=" + pmgCode)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByPersonIDResult(String identifier) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=personId&queryValue="+identifier)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsBySearchResult(String searchOption) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .when()
                .get("products?queryType=search&queryValue="+searchOption)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByTitleResult(String title) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .param("queryType","name")
                .param("queryValue", title)
                .when()
                .get("/products")
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    //created by Nishant as per search API v2 changes
    public static ProductsMatchedApiObject searchForProductsByaccountableProduct(String accountableProduct) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/products?queryType=accountableProduct&queryValue=" + accountableProduct)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductByParam(String searchTerm, String ParamKey,String ParamValue) throws AzureOauthTokenFetchingException {
        /*
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
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,  AuthorizationService.getAuthToken().getToken())
                .param("queryType","name")
                .param("queryValue",searchTerm)
                .param(ParamKey,ParamValue)
                .when()
                .get("products")
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByaccountableProduct(String accountableProduct) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=accountableProduct&queryValue=" + accountableProduct)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByWorkStatus(String searchKeyword, String workStatus) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=title&queryValue="+searchKeyword+"&workStatus=" + workStatus)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByWorkType(String searchKeyword,String workType) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=title&queryValue="+searchKeyword+"&workType=" + workType)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByManifestationType(String searchKeyword,String manifestationType) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=search&queryValue="+searchKeyword+"&manifestationType=" + manifestationType)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksBySearchWithPMCCode(String searchKeyword,String pmcCode) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=title&queryValue="+searchKeyword+"&pmcCode=" + pmcCode)
                .thenReturn().as(WorksMatchedApiObject.class);
    }


    public static WorksMatchedApiObject searchForWorksBySearchWithPMGCode(String searchKeyword,String pmgCode) throws AzureOauthTokenFetchingException {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER,   AuthorizationService.getAuthToken().getToken())
                .when()
                .get("/works?queryType=search&queryValue="+searchKeyword+"&pmgCode=" + pmgCode)
                .thenReturn().as(WorksMatchedApiObject.class);
    }


}
