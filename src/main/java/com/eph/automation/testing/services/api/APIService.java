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

//    private static Object ProductApiObject;

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

    public static WorkApiObject searchForWorkByIDResult(String workID) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/work/" + workID)
                .thenReturn().as(WorkApiObject.class);

    }

    public static WorksMatchedApiObject searchForWorkByPMCResult(String pmcCode) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/works/pmcCode/" + pmcCode)
                .thenReturn().as(WorksMatchedApiObject.class);

    }

    public static ProductsMatchedApiObject searchForProductsByPMCResult(String pmcCode) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/products/pmcCode/" + pmcCode)
                .thenReturn().as(ProductsMatchedApiObject.class);

    }

    public static WorksMatchedApiObject searchForWorkByPMGResult(String pmgCode) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/works/pmgCode/" + pmgCode)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByPMGResult(String pmgCode) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/works/pmgCode/" + pmgCode)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorkByTitleResult(String title) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/works/title/?title="+title)
                .thenReturn().as(WorksMatchedApiObject.class);

    }

    public static WorksMatchedApiObject searchForWorksByIdentifierResult(String identifier) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/works/identifier/"+identifier)
                .thenReturn().as(WorksMatchedApiObject.class);

    }

    public static WorksMatchedApiObject searchForWorksByIdentifierAndTypeResult(String identifier, String identifierType) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .param("identifierType", identifierType)
                .when()
                .get("/works/identifier/"+identifier).thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksByPersonIDResult(String identifier) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/works/personId/"+identifier)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByPersonIDResult(String identifier) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/products/personId/"+identifier)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksBySearchOptionResult(String searchFor) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/works/search?query="+searchFor)
                .thenReturn().as(WorksMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByPackageResult(String packageID) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/products/isInPackages/"+packageID)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByComponentsResult(String componentID) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/products/hasComponents?ids="+componentID)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductApiObject searchForProductResult(String productID) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/product/" + productID)
                .thenReturn().as(ProductApiObject.class);

    }

    public static ProductsMatchedApiObject searchForProductsByIdentifierResult(String identifier) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/products/identifier/"+identifier)
                .thenReturn().as(ProductsMatchedApiObject.class);

    }

    public static ProductsMatchedApiObject searchForProductssByIdentifierAndTypeResult(String identifier, String identifierType) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .param("identifierType", identifierType)
                .when()
                .get("/products/identifier/"+identifier)
                .thenReturn().as(ProductsMatchedApiObject.class);

    }

    public static ProductsMatchedApiObject searchForProductsBySearchResult(String searchOption) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/products/search?query="+searchOption)
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByTitleResult(String title) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .param("name", title)
                .when()
                .get("/products/name/")
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static boolean checkProductExists(String productID) {
        int statusCode = given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/product/" + productID)
                .thenReturn().statusCode();

        if(statusCode==200){
            return true;
        } else {
            return false;
        }

    }

    public static ProductsMatchedApiObject searchForProductByTitleResult(String title) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/products/title?title="+title)
                .thenReturn().as(ProductsMatchedApiObject.class);

    }

    public static Boolean checkWorkExists(String workID) {

        int statusCode = given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/work/" + workID)
                .thenReturn().statusCode();

        if(statusCode==200){
            return true;
        } else {
            return false;
        }
    }

}
