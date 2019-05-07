package com.eph.automation.testing.services.api;

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
import static org.hamcrest.core.IsCollectionContaining.hasItems;


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

    public static WorksMatchedApiObject searchForWorkByTitleResult(String title) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .param("title", title)
                .when()
                .get("/works")
                .thenReturn().as(WorksMatchedApiObject.class);

    }

    public static WorksMatchedApiObject searchForWorksByIdentifierResult(String identifier) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .param("identifier", identifier)
                .when()
                .get("/works")
                .thenReturn().as(WorksMatchedApiObject.class);

    }

    public static WorksMatchedApiObject searchForWorksByIdentifierAndTypeResult(String identifier, String identifierType) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .param("identifier", identifier)
                .param("identifierType", identifierType)
                .when()
                .get("/works").thenReturn().as(WorksMatchedApiObject.class);
    }

    public static WorksMatchedApiObject searchForWorksBySearchOptionResult(String searchFor) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .param("search", searchFor)
                .when()
                .get("/works")
                .thenReturn().as(WorksMatchedApiObject.class);

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
                .param("identifier", identifier)
                .when()
                .get("/products")
                .thenReturn().as(ProductsMatchedApiObject.class);

    }

    public static ProductsMatchedApiObject searchForProductssByIdentifierAndTypeResult(String identifier, String identifierType) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .param("identifier", identifier)
                .param("identifierType", identifierType)
                .when()
                .get("/products")
                .thenReturn().as(ProductsMatchedApiObject.class);

    }

    public static ProductsMatchedApiObject searchForProductsBySearchResult(String searchOption) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .param("search", searchOption)
                .when()
                .get("/products")
                .thenReturn().as(ProductsMatchedApiObject.class);
    }

    public static ProductsMatchedApiObject searchForProductsByTitleResult(String title) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .param("title", title)
                .when()
                .get("/products")
                .thenReturn().as(ProductsMatchedApiObject.class);

    }

    public static int checkProductExists(String productID) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .when()
                .get("/product/" + productID)
                .thenReturn().statusCode();

    }

    public static ProductsMatchedApiObject searchForProductByTitleResult(String title) {
        return given()
                .baseUri(PRODUCT_SEARCH_END_POINT_SIT)
                .header(Constants.AUTHORIZATION_HEADER, Constants.SIT_GATEWAY_AUTHORIZATION_HEADER + AuthorizationService.getAuthToken())
                .param("title", title)
                .when()
                .get("/products")
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
