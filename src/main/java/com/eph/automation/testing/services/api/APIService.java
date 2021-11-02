package com.eph.automation.testing.services.api;
/** Created by GVLAYKOV updated by Nishant in Apr-May 2020 for data model changes */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.RESTEndPoints;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.api.ProductApiObject;
import com.eph.automation.testing.models.api.ProductsMatchedApiObject;
import com.eph.automation.testing.models.api.WorkApiObject;
import com.eph.automation.testing.models.api.WorksMatchedApiObject;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.jayway.restassured.internal.mapper.ObjectMapperType;
import com.jayway.restassured.response.Response;
import org.junit.Assert;
import java.util.concurrent.ThreadLocalRandom;
import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;

public class APIService {
  private static String searchAPIEndPoint;
  private static String responseCodeMessage = "API response code ";
  private static String worksBySearchResource =
      "/product-hub-works/works?queryType=search&queryValue=";
  private static String worksByTitleResource =
      "/product-hub-works/works?queryType=title&queryValue=";

  public APIService() {setApiEndpoint(); }


  // by Nishant - updated for search API v2 - complete as of 27 Nov 2019
  // updated by Nishant @ 28 Apr 2020 for data model changes

  // getProducts APIs

  public static int checkProductExists(String productID) throws AzureOauthTokenFetchingException {
    return given()
        .baseUri(searchAPIEndPoint)
        .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
        .when()
        .get("/product-hub-products/products/" + productID)
        .thenReturn()
        .statusCode();
  }

  public static ProductApiObject searchForProductResult(String productID)
      throws AzureOauthTokenFetchingException {
    // updated by Nishant @ 28 Apr 2020
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-products/products/" + productID);

    DataQualityContext.api_response = response;
 //   DataQualityContext.api_response.prettyPrint();
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(ProductApiObject.class);
  }

  public static ProductsMatchedApiObject searchForProductsByTitleResult(String title)
      throws AzureOauthTokenFetchingException {
    // updated by Nishant @ 4 May 2020
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-products/products?queryType=name&queryValue=" + title);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject searchForProductsByIdentifierResult(String identifier)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-products/products?queryType=identifier&queryValue=" + identifier);
    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject searchForProductssByIdentifierAndTypeResult(
      String identifier, String identifierType) throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .param("identifierType", identifierType)
            .when()
            .get("/product-hub-products/products?queryType=identifier&queryValue=" + identifier);
    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject searchForProductsByPackageResult(String packageID)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-products/products?queryType=isInPackages&queryValue=" + packageID);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject searchForProductsByComponentsResult(String componentID)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get(
                "/product-hub-products/products?queryType=HasComponents&queryValue=" + componentID);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject searchForProductsByaccountableProduct(
      String accountableProduct) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get(
                "/product-hub-products/products?queryType=accountableProduct&queryValue="
                    + accountableProduct);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject searchForProductsByPersonIDResult(String personId)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-products/products?queryType=personId&queryValue=" + personId);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject searchForProductsByPMCResult(String pmcCode)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-products/products?queryType=pmcCode&queryValue=" + pmcCode);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject searchForProductsByPMGResult(String pmgCode)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-products/products?queryType=pmgCode&queryValue=" + pmgCode);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject searchForProductsBySearchResult(String searchOption)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-products/products?queryType=search&queryValue=" + searchOption);

    DataQualityContext.api_response = response;

    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    // updates by Nishant to fix escaped char in json response issue
    return response.thenReturn().as(ProductsMatchedApiObject.class, ObjectMapperType.JACKSON_2);
  }

  public static ProductsMatchedApiObject searchForProductByParam(
      String searchTerm, String paramKey, String paramValue)
      throws AzureOauthTokenFetchingException {
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

    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .param(paramKey, paramValue)
            .param("queryType", "name")
            .param("queryValue", searchTerm)
            .when()
            .get("/product-hub-products/products");

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  // getWorks APIs
  public static int checkWorkExists(String workID) throws AzureOauthTokenFetchingException {
    return given()
        .baseUri(searchAPIEndPoint)
        .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
        .when()
        .get("/product-hub-works/works/" + workID)
        .thenReturn()
        .statusCode();
  }

  public static WorkApiObject searchForWorkByIDResult(String workID)
      throws AzureOauthTokenFetchingException {
    // updated by Nishant @ 30 Mar 2020 as per latest API changes
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-works/works/" + workID);

    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    DataQualityContext.api_response = response;
    return response.thenReturn().as(WorkApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorkByTitleResult(String title)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get(worksByTitleResource + title);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksByIdentifierResult(String identifier)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-works/works?queryType=identifier&queryValue=" + identifier);
    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksByIdentifierAndTypeResult(
      String identifier, String identifierType) throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .param("identifierType", identifierType)
            .when()
            .get(
                "/product-hub-works/works?queryType=identifier&queryValue="
                    + identifier
                    + "&identifierType="
                    + identifierType);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksBySearchOptionResult(String searchFor)
      throws AzureOauthTokenFetchingException {

    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get(worksBySearchResource + searchFor);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksByQueryTypeResult(
      String queryType, String queryValue) throws AzureOauthTokenFetchingException {

    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-works/works?queryType=" + queryType + "&queryValue=" + queryValue);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorkByPMCResult(String pmcCode)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-works/works?queryType=pmcCode&queryValue=" + pmcCode);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorkByPMGResult(String pmgCode)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-works/works?queryType=pmgCode&queryValue=" + pmgCode);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksByPersonIDResult(String identifier)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get("/product-hub-works/works?queryType=personId&queryValue=" + identifier);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksByPersonFullNameResult(String queryValue)
      throws AzureOauthTokenFetchingException {
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get(
                "/product-hub-works/works?queryType=personFullNameCurrent&queryValue="
                    + queryValue);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksByaccountableProduct(String accountableProduct)
      throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get(
                "/product-hub-works/works?queryType=accountableProduct&queryValue="
                    + accountableProduct);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksByWorkStatus(
      String searchKeyword, String workStatus) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get(worksByTitleResource + searchKeyword + "&workStatus=" + workStatus);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksByWorkType(
      String searchKeyword, String workType) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get(worksByTitleResource + searchKeyword + "&workType=" + workType);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksByManifestationType(
      String searchKeyword, String manifestationType) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get(worksBySearchResource + searchKeyword + "&manifestationType=" + manifestationType);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksBySearchWithPMCCode(
      String searchKeyword, String pmcCode) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get(worksByTitleResource + searchKeyword + "&pmcCode=" + pmcCode);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorksBySearchWithPMGCode(
      String searchKeyword, String pmgCode) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .when()
            .get(worksBySearchResource + searchKeyword + "&pmgCode=" + pmgCode);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorkByHasWorkComponents(String workID)
          throws AzureOauthTokenFetchingException {
    Response response =
            given()
                    .baseUri(searchAPIEndPoint)
                    .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
                    .when()
                    .get("/product-hub-works/works?_alt=1&queryType=hasWorkComponents&queryValue=" + workID);
    //  response.prettyPrint();

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorkByIsInPackage(String workID)
          throws AzureOauthTokenFetchingException {
    Response response =
            given()
                    .baseUri(searchAPIEndPoint)
                    .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
                    .when()
                    .get("/product-hub-works/works?_alt=1&queryType=isInWorkPackages&queryValue=" + workID);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject searchForWorkByScrollId(String scrollId)
          throws AzureOauthTokenFetchingException {
    Response response =
            given()
                    .baseUri(searchAPIEndPoint)
                    .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
                    .when()
                    .get("/product-hub-works/works/scroll/"+scrollId);

    DataQualityContext.api_response = response;
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }


  public  void setApiEndpoint() {
    if (TestContext.getValues().environment.equalsIgnoreCase("SIT"))
      searchAPIEndPoint = Constants.PRODUCT_SEARCH_END_POINT_SIT;
    else searchAPIEndPoint = Constants.PRODUCT_SEARCH_END_POINT_UAT;
  }

  // ###########below methods are No being used as of 23 Jul 2021

  public static Response getEIPNotificationEndpointResponse() {
    return get(Constants.EIP_NOTIFICATION_WADL_END_POINT_SIT);
  }

  public static Response getPPMChangeHistoryAPIResponse(String dateFrom, String dateTo) {
    return given()
        .header("content-type", "application/json")
        .header("Authorization", RESTEndPoints.Headers.Authorization.toString())
        .baseUri(RESTEndPoints.PPM_CDS_API.PROD.toString())
        .when()
        .get("/changeHistory/metalog/" + dateFrom + "&" + dateTo + "/page_limit=1500&p_from=0")
        .thenReturn();
  }

  public static int getRandomNumForGivenRange(int min, int max) {
    return ThreadLocalRandom.current().nextInt(min, max + 1);
  }

  public static Response getPPMSearchProjectResponse(String projectNumber) {
    return given()
        .header("content-type", "application/json")
        .header("Authorization", RESTEndPoints.Headers.Authorization.toString())
        .baseUri(RESTEndPoints.PPM_CDS_API.UAT.toString())
        .when()
        .get("/search/projno/" + projectNumber)
        .thenReturn();
  }
}
