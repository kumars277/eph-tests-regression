package com.eph.automation.testing.services.api;
/** Created by GVLAYKOV updated by Nishant in Apr-May 2020 for data model changes */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.RESTEndPoints;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.api.ProductApiObject;
import com.eph.automation.testing.models.api.ProductsMatchedApiObject;
import com.eph.automation.testing.models.api.WorkApiObject;
import com.eph.automation.testing.models.api.WorksMatchedApiObject;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.jayway.restassured.RestAssured;
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

  public static ProductApiObject getProductById(String productID) throws AzureOauthTokenFetchingException {
    // updated by Nishant @ 28 Apr 2020
    Response response = getResponse("/product-hub-products/products/" + productID);
    return response.thenReturn().as(ProductApiObject.class);
  }

  public static ProductsMatchedApiObject getProductsByTitle(String title)throws AzureOauthTokenFetchingException {
    // updated by Nishant @ 4 May 2020
    Response response =getResponse("/product-hub-products/products?queryType=name&queryValue=" + title);
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject getProductsByIdentifier(String identifier) throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-products/products?queryType=identifier&queryValue=" + identifier);
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject getProductssByIdentifierAndType( String identifier, String identifierType) throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-products/products?queryType=identifier&queryValue=" + identifier+"&identifierType="+identifierType);
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject getProductsByPackage(String packageID) throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-products/products?queryType=isInPackages&queryValue=" + packageID);
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject getProductsByComponents(String componentID)throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-products/products?queryType=HasComponents&queryValue=" + componentID);
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject getProductsByAccountableProduct(String accountableProduct) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =getResponse("/product-hub-products/products?queryType=accountableProduct&queryValue=" + accountableProduct);
       return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject getProductsByPersonID(String personId)      throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-products/products?queryType=personId&queryValue=" + personId);
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject getProductsByPMC(String pmcCode)      throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-products/products?queryType=pmcCode&queryValue=" + pmcCode);
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject getProductsByPMG(String pmgCode)   throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-products/products?queryType=pmgCode&queryValue=" + pmgCode);
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject getProductsBySearch(String searchOption)   throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-products/products?queryType=search&queryValue=" + searchOption);
     // updates by Nishant to fix escaped char in json response issue
    return response.thenReturn().as(ProductsMatchedApiObject.class, ObjectMapperType.JACKSON_2);
  }

  public static Response getResponse(String getParam) throws AzureOauthTokenFetchingException {
    Response response = null;

      response = given()
              .baseUri(searchAPIEndPoint)
              .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
              .when()
              .get(getParam);

    Log.info(searchAPIEndPoint+getParam);
    DataQualityContext.api_response = response;
    Assert.assertEquals(searchAPIEndPoint+getParam+":"+responseCodeMessage, 200, response.statusCode());
    return response;
  }

  public static ProductsMatchedApiObject getProductByParam( String searchTerm, String paramKey, String paramValue) throws AzureOauthTokenFetchingException {
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
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    Response response =
        given()
            .baseUri(searchAPIEndPoint)
            .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
            .param(paramKey, paramValue)
            .when()
            .get("/product-hub-products/products?queryType=name&queryValue="+searchTerm);

    Log.info(searchAPIEndPoint+"/product-hub-products/products?queryType=name&queryValue="+searchTerm
    +  "&"+paramKey+"="+paramValue);
    DataQualityContext.api_response = response;
  // response.prettyPrint();
    Assert.assertEquals(responseCodeMessage, 200, response.statusCode());

    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  // getWorks APIs
  public static int checkWorkExists(String workID) throws AzureOauthTokenFetchingException {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    return given()
        .baseUri(searchAPIEndPoint)
        .header(Constants.AUTHORIZATION_HEADER, AuthorizationService.getAuthToken().getToken())
        .when()
        .get("/product-hub-works/works/" + workID)
        .thenReturn()
        .statusCode();
  }

  public static WorkApiObject getWorkByID(String workID)      throws AzureOauthTokenFetchingException {
    // updated by Nishant @ 30 Mar 2020 as per latest API changes
    Response response =getResponse("/product-hub-works/works/" + workID);
    return response.thenReturn().as(WorkApiObject.class);
  }

  public static WorksMatchedApiObject getWorkByTitle(String title)      throws AzureOauthTokenFetchingException {
    Response response =getResponse(worksByTitleResource + title);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksByIdentifier(String identifier)      throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-works/works?queryType=identifier&queryValue=" + identifier);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksByIdentifierAndType( String identifier, String identifierType) throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-works/works?queryType=identifier&queryValue=" + identifier   + "&identifierType=" + identifierType);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksBySearchOption(String searchFor)      throws AzureOauthTokenFetchingException {
    Response response =getResponse(worksBySearchResource + searchFor);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksByParam(String queryType, String queryValue) throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-works/works?queryType=" + queryType + "&queryValue=" + queryValue);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorkByPMC(String pmcCode)      throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-works/works?queryType=pmcCode&queryValue=" + pmcCode);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorkByPMG(String pmgCode)      throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-works/works?queryType=pmgCode&queryValue=" + pmgCode);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksByPersonID(String personId) throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-works/works?queryType=personId&queryValue=" + personId);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksByPersonFullName(String queryValue)   throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-works/works?queryType=personFullNameCurrent&queryValue="+ queryValue);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksByaccountableProduct(String accountableProduct)   throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =getResponse("/product-hub-works/works?queryType=accountableProduct&queryValue="+ accountableProduct);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksByWorkStatus( String searchKeyword, String workStatus) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =getResponse(worksByTitleResource + searchKeyword + "&workStatus=" + workStatus);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksByWorkType(      String searchKeyword, String workType) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =getResponse(worksByTitleResource + searchKeyword + "&workType=" + workType);
     return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksByManifestationType(      String searchKeyword, String manifestationType) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =getResponse(worksByTitleResource + searchKeyword + "&manifestationType=" + manifestationType);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksBySearchWithPMC(      String searchKeyword, String pmcCode) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =getResponse(worksByTitleResource + searchKeyword + "&pmcCode=" + pmcCode);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksBySearchWithPMG(      String searchKeyword, String pmgCode) throws AzureOauthTokenFetchingException {
    // created by Nishant as per search API v2 changes
    Response response =getResponse(worksByTitleResource + searchKeyword + "&pmgCode=" + pmgCode);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorkByHasWorkComponents(String workID)          throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-works/works?_alt=1&queryType=hasWorkComponents&queryValue=" + workID);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorkByIsInPackage(String workID)          throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-works/works?queryType=isInWorkPackages&queryValue=" + workID);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static WorksMatchedApiObject getWorksByScrollId(String scrollId)          throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-works/works/scroll/"+scrollId);
    return response.thenReturn().as(WorksMatchedApiObject.class);
  }

  public static ProductsMatchedApiObject getProductsByScrollId(String scrollId)          throws AzureOauthTokenFetchingException {
    Response response =getResponse("/product-hub-products/products/scroll/"+scrollId);
    return response.thenReturn().as(ProductsMatchedApiObject.class);
  }

  public static void setApiEndpoint() {
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
