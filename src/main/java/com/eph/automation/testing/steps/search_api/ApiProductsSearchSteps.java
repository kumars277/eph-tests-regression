package com.eph.automation.testing.steps.search_api;
/** Created by GVLAYKOV */
import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.api.ProductApiObject;
import com.eph.automation.testing.models.api.ProductsMatchedApiObject;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.*;
import com.eph.automation.testing.services.api.APIService;
import com.eph.automation.testing.services.api.AzureOauthTokenFetchingException;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.eph.automation.testing.services.db.sql.PersonProductRoleDataSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.eph.automation.testing.models.contexts.DataQualityContext.*;
import static com.eph.automation.testing.services.api.APIService.*;
import static com.eph.automation.testing.services.api.APIService.getProductsBySearch;

/** Created by Georgi Vlaykov on 11/02/2019 */
public class ApiProductsSearchSteps {

  ApiProductsSearchSteps() {
    APIService apiService = new APIService();
  }

  @StaticInjection  private static String sql;
  private static List<String> packageIds;
  private static List<String> ids;
  private static List<String> manifestationIds;
  private static List<WorkDataObject> workIdentifiers;
  private static List<ProductDataObject> productDataObjects;
  private static List<ManifestationDataObject> manifestationDataObjects;

  private static final String PRW_ID = "PRODUCT_WORK_ID";
  private static final String PRW_TITLE = "PRODUCT_WORK_TITLE";
  private static final String PRW_IDENTIFIER = "PRODUCT_WORK_IDENTIFIER";
  private static final String PRW_ACPRODUCT = "PRODUCT_WORK_ACCOUNTABLE_PRODUCT";
  private static final String PRW_PERSONFULLNAME = "PRODUCT_WORK_PERSONS_FULLNAME";

  private static final String PR_ID = "PRODUCT_ID";
  private static final String PR_TITLE = "PRODUCT_TITLE";
  private static final String PR_IDENTIFIER = "PRODUCT_IDENTIFIER";
  private static final String PR_PERSONFULLNAME = "PRODUCT_PERSONS_FULLNAME";

  private static final String PRM_ID = "PRODUCT_MANIFESTATION_ID";
  private static final String PRM_TITLE = "PRODUCT_MANIFESTATION_TITLE";
  private static final String PRM_IDENTIFIER = "PRODUCT_MANIFESTATION_IDENTIFIER";

  private static final String PRMW_ID = "PRODUCT_MANIFESTATION_WORK_ID";
  private static final String PRMW_TITLE = "PRODUCT_MANIFESTATION_WORK_TITLE";
  private static final String PRMW_IDENTIFIER = "PRODUCT_MANIFESTATION_WORK_IDENTIFIER";
  private static final String PRMW_PERSONFULLNAME = "PRODUCT_MANIFESTATION_WORK_PERSONS_FULLNAME";

  static String from = "&from=";
  static String size = "&size=";
  static String identifier = "identifier";
  static String count = "count";
  static String productCountByProductStatus = "getProductCountByProductStatus";

  @Given("^We get (.*) random search ids for products (.*)$")
  public static void getRandomProductIds(String numberOfRecords, String productProperty) {
    // updated by Nishant @ 25 may 2021 for EPHD-3122
    // Get property when run with jenkins
    // "numberOfRecords = System.getProperty("dbRandomRecordsNumber");"
    switch (productProperty) {
      case PRW_IDENTIFIER:
      case PRW_ID:
      case PRW_ACPRODUCT:
      case PRW_TITLE:
      case PRW_PERSONFULLNAME:
        sql = String.format(APIDataSQL.SELECT_GD_RANDOM_PRODUCT_ID_WITH_WORK);
        break;

      case PR_PERSONFULLNAME:
        sql = String.format(APIDataSQL.SELECT_GD_RANDOM_PRODUCT_ID_WITH_PERSON);
        break;

      case PR_IDENTIFIER:
        sql = String.format(APIDataSQL.SELECT_GD_RANDOM_PRODUCT_ID_WITH_IDENTIFIER);
        break;
      case PRM_IDENTIFIER:
        sql =
            String.format(
                APIDataSQL.SELECT_GD_RANDOM_PRODUCT_ID_WITH_MANIFESTATION_IDENTIFIER,
                numberOfRecords);
        break;
      default:
        sql = String.format(APIDataSQL.SELECT_GD_RANDOM_PRODUCT_ID, numberOfRecords);
        break;
    }

    List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    ids =
        randomProductSearchIds.stream()
            .map(m -> (String) m.get(PR_ID))
            .map(String::valueOf)
            .collect(Collectors.toList());
    Log.info("Environment used..." + System.getProperty("ENV"));
    Log.info("Selected random product ids are : " + ids);
    // added by Nishant @ 26 Dec for debugging failures
    //  ids.clear(); ids.add("EPR-12N992"); Log.info("hard coded product ids are : " + ids);

    if (productProperty.equalsIgnoreCase(PR_IDENTIFIER)) {
      ids.clear();
      ids.add("EPR-10V1T5");
      Log.info("product_identifier hard coded product ids are : " + ids);
    }
    DataQualityContext.breadcrumbMessage += "->" + ids;

    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage + " Verify list with random ids is not empty.",
        ids.isEmpty());
  }




  @Given("^We get (.*) search ids from the db for person roles of products$")
  public static void getRandomPersonRolesIds(String numberOfRecords) {
    sql = String.format(APIDataSQL.SELECT_GD_RANDOM_PRODUCT_PERSON_ROLE, numberOfRecords);
    Log.info(sql);
    List<Map<?, ?>> randomPersonSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

    ids =
        randomPersonSearchIds.stream()
            .map(m -> (BigDecimal) m.get("f_person"))
            .map(String::valueOf)
            .collect(Collectors.toList());
    Log.info("Selected random work ids  : " + ids);
    DataQualityContext.breadcrumbMessage += "->" + ids;

    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage
            + " Verify That list with random person ids is not empty.",
        ids.isEmpty());
  }

  // created by Nishant @ 29 Nov 2019
  @Given("^We get (.*) random package id")
  public static void getRandomPackagesIds(String numberOfRecords) {
    sql = String.format(APIDataSQL.SELECT_GD_RANDOM_PACKAGE_ID, numberOfRecords);
    Log.info(sql);
    List<Map<?, ?>> randomPackageIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    packageIds =
        randomPackageIds.stream()
            .map(m -> (String) m.get("product_id"))
            .map(String::valueOf)
            .collect(Collectors.toList());

    Log.info("Selected random package ids  : " + packageIds);

   //   packageIds.clear();    packageIds.add("EPR-10MYVR"); Log.info("hard coded id is " +  packageIds);

    DataQualityContext.breadcrumbMessage += "->" + packageIds;
    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage + " Verify That list with random ids is not empty.",
        packageIds.isEmpty());
  }

  @And("^We get 1 random search ids from package$")
  public static void getRandomProductIdFromPackage() {
    sql =
        String.format(
            APIDataSQL.SELECT_GD_RANDOM_PRODUCT_FROM_PACKAGE, Joiner.on("','").join(packageIds));
    Log.info(sql);
    List<Map<?, ?>> randomProductFromPackageIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    ids =
        randomProductFromPackageIds.stream()
            .map(m -> (String) m.get("f_component"))
            .map(String::valueOf)
            .collect(Collectors.toList());
    Log.info(
        "Selected random product is "
            + ids.toString()
            + " from package ids  : "
            + packageIds.toString());
    DataQualityContext.breadcrumbMessage += "->" + ids;
    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage + " Verify That list with random ids is not empty.",
        ids.isEmpty());
  }

  @And("^We get the search data from EPH GD for products$")
  public static void getProductsDataFromEPHGD() {
    Log.info("get products data from EPH GD ...");
    sql = String.format(APIDataSQL.GET_GD_DATA_PRODUCT, Joiner.on("','").join(ids));
    productDataObjects =
        DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    productDataObjects.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));
    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage
            + " Verify That product objects list from DB is not empty.",
        productDataObjects.isEmpty());
  }

  @Given("^We set specific product ids for search$")
  public static void setSpecificProductIdsForSearch() {

    ids = new ArrayList<String>();

    if (TestContext.getValues().environment.equalsIgnoreCase("UAT"))
    {ids.add("EPR-11BBFJ") ;
    ids.add("EPR-11BBFK");
    ids.add("EPR-11BBFM");
    ids.add("EPR-11BBFN");
    ids.add("EPR-11BBFR");}
else{
      ids.add("EPR-12JB8J") ;
      ids.add("EPR-12RM3V");
    }

  }

  private static void getWorksDataFromEPHGD(String workId) {
    Log.info("And We get the data from EPH GD for journals ...");
    sql = String.format(APIDataSQL.GET_GD_DATA_WORK, workId);
    DataQualityContext.workDataObjectsFromEPHGD =
        DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    DataQualityContext.workDataObjectsFromEPHGD.sort(
        Comparator.comparing(WorkDataObject::getWORK_ID));
    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage
            + " Verify that list with work objects from DB is not empty",
        DataQualityContext.workDataObjectsFromEPHGD.isEmpty());
  }

  @When("^the product response returned when searched by packages is verified$")
  public void compareProductsRetrievedByIsInPackagesOptionWithDB()
      throws AzureOauthTokenFetchingException {
    ProductsMatchedApiObject returnedProducts = null;
    for (String packageId : packageIds) {
      returnedProducts = getProductsByPackage(packageId);
      Log.info(
          "\n number of package component in API is : " + returnedProducts.getTotalMatchCount());
      returnedProducts.verifyProductsAreReturned();
      returnedProducts.verifyAPIReturnedProductsCount(getCount("getNumberOfPackageComponents"));
    }
  }

  @When("^the product response returned when searched by components is verified$")
  public void compareProductsRetrievedByhasComponentsOptionWithDB()
      throws AzureOauthTokenFetchingException {
    ProductsMatchedApiObject returnedProducts;
    for (String id : ids) {
      returnedProducts = getProductsByComponents(id);
      Log.info(
          "\n number of packages in API : "
              + returnedProducts.getTotalMatchCount()
              + " - for products "
              + id);
      returnedProducts.verifyProductsAreReturned();
      returnedProducts.verifyAPIReturnedProductsCount(getCount("getNumberOfHasComponents"));
    }
  }

  @When("^the product details are retrieved and compared by (.*)$")
  public static void compareProductsRetrievedByaccountableProductWithDB(
      String accounableProductType) throws AzureOauthTokenFetchingException {
    // created by Nishant @ 13 May 2020
    ProductsMatchedApiObject returnedProducts = null;

    for (ProductDataObject productDataObject : productDataObjects) {
      String accountableProductSegmentCode = "";
      switch (accounableProductType) {
        case "PRODUCT_ACCOUNTABLE_PRODUCT":
          // on hold, gd_product table it has none record with accountable product.
          // pending with Dev to clarify as of 13 May 2020
          break;
        case PRW_ACPRODUCT:
          getWorksDataFromEPHGD(productDataObject.getF_PRODUCT_WORK());
          accountableProductSegmentCode =
              getSegmentCode(
                  DataQualityContext.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
          returnedProducts = getProductsByAccountableProduct(accountableProductSegmentCode);
          break;
        case "PRODUCT_MANIFESTATION_WORK_ACCOUNTABLE_PRODUCT":
          getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
          accountableProductSegmentCode =
              getSegmentCode(
                  DataQualityContext.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
          returnedProducts = getProductsByAccountableProduct(accountableProductSegmentCode);
          break;
        default:
          throw new IllegalArgumentException(accounableProductType);
      }
      Log.info("accountableProduct segmentcode is: " + accountableProductSegmentCode);
      assert returnedProducts != null;
      Log.info(
          "\n product count in API by accountableProduct is : "
              + returnedProducts.getTotalMatchCount());
      returnedProducts.verifyProductsAreReturned();
      returnedProducts.verifyAPIReturnedProductsCount(
          getCount("getProductCountByAccountableProducts", accountableProductSegmentCode));
    }
  }

  static String getSegmentCode(String accountableProduct) {
    if (accountableProduct != null) {
      sql = String.format(APIDataSQL.SELECT_GD_PRODUCT_SEGMENT_CODE_BY_WORK, accountableProduct);
      List<Map<String, Object>> listAccountableProduct =
          DBManager.getDBResultMap(sql, Constants.EPH_URL);
      return listAccountableProduct.get(0).get("gl_product_segment_code").toString();
    } else Log.info("accountableProduct id is null, proceeding with segmentCode J018393");
    return "J018393";
  }

  // Updated by Nishant

  @Then("^the product details are retrieved and compared$")
  public static void compareSearchResultsWithDB() throws AzureOauthTokenFetchingException, ParseException {
    ProductApiObject response;
    for (ProductDataObject productDataObject : productDataObjects) {
        response = APIService.getProductById(productDataObject.getPRODUCT_ID());
        response.compareWithDB();
    }
  }

  @When("^the product details are retrieved when searched by (.*) and compared$")
  public void productSearchByTitleAndCompare(String title)throws NullPointerException, ParseException {
    // updated by Nishant @ 4 May 2020
    ProductsMatchedApiObject returnedProducts = null;
    for (ProductDataObject productDataObject : productDataObjects) {
      String searchTitle = "";
      switch (title) {
        case PR_TITLE:    searchTitle = productDataObject.getPRODUCT_NAME();          break;

        case PRM_TITLE:   getManifestationByID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
          searchTitle = manifestationDataObjects.get(0).getMANIFESTATION_KEY_TITLE();      break;

        case PRMW_TITLE:  getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
          searchTitle = DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE();break;

        default:          throw new IllegalArgumentException(title);
      }
      returnedProducts = ProductByTitle_Iterative(searchTitle);
      returnedProducts.verifyProductsAreReturned();
      returnedProducts.verifyProductWithIdIsReturned(productDataObject.getPRODUCT_ID());
    }
  }

  public ProductsMatchedApiObject ProductByTitle_Iterative(String title)
  {
    //created by Nishant @ 5 Jan 2022
    int fromCntr = 0;
    int sizeCntr = 400;
    ProductsMatchedApiObject returnedProducts = null;
    try {
      returnedProducts = getProductsByTitle(title+ from + fromCntr + size + sizeCntr);

      Log.info("Total product found for title... - "+ returnedProducts.getTotalMatchCount());
      while (!returnedProducts.verifyProductWithIdIsReturnedOnly(productDataObjects.get(0).getPRODUCT_ID()) && fromCntr + sizeCntr < returnedProducts.getTotalMatchCount()) {
        fromCntr += sizeCntr;

        Log.info("scanned productID from record " + (fromCntr - sizeCntr) + " to " + fromCntr);
        returnedProducts =getProductsByTitle(title+ from + fromCntr + size + sizeCntr);
      }
    } catch (AzureOauthTokenFetchingException e) {
      e.printStackTrace();
    }
    return returnedProducts;
  }

  @When("^the product details are retrieved and compared when searched by (.*)$")
  public void productSearchByIdentifiersAndCompare(String identifierType)
          throws AzureOauthTokenFetchingException, NullPointerException, ParseException {
    // updated by Nishant @ 5-6 May 2020
    ProductsMatchedApiObject returnedProducts = null;

    for (ProductDataObject productDataObject : productDataObjects) {
      switch (identifierType) {
        case PR_IDENTIFIER:
          getProductIdentifiers(productDataObjects.get(0).getPRODUCT_ID());
          // hard coded by Nishant @ 7 may 2020
          // as there is only 1 record in SIT gd_product_identifier table
          // as per Mark reply on 7th May
          // "This is kind of like a data issue we are having in SIT and UAT
          // - all the identifiers are missing."
          returnedProducts =
              // "getProductsByIdentifier(productDataObjects.get(0).getIDENTIFIER());"
                  getProductsByIdentifier("1234-0707");

          break;
        case PRW_IDENTIFIER:
          getWorkIdentifiersByWorkID(productDataObjects.get(0).getF_PRODUCT_WORK());
          returnedProducts =
                  getProductsByIdentifier(workIdentifiers.get(0).getIDENTIFIER());
          break;

        case PRM_IDENTIFIER:
          List<Map<String, Object>> manifestationIdentifiers =
              getManifestationIdentifierByManifestationID(
                  productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
          returnedProducts = getProductsByIdentifier(  manifestationIdentifiers.get(0).get(identifier).toString());
          break;

        case PRMW_IDENTIFIER:
          getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
          getWorkIdentifiersByWorkID(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
          returnedProducts = getProductsByIdentifier(workIdentifiers.get(0).getIDENTIFIER());
          break;

        case PR_ID:
          returnedProducts = getProductsByIdentifier(productDataObject.getPRODUCT_ID());
          break;

        case PRW_ID:
          returnedProducts =
                  getProductsByIdentifier(productDataObjects.get(0).getF_PRODUCT_WORK());
          break;

        case PRM_ID:
          returnedProducts =
                  getProductsByIdentifier(
                  String.valueOf(productDataObject.getF_PRODUCT_MANIFESTATION_TYP()));
          break;

        case PRMW_ID:
          getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
          returnedProducts =
                  getProductsByIdentifier(
                  DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
          break;
        default:
          throw new IllegalArgumentException(identifierType);
      }
      if (identifierType.equalsIgnoreCase(PR_IDENTIFIER)) {
        returnedProducts.verifyNoProductReturned();
      } else {
        returnedProducts.verifyProductsAreReturned();
        returnedProducts.verifyProductWithIdIsReturned(productDataObject.getPRODUCT_ID());
      }
    }
  }

  @When("^the products detail are retrieved and compared when searched by type and (.*)$")
  public void productSearchByIdentifierWithTypeAndCompare(String identifierType)
          throws AzureOauthTokenFetchingException, NullPointerException, ParseException {
    // updated by Nishant @ 7 May 2020
    ProductsMatchedApiObject returnedProducts = null;

    for (ProductDataObject productDataObject : productDataObjects) {
      switch (identifierType) {
        case PR_IDENTIFIER:
          getProductIdentifiers(productDataObjects.get(0).getPRODUCT_ID());
          // hard coded by Nishant @ 7 may 2020
          // as there is only 1 record in SIT gd_product_identifier table
          // as per Mark reply on 7th May
          // "This is kind of like a data issue we are having in SIT and UAT
          // - all the identifiers are missing."
          returnedProducts = getProductssByIdentifierAndType("1234-0707", "ISBN");
          break;

        case PRW_IDENTIFIER:
          getWorkIdentifiersByWorkID(productDataObjects.get(0).getF_PRODUCT_WORK());
          returnedProducts =
                  getProductssByIdentifierAndType(
                  workIdentifiers.get(0).getIDENTIFIER(), workIdentifiers.get(0).getWORK_TYPE());
          break;

        case PRM_IDENTIFIER:
          List<Map<String, Object>> manifestationIdentifiers =
              getManifestationIdentifierByManifestationID(
                  productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
          returnedProducts =
                  getProductssByIdentifierAndType(
                  manifestationIdentifiers.get(0).get(identifier).toString(),
                  manifestationIdentifiers.get(0).get("f_type").toString());
          break;

        case PRMW_IDENTIFIER:
          getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
          getWorkIdentifiersByWorkID(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
          returnedProducts =
                  getProductssByIdentifierAndType(
                  workIdentifiers.get(0).getIDENTIFIER(), workIdentifiers.get(0).getF_TYPE());
          break;
        default:
          throw new IllegalArgumentException(identifierType);
      }
      if (identifierType.equalsIgnoreCase(PR_IDENTIFIER)) {
        returnedProducts.verifyNoProductReturned();
      } else {
        returnedProducts.verifyProductsAreReturned();
        returnedProducts.verifyProductWithIdIsReturned(productDataObject.getPRODUCT_ID());
      }
    }
  }

  @When("^the product details are retrieved and compared when search option is used with (.*)$")
  public void productSearchBySearchOptionAndCompare(String searchOption)
  {

    try {
      for (ProductDataObject productDataObject : productDataObjects) {

        int fromCntr = 0;
        int sizeCntr = 500;
        String apiResource = "";
        ProductsMatchedApiObject returnedProducts;

        switch (searchOption) {
          case PR_ID:             apiResource = productDataObject.getPRODUCT_ID();                                  break;
          case PRM_ID:            apiResource =String.valueOf(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());  break;
          case PRW_ID:            apiResource =productDataObjects.get(0).getF_PRODUCT_WORK();                       break;
          case PRMW_ID:           getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                                  apiResource =DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID();     break;

          case PR_IDENTIFIER:     getProductIdentifiers(productDataObjects.get(0).getPRODUCT_ID());
            // hard coded by Nishant @ 7 may 2020 as there is only 1 record in SIT gd_product_identifier table
            // as per Mark reply on 7th May "This is kind of like a data issue we are having in SIT and UAT - all the identifiers are missing."
            //"apiResource = productIdentifiers.get(0).getIDENTIFIER();"

                                  apiResource = "123456789";                                                        break;

          case PRW_IDENTIFIER:    getWorkIdentifiersByWorkID(productDataObjects.get(0).getF_PRODUCT_WORK());
                                  apiResource =workIdentifiers.get(0).getIDENTIFIER();                              break;

          case PRM_IDENTIFIER:    List<Map<String, Object>> manifestationIdentifiers = getManifestationIdentifierByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                                  apiResource =manifestationIdentifiers.get(0).get(identifier).toString();           break;

          case PRMW_IDENTIFIER:   getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                                  getWorkIdentifiersByWorkID(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                                  apiResource =workIdentifiers.get(0).getIDENTIFIER();                              break;

          case PR_TITLE:          apiResource = productDataObject.getPRODUCT_NAME();                                break;

          case PRW_TITLE:         getWorksDataFromEPHGD(productDataObjects.get(0).getF_PRODUCT_WORK());
                                  apiResource = DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE();	break;

          case PRM_TITLE:         getManifestationByID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                                  apiResource =   manifestationDataObjects.get(0).getMANIFESTATION_KEY_TITLE();     break;

          case PRMW_TITLE:        getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                                  apiResource =DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE();  break;

          case PR_PERSONFULLNAME: getProductPersonRoleByProductId(productDataObjects.get(0).getPRODUCT_ID());
                                  getPersonDataByPersonId(personProductRoleDataObjectsFromEPHGD.get(0).getF_PERSON());
                                  apiResource = personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " "  + personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME() ;break;

          case PRW_PERSONFULLNAME:getWorkPersonRoleByWorkId(productDataObjects.get(0).getF_PRODUCT_WORK());
                                  getPersonDataByPersonId(personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON());
                                  apiResource = personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME() + " " + personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME();  break;

          case PRMW_PERSONFULLNAME:getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
                                  getWorkPersonRoleByWorkId(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID());
                                  getPersonDataByPersonId(personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON());
                                  apiResource = personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME()+ " " + personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME();  break;

          default:            throw new IllegalStateException("Unexpected value: " + searchOption);
        }

        returnedProducts = getProductsBySearch(apiResource+from + fromCntr + size + sizeCntr);


        if (searchOption.equalsIgnoreCase(PR_IDENTIFIER)) {
          returnedProducts.verifyNoProductReturned();
        } else
          {
             /*
            created by Nishant @8 May 2020
            getProuctByPerson returns sometimes 70000+ records and most probable intended product id does not appear in first 20 records
            hence we need to send API request with size 5000 and check if intended workID is returned
            if not, send request againuntil product id found
            */

          Log.info("Total product found search... - "+ returnedProducts.getTotalMatchCount());
          while (!returnedProducts.verifyProductWithIdIsReturnedOnly(
                  productDataObjects.get(0).getPRODUCT_ID())
                  && fromCntr + sizeCntr < returnedProducts.getTotalMatchCount()) {

            Log.info("scanned product from "+ fromCntr+ " to "+ (fromCntr+ sizeCntr)+ "records...");
            fromCntr += sizeCntr;
            returnedProducts = getProductsBySearch(apiResource+from + fromCntr + size + sizeCntr);

          }

          returnedProducts.verifyProductsAreReturned();
          returnedProducts.verifyProductWithIdIsReturned(productDataObject.getPRODUCT_ID());
        }
      }
    } catch (Exception e) {
      Assert.assertFalse(DataQualityContext.breadcrumbMessage + " " + e.getMessage(), true);
    }
  }



  @When("^the product response returned when searched by personID is verified$")
  public void compareProductsRetrievdByPersonWithDB() throws AzureOauthTokenFetchingException {
    ProductsMatchedApiObject returnedProducts;

    for (String id : ids) {
      returnedProducts = getProductsByPersonID(id);
      Log.info("PersonID chosen : " + id);
      Log.info("Product count in API : " + returnedProducts.getTotalMatchCount());
      returnedProducts.verifyProductsAreReturned();
      returnedProducts.verifyAPIReturnedProductsCount(getCount("getProductCountByPersonIDs", id));
    }
  }

  @When("^the product details are retrieved by PMC Code and compared$")
  public void compareProductSearchByPMCResultsWithDB() throws AzureOauthTokenFetchingException {
    ProductsMatchedApiObject returnedProducts;

    for (ProductDataObject productDataObject : productDataObjects) {
      getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
      String pmcCode = DataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC();
      Log.info("pmcCode to be tested: " + pmcCode);
      returnedProducts = getProductsByPMC(pmcCode);

      returnedProducts.verifyProductsAreReturned();
      returnedProducts.verifyAPIReturnedProductsCount(getNumberOfProductsByPMC(pmcCode));
    }
  }

  @When("^the product details are retrieved by PMG Code and compared$")
  public void compareProductSearchByPMGResultsWithDB() throws AzureOauthTokenFetchingException {
    // update by Nishant @ 14 May 2020
    ProductsMatchedApiObject returnedProducts;

    for (ProductDataObject productDataObject : productDataObjects) {
      getWorkByManifestationID(productDataObject.getF_PRODUCT_MANIFESTATION_TYP());
      String pmgCode = getPMGcodeByPMC(DataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
      // pmgCode="736";//for debugging
      Log.info("pmgCode to be tested: " + pmgCode);
      returnedProducts = getProductsByPMG(pmgCode);
      Log.info("Total count by API..." + returnedProducts.getTotalMatchCount());
      returnedProducts.verifyProductsAreReturned(); // non zero products
      returnedProducts.verifyAPIReturnedProductsCount(getCount("getProductsCountByPMG", pmgCode));
    }
  }

  private static List<Map<String, Object>> getManifestationIdentifierByManifestationID(
      String manifestationID) {
    List<String> manIds = new ArrayList<>();
    manIds.add(manifestationID);
    sql =
        String.format(
            APIDataSQL.SELECT_GD_MANIFESTATION_IDENTIFIER_BY_MANIFESTATION_ID,
            Joiner.on("','").join(manIds));
    return DBManager.getDBResultMap(sql, Constants.EPH_URL);
  }

  private static void getWorkByManifestationID(String manifestationID) {
    List<String> manIds = new ArrayList<>();
    manIds.add(manifestationID);
    sql =
        String.format(
            APIDataSQL.GET_GD_DATA_WORK_BY_MANIFESTATION,
            Joiner.on("','").join(manIds));

    DataQualityContext.workDataObjectsFromEPHGD =
        DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
  }

  private static void getWorkIdentifiersByWorkID(String workID) {
    sql = APIDataSQL.GET_GD_DATA_WORKIDENTIFIER_BY_WORKID.replace("PARAM1", workID);
    workIdentifiers = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
  }

  private static List<ProductDataObject> getProductIdentifiers(String productID) {
    List<ProductDataObject> productIdentifiers;
    sql = APIDataSQL.GET_GD_DATA_PRODUCTIDENTIFIER_BY_PRODUCTID.replace("PARAM1", productID);
    productIdentifiers =
        DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    return productIdentifiers;
  }

  private static void getManifestationByID(String manifestationID) {
    List<String> manIds = new ArrayList<>();
    manIds.add(manifestationID);
    Log.info("And get the manifestations in EPH GD ..");
    sql =
        String.format(
            APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID, Joiner.on("','").join(manIds));

    manifestationDataObjects =
        DBManager.getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
  }

  public static String getPackageIdByProductID(String workID) {
    sql = String.format(APIDataSQL.SELECT_GD_PMG_BY_PMC, workID);
    Log.info(sql);
    List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    return ((String) getPMG.get(0).get("f_pmg"));
  }

  public static String getProductPackageByID(String id) {
    sql = String.format(APIDataSQL.SELECT_GD_PACKAGEID_BY_PRODUCTID, id);
    Log.info(sql);
    List<Map<String, Object>> getPackage = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    return ((String) getPackage.get(0).get("f_package_owner"));
  }

  private static void getWorksByPMC(String pmcCode) {
    sql = String.format(APIDataSQL.SELECT_GD_WORKS_BY_PMC, pmcCode);
    List<Map<?, ?>> workIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    ids =
        workIds.stream()
            .map(m -> (String) m.get("WORK_ID"))
            .map(String::valueOf)
            .collect(Collectors.toList());
    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage
            + " Verify That list with work ids by pmc is not empty.",
        ids.isEmpty());
  }

  public static void getPMGWorksByPMC(String pmcCode) {
    sql = String.format(APIDataSQL.SELECT_GD_WORK_BYPMG_BYPMC, pmcCode);
    List<Map<?, ?>> workIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    ids =
        workIds.stream()
            .map(m -> (String) m.get("WORK_ID"))
            .map(String::valueOf)
            .collect(Collectors.toList());
    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage
            + " Verify That list with work ids by pmc is not empty.",
        ids.isEmpty());
  }

  private static void getManifestationsByWorks() {
    sql = String.format(APIDataSQL.SELECT_GD_MANIFESTATIONS_BY_WORKID, Joiner.on("','").join(ids));
    List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

    manifestationIds =
        randomProductSearchIds.stream()
            .map(m -> (String) m.get("MANIFESTATION_ID"))
            .map(String::valueOf)
            .collect(Collectors.toList());
    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage
            + " Verify That list with manifestation ids by work ids is not empty.",
        ids.isEmpty());
  }

  private int getNumberOfProductsByPMC(String pmcCode) {
    getWorksByPMC(pmcCode);
    getManifestationsByWorks();
    int prcount = getCount("getProductsCountByManifestations") + getCount("getProductsCountByWork");
    Log.info("products count by workType in DB is: " + prcount);
    return prcount;
  }

  private static String getPMGcodeByPMC(String pmcCode) {
    sql = String.format(APIDataSQL.SELECT_GD_PMG_BY_PMC, pmcCode);
    List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    return ((String) getPMG.get(0).get("f_pmg"));
  }

  // created by Nishant @ 24 Apr 2020
  private static void getWorkPersonRoleByWorkId(String workId) {
    Log.info("get person role by Work id..." + workId);
    sql = String.format(APIDataSQL.GET_GD_DATA_WORKPERSON_BY_WORKID, workId);
    personWorkRoleDataObjectsFromEPHGD =
        DBManager.getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);
    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage
            + " Verify person role by work id successfully extracted from EPH DB",
        personWorkRoleDataObjectsFromEPHGD.isEmpty());
  }

  // created by Nishant @ 8 May 2020
  private static void getProductPersonRoleByProductId(String productId) {
    Log.info("get person role by product id..." + productId);
    sql = String.format(PersonProductRoleDataSQL.selectProductPersonByproductId, productId);
    personProductRoleDataObjectsFromEPHGD =
        DBManager.getDBResultAsBeanList(sql, PersonProductRoleDataObject.class, Constants.EPH_URL);
    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage
            + " Verify person role by product id successfully extracted from EPH DB",
        personProductRoleDataObjectsFromEPHGD.isEmpty());
  }

  private static void getPersonDataByPersonId(String personId) {
    Log.info("get person data by person id..." + personId);
    sql = String.format(APIDataSQL.GET_GD_DATA_PERSON_BY_PERSONID, personId);
    personDataObjectsFromEPHGD =
        DBManager.getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
    Assert.assertFalse(
        DataQualityContext.breadcrumbMessage
            + " verify person Data by person id extracted from EPH DB",
        personDataObjectsFromEPHGD.isEmpty());
  }

  @Then("^the product count are retrieved by (.*) compared$")
  public static void theProductDetailsAreRetrievedByParamKeyAndCompared(String paramKey) throws AzureOauthTokenFetchingException {
    /*//logic updated by Nishant @ 04 Jun 2021
    //as per Ron, API product search result returns products even though product name does not contains searchKey but its manifestaion or work title does.
    */
    ProductsMatchedApiObject returnedProducts;
    String defaultSearch = "CELL";
    int productCountDB = 0;

      String searchTerm = (productDataObjects.get(0).getPRODUCT_NAME().replaceAll("[^a-zA-Z0-9]", " ").split(" ")[0]).toUpperCase();
      switch (paramKey) {
        case "productStatus":
          DataQualityContext.breadcrumbMessage += "->" + productDataObjects.get(0).getF_STATUS();

          productCountDB =
              getCount(
                  productCountByProductStatus,
                  defaultSearch,
                  productDataObjects.get(0).getF_STATUS());
          returnedProducts =
                  getProductByParam(
                  defaultSearch, paramKey, productDataObjects.get(0).getF_STATUS());
          break;
        case "productType":
          DataQualityContext.breadcrumbMessage += "->" + productDataObjects.get(0).getF_TYPE();

          returnedProducts =
                  getProductByParam(
                  defaultSearch, paramKey, productDataObjects.get(0).getF_TYPE());
          productCountDB =
              getCount(
                  "getProductCountByProductType",
                  defaultSearch,
                  productDataObjects.get(0).getF_TYPE());
          break;
        case "workType":
          getWorkByManifestationID(productDataObjects.get(0).getF_PRODUCT_MANIFESTATION_TYP());
          DataQualityContext.breadcrumbMessage +=
              "->" + DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE();

          returnedProducts =
                  getProductByParam(
                  defaultSearch,
                  paramKey,
                  DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
          productCountDB =
              getCount(
                  "getProductCountByWorkType",
                  defaultSearch,
                  DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
          break;
        case "manifestationType":
          getManifestationByID(productDataObjects.get(0).getF_PRODUCT_MANIFESTATION_TYP());
          DataQualityContext.breadcrumbMessage +=
              "->" + manifestationDataObjects.get(0).getF_TYPE();
          returnedProducts =
                  getProductByParam(
                  defaultSearch, paramKey, manifestationDataObjects.get(0).getF_TYPE());
          productCountDB =
              getCount(
                  "getProductCountByManifestationType",
                  defaultSearch,
                  manifestationDataObjects.get(0).getF_TYPE());
          break;
        case "pmcCode":
          getWorkByManifestationID(productDataObjects.get(0).getF_PRODUCT_MANIFESTATION_TYP());
          DataQualityContext.breadcrumbMessage +="->" + DataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC();
          returnedProducts =getProductByParam(searchTerm,paramKey,DataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
          productCountDB =getCount("getProductCountByPMCCode",searchTerm,DataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
          break;

        case "pmgCode":
          getWorkByManifestationID(productDataObjects.get(0).getF_PRODUCT_MANIFESTATION_TYP());
          String pmgCode =
              getPMGcodeByPMC(DataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
          DataQualityContext.breadcrumbMessage += "->" + pmgCode;
          returnedProducts = getProductByParam(searchTerm, paramKey, pmgCode);
          productCountDB = getCount("getProductCountByPMGCode", searchTerm, pmgCode);
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + paramKey);
      }

      returnedProducts.verifyProductsAreReturned();
      returnedProducts.verifyAPIReturnedProductsCount(productCountDB);

  }

  @Then("^the product title are retrieved by (.*) compared$")
  public static void theProductTitleAreRetrievedAndCotainsSearchKey(String paramKey)
      throws AzureOauthTokenFetchingException, NullPointerException {
    String defaultSearch = "CELL";
    int productCountDB = 0;
    int fromCntr = 0;
    int sizeCntr = 400;

    ProductsMatchedApiObject returnedProducts = new ProductsMatchedApiObject();

    if (paramKey.equalsIgnoreCase("productStatus")) {
      DataQualityContext.breadcrumbMessage += "-> product status " + productDataObjects.get(0).getF_STATUS();
      productCountDB =getCount(productCountByProductStatus, defaultSearch, productDataObjects.get(0).getF_STATUS());

      returnedProducts =getProductByParam(defaultSearch + from + fromCntr + size + sizeCntr,paramKey,productDataObjects.get(0).getF_STATUS());
      Log.info("Total product found for product status with search... - "+ returnedProducts.getTotalMatchCount());
      while (!returnedProducts.verifyAllTitleContainsSearchKey(defaultSearch)&& fromCntr + sizeCntr < returnedProducts.getTotalMatchCount()) {
        fromCntr += sizeCntr;
        Log.info("scanned productID from " + (fromCntr - sizeCntr) + " to " + fromCntr + " records...");
        returnedProducts =getProductByParam(defaultSearch + from + fromCntr + size + sizeCntr,paramKey,productDataObjects.get(0).getF_STATUS());
      }
    }

  //  returnedProducts.verifyProductsAreReturned();
    returnedProducts.verifyAPIReturnedProductsCount(productCountDB);
  }


  @When("^verify pagination duplicate ids retried for product (.*) with (.*)$")
  public void verifyPaginationForProduct(String keyword, String scroll)  throws AzureOauthTokenFetchingException {
  //created by Nishant @ 30 Nov 2021
    ProductsMatchedApiObject returnedProducts = null;
    Log.info("searching for product..." + keyword);

    int fromCntr = 0;    int sizeCntr = 50;    String scrollId = "";
    ArrayList<String> idsToVerify = new ArrayList<>();

    do{
      //call this API first
      if (fromCntr == 0) {
        returnedProducts =

                getProductsBySearch(keyword +"&_alt=1"+ from + fromCntr + size + sizeCntr + "&reverse=true&scroll=" + scroll);
        scrollId = returnedProducts.getScrollId(); //get scroll id from response
      }
      else //call scroll API for rest of iterations
      {
        returnedProducts = APIService.getProductsByScrollId(scrollId+"?scroll=" + scroll);
      }

      Log.info("Total work found - " + returnedProducts.getTotalMatchCount());
      Log.info("scanning workID from " + (fromCntr) + " to " + (fromCntr+sizeCntr) + " records...");

      returnedProducts.verifyProductsAreReturned();
      ProductApiObject[] items = returnedProducts.getItems().clone();
      Log.info("previous ids...");      Log.info(idsToVerify.toString());

      for (int i = 0; i < items.length; i++) {
        Assert.assertFalse("duplicate found at index "+i+" "+items[i].getId().toString(),
                idsToVerify.contains(items[i].getId().toString()));
        idsToVerify.add(items[i].getId());
      }
      fromCntr += sizeCntr;
    }
    while (fromCntr < returnedProducts.getTotalMatchCount());
  }



  private static int getCount(String countType) {
    switch (countType) {
      case "getNumberOfPackageComponents":
        sql =
            String.format(
                APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_PACKAGE,
                Joiner.on("','").join(packageIds));
        break;

      case "getNumberOfHasComponents":
        sql =
            String.format(
                APIDataSQL.SELECT_GD_COUNT_PACKAGE_BY_PRODUCT, Joiner.on("','").join(ids));
        break;

      case "getProductsCountByManifestations":
        sql =
            String.format(
                APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_MANIFESTATION,
                Joiner.on("','").join(manifestationIds));
        break;

      case "getProductsCountByWork":
        sql = String.format(APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_WORK, Joiner.on("','").join(ids));
        break;
      default:
        throw new IllegalArgumentException(countType);
    }

    List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    return ((Long) getCount.get(0).get(count)).intValue();
  }

  private static int getCount(String countType, String param1) {
    switch (countType) {
      case "getProductCountByAccountableProducts":
        sql = String.format(APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_SEGMENT_CODE, param1, param1);
        break;

      case "getProductsCountByPMG":
        sql = String.format(APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_PMG, param1);
        break;

      case "getProductCountByPersonIDs":
        sql = String.format(APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_PERSONID, param1, param1);
        break;

      case "getWorkCountByPMG":
        sql = String.format(APIDataSQL.SELECT_GD_COUNT_WORK_BY_PMG, param1);
        break;
      default:
        throw new IllegalArgumentException(countType);
    }

    List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    return ((Long) getCount.get(0).get(count)).intValue();
  }

  private static int getCount(String countType, String param1, String param2) {
    switch (countType) {
      case "getProductCountByProductStatus":
        sql =
            APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_PRODUCTSTATUS
                .replace("param1", param1)
                .replace("param2", param2);
        break;

      case "getProductCountByProductType":
        sql = String.format(APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_PRODUCTTYPE, param1, param2);
        break;

      case "getProductCountByWorkType":
        sql =
            APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_WORKTYPE
                .replace("param1", param1)
                .replace("param2", param2);
        break;

      case "getProductCountByManifestationType":
        sql = String.format(APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_MANIFESTATIONTYPE, param1, param2);
        break;

      case "getProductCountByPMCCode":
        sql = String.format(APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_PMC_WITHSEARCH, param1, param2);
        break;

      case "getProductCountByPMGCode":
        sql = String.format(APIDataSQL.SELECT_GD_COUNT_PRODUCT_BY_PMG_WITHSEARCH, param1, param2);
        break;

      default:
        throw new IllegalArgumentException(countType);
    }

    List<Map<String, Object>> countByProductStatus =
        DBManager.getDBResultMap(sql, Constants.EPH_URL);
    return ((Long) countByProductStatus.get(0).get(count)).intValue();
  }


}
