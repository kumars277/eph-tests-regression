package com.eph.automation.testing.steps.search_api;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.api.*;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.*;
import com.eph.automation.testing.services.api.APIService;
import com.eph.automation.testing.services.api.AzureOauthTokenFetchingException;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import static com.eph.automation.testing.models.contexts.DataQualityContext.*;
import static com.eph.automation.testing.services.api.APIService.getWorkByIsInPackage;

import com.eph.automation.testing.steps.GenericFunctions;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import cucumber.api.java.en.*;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.Assert;

/*
 * Created by GVLAYKOV on 11/02/2019
 * updated by Nishant @ 20 April 2020 for data model changes
 */

public class ApiWorksSearchSteps {
  @StaticInjection
  private APIService apiService = new APIService();
  private ApiReusableFunctions apiFun = new ApiReusableFunctions();
  private static String sql;
  private static List<String> manifestaionids;
  private WorkApiObject workApi_response;
  private static List<WorkDataObject> workIdentifiers;

  private static final String W_ID = "WORK_ID";
  private static final String W_TITLE = "WORK_TITLE";
  private static final String W_COMPONENT="HAS_WORK_COMPONENTS";
  private static final String W_IN_PACKAGE = "IS_IN_WORK_PACKAGES";
  private static final String W_IDENTIFIER = "WORK_IDENTIFIER";
  private static final String WPR_SUMMARYNAME = "WORK_PRODUCT_SUMMARY_NAME";
  private static final String WPR_ID = "WORK_PRODUCT_ID";
  private static final String WM_ID = "WORK_MANIFESTATION_ID";
  private static final String WM_IDENTIFIER = "WORK_MANIFESTATION_IDENTIFIER";
  private static final String WM_TITLE = "WORK_MANIFESTATION_TITLE";
  private static final String WMPR_SUMMARYNAME = "WORK_MANIFESTATION_PRODUCT_SUMMARY_NAME";
  private static final String WMPR_ID = "WORK_MANIFESTATION_PRODUCT_ID";
  private static final String PER_FULLNAME_CURRENT = "personFullNameCurrent";

  static String from = "&from=";
  static String size = "&size=";
  static String activeWorkTypeStatus="&workType=ABS,JBB,JNL,NWL&workStatus=WLA";//,WDA,WTA,WVA";


  public ApiWorksSearchSteps() {/**/}

  @Given("^We get (.*) random search ids for works (.*)$")
  public static void getRandomWorkIds(String numberOfRecords, String workProperty) {
    // updated by Nishant @ 25 May 2021

    switch (workProperty) {
      case WPR_ID           :
      case WPR_SUMMARYNAME  :sql = String.format(APIDataSQL.SELECT_GD_RANDOM_WORK_ID_WITH_PRODUCT, numberOfRecords);        break;
      case WMPR_SUMMARYNAME :sql = String.format(APIDataSQL.SELECT_GD_RANDOM_WORK_WITH_MANIFESTATION_WITH_PRODUCT, numberOfRecords);        break;
      case WM_IDENTIFIER    :sql = String.format(APIDataSQL.SELECT_GD_RANDOM_WORK_WITH_MANIFESTATION_IDENTIFIER, numberOfRecords);        break;
      case W_COMPONENT      :sql = String.format(APIDataSQL.SELECT_GD_RANDOM_WORK_WITH_HASWORKCOMPONENT,numberOfRecords);        break;
      case W_IN_PACKAGE     :sql = String.format(APIDataSQL.SELECT_GD_RANDOM_WORK_IN_PACKAGE,numberOfRecords);        break;
      default               :sql = String.format(APIDataSQL.SELECT_GD_RANDOM_WORK_ID, numberOfRecords);        break;
    }
    List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    ids =
            randomProductSearchIds.stream()
                    .map(m -> (String) m.get(W_ID))
                    .map(String::valueOf)
                    .collect(Collectors.toList());

    Log.info("Selected random work ids  : " + ids + "on environment " + TestContext.getValues().environment);
    // added by Nishant @ 27 Dec for debugging failures
  // ids.clear();ids.add("EPR-W-105C87");Log.info("hard coded work id is : " + ids);
    setBreadcrumbMessage(ids.toString());
    Assert.assertFalse(getBreadcrumbMessage() + "- Verify random id list is not empty.",
            ids.isEmpty());
  }

  @Given("^We get (.*) random journal ids to search (.*)")
  public static void getRandomJournalIds(String numberOfRecords,String searchType) { // created by Nishant @ 25 Jun 2020

    switch(searchType)
    {
      case "PERSON_NAME":
      case "PEOPLE_HUB_ID":
      case "PERSON_ID":
      case "personFullNameCurrent":
        sql = String.format(APIDataSQL.SELECT_GD_RANDOM_JOURNAL_ID_personFullNameCurrent, numberOfRecords);
        break;
      default: sql = String.format(APIDataSQL.SELECT_GD_RANDOM_JOURNAL_ID, numberOfRecords);break;
    }

    List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    ids =
            randomProductSearchIds.stream()
                    .map(m -> (String) m.get(W_ID))
                    .map(String::valueOf)
                    .collect(Collectors.toList());

    Log.info("Selected random Journal ids  : " + ids +" on "+ TestContext.getValues().environment);
    // for debugging failure
   // ids.clear();    ids.add("EPR-W-102S2S");  Log.info("hard coded work ids are : " + ids);
    setBreadcrumbMessage(ids.toString());
    verifyListNotEmpty(ids);
  }

  @Given("^We get (.*) random search ids for Extended works")
  public static void getRandomExtendedWorkIds(String numberOfRecords) {
    // created by Nishant @ 01 Jul 2020

    String dbEnv;
    if (TestContext.getValues().environment.equalsIgnoreCase("UAT")) dbEnv = "uat";
    else dbEnv = "sit";
    sql = String.format(APIDataSQL.SELECT_RANDOM_EXTENDED_WORK_ID, dbEnv, numberOfRecords);
    List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    ids =
            randomProductSearchIds.stream()
                    .map(m -> (String) m.get(W_ID))
                    .map(String::valueOf)
                    .collect(Collectors.toList());
    Log.info("Selected random work ids  : " + ids+" on "+ TestContext.getValues().environment);
    // added by Nishant @ 27 Dec for debugging failures
    // "ids.clear(); ids.add("EPR-W-108TJK");  Log.info("hard coded work ids are : " + ids);"
    setBreadcrumbMessage(ids.toString());
    verifyListNotEmpty(ids);
  }

  @Given("^We get (.*) random search ids for Extended manifestation")
  public static void getRandomExtendedManifestationIds(String numberOfRecords) {
    // created by Nishant @ 01 Jul 2020

    String dbEnv;
    if (TestContext.getValues().environment.equals("UAT")) dbEnv = "uat";
    else dbEnv = "sit";
    sql =
            String.format(APIDataSQL.SELECT_RANDOM_EXTENDED_MANIFESTATION_ID, dbEnv, numberOfRecords);
    List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    manifestaionids =
            randomProductSearchIds.stream()
                    .map(m -> (String) m.get(W_ID))
                    .map(String::valueOf)
                    .collect(Collectors.toList());
    Log.info("Selected random extended manifestation ids  : " + manifestaionids+" on "+TestContext.getValues().environment);
    setBreadcrumbMessage(manifestaionids.toString());
    verifyListNotEmpty(manifestaionids);
  //  manifestaionids.clear();manifestaionids.add("EPR-M-1251CX");manifestaionids.add("EPR-M-11S7FY");
  }

  @Given("^We set specific journal ids for search")
  public static void setspecificJournalIds() { // created by Nishant @ 04 Aug 2020
    ids = new ArrayList<>();
    // production random work ids
    ids.add("EPR-W-102RGY");
    ids.add("EPR-W-102RB6");
    ids.add("EPR-W-102NHD");
    ids.add("EPR-W-102RRG");
    ids.add("EPR-W-102VF4");
    Log.info("hard coded work ids are : " + ids +" on "+ System.getProperty("ENV"));
    setBreadcrumbMessage(ids.toString());
  }

  @Given("^We get (.*) random search ids for person roles")
  public static void getRandomPersonRolesIds(String numberOfRecords) {

    sql = String.format(APIDataSQL.SELECT_GD_RANDOM_WORK_PERSON_ROLE, numberOfRecords);
    List<Map<?, ?>> randomPersonSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    ids =
            randomPersonSearchIds.stream()
                    .map(m -> (BigDecimal) m.get("f_person"))
                    .map(String::valueOf)
                    .collect(Collectors.toList());
    Log.info("Selected random person ids  : " + ids+" on "+ TestContext.getValues().environment) ;//System.getProperty("ENV"));
    //  ids.clear(); ids.add("10088889");  Log.info("hard coded work ids are : " + ids);
    setBreadcrumbMessage(ids.toString());
    Assert.assertFalse(
            getBreadcrumbMessage()
                    + "-> Verify That list with random person roles is not empty.",
            ids.isEmpty());
  }

  @And("^We get the work search data from EPH GD$")
  public static void getWorksDataFromEPHGD() {
    Log.info(
            "We get the work data from EPH GD ..." + Joiner.on("','").join(DataQualityContext.ids));
    sql =
            String.format(
                    APIDataSQL.GET_GD_DATA_WORK,
                    Joiner.on("','").join(DataQualityContext.ids));
    DataQualityContext.workDataObjectsFromEPHGD =
            DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    DataQualityContext.workDataObjectsFromEPHGD.sort(
            Comparator.comparing(WorkDataObject::getWORK_ID));
    Assert.assertFalse(
            getBreadcrumbMessage()
                    + "-> Verify that list with work objects from DB is not empty",
            DataQualityContext.workDataObjectsFromEPHGD.isEmpty());
  }

  @Given("We get the work data from EPH GD for (.*)")
  public static void getSpecificWorkDataFromEPHGD(String WorkId) {
    sql = String.format(APIDataSQL.GET_GD_DATA_WORK, WorkId);
    DataQualityContext.workDataObjectsFromEPHGD =
            DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    DataQualityContext.workDataObjectsFromEPHGD.sort(
            Comparator.comparing(WorkDataObject::getWORK_ID));
  }

  // Updated by Nishant for data model changes in Apr 2020
  @When("^the work details are retrieved and compared$")
  public void compareWorkSearchResultsWithDB() throws AzureOauthTokenFetchingException, ParseException {
    int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
    for (int i = 0; i < bound; i++) {
      Log.info("Verifying " + DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
      workApi_response =APIService.getWorkByID(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
      workApi_response.compareWithDB();
    }
  }

  @Then("^the work details are retrieved by accountableProduct and compared$")
  public void compareWorkByAccountableProductWithDB() throws AzureOauthTokenFetchingException {
    WorksMatchedApiObject returnedWorks;
    int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
    for (int i = 0; i < bound; i++) {
      String accountableProduct_SegmentCode =
              ApiProductsSearchSteps.getSegmentCode(
                      DataQualityContext.workDataObjectsFromEPHGD.get(i).getF_accountable_product());
      Log.info("acountableProduct to be tested..." + accountableProduct_SegmentCode);

      returnedWorks = APIService.getWorksByaccountableProduct(accountableProduct_SegmentCode);

      printTotalWorkCount(returnedWorks);

      returnedWorks.verifyWorksAreReturned();
      returnedWorks.verifyWorksReturnedCount(
              getNumberOfWorksByAccountableProduct(accountableProduct_SegmentCode));
    }
  }

  @Then("^the work details are retrieved by workStatus and compared$")
  public void compareWorksByWorkStatusWithDB() throws AzureOauthTokenFetchingException {
    WorksMatchedApiObject returnedWorks;

      int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
      for (int i = 0; i < bound; i++) {

        String searchKeyword = ApiReusableFunctions.getSearchKeyword(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        String workStatus = DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_STATUS();
        Log.info("searchKeyword - workStatus: " + searchKeyword +" - " + workStatus);

        returnedWorks = APIService.getWorksByWorkStatus(searchKeyword, workStatus);
        printTotalWorkCount(returnedWorks);

        returnedWorks.verifyWorksAreReturned();
        returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByWorkStatus(searchKeyword, workStatus));
      }

  }

  @Then("^the work details are retrieved by workType and compared$")
  public void compareWorksByWorkTypeWithDB() throws AzureOauthTokenFetchingException {
    WorksMatchedApiObject returnedWorks;
   // boolean failed = false;

      int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
      for (int i = 0; i < bound; i++) {
        String searchKeyword = ApiReusableFunctions.getSearchKeyword( DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        String workType = DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TYPE();
        Log.info("searchKeyword and workType: " + searchKeyword + " - " + workType);

        returnedWorks = APIService.getWorksByWorkType(searchKeyword, workType);
        printTotalWorkCount(returnedWorks);
        returnedWorks.verifyWorksAreReturned();
        returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByWorkType(searchKeyword, workType));
      }
  }

  @Then("^the work details are retrieved by manifestationType and compared$")
  public void compareWorksByManifestationTypeWithDB() throws AzureOauthTokenFetchingException {
    WorksMatchedApiObject returnedWorks;

      int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
      for (int i = 0; i < bound; i++) {
        getManifestationsByWorkID(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
          String searchKeyword =  ApiReusableFunctions.getSearchKeyword(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE()).toUpperCase();

        String ManifestationType =
                DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getF_TYPE();
        Log.info("searchKeyword and ManifestationType: " + searchKeyword + " - " + ManifestationType);
        setBreadcrumbMessage("searchKeyword-ManifestationType: " + searchKeyword + " - " + ManifestationType);
        returnedWorks =
                APIService.getWorksByManifestationType(searchKeyword, ManifestationType);
        printTotalWorkCount(returnedWorks);
        returnedWorks.verifyWorksAreReturned();
        returnedWorks.verifyWorksReturnedCount(
                getNumberOfWorksByManifestationType(searchKeyword, ManifestationType));
      }


  }

  @Then("^the work details are retrieved by search with PMC code and compared$")
  public void compareWorkBySearchWithPMCCodeWithDB() {
    WorksMatchedApiObject returnedWorks;
    try{
      int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
      String searchKeyword = ApiReusableFunctions.getSearchKeyword(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
      //String searchKeyword =  DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE().replaceAll("[^a-zA-Z0-9]", "").split(" ")[0];

      for (int i = 0; i < bound; i++) {
        Log.info("search keyword '"+ searchKeyword+ "' and pmcCode '"
                        + DataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC()+ "'");
        returnedWorks =
                APIService.getWorksBySearchWithPMC(
                        searchKeyword, DataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC());
        printTotalWorkCount(returnedWorks);
        returnedWorks.verifyWorksAreReturned();
        returnedWorks.verifyWorksReturnedCount(
                getNumberOfWorksBySearchWithPMCCode(
                        searchKeyword, DataQualityContext.workDataObjectsFromEPHGD.get(0).getPMC()));
      }
    } catch (Exception e) {
      e.getMessage();
      scenarioFailed();
    }
  }

  @Then("^the work details are retrieved by search with PMG code and compared$")
  public void compareWorkBySearchWithPMGCodeWithDB() throws AzureOauthTokenFetchingException {
    WorksMatchedApiObject returnedWorks;

      int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
      String searchKeyword = ApiReusableFunctions.getSearchKeyword(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE()).toUpperCase();
              /*DataQualityContext
                      .workDataObjectsFromEPHGD
                      .get(0)
                      .getWORK_TITLE()
                      .split(" ")[0]
                      .toUpperCase();*/
      for (int i = 0; i < bound; i++) {
        String PMGCode =
                getPMGcodeByPMC(DataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
        Log.info("search keyword '" + searchKeyword + "' and pmgCode '" + PMGCode + "'");

        returnedWorks = APIService.getWorksBySearchWithPMG(searchKeyword, PMGCode);
        Log.info("API total matched count..." + returnedWorks.getTotalMatchCount());
        returnedWorks.verifyWorksAreReturned();
        returnedWorks.verifyWorksReturnedCount(
                getNumberOfWorksBySearchWithPMGCode(searchKeyword, PMGCode));
      }


  }

  @When("^the work details are retrieved by title (.*) and compared$")
  public void compareWorkSearchByTitleResultsWithDB(String titleType)throws AzureOauthTokenFetchingException {
      int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
      for (int i = 0; i < bound; i++) {
        Log.info("###########-----getWorkByTitle - " + titleType);
        String searchTitle = "";

        switch (titleType) {
          case W_TITLE:
            searchTitle = DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE();            break;
          // implemented by Nishant @ 20 Apr 2020
          case WM_TITLE:
            getManifestationsByWorkID(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            searchTitle = DataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_KEY_TITLE();break;
          // implemented by Nishant @ 22 Apr 2020
          case WPR_SUMMARYNAME:
            getProductDetailByWorkId(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            searchTitle =  DataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_NAME();
            break;
          // implemented by Nishant @ 21 Apr 2020
          case WMPR_SUMMARYNAME:
            getManifestationsByWorkID(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            getProductDetailByManifestationId(DataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
            searchTitle = DataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_NAME();
            break;
          default:throw new IllegalArgumentException(titleType);
        }

        WorksMatchedApiObject returnedWorks =apiFun.workByTitle_Iterative(searchTitle,i);
        returnedWorks.verifyWorksAreReturned();
        returnedWorks.verifyWorkWithIdIsReturned(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
      }
  }
/*
  public WorksMatchedApiObject workByTitle_Iterative(String title,int i) {
    //created by Nishant @ 5 Jan 2022
    int fromCntr = 0;
    int sizeCntr = 400;
    WorksMatchedApiObject returnedWorks = null;
    try {
      returnedWorks = APIService.getWorkByTitle(title+ from + fromCntr + size + sizeCntr);

      Log.info("Total work found for title... - "+ returnedWorks.getTotalMatchCount());
      while (!returnedWorks.verifyWorkWithIdIsReturnedOnly(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID()) &&
              fromCntr + sizeCntr < returnedWorks.getTotalMatchCount()) {
        fromCntr += sizeCntr;
        Log.info("scanned productID from record " + (fromCntr - sizeCntr) + " to " + fromCntr);
        returnedWorks =APIService.getWorkByTitle(title+ from + fromCntr + size + sizeCntr);
      }
    } catch (AzureOauthTokenFetchingException e) {
      e.printStackTrace();
    }
    return returnedWorks;
  }
*/

  @When("^the works search by identifier (.*) details are retrieved and compared$")
  public void compareWorkSearchByIdentifierResultsWithDB(String identifierType)
          throws AzureOauthTokenFetchingException {
    WorksMatchedApiObject returnedWorks = null;
    boolean failed = false;
    try {
      int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
      for (int i = 0; i < bound; i++) {

        int fromCntr = 0;
        int sizeCntr = 500;

        switch (identifierType) {
          case W_ID:
            returnedWorks =
                    APIService.getWorksByIdentifier(
                            DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            break;
          case W_IDENTIFIER:
            getWorkIdentifiersByWorkID(
                    DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            returnedWorks =
                    APIService.getWorksByIdentifier(workIdentifiers.get(i).getIDENTIFIER());
            break;
          case WM_ID:
            getManifestationsByWorkID(
                    DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            returnedWorks =
                    APIService.getWorksByIdentifier(
                            DataQualityContext
                                    .manifestationDataObjectsFromEPHGD
                                    .get(0)
                                    .getMANIFESTATION_ID());
            break;
          case WM_IDENTIFIER:
            List<Map<String, Object>> manifIdentifiers =
                    getEPHGDManifestationIdentifiersByWorkID(
                            DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            returnedWorks =
                    APIService.getWorksByIdentifier(
                            manifIdentifiers.get(i).get("identifier").toString()
                                    + from
                                    + fromCntr
                                    + size
                                    + sizeCntr);

            Log.info(
                    "Total work found for WORK_MANIFESTATION_IDENTIFIER... - "
                            + returnedWorks.getTotalMatchCount());
            while (!returnedWorks.verifyWorkWithIdIsReturnedOnly(
                    DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID())
                    && fromCntr + sizeCntr < returnedWorks.getTotalMatchCount()) {
              fromCntr += sizeCntr;
              Log.info("scanned workId from " + (fromCntr - sizeCntr) + " to " + fromCntr + " records...");
              returnedWorks =
                      APIService.getWorksByIdentifier(
                              manifIdentifiers.get(i).get("identifier").toString()
                                      + from
                                      + fromCntr
                                      + size
                                      + sizeCntr);
            }

            break;



          default: throw new IllegalArgumentException(identifierType);
        }

        returnedWorks.verifyWorksAreReturned();
        returnedWorks.verifyWorkWithIdIsReturned(
                DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
      }

    } catch (Exception e) {
      e.getMessage();
      scenarioFailed();
    }
  }

  @When("^the work search by identifier (.*) and type details are retrieved and compared$")
  public void compareSearchByIdentifierAndTypeResultsWithDB(String identifierType) throws AzureOauthTokenFetchingException, ParseException {
    WorksMatchedApiObject returnedWorks=null;
    boolean failed = false;

      int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
      for (int i = 0; i < bound; i++) {
        if (identifierType.equals(W_IDENTIFIER)) {
          getWorkIdentifiersByWorkID(
                  DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
          returnedWorks =
                  APIService.getWorksByIdentifierAndType(
                          workIdentifiers.get(i).getIDENTIFIER(), workIdentifiers.get(i).getF_TYPE());
        } else if (identifierType.equals(WM_IDENTIFIER)) {
          List<Map<String, Object>> manifIdentifiers =
                  getEPHGDManifestationIdentifiersByWorkID(
                          DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
          returnedWorks =
                  APIService.getWorksByIdentifierAndType(
                          manifIdentifiers.get(i).get("identifier").toString(),
                          manifIdentifiers.get(i).get("f_type").toString());
        }
        returnedWorks.verifyWorksAreReturned();
        returnedWorks.verifyWorkWithIdIsReturned(
                DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
      }


  }

  @When("^the works retrieved by search (.*) details are retrieved and compared$")
  public void compareWorksRetrievdBySearchOptionWithDB(String searchType)
          throws AzureOauthTokenFetchingException, ParseException {
    // completed by Nishant @ 23-24 Apr 2020
    WorksMatchedApiObject returnedWorks = null;

    Log.info("searching work by..." + searchType);
    int bound = DataQualityContext.workDataObjectsFromEPHGD.size();

    for (int i = 0; i < bound; i++) {
      String searchOption = "";
      Boolean shouldResultOnTop=false;
      String workId = DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID();
      switch (searchType) {
        case W_ID:
        case "EPR_ID":        searchOption = workId;shouldResultOnTop = true;break;

        case W_TITLE:
        case "TITLE":  searchOption =  DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE();
          shouldResultOnTop = true;break;

        case W_IDENTIFIER:          getWorkIdentifiersByWorkID(workId);
                                    searchOption =workIdentifiers.get(i).getIDENTIFIER();
                                    shouldResultOnTop = true; break;

        case "JOURNAL_ACRONYM":     getWorkIdentifiersByWorkID(workId);
                                  for (WorkDataObject workIdentifier : workIdentifiers) {
                                    if (workIdentifier.getF_TYPE().equalsIgnoreCase("JOURNAL ACRONYM"))
                                      searchOption = workIdentifier.getIDENTIFIER();break;}
          shouldResultOnTop = true;break;

        case "JOURNAL_NUMBER":     getWorkIdentifiersByWorkID(workId);
                                  for (WorkDataObject workIdentifier : workIdentifiers) {
                                   if (workIdentifier.getF_TYPE().equalsIgnoreCase("ELSEVIER JOURNAL NUMBER"))
                                    searchOption = workIdentifier.getIDENTIFIER();break;}
          shouldResultOnTop = true;break;

        case "ISSN":              getWorkIdentifiersByWorkID(workId);
                                 for (WorkDataObject workIdentifier : workIdentifiers) {
                                    if (workIdentifier.getF_TYPE().equalsIgnoreCase("ISSN-L"))
                                      searchOption = workIdentifier.getIDENTIFIER();break;}
          shouldResultOnTop = true;break;
        case "WORK_PERSONS_FULLNAME":
          // implemented by Nishant @ 24 Apr 2020
          getWorkPersonRoleByWorkId(DataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_ID(),"all");
          getPersonDataByPersonId(DataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON());
          searchOption =DataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME()
                  + " "
                  + DataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME();break;

        case WM_ID:getManifestationsByWorkID(workId);
          searchOption =DataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID();break;

        case WM_TITLE:
          getManifestationsByWorkID(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
          searchOption =  DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE();break;

        case WM_IDENTIFIER:  List<Map<String, Object>> manifIdentifiers =getEPHGDManifestationIdentifiersByWorkID(workId);
          searchOption =  manifIdentifiers.get(0).get("identifier").toString();          break;

        case WMPR_ID:
          getManifestationsByWorkID(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
          getProductDetailByManifestationId(DataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
          searchOption =  DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID();          break;

        case WMPR_SUMMARYNAME:
          getManifestationsByWorkID(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
          getProductDetailByManifestationId(DataQualityContext.manifestationDataObjectsFromEPHGD.get(i).getMANIFESTATION_ID());
          searchOption =DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME();break;

        case WPR_ID:    getProductDetailByWorkId(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
          searchOption =  DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_ID();          break;

        case WPR_SUMMARYNAME:      getProductDetailByWorkId(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
          searchOption =  DataQualityContext.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME();break;

        default:throw new IllegalArgumentException(searchType);
      }
      returnedWorks = apiFun.workBySeachOption_Iterative(searchOption,i,shouldResultOnTop);
      assert returnedWorks != null;
      //returnedWorks.verifyWorksAreReturned();
      if(returnedWorks.verifyWorkWithIdIsReturnedOnly(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID()))
      {
      returnedWorks.verifyWorkWithIdIsReturned(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
      }
    }
  }

  @When("^the journal by search (.*) details are retrieved and compared$")
  public void compareWorksRetrievdByJournalSearchOptionWithDB(String searchType)throws AzureOauthTokenFetchingException {
    // created by Nishant @ 24-25 Apr 2020
    Log.info("searching by..." + searchType);
    int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
    for (int i = 0; i < bound; i++) {
      String workId = DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID();
      String resourceString = "";
      Boolean shouldResultOnTop = false;
      switch (searchType) {
        case "EPR_ID":              resourceString = workId; shouldResultOnTop=true;         break;
        case "TITLE":               resourceString = DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE().replace("&","%26");
                                      shouldResultOnTop=true;break;

        case "JOURNAL_ACRONYM":     getWorkIdentifiersByWorkID(workId);
                                  for (WorkDataObject workIdentifier : workIdentifiers) {
                                    if (workIdentifier.getF_TYPE().equalsIgnoreCase("JOURNAL ACRONYM"))
                                    {resourceString = workIdentifier.getIDENTIFIER();break;}
                                  }
                                  shouldResultOnTop=true;break;

        case "JOURNAL_NUMBER":      getWorkIdentifiersByWorkID(workId);
                                  for (WorkDataObject workIdentifier : workIdentifiers) {
                                    if (workIdentifier.getF_TYPE().equalsIgnoreCase("ELSEVIER JOURNAL NUMBER"))
                                    {resourceString = workIdentifier.getIDENTIFIER();break;}
                                  }
                          shouldResultOnTop=true;break;

        case "ISSN":                getWorkIdentifiersByWorkID(workId);
                                  for (WorkDataObject workIdentifier : workIdentifiers) {
                                    if (workIdentifier.getF_TYPE().equalsIgnoreCase("ISSN-L")
                                            & workIdentifier.getIDENTIFIER_EFFECTIVE_END_DATE()==null)
                                    {resourceString = workIdentifier.getIDENTIFIER();break;}
                                  }
          shouldResultOnTop=true;break;

        default:throw new IllegalArgumentException(searchType);

      }
      WorksMatchedApiObject returnedWorks = apiFun.workBySeachOption_Iterative(resourceString,i,shouldResultOnTop);
      assert returnedWorks != null;
      returnedWorks.verifyWorkWithIdIsReturned(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
    }
  }

  @When("^the work details are retrieved by PMC Code and compared$")
  public void compareWorkSearchByPMCResultsWithDB() throws AzureOauthTokenFetchingException {
    WorksMatchedApiObject returnedWorks;
    boolean failed = false;
      int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
      for (int i = 0; i < bound; i++) {
        Log.info(
                "pmc to be tested..." + DataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
        returnedWorks =
                APIService.getWorkByPMC(
                        DataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
        returnedWorks.verifyWorksAreReturned();
        printTotalWorkCount(returnedWorks);
        returnedWorks.verifyWorksReturnedCount(
                getNumberOfWorksByPMC(DataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC()));
      }
}

  @When("^the work details are retrieved by PMG Code and compared$")
  public void compareWorkSearchByPMGResultsWithDB() throws AzureOauthTokenFetchingException {
    WorksMatchedApiObject returnedWorks;
    boolean failed = false;
    try {
      int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
      for (int i = 0; i < bound; i++) {
        String pmgCode =
                getPMGcodeByPMC(DataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC());
        returnedWorks = APIService.getWorkByPMG(pmgCode);
        returnedWorks.verifyWorksAreReturned();
        printTotalWorkCount(returnedWorks);
        returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByPMG(pmgCode));
      }
    } catch (Exception e) {
      e.getMessage();
      scenarioFailed();
    }
  }

  @When("^the work response count is compared with the count in the DB for Person Id$")
  public void compareSearchResultCountForPersonIdresponse() throws AzureOauthTokenFetchingException {
    WorksMatchedApiObject returnedWorks;

    for (String id : ids) {
      Log.info("personId to be tested..." + id);
      returnedWorks = APIService.getWorksByPersonID(id+activeWorkTypeStatus);
     // returnedWorks.verifyWorksAreReturned();
      Log.info("Total API count matched..." + returnedWorks.getTotalMatchCount());
      Assert.assertEquals("personId:"+id,returnedWorks.getTotalMatchCount(),getNumberOfWorksByPerson("",id));
      //returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByPerson("",id));
    }
  }

  @When("^work response is compared with the DB for (.*)$")
  public void compareSearchResultCountForPersonOptions(String personSearchOption) throws AzureOauthTokenFetchingException {
    //created by Nishant @24 Apr 2020
    // updated by Nishant @ 10 Jun 2021, @19 Jan 2022
    WorksMatchedApiObject returnedWorks = null;
    int dbCount = 0;

    for (int i = 0; i < ids.size(); i++) {
      String resourceString = "";
      String workId = DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID();
      getWorkPersonRoleByWorkId(workId,"all");
      getPersonDataByPersonId(DataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON());

      Log.info("personId to be tested..." + personDataObjectsFromEPHGD.get(0).getPERSON_ID());
      setBreadcrumbMessage(personDataObjectsFromEPHGD.get(0).getPERSON_ID());

      switch (personSearchOption) {
        case "PERSON_NAME": resourceString = DataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FIRST_NAME()
                  + " "+ DataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FAMILY_NAME();
          dbCount =getNumberOfWorksByPerson(personSearchOption,DataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FIRST_NAME(),
                  DataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FAMILY_NAME());
          resourceString += activeWorkTypeStatus;
          returnedWorks = apiFun.workByParam_Iterative("personName",resourceString,i);    break;

        case PER_FULLNAME_CURRENT:
          getWorkPersonRoleByWorkId(workId,"ActivePersonOnly");
          getPersonDataByPersonId(DataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON());

          resourceString = DataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FIRST_NAME()
                 + " "+ DataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FAMILY_NAME();
          dbCount =getNumberOfWorksByPerson(personSearchOption,DataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FIRST_NAME(),
                  DataQualityContext.personDataObjectsFromEPHGD.get(i).getPERSON_FAMILY_NAME());
          resourceString += activeWorkTypeStatus;
          returnedWorks = apiFun.workByParam_Iterative("personFullNameCurrent",resourceString,i);    break;

        case "PEOPLE_HUB_ID":       resourceString = personDataObjectsFromEPHGD.get(0).getPEOPLEHUB_ID();
          dbCount =getNumberOfWorksByPerson(personSearchOption,resourceString);
          resourceString += activeWorkTypeStatus;
          returnedWorks = apiFun.workByParam_Iterative("personId",resourceString,i);       break;

        case "PERSON_ID":
        case "personIdCurrent":     resourceString = personDataObjectsFromEPHGD.get(0).getPERSON_ID();
          dbCount =getNumberOfWorksByPerson(personSearchOption,resourceString);
          resourceString += activeWorkTypeStatus;
          returnedWorks = apiFun.workByParam_Iterative("personId",resourceString,i);       break;

        default: throw new IllegalArgumentException(personSearchOption);
      }
      Log.info("Total API count - " + returnedWorks.getTotalMatchCount());

      returnedWorks.verifyWorksReturnedCount(dbCount);

      if (!personSearchOption.equalsIgnoreCase(PER_FULLNAME_CURRENT)) {
        if(returnedWorks.verifyWorkWithIdIsReturnedOnly(workId))
        {
          returnedWorks.verifyWorkWithIdIsReturned(workId);
          Log.info("verified intended work in search result...");
        }
        else
        {
          Log.info("intended work "+workId+" is missing in search result");
          Assert.assertFalse(getBreadcrumbMessage()+" intended work "+workId+" is missing in search result",true);
        }

      }
    }
  }

  public WorksMatchedApiObject callAPI_workByOption(String personSearchOption, String queryValue)
          throws AzureOauthTokenFetchingException {
    // created by Nishant @ 10 Jul 2020
    // verify Journal Finder search result with API
    Log.info("calling workAPI for... " + personSearchOption);
    WorksMatchedApiObject returnedWorks;
    returnedWorks = APIService.getWorksByParam(personSearchOption, queryValue);
    return returnedWorks;
  }

  @And("work details are retrieved by hasWorkComponent and compared")
  public void compareSearchResultCountForHasWorkComponent() throws AzureOauthTokenFetchingException {
    WorksMatchedApiObject returnedWorks;
    int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
    for (int i = 0; i < bound; i++) {
      returnedWorks =
              APIService.getWorkByHasWorkComponents(
                      DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
      returnedWorks.verifyWorksAreReturned();
      printTotalWorkCount(returnedWorks);
      returnedWorks.verifyWorksReturnedCount(
              getNumberOfWorksByHasWorkComponents(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID()));

    }

  }

  @And("work details are retrieved by isInPackage and compared")
  public void compareSearchResultCountForIsInPackage() throws AzureOauthTokenFetchingException {
    WorksMatchedApiObject returnedWorks;
    int bound = DataQualityContext.workDataObjectsFromEPHGD.size();
    for (int i = 0; i < bound; i++) {
      returnedWorks =getWorkByIsInPackage(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
      returnedWorks.verifyWorksAreReturned();
      printTotalWorkCount(returnedWorks);
      returnedWorks.verifyWorksReturnedCount(getNumberOfWorksByIsInPackage(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID()));
    }

  }

  @When("^verify works retrieved by search (.*) for excludeNonElsevier$")
  public void compareWorksRetrievdByexcludeNonElsevier(String searchKey)
          throws AzureOauthTokenFetchingException {

    WorksMatchedApiObject returnedWorks = null;
    Log.info("searching by..." + searchKey);

    int fromCntr = 0;
    int sizeCntr = 500;

    do{
      returnedWorks =
              APIService.getWorksBySearchOption(
                      searchKey+"&excludeNonElsevier=true"
                              + from
                              + fromCntr
                              + size
                              + sizeCntr);
      Log.info("Total work found to verify for nonElsevierInd to be false - " + returnedWorks.getTotalMatchCount());

      returnedWorks.verifyWorksAreReturned();
      WorkApiObject[] items = returnedWorks.getItems().clone();

      for (int i = 0; i < items.length; i++) {

          if (TestContext.getValues().environment.equalsIgnoreCase("UAT")) {
            Assert.assertEquals(
                "found nonElsevierInd work at index " + i,
                items[i]
                    .getWorkCore()
                    .getType()
                    .get("nonElsevierInd")
                    .toString()
                    .equalsIgnoreCase("false"),
                true);
       }
          else
          {
            try{
            if (items[i].getWorkCore().getType().get("nonElsevierInd") != null)
            {
              Assert.assertEquals(
                  "found nonElsevierInd work at index " + i,
                  items[i]
                      .getWorkCore()
                      .getType()
                      .get("nonElsevierInd")
                      .toString()
                      .equalsIgnoreCase("false"),
                  true);
            }
            }
            catch(Exception e)
            {
              Log.info(e.getMessage());
            }
          }


      }

      fromCntr += sizeCntr;
      Log.info("scanned workID from " + (fromCntr - sizeCntr) + " to " + fromCntr + " records...");

    }
    while (fromCntr < returnedWorks.getTotalMatchCount());

  }


  @When("^verify pagination duplicate ids retried by search (.*)$")
  public void verifyPaginationByworkSearch(String searchKey)
          throws AzureOauthTokenFetchingException {

    WorksMatchedApiObject returnedWorks = null;
    Log.info("searching by..." + searchKey);

    int fromCntr = 0;
    int sizeCntr = 20;

    ArrayList<String> idsToVerify = new ArrayList<>();
    do{
      returnedWorks =
              APIService.getWorksBySearchOption(
                      searchKey
                              + from
                              + fromCntr
                              + size
                              + sizeCntr);
      Log.info("Total work found - " + returnedWorks.getTotalMatchCount());
      Log.info("scanning workID from " + (fromCntr) + " to " + (fromCntr+sizeCntr) + " records...");

      returnedWorks.verifyWorksAreReturned();
      WorkApiObject[] items = returnedWorks.getItems().clone();
      Log.info("previous ids...");
      Log.info(idsToVerify.toString());

      for (int i = 0; i < items.length; i++) {
        Assert.assertFalse("duplicate found at index "+i+" "+items[i].getId().toString(),
                idsToVerify.contains(items[i].getId().toString()));
        idsToVerify.add(items[i].getId());
      }
      fromCntr += sizeCntr;
    }
    while (fromCntr < returnedWorks.getTotalMatchCount());

  }


  @When("^verify pagination duplicate ids retried for work (.*) with (.*)$")
  public void verifyPaginationForWork(String keyword, String scroll)  throws AzureOauthTokenFetchingException {

    WorksMatchedApiObject returnedWorks = null;
    Log.info("searching for workPackage..." + keyword);

    int fromCntr = 0;    int sizeCntr = 50;    String scrollId = "";
    ArrayList<String> idsToVerify = new ArrayList<>();

    do{
      //call this API first
      if (fromCntr == 0) {
        returnedWorks =
                APIService.getWorksBySearchOption(keyword +"&_alt=1"+ from + fromCntr + size + sizeCntr + "&reverse=true&scroll=" + scroll);
        scrollId = returnedWorks.getScrollId(); //get scroll id from response
      }
      else //call scroll API for rest of iterations
      {
        returnedWorks = APIService.getWorksByScrollId(scrollId+"?scroll=" + scroll);
      }

      Log.info("Total work found - " + returnedWorks.getTotalMatchCount());
      Log.info("scanning workID from " + (fromCntr) + " to " + (fromCntr+sizeCntr) + " records...");

      returnedWorks.verifyWorksAreReturned();
      WorkApiObject[] items = returnedWorks.getItems().clone();
      Log.info("previous ids...");      Log.info(idsToVerify.toString());

      for (int i = 0; i < items.length; i++) {
        Assert.assertFalse("duplicate found at index "+i+" "+items[i].getId().toString(),
                idsToVerify.contains(items[i].getId().toString()));
        idsToVerify.add(items[i].getId());
      }
      fromCntr += sizeCntr;
    }
    while (fromCntr < returnedWorks.getTotalMatchCount());
  }





  @And("^get work by manifestation$")
  public static void getWorkByManifestation() {
    sql = String.format(APIDataSQL.GET_GD_DATA_WORK_BY_MANIFESTATION,Joiner.on("','").join(manifestaionids));
    List<Map<?, ?>> workIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    ids =
            workIds.stream()
                    .map(m -> (String) m.get(W_ID))
                    .map(String::valueOf)
                    .collect(Collectors.toList());
  }

  private static int getNumberOfWorksByAccountableProduct(String accountableProduct) {
    sql = String.format(APIDataSQL.SELECT_GD_COUNT_WORK_BY_ACCOUNTABLEPRODUCT, accountableProduct);
    List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    int count = ((Long) getCount.get(0).get("count")).intValue();
    Log.info("EPH work count..." + count);
    return count;
  }

  private static int getNumberOfWorksByWorkStatus(String searchKeyword, String workStatus) {
    sql =APIDataSQL.SELECT_GD_COUNT_WORK_BY_WORKSTATUS_WITHSEARCH
            .replaceAll("PARAM1", workStatus )
            .replaceAll("PARAM2",searchKeyword);
    List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    int count = ((Long) getCount.get(0).get("count")).intValue();
    Log.info("EPH work count..." + count);
    return count;
  }

  private static int getNumberOfWorksByWorkType(String searchKeyword, String workType) {
    sql =APIDataSQL.SELECT_GD_COUNT_WORK_BY_WORKTYPE_WITHSEARCH
            .replaceAll("PARAM1",workType)
            .replaceAll("PARAM2",searchKeyword);
    List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    int count = ((Long) getCount.get(0).get("count")).intValue();
    Log.info("EPH work count..." + count);
    return count;
  }

  private static int getNumberOfWorksByManifestationType(String searchKeyword, String manifestationType) {
    sql =
            String.format(
                    APIDataSQL.SELECT_GD_COUNT_WORK_BY_MANIFESTATIONTPYE_WITHSEARCH,
                    searchKeyword,
                    manifestationType);
    List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    int count = ((Long) getCount.get(0).get("count")).intValue();
    Log.info("EPH count..." + count);
    return count;
  }

  private static int getNumberOfWorksBySearchWithPMCCode(String searchKeyword, String PMCCode) {
    sql =
            String.format(
                    APIDataSQL.SELECT_GD_COUNT_WORK_BY_PMC_WITHSEARCH, searchKeyword, PMCCode);
    List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    int count = ((Long) getCount.get(0).get("count")).intValue();
    Log.info("EPH count..." + count);
    return count;
  }

  private static int getNumberOfWorksBySearchWithPMGCode(String searchKeyword, String PMCCode) {
    int count=0;
    sql =
            String.format(
                    APIDataSQL.SELECT_GD_COUNT_WORK_BY_PMG_WITHSEARCH, searchKeyword, PMCCode);
    try{
    List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
     count = ((Long) getCount.get(0).get("count")).intValue();
    }
    catch (NullPointerException e)
    {
      Log.info(sql);
      Assert.assertFalse(getBreadcrumbMessage()+e.getStackTrace(),true);
    }
    Log.info("EPH count..." + count);
    return count;
  }

  private static int getNumberOfWorksByPMC(String pmcCode) {
    sql = String.format(APIDataSQL.SELECT_GD_COUNT_WORK_BY_PMC, pmcCode);
    List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    int count = ((Long) getCount.get(0).get("count")).intValue();
    Log.info("EPH work count... " + count);
    return count;
  }


  public static int getNumberOfWorksByHasWorkComponents(String WorkId) {
    sql = String.format(APIDataSQL.SELECT_GD_COUNT_WORK_BY_HASWORKCOMPONENTS, WorkId);
    List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    int count = ((Long) getCount.get(0).get("count")).intValue();
    Log.info("EPH work count... " + count);
    return count;
  }


  public static int getNumberOfWorksByIsInPackage(String WorkId) {
    sql = String.format(APIDataSQL.SELECT_GD_COUNT_WORK_BY_ISINPACKAGE, WorkId);
    List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    int count = ((Long) getCount.get(0).get("count")).intValue();
    Log.info("EPH work count... " + count);
    return count;
  }

  private static int getNumberOfWorksByPerson(String searchType, String fName,String lName) {

  //  String specialName = "";
    List<String> partialName = new ArrayList<String>();

    fName=fName.replaceAll("-"," ").replace("'"," ");
    lName=lName.replaceAll("-"," ").replace("'"," ");
  String[] arr_partialName = (fName+" "+lName).replaceAll("-"," ").split(" ");

//if(arr_specialName.length>1) specialName = arr_specialName[1];
//if(specialName.equalsIgnoreCase(""))specialName=fName;
    String partialQuery1 = "";
    String partialQuery2 = "";
    for(int i=0;i<arr_partialName.length;i++)
    {
      if(arr_partialName[i].length()<2)continue;
      partialQuery1 += " or name ~* '(?c)\\\\m"+arr_partialName[i]+"\\\\M'\n";
      partialQuery2 += " or given_name ~* '(?c)\\\\m"+arr_partialName[i]+"\\\\M'\n";
    }


  //  for(int i=0;i<partialName.size();i++)
  //  {partialQuery2 += "or given_name ~* '\\m"+partialName.get(i)+"\\M'";}

    for(int i=0;i<arr_partialName.length;i++)
    {
      if(arr_partialName[i].length()<2)continue;
      partialQuery2 += " or family_name ~* '(?c)\\\\m"+arr_partialName[i]+"\\\\M'\n";}

    int count =0;
    switch (searchType) {
      case PER_FULLNAME_CURRENT:sql = String.format(APIDataSQL.SELET_GD_COUNT_WORK_BY_PERSONNAMECURRENT, fName,lName);break;
      default://sql = String.format(APIDataSQL.SELET_GD_COUNT_WORK_BY_PERSONNAME_withExtendedperson, fName,lName,fName,lName);     break;
        sql = APIDataSQL.SELET_GD_COUNT_WORK_BY_PERSONNAME_withExtendedperson
                .replaceAll("FIRSTNAME",fName)
                .replaceAll("LASTNAME",lName)
                .replaceAll("PARTIALQUERY1",partialQuery1)
                .replaceAll("PARTIALQUERY2",partialQuery2);
        break;
         }

    try
    {
      List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
      count = ((Long) getCount.get(0).get("count")).intValue();
    }
    catch(Exception e )
    {
      Assert.assertFalse(getBreadcrumbMessage(),true);
    }
    Log.info("EPH work count..." + count);
    return count;
  }

  private static int getNumberOfWorksByPerson(String searchType, String personId) {
    int count =0;
    switch (searchType) {
      case "PEOPLE_HUB_ID":sql = String.format(APIDataSQL.SELECT_GD_COUNT_WORK_BY_PEOPLEHUBID, personId);        break;
      default:        sql = String.format(APIDataSQL.SELECT_GD_COUNT_WORK_BY_PERSONID, personId);        break;
    }
    try{
      List<Map<String, Object>> getCount = DBManager.getDBResultMap(sql, Constants.EPH_URL);
      count = ((Long) getCount.get(0).get("count")).intValue();
    }
    catch(Exception e )
    {
      Assert.assertFalse(getBreadcrumbMessage(),true);
    }
    Log.info("EPH work count..." + count);
    return count;
  }

  private static int getNumberOfWorksByPMG(String pmgCode) {
    sql = String.format(APIDataSQL.SELECT_GD_COUNT_WORK_BY_PMG, pmgCode);
    List<Map<String, Object>> subjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    int count = ((Long) subjectAreaNumber.get(0).get("count")).intValue();
    Log.info("EPH work count..." + count);
    return count;
  }

  public static String getPMGcodeByPMC(String pmcCode) {
    sql = String.format(APIDataSQL.SELECT_GD_PMG_BY_PMC, pmcCode);
    Log.info(sql);
    List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    String pmgCode = ((String) getPMG.get(0).get("f_pmg"));
    Log.info("pmg code..." + pmgCode);
    return pmgCode;
  }

  public static List<String> getManifestationIdsForWorkID(String workID) {
    sql = String.format(APIDataSQL.SELECT_GD_MANIFESTATIONS_BY_WORKID, workID);
    List<Map<?, ?>> manifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    List<String> ids =
            manifestationIds.stream()
                    .map(m -> (String) m.get("manifestation_id"))
                    .map(String::valueOf)
                    .collect(Collectors.toList());
    Log.info("Manifestation ids for the work: " + ids);
    Assert.assertFalse(
            getBreadcrumbMessage()
                    + "-> Verify that manifestation ids can be successfully extracted from db by work ids",
            ids.isEmpty());
    return ids;
  }

  // created by Nishant @ 22 Apr 2020
  public static void getProductDetailByManifestationId(String manifestationId) {
    Log.info("get product by manifestation id of the work...");
    sql = String.format(APIDataSQL.GET_GD_DATA_PRODUCT_BY_MANIFESTATIONID, manifestationId);
    DataQualityContext.productDataObjectsFromEPHGD =
            DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    Assert.assertFalse(
            getBreadcrumbMessage()
                    + "-> Verify that product by manifestation id is extracted successfully from the DB",
            DataQualityContext.productDataObjectsFromEPHGD.isEmpty());
  }

  public static void getProductDetailByWorkId(String workId) {
    Log.info("get product by work id...");
    sql = String.format(APIDataSQL.GET_GD_DATA_PRODUCT_BY_WORKID, workId);
    DataQualityContext.productDataObjectsFromEPHGD =
            DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    Assert.assertFalse(
            getBreadcrumbMessage()
                    + " - Verify that product by work id is extracted successfully from the DB",
            productDataObjectsFromEPHGD.isEmpty());
  }

  // created by Nishant @ 24 Apr 2020
  private static void getWorkPersonRoleByWorkId(String workId,String personType) {

    if(personType.equalsIgnoreCase("ActivePersonOnly"))
    {
      sql = String.format(APIDataSQL.GET_GD_DATA_ACTIVE_WORKPERSON_BY_WORKID, workId);
    } else {
      sql = String.format(APIDataSQL.GET_GD_DATA_WORKPERSON_BY_WORKID, workId);
    }
    DataQualityContext.personWorkRoleDataObjectsFromEPHGD =
            DBManager.getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);
    Assert.assertFalse(
            getBreadcrumbMessage()
                    + "-> Verify person role by work id successfully extracted from EPH DB",
            personWorkRoleDataObjectsFromEPHGD.isEmpty());
  }

  private static List getEPHGDManifestationIdentifiersByWorkID(String workID) {
    sql =
            String.format(
                    APIDataSQL.SELECT_GD_MANIFESTATION_IDENTIFIER_BY_MANIFESTATION_ID,
                    Joiner.on("','").join(getManifestationIdsForWorkID(workID)));

    return DBManager.getDBResultMap(sql, Constants.EPH_URL);
  }

  public static void getWorkIdentifiersByWorkID(String workID) {
    sql = APIDataSQL.GET_GD_DATA_WORKIDENTIFIER_BY_WORKID.replace("PARAM1", workID);
    workIdentifiers = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    // added by Nishant @ 28 Dec 2019 to handle works without identifiers
    if (workIdentifiers.size() == 0) {
      Log.info("Error - random work do not have any identifier hence scenario failure");
    }
  }

  private static void getManifestationsByWorkID(String workID) {
    Log.info("get manifestations for the work in EPH GD ..");
    sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_WORKID, workID);
    DataQualityContext.manifestationDataObjectsFromEPHGD =
            DBManager.getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    Assert.assertFalse(
            getBreadcrumbMessage()
                    + "-> Verify that manifestaion by work id is extracted successfully from the DB",
            manifestationDataObjectsFromEPHGD.isEmpty());
  }

  private static void getPersonDataByPersonId(String personId) {
    sql = String.format(APIDataSQL.GET_GD_DATA_PERSON_BY_PERSONID, personId);
    DataQualityContext.personDataObjectsFromEPHGD =
            DBManager.getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
    Assert.assertFalse(
            getBreadcrumbMessage()
                    + "-> verify person Data by person id extracted from EPH DB",
            personDataObjectsFromEPHGD.isEmpty());
  }

  private static void getPersonDataByPersonId(List<String> personId) {
    sql = String.format(APIDataSQL.GET_GD_DATA_PERSON_BY_PERSONID, Joiner.on("','").join(personId));
    DataQualityContext.personDataObjectsFromEPHGD =
            DBManager.getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
    DataQualityContext.personDataObjectsFromEPHGD.sort(
            Comparator.comparing(PersonDataObject::getPERSON_ID));
    Assert.assertFalse(
            getBreadcrumbMessage()
                    + "-> verify person Data by person id extracted from EPH DB",
            personDataObjectsFromEPHGD.isEmpty());
  }

  public static void getJsonToObject(String workId) {
    // created by Nishant @ 19 Jun 2020 to verify extended json value with APIv3
    String dbEnv = "uat";
    if (TestContext.getValues().environment.equalsIgnoreCase("UAT"))
      dbEnv = "uat";
    else if (TestContext.getValues().environment.equalsIgnoreCase("SIT")) dbEnv = "sit";
    sql =
            "SELECT \"json\" FROM eph"+dbEnv+"_extended_data_stitch.stch_work_ext_json where epr_id='"
                    + workId
                    + "'";


    List<Map<String, String>> jsonValue = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    DataQualityContext.workExtendedTestClass =
            new Gson().fromJson(jsonValue.get(0).get("json"), WorkExtendedTestClass.class);
  }

  @And("^get person data from EPH DB$")
  public void getPersonDataByPersonId() {
    Log.info("We get the work data from EPH GD ...");
    getPersonDataByPersonId(ids);

  }

  public static void verifyListNotEmpty(List<String> ids)
  {
    Assert.assertFalse(getBreadcrumbMessage() + "-> Verify That list with random ids is not empty.",ids.isEmpty());
  }
  public static void printTotalWorkCount(WorksMatchedApiObject returnedWorks )
  {
    Log.info("API total matched count..." + returnedWorks.getTotalMatchCount());
  }

  public static void scenarioFailed()
  {Assert.assertFalse(getBreadcrumbMessage() + " scenario Failed ", true);}
/*
  public static String getSearchKeyword(String title)
  {
    //created by Nishant @ 31 Jan 2022
    String keyword = "";
    String[] arr_title= title.replaceAll("[^a-zA-Z0-9]", " ").split(" ");
    keyword=arr_title[arr_title.length-1];
    return keyword;
  }*/
}
