package com.eph.automation.testing.steps.sdbooks;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.SDAccessDLContext;
import com.eph.automation.testing.models.dao.sdbooks.SDBooksDLAccessObject;
import com.eph.automation.testing.services.db.sdbookssql.SDBooksDataChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class SDBooksDataChecksSteps {

    private static String sql;
    private static List<String> ids;

   @Given("^We get the (.*) random ISBN ids (.*)$")
    public static void getRandomISBNIds(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random URL ISBN Ids...");
        switch (tableName) {
            case "sdbooks_inbound_part":
                sql = String.format(SDBooksDataChecksSQL.GET_RANDOM_ISBN_INBOUND, numberOfRecords);
               break;
            case "sdbooks_transform_current_urls":
                sql = String.format(SDBooksDataChecksSQL.GET_RANDOM_ISBN_CURRENT_URL, numberOfRecords);
                 break;
            case "sdbooks_transform_file_history_urls_part":
                sql = String.format(SDBooksDataChecksSQL.GET_RANDOM_ISBN_CURRENT_URL, numberOfRecords);
                break;
            case "sdbooks_delta_current_urls":
                sql = String.format(SDBooksDataChecksSQL.GET_RANDOM_ISBN_URL_DIFF_CURR_PREV_TRANS_FILE, numberOfRecords);
                break;
            case "sdbooks_transform_history_excl_delta":
                sql = String.format(SDBooksDataChecksSQL.GET_RANDOM_ISBN_DIFF_DELTA_CURR_HIST_URL, numberOfRecords);
                break;
            case "sdbooks_transform_latest_urls":
                sql = String.format(SDBooksDataChecksSQL.GET_RANDOM_ISBN_SUM_DELTA_EXCL_URL, numberOfRecords);
                break;
            case "sdbooks_delta_history_urls_part":
                sql = String.format(SDBooksDataChecksSQL.GET_RANDOM_ISBN_DELTA_CURR, numberOfRecords);
                break;
            default:
                Log.info("No tables found");
          }
        List<Map<?, ?>> randomIsbnIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomIsbnIds.stream().map(m -> (String) m.get("isbn")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^Get the records from data inbound for URL$")
    public static void getUrlRecordsFromFullLoad() {
        Log.info("We get the Inbound Load URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_URL_INBOUND, String.join("','",ids));
        Log.info(sql);
        SDAccessDLContext.recordsFromInboundData = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform SD current URL$")
    public static void getRecordsFromCurrentURL() {
        Log.info("We get the records from Current URL table...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_CURRENT_URL, String.join("','",ids));
        Log.info(sql);
        SDAccessDLContext.recordsFromCurrentUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^we compare records of Inbound and current URL$")
    public static void compareDataFullandCurrentUrl() {
        if (SDAccessDLContext.recordsFromInboundData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL Full Load and Current URL...");
            for (int i = 0; i < SDAccessDLContext.recordsFromInboundData.size(); i++) {

                SDAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(SDBooksDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDAccessDLContext.recordsFromCurrentUrl.sort(Comparator.comparing(SDBooksDLAccessObject::getisbn));

                Log.info("Full Load -> ISBN => " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() +
                        "Current_URL -> ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn());
                if (SDAccessDLContext.recordsFromInboundData.get(i).getisbn() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() != null)) {
                    Assert.assertEquals("The ISBN is =" + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() + " is missing/not found in Current_URL table",
                            SDAccessDLContext.recordsFromInboundData.get(i).getisbn(),
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() +
                        " BOOK_TITLE => Full_Load =" + SDAccessDLContext.recordsFromInboundData.get(i).getbookTitle() +
                        " Current_URL=" + SDAccessDLContext.recordsFromCurrentUrl.get(i).getbookTitle());

                if (SDAccessDLContext.recordsFromInboundData.get(i).getbookTitle() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrl.get(i).getbookTitle() != null)) {
                   Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() ,
                           SDAccessDLContext.recordsFromInboundData.get(i).getbookTitle(),
                           SDAccessDLContext.recordsFromCurrentUrl.get(i).getbookTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() +
                        " URL => Full_Load =" + SDAccessDLContext.recordsFromInboundData.get(i).geturl() +
                        " Current_URL=" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturl());

                if (SDAccessDLContext.recordsFromInboundData.get(i).geturl() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturl() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromInboundData.get(i).geturl(),
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturl());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() +
                        " URL_CODE => Full_Load =" + SDAccessDLContext.recordsFromInboundData.get(i).geturlCode() +
                        " Current_URL=" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlCode());

                if (SDAccessDLContext.recordsFromInboundData.get(i).geturlCode() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlCode() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromInboundData.get(i).geturlCode(),
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlCode());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() +
                        " URL_NAME => Full_Load =" + SDAccessDLContext.recordsFromInboundData.get(i).geturlName() +
                        " Current_URL=" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlName());

                if (SDAccessDLContext.recordsFromInboundData.get(i).geturlName() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlName() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromInboundData.get(i).geturlName(),
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlName());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() +
                        " URL_TITLE => Full_Load =" + SDAccessDLContext.recordsFromInboundData.get(i).geturlTitle() +
                        " Current_URL=" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlTitle());

                if (SDAccessDLContext.recordsFromInboundData.get(i).geturlTitle() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlTitle() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromInboundData.get(i).geturlTitle(),
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() +
                        " EPRID => Full_Load =" + SDAccessDLContext.recordsFromInboundData.get(i).geteprid() +
                        " Current_URL=" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geteprid());

                if (SDAccessDLContext.recordsFromInboundData.get(i).geteprid() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrl.get(i).geteprid() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromInboundData.get(i).geteprid(),
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geteprid());
                }
                Log.info("ISBN => " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() +
                        " EPRID => Full_Load =" + SDAccessDLContext.recordsFromInboundData.get(i).getworkType() +
                        " Current_URL=" + SDAccessDLContext.recordsFromCurrentUrl.get(i).getworkType());

                if (SDAccessDLContext.recordsFromInboundData.get(i).geteprid() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrl.get(i).geteprid() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + SDAccessDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromInboundData.get(i).getworkType(),
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).getworkType());
                }
            }
        }
    }

    @When("^We Get the records from transform SD url History (.*)$")
    public static void getUrlRecordsFromCurrentHist(String table) {
        Log.info("We get the transform Hist URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_CURRENT_HIST_URL, String.join("','",ids));
        SDAccessDLContext.recordsFromCurrentUrlHistory = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }


    @When("^We Get the records from transform File SD url$")
    public static void getUrlRecFromTransformFile() {
         Log.info("We get the transform File URL records...");
         sql = String.format(SDBooksDataChecksSQL.GET_REC_CURRENT_TRANS_FILE_URL, String.join("','",ids));
         SDAccessDLContext.recordsFromCurrentUrlTranFile = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
         Log.info(sql);
    }

    @And("^we compare records of SD current url and SD current url history$")
    public void compareDataCurrentAndHistUrl() {
        if (SDAccessDLContext.recordsFromCurrentUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL Current and Curr Hist URL...");
            for (int i = 0; i < SDAccessDLContext.recordsFromCurrentUrl.size(); i++) {

                SDAccessDLContext.recordsFromCurrentUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDAccessDLContext.recordsFromCurrentUrlHistory.sort(Comparator.comparing(SDBooksDLAccessObject::getisbn));

                Log.info("Current_URL -> ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        "Current_URL_Hist -> ISBN => " + SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).getisbn());
                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).getisbn() != null)) {
                    Assert.assertEquals("The ISBN is =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() + " is missing/not found in Current_URL table",
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn(),
                            SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).getisbn());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " BOOK_TITLE => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).getbookTitle() +
                        " Current_URL_Hist=" + SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).getbookTitle());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).getbookTitle() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).getbookTitle() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).getbookTitle(),
                            SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).getbookTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " URL => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturl() +
                        " Current_URL_Hist=" + SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geturl());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturl() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturl() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturl(),
                            SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geturl());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " URL_CODE => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlCode() +
                        " Current_URL_Hist=" + SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geturlCode());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlCode() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geturlCode() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlCode(),
                            SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geturlCode());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " URL_NAME => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlName() +
                        " Current_URL_Hist=" + SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geturlName());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlName() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geturlName() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlName(),
                            SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geturlName());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " URL_TITLE => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlTitle() +
                        " Current_URL_Hist =" + SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geturlTitle());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlTitle() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geturlTitle() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlTitle(),
                            SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geturlTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " EPRID => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geteprid() +
                        " Current_URL_Hist =" + SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geteprid());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).geteprid() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geteprid() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geteprid(),
                            SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).geteprid());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " WORK_TYPE => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).getworkType() +
                        " Current_URL_Hist=" + SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).getworkType());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).getworkType() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).getworkType() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).getworkType(),
                            SDAccessDLContext.recordsFromCurrentUrlHistory.get(i).getworkType());
                }
            }
        }
    }

    @And("^we compare records of SD current url and SD transform file url history$")
    public void compareDataCurrentAndTransformFile() {
        if (SDAccessDLContext.recordsFromCurrentUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL Current and Transform File URL...");
            for (int i = 0; i < SDAccessDLContext.recordsFromCurrentUrl.size(); i++) {

                SDAccessDLContext.recordsFromCurrentUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDAccessDLContext.recordsFromCurrentUrlTranFile.sort(Comparator.comparing(SDBooksDLAccessObject::getisbn));

                Log.info("Current_URL -> ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        "Trans_File -> ISBN => " + SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).getisbn());
                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).getisbn() != null)) {
                    Assert.assertEquals("The ISBN is =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() + " is missing/not found in Current_URL table",
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn(),
                            SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).getisbn());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " BOOK_TITLE => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).getbookTitle() +
                        " Trans_File=" + SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).getbookTitle());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).getbookTitle() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).getbookTitle() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).getbookTitle(),
                            SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).getbookTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " URL => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturl() +
                        " Trans_File=" + SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geturl());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturl() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturl() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturl(),
                            SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geturl());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " URL_CODE => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlCode() +
                        " Trans_File=" + SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geturlCode());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlCode() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geturlCode() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlCode(),
                            SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geturlCode());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " URL_NAME => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlName() +
                        " Trans_File=" + SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geturlName());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlName() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geturlName() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlName(),
                            SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geturlName());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " URL_TITLE => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlTitle() +
                        " Trans_File =" + SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geturlTitle());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlTitle() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geturlTitle() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geturlTitle(),
                            SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geturlTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " EPRID => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).geteprid() +
                        " Trans_File =" + SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geteprid());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).geteprid() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geteprid() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).geteprid(),
                            SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).geteprid());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() +
                        " WORK_TYPE => Current_URL =" + SDAccessDLContext.recordsFromCurrentUrl.get(i).getworkType() +
                        " Trans_File=" + SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).getworkType());

                if (SDAccessDLContext.recordsFromCurrentUrl.get(i).getworkType() != null ||
                        (SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).getworkType() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + SDAccessDLContext.recordsFromCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromCurrentUrl.get(i).getworkType(),
                            SDAccessDLContext.recordsFromCurrentUrlTranFile.get(i).getworkType());
                }
            }
        }
    }


    @Then("^Get the records from SD transform previous url$")
    public static void getRecordsFromPreviousURL() {
        Log.info("We get the records from Previous URL table...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_PREVIOUS_URL, String.join("','",ids));
        Log.info(sql);
        SDAccessDLContext.recordsFromPreviousUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of SD previous url and previous url history$")
    public void compareDataPreviousAndHistUrl() {
        if (SDAccessDLContext.recordsFromPreviousUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL PRevious and Hist URL...");
            for (int i = 0; i < SDAccessDLContext.recordsFromPreviousUrl.size(); i++) {

                SDAccessDLContext.recordsFromPreviousUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDAccessDLContext.recordsFromPreviousUrlHistory.sort(Comparator.comparing(SDBooksDLAccessObject::getisbn));

                Log.info("Previous_URL -> ISBN => " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() +
                        "Previous_URL_Hist -> ISBN => " + SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).getisbn());
                if (SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() != null ||
                        (SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).getisbn() != null)) {
                    Assert.assertEquals("The ISBN is =" + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() + " is missing/not found in Previous_URL table",
                            SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn(),
                            SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).getisbn());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() +
                        " BOOK_TITLE => Previous_URL =" + SDAccessDLContext.recordsFromPreviousUrl.get(i).getbookTitle() +
                        " Previous_URL_Hist=" + SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).getbookTitle());

                if (SDAccessDLContext.recordsFromPreviousUrl.get(i).getbookTitle() != null ||
                        (SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).getbookTitle() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromPreviousUrl.get(i).getbookTitle(),
                            SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).getbookTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() +
                        " URL => Previous_URL =" + SDAccessDLContext.recordsFromPreviousUrl.get(i).geturl() +
                        " Previous_URL_Hist=" + SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geturl());

                if (SDAccessDLContext.recordsFromPreviousUrl.get(i).geturl() != null ||
                        (SDAccessDLContext.recordsFromPreviousUrl.get(i).geturl() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromPreviousUrl.get(i).geturl(),
                            SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geturl());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() +
                        " URL_CODE => Previous_URL =" + SDAccessDLContext.recordsFromPreviousUrl.get(i).geturlCode() +
                        " Previous_URL_Hist=" + SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geturlCode());

                if (SDAccessDLContext.recordsFromPreviousUrl.get(i).geturlCode() != null ||
                        (SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geturlCode() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromPreviousUrl.get(i).geturlCode(),
                            SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geturlCode());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() +
                        " URL_NAME => Previous_URL =" + SDAccessDLContext.recordsFromPreviousUrl.get(i).geturlName() +
                        " Previous_URL_Hist=" + SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geturlName());

                if (SDAccessDLContext.recordsFromPreviousUrl.get(i).geturlName() != null ||
                        (SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geturlName() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromPreviousUrl.get(i).geturlName(),
                            SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geturlName());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() +
                        " URL_TITLE => Previous_URL =" + SDAccessDLContext.recordsFromPreviousUrl.get(i).geturlTitle() +
                        " Previous_URL_Hist=" + SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geturlTitle());

                if (SDAccessDLContext.recordsFromPreviousUrl.get(i).geturlTitle() != null ||
                        (SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geturlTitle() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromPreviousUrl.get(i).geturlTitle(),
                            SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geturlTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() +
                        " EPRID => Previous_URL =" + SDAccessDLContext.recordsFromPreviousUrl.get(i).geteprid() +
                        " Previous_URL_Hist=" + SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geteprid());

                if (SDAccessDLContext.recordsFromPreviousUrl.get(i).geteprid() != null ||
                        (SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geteprid() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromPreviousUrl.get(i).geteprid(),
                            SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).geteprid());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() +
                        " WORK_TYPE => Full_Load =" + SDAccessDLContext.recordsFromPreviousUrl.get(i).getworkType() +
                        " Previous_URL=" + SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).getworkType());

                if (SDAccessDLContext.recordsFromPreviousUrl.get(i).getworkType() != null ||
                        (SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).getworkType() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + SDAccessDLContext.recordsFromPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromPreviousUrl.get(i).getworkType(),
                            SDAccessDLContext.recordsFromPreviousUrlHistory.get(i).getworkType());
                }
            }
        }
    }

    @When("^Get the records from SDBooks for Delta Current Url$")
    public static void getRecordsDeltaCurrURL() {
        Log.info("We get the records from Delta Current URL table...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_DELTA_CURRENT_URL, String.join("','",ids));
        Log.info(sql);
        SDAccessDLContext.recordsFromDeltaCurrentUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^We Get the data from the difference of SD Current and Previous transform_file table$")
    public static void getRecordsFromDiffofCurrPrevURL() {
        Log.info("We get the records from Diff from Current and previous URL table...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_DIFF_OF_CURR_PREV_TRANS_FILE, String.join("','",ids));
        Log.info(sql);
        SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^compare the records of SD delta url with difference of current and previous transform_file$")
    public void compareDataDeltaCurrentUrl() {
        if (SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL PRevious and Hist URL...");
            for (int i = 0; i < SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.size(); i++) {

                SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDAccessDLContext.recordsFromDeltaCurrentUrl.sort(Comparator.comparing(SDBooksDLAccessObject::getisbn));

                Log.info("Diff_Curr_Prev -> ISBN => " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() +
                        "Delta_Current -> ISBN => " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn());
                if (SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() != null)) {
                    Assert.assertEquals("The ISBN is =" + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() + " is missing/not found in Diff_Curr_Prev table",
                            SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn(),
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() +
                        " BOOK_TITLE => Diff_Curr_Prev =" + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getbookTitle() +
                        " Delta_Current=" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getbookTitle());

                if (SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getbookTitle() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getbookTitle() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getbookTitle(),
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getbookTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() +
                        " URL => Diff_Curr_Prev =" + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturl() +
                        " Delta_Current=" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturl());

                if (SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturl() != null ||
                        (SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturl() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturl(),
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturl());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() +
                        " URL_CODE => Diff_Curr_Prev =" + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturlCode() +
                        " Delta_Current=" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlCode());

                if (SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturlCode() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlCode() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturlCode(),
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlCode());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() +
                        " URL_NAME => Diff_Curr_Prev =" + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturlName() +
                        " Delta_Current=" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlName());

                if (SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturlName() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlName() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturlName(),
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlName());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() +
                        " URL_TITLE => Diff_Curr_Prev =" + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturlTitle() +
                        " Delta_Current=" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlTitle());

                if (SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturlTitle() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlTitle() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geturlTitle(),
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() +
                        " EPRID => Diff_Curr_Prev =" + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geteprid() +
                        " Delta_Current=" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geteprid());

                if (SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geteprid() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geteprid() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).geteprid(),
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geteprid());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() +
                        " WORK_TYPE => Diff_Curr_Prev =" + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getworkType() +
                        " Delta_Current=" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getworkType());

                if (SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getworkType() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getworkType() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getworkType(),
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getworkType());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() +
                        " DELTA_MODE => Diff_Curr_Prev =" + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getdeltaMode() +
                        " Delta_Current=" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getdeltaMode());

                if (SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getdeltaMode() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getdeltaMode() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getdeltaMode(),
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getdeltaMode());
                }
            }
        }
    }

    @When("^we get the records from the difference of SD Delta_current_url and url_history$")
    public static void getDiffDeltaAndCurrentHist() {
        Log.info("We get the Difference of Delta and Current Hist URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_DIFF_DELTA_CURR_HIST_URL, String.join("','",ids));
        Log.info(sql);
        SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^We know the records from SDBooks URL Excl Table$")
    public static void getUrlExclude() {
        Log.info("We get the Exclude URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_EXCL_URL, String.join("','",ids));
        Log.info(sql);
        SDAccessDLContext.recordsFromExcludeUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^we compare the records of SD url Exclude with difference of Delta_current_url and url_history$")
    public void compareExcludeDataUrl() {
        if (SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL PRevious and Hist URL...");
            for (int i = 0; i < SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.size(); i++) {

                SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDAccessDLContext.recordsFromExcludeUrl.sort(Comparator.comparing(SDBooksDLAccessObject::getisbn));

                Log.info("Diff_DElta_Curr_Hist -> ISBN => " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() +
                        "Exclude -> ISBN => " + SDAccessDLContext.recordsFromExcludeUrl.get(i).getisbn());
                if (SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() != null ||
                        (SDAccessDLContext.recordsFromExcludeUrl.get(i).getisbn() != null)) {
                    Assert.assertEquals("The ISBN is =" + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() + " is missing/not found in Diff_DElta_Curr_Hist table",
                            SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn(),
                            SDAccessDLContext.recordsFromExcludeUrl.get(i).getisbn());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() +
                        " BOOK_TITLE => Diff_DElta_Curr_Hist =" + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getbookTitle() +
                        " Exclude=" + SDAccessDLContext.recordsFromExcludeUrl.get(i).getbookTitle());

                if (SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getbookTitle() != null ||
                        (SDAccessDLContext.recordsFromExcludeUrl.get(i).getbookTitle() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getbookTitle(),
                            SDAccessDLContext.recordsFromExcludeUrl.get(i).getbookTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() +
                        " URL => Diff_DElta_Curr_Hist =" + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturl() +
                        " Exclude=" + SDAccessDLContext.recordsFromExcludeUrl.get(i).geturl());

                if (SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturl() != null ||
                        (SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturl() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturl(),
                            SDAccessDLContext.recordsFromExcludeUrl.get(i).geturl());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() +
                        " URL_CODE => Diff_DElta_Curr_Hist =" + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturlCode() +
                        " Exclude=" + SDAccessDLContext.recordsFromExcludeUrl.get(i).geturlCode());

                if (SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturlCode() != null ||
                        (SDAccessDLContext.recordsFromExcludeUrl.get(i).geturlCode() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturlCode(),
                            SDAccessDLContext.recordsFromExcludeUrl.get(i).geturlCode());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() +
                        " URL_NAME => Diff_DElta_Curr_Hist =" + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturlName() +
                        " Exclude=" + SDAccessDLContext.recordsFromExcludeUrl.get(i).geturlName());

                if (SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturlName() != null ||
                        (SDAccessDLContext.recordsFromExcludeUrl.get(i).geturlName() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturlName(),
                            SDAccessDLContext.recordsFromExcludeUrl.get(i).geturlName());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() +
                        " URL_TITLE => Diff_DElta_Curr_Hist =" + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturlTitle() +
                        " Exclude=" + SDAccessDLContext.recordsFromExcludeUrl.get(i).geturlTitle());

                if (SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturlTitle() != null ||
                        (SDAccessDLContext.recordsFromExcludeUrl.get(i).geturlTitle() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geturlTitle(),
                            SDAccessDLContext.recordsFromExcludeUrl.get(i).geturlTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() +
                        " EPRID => Diff_DElta_Curr_Hist =" + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geteprid() +
                        " Exclude=" + SDAccessDLContext.recordsFromExcludeUrl.get(i).geteprid());

                if (SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geteprid() != null ||
                        (SDAccessDLContext.recordsFromExcludeUrl.get(i).geteprid() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).geteprid(),
                            SDAccessDLContext.recordsFromExcludeUrl.get(i).geteprid());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() +
                        " WORK_TYPE => Diff_DElta_Curr_Hist =" + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getworkType() +
                        " Exclude=" + SDAccessDLContext.recordsFromExcludeUrl.get(i).getworkType());

                if (SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getworkType() != null ||
                        (SDAccessDLContext.recordsFromExcludeUrl.get(i).getworkType() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getworkType(),
                            SDAccessDLContext.recordsFromExcludeUrl.get(i).getworkType());
                }

            }
        }
    }

    @When("^Get the records from the addition of SDbooks Delta_URL and URL_Exclude$")
    public static void getSumOfDeltaAndExclude() {
        Log.info("We get the Sum of Delta and Excude URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_SUM_DELTA_EXCL_URL, String.join("','",ids));
        Log.info(sql);
        SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from SDbooks URL latest table$")
    public static void getUrlLatest() {
        Log.info("We get the Latest URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_LATEST_URL, String.join("','",ids));
        Log.info(sql);
        SDAccessDLContext.recordsFromLAtestUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^we compare records of SDbooks URL Latest with addition of Delta_current_Person and Person_Exclude$")
    public void compareLatestDataUrl() {
        if (SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL PRevious and Hist URL...");
            for (int i = 0; i < SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.size(); i++) {

                SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDAccessDLContext.recordsFromLAtestUrl.sort(Comparator.comparing(SDBooksDLAccessObject::getisbn));

                Log.info("Sum_Delta_Excl -> ISBN => " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() +
                        "Latest -> ISBN => " + SDAccessDLContext.recordsFromLAtestUrl.get(i).getisbn());
                if (SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() != null ||
                        (SDAccessDLContext.recordsFromLAtestUrl.get(i).getisbn() != null)) {
                    Assert.assertEquals("The ISBN is =" + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() + " is missing/not found in Sum_Delta_Excl table",
                            SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn(),
                            SDAccessDLContext.recordsFromLAtestUrl.get(i).getisbn());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() +
                        " BOOK_TITLE => Sum_Delta_Excl =" + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getbookTitle() +
                        " Latest=" + SDAccessDLContext.recordsFromLAtestUrl.get(i).getbookTitle());

                if (SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getbookTitle() != null ||
                        (SDAccessDLContext.recordsFromLAtestUrl.get(i).getbookTitle() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getbookTitle(),
                            SDAccessDLContext.recordsFromLAtestUrl.get(i).getbookTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() +
                        " URL => Sum_Delta_Excl =" + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturl() +
                        " Latest=" + SDAccessDLContext.recordsFromLAtestUrl.get(i).geturl());

                if (SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturl() != null ||
                        (SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturl() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturl(),
                            SDAccessDLContext.recordsFromLAtestUrl.get(i).geturl());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() +
                        " URL_CODE => Sum_Delta_Excl =" + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturlCode() +
                        " Latest=" + SDAccessDLContext.recordsFromLAtestUrl.get(i).geturlCode());

                if (SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturlCode() != null ||
                        (SDAccessDLContext.recordsFromLAtestUrl.get(i).geturlCode() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturlCode(),
                            SDAccessDLContext.recordsFromLAtestUrl.get(i).geturlCode());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() +
                        " URL_NAME => Sum_Delta_Excl =" + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturlName() +
                        " Latest=" + SDAccessDLContext.recordsFromLAtestUrl.get(i).geturlName());

                if (SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturlName() != null ||
                        (SDAccessDLContext.recordsFromLAtestUrl.get(i).geturlName() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturlName(),
                            SDAccessDLContext.recordsFromLAtestUrl.get(i).geturlName());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() +
                        " URL_TITLE => Sum_Delta_Excl =" + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturlTitle() +
                        " Latest=" + SDAccessDLContext.recordsFromLAtestUrl.get(i).geturlTitle());

                if (SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturlTitle() != null ||
                        (SDAccessDLContext.recordsFromLAtestUrl.get(i).geturlTitle() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geturlTitle(),
                            SDAccessDLContext.recordsFromLAtestUrl.get(i).geturlTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() +
                        " EPRID => Sum_Delta_Excl =" + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geteprid() +
                        " Latest=" + SDAccessDLContext.recordsFromLAtestUrl.get(i).geteprid());

                if (SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geteprid() != null ||
                        (SDAccessDLContext.recordsFromLAtestUrl.get(i).geteprid() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).geteprid(),
                            SDAccessDLContext.recordsFromLAtestUrl.get(i).geteprid());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() +
                        " WORK_TYPE => Sum_Delta_Excl =" + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getworkType() +
                        " Latest=" + SDAccessDLContext.recordsFromLAtestUrl.get(i).getworkType());

                if (SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getworkType() != null ||
                        (SDAccessDLContext.recordsFromLAtestUrl.get(i).getworkType() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromAddDeltaAndExcludeUrl.get(i).getworkType(),
                            SDAccessDLContext.recordsFromLAtestUrl.get(i).getworkType());
                }

            }
        }
    }


    @Then("^We Get the records from SD transform Delta Current History$")
    public static void getDeltaCurrentHist() {
        Log.info("We get the Delta Current Hist URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_DELTA_CURR_HIST, String.join("','",ids));
        Log.info(sql);
        SDAccessDLContext.recordsFromDeltaCurrUrlHistory = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^we compare the records of SDBooks delta Current and delta Current history$")
    public void compareDeltaCurrHist() {
        if (SDAccessDLContext.recordsFromDeltaCurrentUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL PRevious and Hist URL...");
            for (int i = 0; i < SDAccessDLContext.recordsFromDeltaCurrentUrl.size(); i++) {

                SDAccessDLContext.recordsFromDeltaCurrentUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDAccessDLContext.recordsFromDeltaCurrUrlHistory.sort(Comparator.comparing(SDBooksDLAccessObject::getisbn));

                Log.info("Delta_Curr -> ISBN => " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() +
                        "Delta_Curr_Hist -> ISBN => " + SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getisbn());
                if (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getisbn() != null)) {
                    Assert.assertEquals("The ISBN is =" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() + " is missing/not found in Delta_Curr table",
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn(),
                            SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getisbn());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() +
                        " BOOK_TITLE => Delta_Curr =" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getbookTitle() +
                        " Delta_Curr_Hist=" + SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getbookTitle());

                if (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getbookTitle() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getbookTitle() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getbookTitle(),
                            SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getbookTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() +
                        " URL => Delta_Curr =" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturl() +
                        " Delta_Curr_Hist=" + SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geturl());

                if (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturl() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturl() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturl(),
                            SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geturl());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() +
                        " URL_CODE => Delta_Curr =" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlCode() +
                        " Delta_Curr_Hist=" + SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geturlCode());

                if (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlCode() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geturlCode() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlCode(),
                            SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geturlCode());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() +
                        " URL_NAME => Delta_Curr =" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlName() +
                        " Delta_Curr_Hist=" + SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geturlName());

                if (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlName() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geturlName() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlName(),
                            SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geturlName());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() +
                        " URL_TITLE => Delta_Curr =" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlTitle() +
                        " Delta_Curr_Hist=" + SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geturlTitle());

                if (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlTitle() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geturlTitle() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geturlTitle(),
                            SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geturlTitle());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() +
                        " EPRID => Delta_Curr =" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geteprid() +
                        " Delta_Curr_Hist=" + SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geteprid());

                if (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geteprid() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geteprid() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).geteprid(),
                            SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).geteprid());
                }

                Log.info("ISBN => " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() +
                        " WORK_TYPE => Delta_Curr =" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getworkType() +
                        " Delta_Curr_Hist=" + SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getworkType());

                if (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getworkType() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getworkType() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getworkType(),
                            SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getworkType());
                }
                Log.info("ISBN => " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() +
                        " DELTA_MODE => Delta_Curr =" + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getdeltaMode() +
                        " Delta_Curr_Hist=" + SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getdeltaMode());

                if (SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getdeltaMode() != null ||
                        (SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getdeltaMode() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for ISBN = " + SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getisbn() ,
                            SDAccessDLContext.recordsFromDeltaCurrentUrl.get(i).getdeltaMode(),
                            SDAccessDLContext.recordsFromDeltaCurrUrlHistory.get(i).getdeltaMode());
                }
            }
        }
    }




}
