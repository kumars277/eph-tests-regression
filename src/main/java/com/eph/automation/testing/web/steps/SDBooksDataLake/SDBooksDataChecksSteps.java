package com.eph.automation.testing.web.steps.SDBooksDataLake;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.SDAccessDLContext;
import com.eph.automation.testing.models.dao.SDBooksDataLake.SDBooksDLAccessObject;
import com.eph.automation.testing.services.db.SDBooksDataLakeSQL.SDBooksDataChecksSQL;
import com.google.common.base.Joiner;
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

    public SDAccessDLContext dataQualitySDContext;
    private static String sql;
    private static List<String> Ids;
    private SDBooksDataChecksSQL sdBookObj = new SDBooksDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^We get the (.*) random ISBN ids (.*)$")
    public void getRandomISBNIds(String numberOfRecords, String tableName) {
      // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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


          }
        List<Map<?, ?>> randomIsbnIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIsbnIds.stream().map(m -> (String) m.get("ISBN")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the records from data inbound for URL$")
    public void getUrlRecordsFromFullLoad() {
        Log.info("We get the Inbound Load URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_URL_INBOUND, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDContext.recordsFromInboundData = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from transform SD current URL$")
    public void getRecordsFromCurrentURL() {
        Log.info("We get the records from Current URL table...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_CURRENT_URL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDContext.recordsFromCurrentUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of Inbound and current URL$")
    public void compareDataFullandCurrentUrl() {
        if (dataQualitySDContext.recordsFromInboundData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL Full Load and Current URL...");
            for (int i = 0; i < dataQualitySDContext.recordsFromInboundData.size(); i++) {

                dataQualitySDContext.recordsFromInboundData.sort(Comparator.comparing(SDBooksDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDContext.recordsFromCurrentUrl.sort(Comparator.comparing(SDBooksDLAccessObject::getISBN));

                Log.info("Full Load -> ISBN => " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() +
                        "Current_URL -> ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN());
                if (dataQualitySDContext.recordsFromInboundData.get(i).getISBN() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() + " is missing/not found in Current_URL table",
                            dataQualitySDContext.recordsFromInboundData.get(i).getISBN(),
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() +
                        " BOOK_TITLE => Full_Load =" + dataQualitySDContext.recordsFromInboundData.get(i).getBOOK_TITLE() +
                        " Current_URL=" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getBOOK_TITLE());

                if (dataQualitySDContext.recordsFromInboundData.get(i).getBOOK_TITLE() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrl.get(i).getBOOK_TITLE() != null)) {
                   Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() ,
                           dataQualitySDContext.recordsFromInboundData.get(i).getBOOK_TITLE(),
                           dataQualitySDContext.recordsFromCurrentUrl.get(i).getBOOK_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() +
                        " URL => Full_Load =" + dataQualitySDContext.recordsFromInboundData.get(i).getURL() +
                        " Current_URL=" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL());

                if (dataQualitySDContext.recordsFromInboundData.get(i).getURL() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromInboundData.get(i).getURL(),
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() +
                        " URL_CODE => Full_Load =" + dataQualitySDContext.recordsFromInboundData.get(i).getURL_CODE() +
                        " Current_URL=" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_CODE());

                if (dataQualitySDContext.recordsFromInboundData.get(i).getURL_CODE() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_CODE() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromInboundData.get(i).getURL_CODE(),
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_CODE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() +
                        " URL_NAME => Full_Load =" + dataQualitySDContext.recordsFromInboundData.get(i).getURL_NAME() +
                        " Current_URL=" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_NAME());

                if (dataQualitySDContext.recordsFromInboundData.get(i).getURL_NAME() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_NAME() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromInboundData.get(i).getURL_NAME(),
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_NAME());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() +
                        " URL_TITLE => Full_Load =" + dataQualitySDContext.recordsFromInboundData.get(i).getURL_TITLE() +
                        " Current_URL=" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_TITLE());

                if (dataQualitySDContext.recordsFromInboundData.get(i).getURL_TITLE() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_TITLE() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromInboundData.get(i).getURL_TITLE(),
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() +
                        " EPRID => Full_Load =" + dataQualitySDContext.recordsFromInboundData.get(i).getEPRID() +
                        " Current_URL=" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getEPRID());

                if (dataQualitySDContext.recordsFromInboundData.get(i).getEPRID() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrl.get(i).getEPRID() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromInboundData.get(i).getEPRID(),
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getEPRID());
                }
                Log.info("ISBN => " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() +
                        " EPRID => Full_Load =" + dataQualitySDContext.recordsFromInboundData.get(i).getWORK_TYPE() +
                        " Current_URL=" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getWORK_TYPE());

                if (dataQualitySDContext.recordsFromInboundData.get(i).getEPRID() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrl.get(i).getEPRID() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + dataQualitySDContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromInboundData.get(i).getWORK_TYPE(),
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getWORK_TYPE());
                }
            }
        }
    }

    @When("^We Get the records from transform SD url History (.*)$")
    public void getUrlRecordsFromCurrentHist(String table) {
        Log.info("We get the transform Hist URL records...");
        switch (table){
            case "sdbooks_transform_current_urls":
                sql = String.format(SDBooksDataChecksSQL.GET_REC_CURRENT_HIST_URL, Joiner.on("','").join(Ids));
                dataQualitySDContext.recordsFromCurrentUrlHistory = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
                break;
/*
            case "sdbooks_transform_previous_urls":
                sql = String.format(SDBooksDataChecksSQL.GET_REC_PREVIOUS_HIST_URL, Joiner.on("','").join(Ids));
                dataQualitySDContext.recordsFromPreviousUrlHistory = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
                break;
   */     }
        Log.info(sql);
    }


    @When("^We Get the records from transform File SD url (.*)$")
    public void getUrlRecFromTransformFile(String table) {
        Log.info("We get the transform File URL records...");
        switch (table){
            case "sdbooks_transform_file_history_urls_part":
                sql = String.format(SDBooksDataChecksSQL.GET_REC_CURRENT_TRANS_FILE_URL, Joiner.on("','").join(Ids));
                dataQualitySDContext.recordsFromCurrentUrlTranFile = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
                break;
     }
        Log.info(sql);
    }

    @And("^Compare the records of SD current url and SD current url history$")
    public void compareDataCurrentAndHistUrl() {
        if (dataQualitySDContext.recordsFromCurrentUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL Current and Curr Hist URL...");
            for (int i = 0; i < dataQualitySDContext.recordsFromCurrentUrl.size(); i++) {

                dataQualitySDContext.recordsFromCurrentUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDContext.recordsFromCurrentUrlHistory.sort(Comparator.comparing(SDBooksDLAccessObject::getISBN));

                Log.info("Current_URL -> ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        "Current_URL_Hist -> ISBN => " + dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getISBN());
                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() + " is missing/not found in Current_URL table",
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN(),
                            dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " BOOK_TITLE => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getBOOK_TITLE() +
                        " Current_URL_Hist=" + dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getBOOK_TITLE());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getBOOK_TITLE() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getBOOK_TITLE() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getBOOK_TITLE(),
                            dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getBOOK_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " URL => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL() +
                        " Current_URL_Hist=" + dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getURL());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL(),
                            dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getURL());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " URL_CODE => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_CODE() +
                        " Current_URL_Hist=" + dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getURL_CODE());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_CODE() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getURL_CODE() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_CODE(),
                            dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getURL_CODE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " URL_NAME => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_NAME() +
                        " Current_URL_Hist=" + dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getURL_NAME());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_NAME() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getURL_NAME() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_NAME(),
                            dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getURL_NAME());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " URL_TITLE => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_TITLE() +
                        " Current_URL_Hist =" + dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getURL_TITLE());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_TITLE() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getURL_TITLE() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_TITLE(),
                            dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getURL_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " EPRID => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getEPRID() +
                        " Current_URL_Hist =" + dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getEPRID());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getEPRID() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getEPRID() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getEPRID(),
                            dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getEPRID());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " WORK_TYPE => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getWORK_TYPE() +
                        " Current_URL_Hist=" + dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getWORK_TYPE());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getWORK_TYPE() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getWORK_TYPE(),
                            dataQualitySDContext.recordsFromCurrentUrlHistory.get(i).getWORK_TYPE());
                }
            }
        }
    }

    @And("^Compare the records of SD current url and SD transform file url history$")
    public void compareDataCurrentAndTransformFile() {
        if (dataQualitySDContext.recordsFromCurrentUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL Current and Transform File URL...");
            for (int i = 0; i < dataQualitySDContext.recordsFromCurrentUrl.size(); i++) {

                dataQualitySDContext.recordsFromCurrentUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDContext.recordsFromCurrentUrlTranFile.sort(Comparator.comparing(SDBooksDLAccessObject::getISBN));

                Log.info("Current_URL -> ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        "Trans_File -> ISBN => " + dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getISBN());
                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() + " is missing/not found in Current_URL table",
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN(),
                            dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " BOOK_TITLE => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getBOOK_TITLE() +
                        " Trans_File=" + dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getBOOK_TITLE());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getBOOK_TITLE() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getBOOK_TITLE() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getBOOK_TITLE(),
                            dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getBOOK_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " URL => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL() +
                        " Trans_File=" + dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getURL());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL(),
                            dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getURL());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " URL_CODE => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_CODE() +
                        " Trans_File=" + dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getURL_CODE());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_CODE() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getURL_CODE() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_CODE(),
                            dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getURL_CODE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " URL_NAME => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_NAME() +
                        " Trans_File=" + dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getURL_NAME());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_NAME() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getURL_NAME() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_NAME(),
                            dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getURL_NAME());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " URL_TITLE => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_TITLE() +
                        " Trans_File =" + dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getURL_TITLE());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_TITLE() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getURL_TITLE() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getURL_TITLE(),
                            dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getURL_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " EPRID => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getEPRID() +
                        " Trans_File =" + dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getEPRID());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getEPRID() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getEPRID() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getEPRID(),
                            dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getEPRID());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() +
                        " WORK_TYPE => Current_URL =" + dataQualitySDContext.recordsFromCurrentUrl.get(i).getWORK_TYPE() +
                        " Trans_File=" + dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getWORK_TYPE());

                if (dataQualitySDContext.recordsFromCurrentUrl.get(i).getWORK_TYPE() != null ||
                        (dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + dataQualitySDContext.recordsFromCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromCurrentUrl.get(i).getWORK_TYPE(),
                            dataQualitySDContext.recordsFromCurrentUrlTranFile.get(i).getWORK_TYPE());
                }
            }
        }
    }


    @Then("^Get the records from SD transform previous url$")
    public void getRecordsFromPreviousURL() {
        Log.info("We get the records from Previous URL table...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_PREVIOUS_URL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDContext.recordsFromPreviousUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of SD previous url and previous url history$")
    public void compareDataPreviousAndHistUrl() {
        if (dataQualitySDContext.recordsFromPreviousUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL PRevious and Hist URL...");
            for (int i = 0; i < dataQualitySDContext.recordsFromPreviousUrl.size(); i++) {

                dataQualitySDContext.recordsFromPreviousUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDContext.recordsFromPreviousUrlHistory.sort(Comparator.comparing(SDBooksDLAccessObject::getISBN));

                Log.info("Previous_URL -> ISBN => " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() +
                        "Previous_URL_Hist -> ISBN => " + dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getISBN());
                if (dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() != null ||
                        (dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() + " is missing/not found in Previous_URL table",
                            dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN(),
                            dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() +
                        " BOOK_TITLE => Previous_URL =" + dataQualitySDContext.recordsFromPreviousUrl.get(i).getBOOK_TITLE() +
                        " Previous_URL_Hist=" + dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getBOOK_TITLE());

                if (dataQualitySDContext.recordsFromPreviousUrl.get(i).getBOOK_TITLE() != null ||
                        (dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getBOOK_TITLE() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromPreviousUrl.get(i).getBOOK_TITLE(),
                            dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getBOOK_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() +
                        " URL => Previous_URL =" + dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL() +
                        " Previous_URL_Hist=" + dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getURL());

                if (dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL() != null ||
                        (dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL(),
                            dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getURL());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() +
                        " URL_CODE => Previous_URL =" + dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL_CODE() +
                        " Previous_URL_Hist=" + dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getURL_CODE());

                if (dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL_CODE() != null ||
                        (dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getURL_CODE() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL_CODE(),
                            dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getURL_CODE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() +
                        " URL_NAME => Previous_URL =" + dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL_NAME() +
                        " Previous_URL_Hist=" + dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getURL_NAME());

                if (dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL_NAME() != null ||
                        (dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getURL_NAME() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL_NAME(),
                            dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getURL_NAME());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() +
                        " URL_TITLE => Previous_URL =" + dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL_TITLE() +
                        " Previous_URL_Hist=" + dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getURL_TITLE());

                if (dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL_TITLE() != null ||
                        (dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getURL_TITLE() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromPreviousUrl.get(i).getURL_TITLE(),
                            dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getURL_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() +
                        " EPRID => Previous_URL =" + dataQualitySDContext.recordsFromPreviousUrl.get(i).getEPRID() +
                        " Previous_URL_Hist=" + dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getEPRID());

                if (dataQualitySDContext.recordsFromPreviousUrl.get(i).getEPRID() != null ||
                        (dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getEPRID() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromPreviousUrl.get(i).getEPRID(),
                            dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getEPRID());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() +
                        " WORK_TYPE => Full_Load =" + dataQualitySDContext.recordsFromPreviousUrl.get(i).getWORK_TYPE() +
                        " Previous_URL=" + dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getWORK_TYPE());

                if (dataQualitySDContext.recordsFromPreviousUrl.get(i).getWORK_TYPE() != null ||
                        (dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + dataQualitySDContext.recordsFromPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromPreviousUrl.get(i).getWORK_TYPE(),
                            dataQualitySDContext.recordsFromPreviousUrlHistory.get(i).getWORK_TYPE());
                }
            }
        }
    }

    @When("^Get the records from SDBooks for Delta Current Url$")
    public void getRecordsDeltaCurrURL() {
        Log.info("We get the records from Delta Current URL table...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_DELTA_CURRENT_URL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDContext.recordsFromDeltaCurrentUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^We Get the data from the difference of SD Current and Previous transform_file table$")
    public void getRecordsFromDiffofCurrPrevURL() {
        Log.info("We get the records from Diff from Current and previous URL table...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_DIFF_OF_CURR_PREV_TRANS_FILE, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of SD delta url with difference of current and previous transform_file$")
    public void compareDataDeltaCurrentUrl() {
        if (dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL PRevious and Hist URL...");
            for (int i = 0; i < dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.size(); i++) {

                dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDContext.recordsFromDeltaCurrentUrl.sort(Comparator.comparing(SDBooksDLAccessObject::getISBN));

                Log.info("Diff_Curr_Prev -> ISBN => " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() +
                        "Delta_Current -> ISBN => " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN());
                if (dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() + " is missing/not found in Diff_Curr_Prev table",
                            dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN(),
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() +
                        " BOOK_TITLE => Diff_Curr_Prev =" + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getBOOK_TITLE() +
                        " Delta_Current=" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getBOOK_TITLE());

                if (dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getBOOK_TITLE() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getBOOK_TITLE() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getBOOK_TITLE(),
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getBOOK_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() +
                        " URL => Diff_Curr_Prev =" + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL() +
                        " Delta_Current=" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL());

                if (dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL() != null ||
                        (dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL(),
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() +
                        " URL_CODE => Diff_Curr_Prev =" + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL_CODE() +
                        " Delta_Current=" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_CODE());

                if (dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL_CODE() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_CODE() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL_CODE(),
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_CODE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() +
                        " URL_NAME => Diff_Curr_Prev =" + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL_NAME() +
                        " Delta_Current=" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_NAME());

                if (dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL_NAME() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_NAME() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL_NAME(),
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_NAME());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() +
                        " URL_TITLE => Diff_Curr_Prev =" + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL_TITLE() +
                        " Delta_Current=" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_TITLE());

                if (dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL_TITLE() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_TITLE() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getURL_TITLE(),
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() +
                        " EPRID => Diff_Curr_Prev =" + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getEPRID() +
                        " Delta_Current=" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getEPRID());

                if (dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getEPRID() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getEPRID() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getEPRID(),
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getEPRID());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() +
                        " WORK_TYPE => Diff_Curr_Prev =" + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getWORK_TYPE() +
                        " Delta_Current=" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getWORK_TYPE());

                if (dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getWORK_TYPE() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getWORK_TYPE(),
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getWORK_TYPE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() +
                        " DELTA_MODE => Diff_Curr_Prev =" + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getDELTA_MODE() +
                        " Delta_Current=" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getDELTA_MODE());

                if (dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getDELTA_MODE() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getDELTA_MODE() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffCurrentAndPreviousUrl.get(i).getDELTA_MODE(),
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getDELTA_MODE());
                }
            }
        }
    }

    @When("^Get the records from the difference of SD Delta_current_url and url_history$")
    public void getDiffDeltaAndCurrentHist() {
        Log.info("We get the Difference of Delta and Current Hist URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_DIFF_DELTA_CURR_HIST_URL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^We know the records from SDBooks URL Excl Table$")
    public void getUrlExclude() {
        Log.info("We get the Exclude URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_EXCL_URL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDContext.recordsFromExcludeUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of SD url Exclude with difference of Delta_current_url and url_history$")
    public void compareExcludeDataUrl() {
        if (dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL PRevious and Hist URL...");
            for (int i = 0; i < dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.size(); i++) {

                dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDContext.recordsFromExcludeUrl.sort(Comparator.comparing(SDBooksDLAccessObject::getISBN));

                Log.info("Diff_DElta_Curr_Hist -> ISBN => " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() +
                        "Exclude -> ISBN => " + dataQualitySDContext.recordsFromExcludeUrl.get(i).getISBN());
                if (dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() != null ||
                        (dataQualitySDContext.recordsFromExcludeUrl.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() + " is missing/not found in Diff_DElta_Curr_Hist table",
                            dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN(),
                            dataQualitySDContext.recordsFromExcludeUrl.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() +
                        " BOOK_TITLE => Diff_DElta_Curr_Hist =" + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getBOOK_TITLE() +
                        " Exclude=" + dataQualitySDContext.recordsFromExcludeUrl.get(i).getBOOK_TITLE());

                if (dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getBOOK_TITLE() != null ||
                        (dataQualitySDContext.recordsFromExcludeUrl.get(i).getBOOK_TITLE() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getBOOK_TITLE(),
                            dataQualitySDContext.recordsFromExcludeUrl.get(i).getBOOK_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() +
                        " URL => Diff_DElta_Curr_Hist =" + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL() +
                        " Exclude=" + dataQualitySDContext.recordsFromExcludeUrl.get(i).getURL());

                if (dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL() != null ||
                        (dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL(),
                            dataQualitySDContext.recordsFromExcludeUrl.get(i).getURL());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() +
                        " URL_CODE => Diff_DElta_Curr_Hist =" + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL_CODE() +
                        " Exclude=" + dataQualitySDContext.recordsFromExcludeUrl.get(i).getURL_CODE());

                if (dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL_CODE() != null ||
                        (dataQualitySDContext.recordsFromExcludeUrl.get(i).getURL_CODE() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL_CODE(),
                            dataQualitySDContext.recordsFromExcludeUrl.get(i).getURL_CODE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() +
                        " URL_NAME => Diff_DElta_Curr_Hist =" + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL_NAME() +
                        " Exclude=" + dataQualitySDContext.recordsFromExcludeUrl.get(i).getURL_NAME());

                if (dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL_NAME() != null ||
                        (dataQualitySDContext.recordsFromExcludeUrl.get(i).getURL_NAME() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL_NAME(),
                            dataQualitySDContext.recordsFromExcludeUrl.get(i).getURL_NAME());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() +
                        " URL_TITLE => Diff_DElta_Curr_Hist =" + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL_TITLE() +
                        " Exclude=" + dataQualitySDContext.recordsFromExcludeUrl.get(i).getURL_TITLE());

                if (dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL_TITLE() != null ||
                        (dataQualitySDContext.recordsFromExcludeUrl.get(i).getURL_TITLE() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getURL_TITLE(),
                            dataQualitySDContext.recordsFromExcludeUrl.get(i).getURL_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() +
                        " EPRID => Diff_DElta_Curr_Hist =" + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getEPRID() +
                        " Exclude=" + dataQualitySDContext.recordsFromExcludeUrl.get(i).getEPRID());

                if (dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getEPRID() != null ||
                        (dataQualitySDContext.recordsFromExcludeUrl.get(i).getEPRID() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getEPRID(),
                            dataQualitySDContext.recordsFromExcludeUrl.get(i).getEPRID());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() +
                        " WORK_TYPE => Diff_DElta_Curr_Hist =" + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getWORK_TYPE() +
                        " Exclude=" + dataQualitySDContext.recordsFromExcludeUrl.get(i).getWORK_TYPE());

                if (dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getWORK_TYPE() != null ||
                        (dataQualitySDContext.recordsFromExcludeUrl.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDiffDeltaAndCurrentHistoryUrl.get(i).getWORK_TYPE(),
                            dataQualitySDContext.recordsFromExcludeUrl.get(i).getWORK_TYPE());
                }

            }
        }
    }

    @When("^Get the records from the addition of SDbooks Delta_URL and URL_Exclude$")
    public void getSumOfDeltaAndExclude() {
        Log.info("We get the Sum of Delta and Excude URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_SUM_DELTA_EXCL_URL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from SDbooks URL latest table$")
    public void getUrlLatest() {
        Log.info("We get the Latest URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_LATEST_URL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDContext.recordsFromLAtestUrl = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of SDbooks URL Latest with addition of Delta_current_Person and Person_Exclude$")
    public void compareLatestDataUrl() {
        if (dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL PRevious and Hist URL...");
            for (int i = 0; i < dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.size(); i++) {

                dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDContext.recordsFromLAtestUrl.sort(Comparator.comparing(SDBooksDLAccessObject::getISBN));

                Log.info("Sum_Delta_Excl -> ISBN => " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() +
                        "Latest -> ISBN => " + dataQualitySDContext.recordsFromLAtestUrl.get(i).getISBN());
                if (dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() != null ||
                        (dataQualitySDContext.recordsFromLAtestUrl.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() + " is missing/not found in Sum_Delta_Excl table",
                            dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN(),
                            dataQualitySDContext.recordsFromLAtestUrl.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() +
                        " BOOK_TITLE => Sum_Delta_Excl =" + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getBOOK_TITLE() +
                        " Latest=" + dataQualitySDContext.recordsFromLAtestUrl.get(i).getBOOK_TITLE());

                if (dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getBOOK_TITLE() != null ||
                        (dataQualitySDContext.recordsFromLAtestUrl.get(i).getBOOK_TITLE() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getBOOK_TITLE(),
                            dataQualitySDContext.recordsFromLAtestUrl.get(i).getBOOK_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() +
                        " URL => Sum_Delta_Excl =" + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL() +
                        " Latest=" + dataQualitySDContext.recordsFromLAtestUrl.get(i).getURL());

                if (dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL() != null ||
                        (dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL(),
                            dataQualitySDContext.recordsFromLAtestUrl.get(i).getURL());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() +
                        " URL_CODE => Sum_Delta_Excl =" + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL_CODE() +
                        " Latest=" + dataQualitySDContext.recordsFromLAtestUrl.get(i).getURL_CODE());

                if (dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL_CODE() != null ||
                        (dataQualitySDContext.recordsFromLAtestUrl.get(i).getURL_CODE() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL_CODE(),
                            dataQualitySDContext.recordsFromLAtestUrl.get(i).getURL_CODE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() +
                        " URL_NAME => Sum_Delta_Excl =" + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL_NAME() +
                        " Latest=" + dataQualitySDContext.recordsFromLAtestUrl.get(i).getURL_NAME());

                if (dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL_NAME() != null ||
                        (dataQualitySDContext.recordsFromLAtestUrl.get(i).getURL_NAME() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL_NAME(),
                            dataQualitySDContext.recordsFromLAtestUrl.get(i).getURL_NAME());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() +
                        " URL_TITLE => Sum_Delta_Excl =" + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL_TITLE() +
                        " Latest=" + dataQualitySDContext.recordsFromLAtestUrl.get(i).getURL_TITLE());

                if (dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL_TITLE() != null ||
                        (dataQualitySDContext.recordsFromLAtestUrl.get(i).getURL_TITLE() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getURL_TITLE(),
                            dataQualitySDContext.recordsFromLAtestUrl.get(i).getURL_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() +
                        " EPRID => Sum_Delta_Excl =" + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getEPRID() +
                        " Latest=" + dataQualitySDContext.recordsFromLAtestUrl.get(i).getEPRID());

                if (dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getEPRID() != null ||
                        (dataQualitySDContext.recordsFromLAtestUrl.get(i).getEPRID() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getEPRID(),
                            dataQualitySDContext.recordsFromLAtestUrl.get(i).getEPRID());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() +
                        " WORK_TYPE => Sum_Delta_Excl =" + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getWORK_TYPE() +
                        " Latest=" + dataQualitySDContext.recordsFromLAtestUrl.get(i).getWORK_TYPE());

                if (dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getWORK_TYPE() != null ||
                        (dataQualitySDContext.recordsFromLAtestUrl.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromAddDeltaAndExcludeUrl.get(i).getWORK_TYPE(),
                            dataQualitySDContext.recordsFromLAtestUrl.get(i).getWORK_TYPE());
                }

            }
        }
    }


    @Then("^We Get the records from SD transform Delta Current History$")
    public void getDeltaCurrentHist() {
        Log.info("We get the Delta Current Hist URL records...");
        sql = String.format(SDBooksDataChecksSQL.GET_REC_DELTA_CURR_HIST, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDContext.recordsFromDeltaCurrUrlHistory = DBManager.getDBResultAsBeanList(sql, SDBooksDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of SDBooks delta Current and delta Current history$")
    public void compareDeltaCurrHist() {
        if (dataQualitySDContext.recordsFromDeltaCurrentUrl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between URL PRevious and Hist URL...");
            for (int i = 0; i < dataQualitySDContext.recordsFromDeltaCurrentUrl.size(); i++) {

                dataQualitySDContext.recordsFromDeltaCurrentUrl  .sort(Comparator.comparing(SDBooksDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDContext.recordsFromDeltaCurrUrlHistory.sort(Comparator.comparing(SDBooksDLAccessObject::getISBN));

                Log.info("Delta_Curr -> ISBN => " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() +
                        "Delta_Curr_Hist -> ISBN => " + dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getISBN());
                if (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() + " is missing/not found in Delta_Curr table",
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN(),
                            dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() +
                        " BOOK_TITLE => Delta_Curr =" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getBOOK_TITLE() +
                        " Delta_Curr_Hist=" + dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getBOOK_TITLE());

                if (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getBOOK_TITLE() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getBOOK_TITLE() != null)) {
                    Assert.assertEquals("The BOOK_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getBOOK_TITLE(),
                            dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getBOOK_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() +
                        " URL => Delta_Curr =" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL() +
                        " Delta_Curr_Hist=" + dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getURL());

                if (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL() != null)) {
                    Assert.assertEquals("The URL is incorrect for ISBN = " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL(),
                            dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getURL());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() +
                        " URL_CODE => Delta_Curr =" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_CODE() +
                        " Delta_Curr_Hist=" + dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getURL_CODE());

                if (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_CODE() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getURL_CODE() != null)) {
                    Assert.assertEquals("The URL_CODE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_CODE(),
                            dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getURL_CODE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() +
                        " URL_NAME => Delta_Curr =" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_NAME() +
                        " Delta_Curr_Hist=" + dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getURL_NAME());

                if (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_NAME() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getURL_NAME() != null)) {
                    Assert.assertEquals("The URL_NAME is incorrect for ISBN = " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_NAME(),
                            dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getURL_NAME());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() +
                        " URL_TITLE => Delta_Curr =" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_TITLE() +
                        " Delta_Curr_Hist=" + dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getURL_TITLE());

                if (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_TITLE() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getURL_TITLE() != null)) {
                    Assert.assertEquals("The URL_TITLE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getURL_TITLE(),
                            dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getURL_TITLE());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() +
                        " EPRID => Delta_Curr =" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getEPRID() +
                        " Delta_Curr_Hist=" + dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getEPRID());

                if (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getEPRID() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getEPRID() != null)) {
                    Assert.assertEquals("The EPRID is incorrect for ISBN = " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getEPRID(),
                            dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getEPRID());
                }

                Log.info("ISBN => " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() +
                        " WORK_TYPE => Delta_Curr =" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getWORK_TYPE() +
                        " Delta_Curr_Hist=" + dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getWORK_TYPE());

                if (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getWORK_TYPE() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getWORK_TYPE() != null)) {
                    Assert.assertEquals("The WORK_TYPE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getWORK_TYPE(),
                            dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getWORK_TYPE());
                }
                Log.info("ISBN => " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() +
                        " DELTA_MODE => Delta_Curr =" + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getDELTA_MODE() +
                        " Delta_Curr_Hist=" + dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getDELTA_MODE());

                if (dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getDELTA_MODE() != null ||
                        (dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getDELTA_MODE() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for ISBN = " + dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getISBN() ,
                            dataQualitySDContext.recordsFromDeltaCurrentUrl.get(i).getDELTA_MODE(),
                            dataQualitySDContext.recordsFromDeltaCurrUrlHistory.get(i).getDELTA_MODE());
                }
            }
        }
    }




}
