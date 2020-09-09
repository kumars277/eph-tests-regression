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
                List<Map<?, ?>> randomIsbnIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomIsbnIds.stream().map(m -> (String) m.get("ISBN")).collect(Collectors.toList());
                break;
          }
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

    @Then("^Get the records from transform current URL$")
    public void getRecordsFromCurrentPerson() {
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

            }
        }
    }


}