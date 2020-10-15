package com.eph.automation.testing.web.steps.BCS_ETLCore;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSETL_CoreAccessDLContext;
import com.eph.automation.testing.models.dao.BCS_ETLCore.BCS_ETLCoreDLAccessObject;
import com.eph.automation.testing.services.db.BCS_ETLCoreSQL.BCS_ETLCoreDataChecksSQL;
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


public class BCS_ETLCoreDataChecksSteps {

    public BCSETL_CoreAccessDLContext dataQualityBCSContext;
    private static String sql;
    private static List<String> Ids;
    private BCS_ETLCoreDataChecksSQL bcsCoreObj = new BCS_ETLCoreDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^Get the (.*) of BCS Core from Current Tables (.*)$")
    public void getRandomIds(String numberOfRecords, String tableName) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core....");
        List<Map<?, ?>> randomIds;
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_ACCPROD_KEY_INBOUND, numberOfRecords);
                 randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomIds.stream().map(m -> (String) m.get("id")).collect(Collectors.toList());

                break;
            case "etl_manifestation_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_person_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PERSON_KEY_INBOUND, numberOfRecords);
                 randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomIds.stream().map(m -> (Integer) m.get("id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WRK_RELT_KEY_INBOUND, numberOfRecords);
                randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomIds.stream().map(m -> (String) m.get("id")).collect(Collectors.toList());
                break;
        }
        Log.info(Ids.toString());
        Log.info(sql);

    }

    @When("^Get the Data from the Inbound Tables (.*)$")
    public void getIngestRecords(String tableName) {
        Log.info("We get the BCS Ingest records...");
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_KEY_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_person_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_INBOUND_DATA, Joiner.on(",").join(Ids));
                break;

        }
        Log.info(sql);
        dataQualityBCSContext.recordsFromInboundData = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the Data from the BCS Core Current Tables(.*)$")
    public void getRecordsFromCoreCurrent(String tableName){
        Log.info("We get the records from Current URL table...");
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_KEY_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_person_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_CURR_DATA, Joiner.on(",").join(Ids));
                break;
        }
        Log.info(sql);
        dataQualityBCSContext.recordsFromCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare data of BCS Inbound and BCS Core (.*) tables are identical$")
    public void compareIngestandCurrent(String tableName) {
        if (dataQualityBCSContext.recordsFromInboundData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Ingest and current...");
            for (int i = 0; i < dataQualityBCSContext.recordsFromInboundData.size(); i++) {
                switch (tableName) {
                    case "etl_accountable_product_current_v":
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        Log.info("Inbound -> UKEY => " + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                "Acc_Prod_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY());
                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null)) {

                            Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() + " is missing/not found in Acc_Prod_Curr",
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY());
                        }

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                " SOURCEREF => Inbound =" + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() +
                                " Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF());

                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null)) {
                            Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF());
                        }

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                " ACCOUNTABLEPRODUCT => Inbound =" + dataQualityBCSContext.recordsFromInboundData.get(i).getACCOUNTABLEPRODUCT() +
                                " Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPRODUCT());

                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getACCOUNTABLEPRODUCT() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPRODUCT() != null)) {
                            Assert.assertEquals("The ACCOUNTABLEPRODUCT is incorrect for UKEY = " + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getACCOUNTABLEPRODUCT(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPRODUCT());
                        }

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                " ACCOUNTABLENAME => Inbound =" + dataQualityBCSContext.recordsFromInboundData.get(i).getACCOUNTABLENAME() +
                                " Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLENAME());

                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getACCOUNTABLENAME() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLENAME() != null)) {
                            Assert.assertEquals("The ACCOUNTABLENAME is incorrect for UKEY = " + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getACCOUNTABLENAME(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLENAME());
                        }


                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                " ACCOUNTABLEPARENT => Inbound =" + dataQualityBCSContext.recordsFromInboundData.get(i).getACCOUNTABLEPARENT() +
                                " Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPARENT());

                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getACCOUNTABLEPARENT() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPARENT() != null)) {
                            Assert.assertEquals("The ACCOUNTABLEPARENT is incorrect for UKEY = " + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getACCOUNTABLEPARENT(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPARENT());
                        }

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                " DQ_ERR => Inbound =" + dataQualityBCSContext.recordsFromInboundData.get(i).getDQ_ERR() +
                                " Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR());

                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getDQ_ERR() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null)) {
                            Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getDQ_ERR(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR());
                        }
                        break;
                    case "etl_person_current_v":
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getSOURCEREF)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getSOURCEREF));

                        Log.info("Inbound -> SOURCEREF => " + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() +
                                "Person_Curr -> SOURCEREF => " + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF());
                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null)) {

                            Assert.assertEquals("The SOURCEREF is =" + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() + " is missing/not found in Person_Curr",
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF());
                        }

                        Log.info("SOURCEREF => " + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() +
                                " FIRSTNAME => Inbound =" + dataQualityBCSContext.recordsFromInboundData.get(i).getFIRSTNAME() +
                                " Person_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getFIRSTNAME());

                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getFIRSTNAME() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getFIRSTNAME() != null)) {
                            Assert.assertEquals("The FIRSTNAME is incorrect for SOURCEREF = " + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF(),
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getFIRSTNAME(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getFIRSTNAME());
                        }
                        Log.info("SOURCEREF => " + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() +
                                " FAMILYNAME => Inbound =" + dataQualityBCSContext.recordsFromInboundData.get(i).getFAMILYNAME() +
                                " Person_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getFAMILYNAME());

                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getFAMILYNAME() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getFAMILYNAME() != null)) {
                            Assert.assertEquals("The FAMILYNAME is incorrect for SOURCEREF = " + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF(),
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getFAMILYNAME(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getFAMILYNAME());
                        }
                        Log.info("SOURCEREF => " + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() +
                                " PEOPLEHUBID => Inbound =" + dataQualityBCSContext.recordsFromInboundData.get(i).getPEOPLEHUBID() +
                                " Person_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getPEOPLEHUBID());

                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getPEOPLEHUBID() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getPEOPLEHUBID() != null)) {
                            Assert.assertEquals("The PEOPLEHUBID is incorrect for SOURCEREF = " + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF(),
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getPEOPLEHUBID(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getPEOPLEHUBID());
                        }

                        Log.info("SOURCEREF => " + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() +
                                " EMAIL => Inbound =" + dataQualityBCSContext.recordsFromInboundData.get(i).getEMAIL() +
                                " Person_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getEMAIL());

                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getEMAIL() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getEMAIL() != null)) {
                            Assert.assertEquals("The EMAIL is incorrect for SOURCEREF = " + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF(),
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getEMAIL(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getEMAIL());
                        }
                        Log.info("SOURCEREF => " + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() +
                                " DQ_ERR => Inbound =" + dataQualityBCSContext.recordsFromInboundData.get(i).getDQ_ERR() +
                                " Person_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR());

                        if (dataQualityBCSContext.recordsFromInboundData.get(i).getDQ_ERR() != null ||
                                (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null)) {
                            Assert.assertEquals("The DQ_ERR is incorrect for SOURCEREF = " + dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF(),
                                    dataQualityBCSContext.recordsFromInboundData.get(i).getDQ_ERR(),
                                    dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR());
                        }
                        break;

                }


            }

        }

    }
}




