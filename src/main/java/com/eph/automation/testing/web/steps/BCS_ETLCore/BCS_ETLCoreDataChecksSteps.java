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

    @Given("^Get the (.*) of BCS Core data from Current Tables (.*)$")
    public void getRandomIdsFromCurrent(String numberOfRecords, String tableName) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core Current Tables....");

        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_ACCPROD_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_IDENT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PRODUCT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_PERS_ROLE_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_RELATION_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_IDENT_KEY_CURRENT, numberOfRecords);
                break;
       }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("u_key")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }


    @Then("^Get the Data from the BCS Core Current Tables (.*)$")
    public void getRecFromCurrent(String tableName){
        Log.info("We get the records from Current BCS Core table...");
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_work_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_work_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_CURR_REC, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recordsFromCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }
/*
    @When("^Get the Data from the Inbound Tables (.*)$")
    public void getIngestRecords(String tableName) {
        Log.info("We get the BCS Ingest records...");
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_INBOUND_DATA, Joiner.on("','").join(Ids));
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
    }*/



   @Then("^We Get the records from transform BCS Current History (.*)$")
   public void getRecFromCurrentHistory(String tableName){
       Log.info("We get the records from Current_History BCS Core table...");
       switch (tableName) {
           case "etl_transform_history_accountable_product_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_manifestation_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_manifestation_identifier_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_product_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_work_person_role_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_work_relationship_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_work_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_work_identifier_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
       }
       dataQualityBCSContext.recFromCurrentHist = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
       Log.info(sql);

   }

    @And("^Compare the records of BCS Core current and BCS Current_History (.*)$")
    public void compareCurrentandCurrHist(String targetTableName) {
        if (dataQualityBCSContext.recordsFromCurrent.isEmpty()) {
            Log.info("No Data Found in the Current Tables ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Current and current hist tables...");
        }
        for (int i = 0; i < dataQualityBCSContext.recordsFromCurrent.size(); i++) {
            switch (targetTableName) {
                case "etl_transform_history_accountable_product_part":
                    Log.info("etl_transform_history_accountable_product_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Acc_Prod_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "Acc_Prod_Curr_hist -> UKEY => " + dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY() != null)) {

                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in Acc_Prod_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " Acc_Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " ACCOUNTABLEPRODUCT => Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPRODUCT() +
                            " Acc_Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getACCOUNTABLEPRODUCT());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPRODUCT() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getACCOUNTABLEPRODUCT() != null)) {
                        Assert.assertEquals("The ACCOUNTABLEPRODUCT is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPRODUCT(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getACCOUNTABLEPRODUCT());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " ACCOUNTABLENAME => Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLENAME() +
                            " Acc_Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getACCOUNTABLENAME());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLENAME() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getACCOUNTABLENAME() != null)) {
                        Assert.assertEquals("The ACCOUNTABLENAME is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLENAME(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getACCOUNTABLENAME());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " ACCOUNTABLEPARENT => Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPARENT() +
                            " Acc_Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getACCOUNTABLEPARENT());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPARENT() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getACCOUNTABLEPARENT() != null)) {
                        Assert.assertEquals("The ACCOUNTABLEPARENT is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPARENT(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getACCOUNTABLEPARENT());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DQ_ERR => Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() +
                            " Acc_Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_transform_history_manifestation_part":
                    Log.info("etl_transform_history_manifestation_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Manif_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "Manif_Curr_hist -> UKEY => " + dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY() != null)) {

                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in Manif_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " Manif_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " TITLE => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE() +
                            " Manif_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getTITLE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getTITLE() != null)) {
                        Assert.assertEquals("The TITLE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " intereditionflag => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getINTEREDITIONFLAG() +
                            " Manif_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getINTEREDITIONFLAG());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getINTEREDITIONFLAG() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getINTEREDITIONFLAG() != null)) {
                        Assert.assertEquals("The INTEREDITIONFLAG is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getINTEREDITIONFLAG(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getINTEREDITIONFLAG());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " firstpublisheddate => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getFIRSTPUBLISHEDDATE() +
                            " Manif_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getFIRSTPUBLISHEDDATE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getFIRSTPUBLISHEDDATE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getFIRSTPUBLISHEDDATE() != null)) {
                        Assert.assertEquals("The INTEREDITIONFLAG is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getFIRSTPUBLISHEDDATE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getFIRSTPUBLISHEDDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " binding => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getBINDING() +
                            " Manif_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getBINDING());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getBINDING() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getBINDING() != null)) {
                        Assert.assertEquals("The BINDING is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getBINDING(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getBINDING());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " MANIF_TYPE => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONTYPE() +
                            " Manif_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getMANIFESTATIONTYPE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONTYPE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getMANIFESTATIONTYPE() != null)) {
                        Assert.assertEquals("The MANIF_TYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONTYPE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getMANIFESTATIONTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " STATUS => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS() +
                            " Manif_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSTATUS());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSTATUS() != null)) {
                        Assert.assertEquals("The STATUS is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSTATUS());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " WORKID => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getWORKID() +
                            " Manif_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getWORKID());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKID() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getWORKID() != null)) {
                        Assert.assertEquals("The WORKID is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getWORKID(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getWORKID());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " last_pub_date => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getLASTPUBDATE() +
                            " Manif_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getLASTPUBDATE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKID() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getWORKID() != null)) {
                        Assert.assertEquals("The LASTPUBDATE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getLASTPUBDATE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getLASTPUBDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DQ_ERR => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() +
                            " Manif_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_transform_history_manifestation_identifier_part":
                    Log.info("etl_transform_history_manifestation_identifier_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Manif_Ident_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "Manif_Ident_Curr_hist -> UKEY => " + dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in Manif_Ident_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => Manif_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " Manif_Ident_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " IDENTIFIER => Manif_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER() +
                            " Manif_Ident_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIER());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIER() != null)) {
                        Assert.assertEquals("The IDENTIFIER is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIER());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " IDENTIFIERTYPE => Manif_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE() +
                            " Manif_Ident_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIERTYPE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIERTYPE() != null)) {
                        Assert.assertEquals("The IDENTIFIERTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIERTYPE());
                    }
                    break;
                case "etl_transform_history_product_part":
                    Log.info("etl_transform_history_product_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Prod_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "Prod_Curr_hist -> UKEY => " + dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in Prod_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " BINDINGCODE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getBINDINGCODE() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getBINDINGCODE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getBINDINGCODE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getBINDINGCODE() != null)) {
                        Assert.assertEquals("The BINDINGCODE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getBINDINGCODE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getBINDINGCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " NAME => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getNAME() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getNAME());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getNAME() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getNAME() != null)) {
                        Assert.assertEquals("The NAME is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getNAME(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getNAME());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SHORTTITLE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSHORTTITLE() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSHORTTITLE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSHORTTITLE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSHORTTITLE() != null)) {
                        Assert.assertEquals("The SHORTTITLE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSHORTTITLE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSHORTTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " LAUNCHDATE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getLAUNCHDATE() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getLAUNCHDATE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getLAUNCHDATE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getLAUNCHDATE() != null)) {
                        Assert.assertEquals("The LAUNCHDATE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getLAUNCHDATE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getLAUNCHDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " TAXCODE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getTAXCODE() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getTAXCODE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getTAXCODE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getTAXCODE() != null)) {
                        Assert.assertEquals("The TAXCODE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getTAXCODE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getTAXCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " STATUS => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSTATUS());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSTATUS() != null)) {
                        Assert.assertEquals("The STATUS is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSTATUS());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " MANIFESTATIONREF => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONREF() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getMANIFESTATIONREF());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getMANIFESTATIONREF() != null)) {
                        Assert.assertEquals("The MANIFESTATIONREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getMANIFESTATIONREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " WORKSOURCE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCE() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getWORKSOURCE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getWORKSOURCE() != null)) {
                        Assert.assertEquals("The WORKSOURCE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getWORKSOURCE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " WORKTYPE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getWORKTYPE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getWORKTYPE() != null)) {
                        Assert.assertEquals("The WORKTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getWORKTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SEPRATELYSALEINDICATOR => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSEPRATELYSALEINDICATOR() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSEPRATELYSALEINDICATOR());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSEPRATELYSALEINDICATOR() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSEPRATELYSALEINDICATOR() != null)) {
                        Assert.assertEquals("The SEPRATELYSALEINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSEPRATELYSALEINDICATOR(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSEPRATELYSALEINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " TRIALALLOWEDINDICATOR => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getTRIALALLOWEDINDICATOR() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getTRIALALLOWEDINDICATOR());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getTRIALALLOWEDINDICATOR() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getTRIALALLOWEDINDICATOR() != null)) {
                        Assert.assertEquals("The TRIALALLOWEDINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getTRIALALLOWEDINDICATOR(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getTRIALALLOWEDINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " FWORKSOURCEREF => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getFWORKSOURCEREF() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getFWORKSOURCEREF());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getFWORKSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getFWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The FWORKSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getFWORKSOURCEREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getFWORKSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " PRODUCTTYPE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getPRODUCTTYPE() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getPRODUCTTYPE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getPRODUCTTYPE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getPRODUCTTYPE() != null)) {
                        Assert.assertEquals("The PRODUCTTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getPRODUCTTYPE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getPRODUCTTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " REVENUEMODEL => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getREVENUEMODEL() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getREVENUEMODEL());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getREVENUEMODEL() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getREVENUEMODEL() != null)) {
                        Assert.assertEquals("The REVENUEMODEL is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getREVENUEMODEL(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getREVENUEMODEL());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DQ_ERR => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() +
                            " Prod_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_transform_history_work_person_role_part":
                      Log.info("etl_transform_history_work_person_role_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("PERS_ROLE_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "PERS_ROLE_Curr_hist -> UKEY => " + dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in PERS_ROLE_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " WORKSOURCEREF => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCEREF() +
                            " PERS_ROLE_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getWORKSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The WORKSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCEREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getWORKSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " PERSONSOURCEREF => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getPERSONSOURCEREF() +
                            " PERS_ROLE_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getPERSONSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getPERSONSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getPERSONSOURCEREF() != null)) {
                        Assert.assertEquals("The PERSONSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getPERSONSOURCEREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getPERSONSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " ROLETYPE => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getROLETYPE() +
                            " PERS_ROLE_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getROLETYPE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getROLETYPE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getROLETYPE() != null)) {
                        Assert.assertEquals("The ROLETYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getROLETYPE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getROLETYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SEQUENCE => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSEQUENCE() +
                            " PERS_ROLE_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSEQUENCE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSEQUENCE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSEQUENCE() != null)) {
                        Assert.assertEquals("The SEQUENCE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSEQUENCE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSEQUENCE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DEDUPLICATOR => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDEDUPLICATOR() +
                            " PERS_ROLE_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getDEDUPLICATOR());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDEDUPLICATOR() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getDEDUPLICATOR() != null)) {
                        Assert.assertEquals("The DEDUPLICATOR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDEDUPLICATOR(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getDEDUPLICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DQ_ERR => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() +
                            " PERS_ROLE_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DEDUPLICATOR is incorrect for DQ_ERR = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_transform_history_work_relationship_part":
                    Log.info("etl_transform_history_work_relationship_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("WORK_RELAT_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "WORK_RELAT_Curr_hist -> UKEY => " + dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in WORK_RELAT_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " PARENTREF => WORK_RELAT_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getPARENTREF() +
                            " WORK_RELAT_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getPARENTREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getPARENTREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getPARENTREF() != null)) {
                        Assert.assertEquals("The PARENTREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getPARENTREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getPARENTREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " CHILDREF => WORK_RELAT_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getCHILDREF() +
                            " WORK_RELAT_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getCHILDREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getCHILDREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getCHILDREF() != null)) {
                        Assert.assertEquals("The CHILDREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getCHILDREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getCHILDREF());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " RELATIONTYPEREF => WORK_RELAT_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getRELATIONTYPEREF() +
                            " WORK_RELAT_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getRELATIONTYPEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getRELATIONTYPEREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getRELATIONTYPEREF() != null)) {
                        Assert.assertEquals("The RELATIONTYPEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getRELATIONTYPEREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getRELATIONTYPEREF());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " LASTUDATEDDATE => WORK_RELAT_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getLASTUDATEDDATE() +
                            " WORK_RELAT_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getLASTUDATEDDATE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getLASTUDATEDDATE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getLASTUDATEDDATE() != null)) {
                        Assert.assertEquals("The LASTUDATEDDATE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getLASTUDATEDDATE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getLASTUDATEDDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DQ_ERR => WORK_RELAT_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() +
                            " WORK_RELAT_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_transform_history_work_part":
                    Log.info("etl_transform_history_work_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("WORK_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "WORK_Curr_hist -> UKEY => " + dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in WORK_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " TITLE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getTITLE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getTITLE() != null)) {
                        Assert.assertEquals("The TITLE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SUBTITLE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSUBTITLE() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSUBTITLE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSUBTITLE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSUBTITLE() != null)) {
                        Assert.assertEquals("The SUBTITLE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSUBTITLE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSUBTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " VOLUMENO => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getVOLUMENO() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getVOLUMENO());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getVOLUMENO() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getVOLUMENO() != null)) {
                        Assert.assertEquals("The VOLUMENO is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getVOLUMENO(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getVOLUMENO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " COPYRIGHTYEAR => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getCOPYRIGHTYEAR() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getCOPYRIGHTYEAR());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getCOPYRIGHTYEAR() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getCOPYRIGHTYEAR() != null)) {
                        Assert.assertEquals("The COPYRIGHTYEAR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getCOPYRIGHTYEAR(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getCOPYRIGHTYEAR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " EDITIONNO => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getEDITIONNO() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getEDITIONNO());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getEDITIONNO() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getEDITIONNO() != null)) {
                        Assert.assertEquals("The EDITIONNO is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getEDITIONNO(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getEDITIONNO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " PMC => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getPMC() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getPMC());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getPMC() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getPMC() != null)) {
                        Assert.assertEquals("The PMC is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getPMC(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getPMC());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " WORKTYPE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getWORKTYPE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getWORKTYPE() != null)) {
                        Assert.assertEquals("The WORKTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getWORKTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " STATUSCODE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUSCODE() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSTATUSCODE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUSCODE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSTATUSCODE() != null)) {
                        Assert.assertEquals("The STATUSCODE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUSCODE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSTATUSCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " IMPRINTCODE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getIMPRINTCODE() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getIMPRINTCODE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getIMPRINTCODE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getIMPRINTCODE() != null)) {
                        Assert.assertEquals("The IMPRINTCODE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getIMPRINTCODE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getIMPRINTCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " TEOPCO => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getTEOPCO() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getTEOPCO());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getTEOPCO() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getTEOPCO() != null)) {
                        Assert.assertEquals("The TEOPCO is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getTEOPCO(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getTEOPCO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " OPCO => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getOPCO() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getOPCO());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getOPCO() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getOPCO() != null)) {
                        Assert.assertEquals("The OPCO is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getOPCO(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getOPCO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " RESPCENTRE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getRESPCENTRE() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getRESPCENTRE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getRESPCENTRE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getRESPCENTRE() != null)) {
                        Assert.assertEquals("The RESPCENTRE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getRESPCENTRE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getRESPCENTRE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " LANGUAGECODE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getLANGUAGECODE() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getLANGUAGECODE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getLANGUAGECODE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getLANGUAGECODE() != null)) {
                        Assert.assertEquals("The LANGUAGECODE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getLANGUAGECODE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getLANGUAGECODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " ELECTRORIGHTSINDICATOR => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getELECTRORIGHTSINDICATOR() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getELECTRORIGHTSINDICATOR());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getELECTRORIGHTSINDICATOR() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getELECTRORIGHTSINDICATOR() != null)) {
                        Assert.assertEquals("The ELECTRORIGHTSINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getELECTRORIGHTSINDICATOR(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getELECTRORIGHTSINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " FOAJOURNALTYPE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getFOAJOURNALTYPE() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getFOAJOURNALTYPE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getFOAJOURNALTYPE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getFOAJOURNALTYPE() != null)) {
                        Assert.assertEquals("The FOAJOURNALTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getFOAJOURNALTYPE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getFOAJOURNALTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " FSOCIETYOWNERSHIP => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getFSOCIETYOWNERSHIP() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getFSOCIETYOWNERSHIP());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getFSOCIETYOWNERSHIP() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getFSOCIETYOWNERSHIP() != null)) {
                        Assert.assertEquals("The FSOCIETYOWNERSHIP is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getFSOCIETYOWNERSHIP(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getFSOCIETYOWNERSHIP());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SUBSCRIPTIONTYPE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSUBSCRIPTIONTYPE() +
                            " WORK_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSUBSCRIPTIONTYPE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSUBSCRIPTIONTYPE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSUBSCRIPTIONTYPE() != null)) {
                        Assert.assertEquals("The SUBSCRIPTIONTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSUBSCRIPTIONTYPE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSUBSCRIPTIONTYPE());
                    }
                    break;
                case "etl_transform_history_work_identifier_part":
                    Log.info("etl_transform_history_work_identifier_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Work_Ident_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "Work_Ident_Curr_hist -> UKEY => " + dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in Manif_Ident_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => Work_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " Work_Ident_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " IDENTIFIER => Work_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER() +
                            " Work_Ident_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIER());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIER() != null)) {
                        Assert.assertEquals("The IDENTIFIER is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIER());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " IDENTIFIERTYPE => Work_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE() +
                            " Work_Ident_Curr_hist =" + dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIERTYPE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE() != null ||
                            (dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIERTYPE() != null)) {
                        Assert.assertEquals("The IDENTIFIERTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE(),
                                dataQualityBCSContext.recFromCurrentHist.get(i).getIDENTIFIERTYPE());
                    }
                    break;
            }

        }
    }

    @Given("^Get the (.*) of BCS Core data from Person Current Tables$")
    public void getRandomKeyFrmPersonCurr(String numberOfRecords) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core Person Current Tables....");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PERSON_KEY_CURRENT, numberOfRecords);
        List<Map<?, ?>> randomPersonIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomPersonIds.stream().map(m -> (Integer) m.get("u_key")).map(String::valueOf).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the Data from the BCS Core Person Current Tables$")
    public void getRecPersonCurr(){
     Log.info("Get Records from Person Current Table");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_CURR_DATA, Joiner.on(",").join(Ids));
        dataQualityBCSContext.recordsFromPersonCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^We Get the records from transform BCS Person Current History$")
    public void getRecPersonCurrHistory(){
        Log.info("Get Records from Person Current History Table");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_CURR_HIST_DATA, Joiner.on(",").join(Ids));
        dataQualityBCSContext.recFromPersonCurrentHist = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And ("^Compare the records of BCS Core Person current and BCS Person Current_History$")
    public void comparePersonCurrentandHist(){
        if (dataQualityBCSContext.recordsFromPersonCurrent.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Person Current and Person current Hist...");
            for (int i = 0; i < dataQualityBCSContext.recordsFromPersonCurrent.size(); i++) {
                dataQualityBCSContext.recordsFromPersonCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                dataQualityBCSContext.recFromPersonCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                Log.info("Person_Curr -> UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() +
                        "Person_Curr_Hist -> UKEY => " + dataQualityBCSContext.recFromPersonCurrentHist.get(i).getUKEY());
                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() != null ||
                        (dataQualityBCSContext.recFromPersonCurrentHist.get(i).getUKEY() != null)) {

                    Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() + " is missing/not found in Person_Curr",
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recFromPersonCurrentHist.get(i).getUKEY());
                }

                Log.info("UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() +
                        " FIRSTNAME => Person_Curr =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFIRSTNAME() +
                        " Person_Curr_Hist =" + dataQualityBCSContext.recFromPersonCurrentHist.get(i).getFIRSTNAME());

                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFIRSTNAME() != null ||
                        (dataQualityBCSContext.recFromPersonCurrentHist.get(i).getFIRSTNAME() != null)) {
                    Assert.assertEquals("The FIRSTNAME is incorrect for UKEY = " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFIRSTNAME(),
                            dataQualityBCSContext.recFromPersonCurrentHist.get(i).getFIRSTNAME());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() +
                        " FAMILYNAME => Person_Curr =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFAMILYNAME() +
                        " Person_Curr_Hist =" + dataQualityBCSContext.recFromPersonCurrentHist.get(i).getFAMILYNAME());

                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFAMILYNAME() != null ||
                        (dataQualityBCSContext.recFromPersonCurrentHist.get(i).getFAMILYNAME() != null)) {
                    Assert.assertEquals("The FAMILYNAME is incorrect for UKEY = " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFAMILYNAME(),
                            dataQualityBCSContext.recFromPersonCurrentHist.get(i).getFAMILYNAME());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() +
                        " PEOPLEHUBID => Person_Curr =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getPEOPLEHUBID() +
                        " Person_Curr_Hist =" + dataQualityBCSContext.recFromPersonCurrentHist.get(i).getPEOPLEHUBID());

                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getPEOPLEHUBID() != null ||
                        (dataQualityBCSContext.recFromPersonCurrentHist.get(i).getPEOPLEHUBID() != null)) {
                    Assert.assertEquals("The PEOPLEHUBID is incorrect for UKEY = " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getPEOPLEHUBID(),
                            dataQualityBCSContext.recFromPersonCurrentHist.get(i).getPEOPLEHUBID());
                }

                Log.info("UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() +
                        " EMAIL => Person_Curr =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getEMAIL() +
                        " Person_Curr_Hist =" + dataQualityBCSContext.recFromPersonCurrentHist.get(i).getEMAIL());

                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getEMAIL() != null ||
                        (dataQualityBCSContext.recFromPersonCurrentHist.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for UKEY = " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getEMAIL(),
                            dataQualityBCSContext.recFromPersonCurrentHist.get(i).getEMAIL());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getSOURCEREF() +
                        " DQ_ERR => PErson_Curr =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getDQ_ERR() +
                        " Person_Curr_Hist =" + dataQualityBCSContext.recFromPersonCurrentHist.get(i).getDQ_ERR());

                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getDQ_ERR() != null ||
                        (dataQualityBCSContext.recFromPersonCurrentHist.get(i).getDQ_ERR() != null)) {
                    Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getDQ_ERR(),
                            dataQualityBCSContext.recFromPersonCurrentHist.get(i).getDQ_ERR());
                }
            }
        }

    }

/*
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

    }*/
}




