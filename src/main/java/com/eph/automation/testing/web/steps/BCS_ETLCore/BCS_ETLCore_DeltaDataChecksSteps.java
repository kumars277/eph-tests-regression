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


public class BCS_ETLCore_DeltaDataChecksSteps {

    public BCSETL_CoreAccessDLContext dataQualityBCSContext;
    private static String sql;
    private static List<String> Ids;
    private BCS_ETLCoreDataChecksSQL bcsCoreObj = new BCS_ETLCoreDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^Get the (.*) of BCS Core data from transform_file Tables (.*)$")
    public void getIdsFromTransformFile(String numberOfRecords, String tableName) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core Transform_File Tables....");

        switch (tableName) {
            case "etl_accountable_product_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_ACCPROD_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_manifestation_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_manifestation_identifier_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_IDENT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_product_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PRODUCT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_person_role_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_relationship_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_RELATION_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_identifier_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_IDENT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
       }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("UKEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }



    @When ("^Get the Data from the Difference of Current and Previous transform_file Tables (.*)$")
    public void getRecFromDiffTransformFile(String tableName){
        Log.info("We get the records from Diff of Transform File BCS Core table...");
        switch (tableName) {
            case "etl_accountable_product_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_identifier_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_product_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_work_relationship_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_work_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_work_identifier_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromDiffOfTransformFile = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @When ("^We Get the records from delta current table BCS core (.*)$")
    public void getRecFromDeltaCurrent(String tableName){
        Log.info("We get the records from Delta Current BCS Core table...");
        switch (tableName) {
            case "etl_delta_current_accountable_product":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_manifestation":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_manifestation_identifier":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_product":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_work_person_role":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_work_relationship":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_work":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_DIFF_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_work_identifier":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromDeltaCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }


    @And("^Compare the records of BCS Core delta current and BCS diff of Transform_File (.*)$")
    public void compareCurrentandCurrHist(String targetTableName) {
        if (dataQualityBCSContext.recFromDiffOfTransformFile.isEmpty()) {
            Log.info("No Data Found in the Current Tables ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Delta Current and Diff of trans_file tables...");
        }
        for (int i = 0; i < dataQualityBCSContext.recFromDiffOfTransformFile.size(); i++) {
            switch (targetTableName) {
                case "etl_delta_current_accountable_product":
                    Log.info("etl_delta_current_accountable_product Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Acc_Prod_Trans_Diff -> UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            "Acc_Prod_Delta_Curr -> UKEY => " + dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY() != null)) {

                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() + " is missing/not found in Acc_Prod_Trans_Diff",
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " SOURCEREF => Acc_Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() +
                            " Acc_Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " ACCOUNTABLEPRODUCT => Acc_Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getACCOUNTABLEPRODUCT() +
                            " Acc_Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getACCOUNTABLEPRODUCT());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getACCOUNTABLEPRODUCT() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getACCOUNTABLEPRODUCT() != null)) {
                        Assert.assertEquals("The ACCOUNTABLEPRODUCT is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getACCOUNTABLEPRODUCT(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getACCOUNTABLEPRODUCT());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " ACCOUNTABLENAME => Acc_Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getACCOUNTABLENAME() +
                            " Acc_Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getACCOUNTABLENAME());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getACCOUNTABLENAME() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getACCOUNTABLENAME() != null)) {
                        Assert.assertEquals("The ACCOUNTABLENAME is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getACCOUNTABLENAME(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getACCOUNTABLENAME());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " ACCOUNTABLEPARENT => Acc_Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getACCOUNTABLEPARENT() +
                            " Acc_Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getACCOUNTABLEPARENT());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getACCOUNTABLEPARENT() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getACCOUNTABLEPARENT() != null)) {
                        Assert.assertEquals("The ACCOUNTABLEPARENT is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getACCOUNTABLEPARENT(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getACCOUNTABLEPARENT());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DElta_Mode => Acc_Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() +
                            " Acc_Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE() != null)) {
                        Assert.assertEquals("The DElta_Mode is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());

                    }
                    break;
                case "etl_delta_current_manifestation":
                    Log.info("etl_delta_current_manifestation Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Manif_Curr -> UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            "Manif_Delta_Curr -> UKEY => " + dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY() != null)) {

                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() + " is missing/not found in Manif_DeltaCurr",
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " SOURCEREF => Manif_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " TITLE => Manif_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTITLE() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getTITLE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTITLE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getTITLE() != null)) {
                        Assert.assertEquals("The TITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTITLE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " intereditionflag => Manif_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getINTEREDITIONFLAG() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getINTEREDITIONFLAG());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getINTEREDITIONFLAG() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getINTEREDITIONFLAG() != null)) {
                        Assert.assertEquals("The INTEREDITIONFLAG is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getINTEREDITIONFLAG(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getINTEREDITIONFLAG());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " firstpublisheddate => Manif_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFIRSTPUBLISHEDDATE() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getFIRSTPUBLISHEDDATE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFIRSTPUBLISHEDDATE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getFIRSTPUBLISHEDDATE() != null)) {
                        Assert.assertEquals("The INTEREDITIONFLAG is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFIRSTPUBLISHEDDATE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getFIRSTPUBLISHEDDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " binding => Manif_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getBINDING() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getBINDING());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getBINDING() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getBINDING() != null)) {
                        Assert.assertEquals("The BINDING is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getBINDING(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getBINDING());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " MANIF_TYPE => Manif_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getMANIFESTATIONTYPE() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getMANIFESTATIONTYPE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getMANIFESTATIONTYPE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getMANIFESTATIONTYPE() != null)) {
                        Assert.assertEquals("The MANIF_TYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getMANIFESTATIONTYPE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getMANIFESTATIONTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " STATUS => Manif_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSTATUS() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSTATUS());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSTATUS() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSTATUS() != null)) {
                        Assert.assertEquals("The STATUS is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSTATUS(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSTATUS());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " WORKID => Manif_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKID() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKID());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKID() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKID() != null)) {
                        Assert.assertEquals("The WORKID is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKID(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKID());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " last_pub_date => Manif_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getLASTPUBDATE() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getLASTPUBDATE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKID() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKID() != null)) {
                        Assert.assertEquals("The LASTPUBDATE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getLASTPUBDATE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getLASTPUBDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DQ_ERR => Manif_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DElta_Mode => Manif_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE() != null)) {
                        Assert.assertEquals("The DElta_Mode is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());

                    }
                    break;
                case "etl_delta_current_manifestation_identifier":
                    Log.info("etl_delta_current_manifestation_identifier Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Manif_Ident_Trans_Diff -> UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            "Manif_Ident_Delta_Curr -> UKEY => " + dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() + " is missing/not found in Manif_Ident_Delta_Curr",
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " SOURCEREF => Manif_Ident_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() +
                            " Manif_Ident_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " IDENTIFIER => Manif_Ident_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIER() +
                            " Manif_Ident_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIER());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIER() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIER() != null)) {
                        Assert.assertEquals("The IDENTIFIER is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIER(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIER());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " IDENTIFIERTYPE => Manif_Ident_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIERTYPE() +
                            " Manif_Ident_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIERTYPE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIERTYPE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIERTYPE() != null)) {
                        Assert.assertEquals("The IDENTIFIERTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIERTYPE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIERTYPE());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DElta_Mode => Manif_Ident_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() +
                            " Manif_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE() != null)) {
                        Assert.assertEquals("The DElta_Mode is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());

                    }
                    break;
                case "etl_delta_current_product":
                    Log.info("etl_delta_current_product Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Prod_Trans_Diff -> UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            "Prod_Delta_Curr -> UKEY => " + dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() + " is missing/not found in Prod_Delta_Curr",
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " SOURCEREF => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " BINDINGCODE => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getBINDINGCODE() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getBINDINGCODE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getBINDINGCODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getBINDINGCODE() != null)) {
                        Assert.assertEquals("The BINDINGCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getBINDINGCODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getBINDINGCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " NAME => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getNAME() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getNAME());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getNAME() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getNAME() != null)) {
                        Assert.assertEquals("The NAME is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getNAME(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getNAME());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " SHORTTITLE => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSHORTTITLE() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSHORTTITLE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSHORTTITLE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSHORTTITLE() != null)) {
                        Assert.assertEquals("The SHORTTITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSHORTTITLE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSHORTTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " LAUNCHDATE => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getLAUNCHDATE() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getLAUNCHDATE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getLAUNCHDATE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getLAUNCHDATE() != null)) {
                        Assert.assertEquals("The LAUNCHDATE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getLAUNCHDATE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getLAUNCHDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " TAXCODE => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTAXCODE() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getTAXCODE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTAXCODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getTAXCODE() != null)) {
                        Assert.assertEquals("The TAXCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTAXCODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getTAXCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " STATUS => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSTATUS() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSTATUS());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSTATUS() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSTATUS() != null)) {
                        Assert.assertEquals("The STATUS is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSTATUS(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSTATUS());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " MANIFESTATIONREF => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getMANIFESTATIONREF() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getMANIFESTATIONREF());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getMANIFESTATIONREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getMANIFESTATIONREF() != null)) {
                        Assert.assertEquals("The MANIFESTATIONREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getMANIFESTATIONREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getMANIFESTATIONREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " WORKSOURCE => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKSOURCE() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKSOURCE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKSOURCE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKSOURCE() != null)) {
                        Assert.assertEquals("The WORKSOURCE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKSOURCE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKSOURCE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " WORKTYPE => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKTYPE() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKTYPE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKTYPE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKTYPE() != null)) {
                        Assert.assertEquals("The WORKTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKTYPE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " SEPRATELYSALEINDICATOR => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSEPRATELYSALEINDICATOR() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSEPRATELYSALEINDICATOR());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSEPRATELYSALEINDICATOR() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSEPRATELYSALEINDICATOR() != null)) {
                        Assert.assertEquals("The SEPRATELYSALEINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSEPRATELYSALEINDICATOR(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSEPRATELYSALEINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " TRIALALLOWEDINDICATOR => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTRIALALLOWEDINDICATOR() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getTRIALALLOWEDINDICATOR());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTRIALALLOWEDINDICATOR() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getTRIALALLOWEDINDICATOR() != null)) {
                        Assert.assertEquals("The TRIALALLOWEDINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTRIALALLOWEDINDICATOR(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getTRIALALLOWEDINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " FWORKSOURCEREF => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFWORKSOURCEREF() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getFWORKSOURCEREF());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFWORKSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getFWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The FWORKSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFWORKSOURCEREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getFWORKSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " PRODUCTTYPE => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPRODUCTTYPE() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getPRODUCTTYPE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPRODUCTTYPE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getPRODUCTTYPE() != null)) {
                        Assert.assertEquals("The PRODUCTTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPRODUCTTYPE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getPRODUCTTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " REVENUEMODEL => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getREVENUEMODEL() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getREVENUEMODEL());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getREVENUEMODEL() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getREVENUEMODEL() != null)) {
                        Assert.assertEquals("The REVENUEMODEL is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getREVENUEMODEL(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getREVENUEMODEL());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DQ_ERR => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DQ_ERR => Prod_Trans_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() +
                            " Prod_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());
                    }
                    break;
                case "etl_delta_current_work_person_role":
                    Log.info("etl_delta_current_work_person_role Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("PERS_ROLE_Tran_Diff -> UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            "PERS_ROLE_Delta_Curr -> UKEY => " + dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() + " is missing/not found in PERS_ROLE_Delta_Curr",
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " WORKSOURCEREF => PERS_ROLE_Tran_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKSOURCEREF() +
                            " PERS_ROLE_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The WORKSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKSOURCEREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " PERSONSOURCEREF => PERS_ROLE_Tran_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPERSONSOURCEREF() +
                            " PERS_ROLE_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getPERSONSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPERSONSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getPERSONSOURCEREF() != null)) {
                        Assert.assertEquals("The PERSONSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPERSONSOURCEREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getPERSONSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " ROLETYPE => PERS_ROLE_Tran_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getROLETYPE() +
                            " PERS_ROLE_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getROLETYPE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getROLETYPE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getROLETYPE() != null)) {
                        Assert.assertEquals("The ROLETYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getROLETYPE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getROLETYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " SEQUENCE => PERS_ROLE_Tran_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSEQUENCE() +
                            " PERS_ROLE_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSEQUENCE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSEQUENCE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSEQUENCE() != null)) {
                        Assert.assertEquals("The SEQUENCE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSEQUENCE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSEQUENCE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DEDUPLICATOR => PERS_ROLE_Tran_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDEDUPLICATOR() +
                            " PERS_ROLE_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDEDUPLICATOR());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDEDUPLICATOR() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDEDUPLICATOR() != null)) {
                        Assert.assertEquals("The DEDUPLICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDEDUPLICATOR(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDEDUPLICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DQ_ERR => PERS_ROLE_Tran_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR() +
                            " PERS_ROLE_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for U_KEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DELTA_MODE => PERS_ROLE_Tran_Diff =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() +
                            " PERS_ROLE_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE() != null)) {
                        Assert.assertEquals("The DELTA_MODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());
                    }
                    break;
                case "etl_delta_current_work_relationship":
                    Log.info("etl_delta_current_work_relationship Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("WORK_RELAT_Trans_File -> UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            "WORK_RELAT_Delta_Curr -> UKEY => " + dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() + " is missing/not found in WORK_RELAT_Delta_Curr",
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " PARENTREF => WORK_RELAT_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPARENTREF() +
                            " WORK_RELAT_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getPARENTREF());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPARENTREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getPARENTREF() != null)) {
                        Assert.assertEquals("The PARENTREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPARENTREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getPARENTREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " CHILDREF => WORK_RELAT_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getCHILDREF() +
                            " WORK_RELAT_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getCHILDREF());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getCHILDREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getCHILDREF() != null)) {
                        Assert.assertEquals("The CHILDREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getCHILDREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getCHILDREF());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " RELATIONTYPEREF => WORK_RELAT_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getRELATIONTYPEREF() +
                            " WORK_RELAT_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getRELATIONTYPEREF());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getRELATIONTYPEREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getRELATIONTYPEREF() != null)) {
                        Assert.assertEquals("The RELATIONTYPEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getRELATIONTYPEREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getRELATIONTYPEREF());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " LASTUDATEDDATE => WORK_RELAT_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getLASTUDATEDDATE() +
                            " WORK_RELAT_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getLASTUDATEDDATE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getLASTUDATEDDATE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getLASTUDATEDDATE() != null)) {
                        Assert.assertEquals("The LASTUDATEDDATE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getLASTUDATEDDATE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getLASTUDATEDDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DQ_ERR => WORK_RELAT_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR() +
                            " WORK_RELAT_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDQ_ERR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DELTA_MODE => WORK_RELAT_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() +
                            " WORK_RELAT_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE() != null)) {
                        Assert.assertEquals("The DELTA_MODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());
                    }

                    break;
                case "etl_delta_current_work":
                    Log.info("etl_delta_current_work Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("WORK_Trans_File -> UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            "WORK_Delta_Curr -> UKEY => " + dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() + " is missing/not found in WORK_Delta_Curr",
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " SOURCEREF => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " TITLE => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTITLE() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getTITLE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTITLE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getTITLE() != null)) {
                        Assert.assertEquals("The TITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTITLE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " SUBTITLE => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSUBTITLE() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSUBTITLE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSUBTITLE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSUBTITLE() != null)) {
                        Assert.assertEquals("The SUBTITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSUBTITLE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSUBTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " VOLUMENO => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getVOLUMENO() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getVOLUMENO());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getVOLUMENO() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getVOLUMENO() != null)) {
                        Assert.assertEquals("The VOLUMENO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getVOLUMENO(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getVOLUMENO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " COPYRIGHTYEAR => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getCOPYRIGHTYEAR() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getCOPYRIGHTYEAR());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getCOPYRIGHTYEAR() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getCOPYRIGHTYEAR() != null)) {
                        Assert.assertEquals("The COPYRIGHTYEAR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getCOPYRIGHTYEAR(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getCOPYRIGHTYEAR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " EDITIONNO => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEDITIONNO() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getEDITIONNO());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEDITIONNO() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getEDITIONNO() != null)) {
                        Assert.assertEquals("The EDITIONNO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEDITIONNO(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getEDITIONNO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " PMC => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPMC() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getPMC());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPMC() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getPMC() != null)) {
                        Assert.assertEquals("The PMC is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getPMC(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getPMC());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " WORKTYPE => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKTYPE() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKTYPE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKTYPE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKTYPE() != null)) {
                        Assert.assertEquals("The WORKTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getWORKTYPE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getWORKTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " STATUSCODE => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSTATUSCODE() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSTATUSCODE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSTATUSCODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSTATUSCODE() != null)) {
                        Assert.assertEquals("The STATUSCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSTATUSCODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSTATUSCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " IMPRINTCODE => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIMPRINTCODE() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getIMPRINTCODE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIMPRINTCODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getIMPRINTCODE() != null)) {
                        Assert.assertEquals("The IMPRINTCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIMPRINTCODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getIMPRINTCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " TEOPCO => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTEOPCO() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getTEOPCO());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTEOPCO() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getTEOPCO() != null)) {
                        Assert.assertEquals("The TEOPCO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getTEOPCO(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getTEOPCO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " OPCO => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getOPCO() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getOPCO());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getOPCO() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getOPCO() != null)) {
                        Assert.assertEquals("The OPCO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getOPCO(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getOPCO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " RESPCENTRE => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getRESPCENTRE() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getRESPCENTRE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getRESPCENTRE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getRESPCENTRE() != null)) {
                        Assert.assertEquals("The RESPCENTRE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getRESPCENTRE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getRESPCENTRE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " LANGUAGECODE => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getLANGUAGECODE() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getLANGUAGECODE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getLANGUAGECODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getLANGUAGECODE() != null)) {
                        Assert.assertEquals("The LANGUAGECODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getLANGUAGECODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getLANGUAGECODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " ELECTRORIGHTSINDICATOR => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getELECTRORIGHTSINDICATOR() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getELECTRORIGHTSINDICATOR());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getELECTRORIGHTSINDICATOR() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getELECTRORIGHTSINDICATOR() != null)) {
                        Assert.assertEquals("The ELECTRORIGHTSINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getELECTRORIGHTSINDICATOR(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getELECTRORIGHTSINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " FOAJOURNALTYPE => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFOAJOURNALTYPE() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getFOAJOURNALTYPE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFOAJOURNALTYPE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getFOAJOURNALTYPE() != null)) {
                        Assert.assertEquals("The FOAJOURNALTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFOAJOURNALTYPE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getFOAJOURNALTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " FSOCIETYOWNERSHIP => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFSOCIETYOWNERSHIP() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getFSOCIETYOWNERSHIP());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFSOCIETYOWNERSHIP() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getFSOCIETYOWNERSHIP() != null)) {
                        Assert.assertEquals("The FSOCIETYOWNERSHIP is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getFSOCIETYOWNERSHIP(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getFSOCIETYOWNERSHIP());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " SUBSCRIPTIONTYPE => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSUBSCRIPTIONTYPE() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSUBSCRIPTIONTYPE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSUBSCRIPTIONTYPE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSUBSCRIPTIONTYPE() != null)) {
                        Assert.assertEquals("The SUBSCRIPTIONTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSUBSCRIPTIONTYPE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSUBSCRIPTIONTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DELTA_MODE => WORK_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() +
                            " WORK_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE() != null)) {
                        Assert.assertEquals("The DELTA_MODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());
                    }

                    break;
                case "etl_delta_current_work_identifier":
                    Log.info("etl_delta_current_work_identifier Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Work_Ident_Trans_File -> UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            "Work_Ident_Delta_Curr -> UKEY => " + dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() + " is missing/not found in Manif_Ident_Curr",
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " SOURCEREF => Work_Ident_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() +
                            " Work_Ident_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " IDENTIFIER => Work_Ident_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIER() +
                            " Work_Ident_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIER());
                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIER() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIER() != null)) {
                        Assert.assertEquals("The IDENTIFIER is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIER(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIER());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " IDENTIFIERTYPE => Work_Ident_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIERTYPE() +
                            " Work_Ident_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIERTYPE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIERTYPE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIERTYPE() != null)) {
                        Assert.assertEquals("The IDENTIFIERTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getIDENTIFIERTYPE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getIDENTIFIERTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                            " DELTA_MODE => Work_Ident_Trans_File =" + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() +
                            " Work_Ident_Delta_Curr =" + dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());

                    if (dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE() != null ||
                            (dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE() != null)) {
                        Assert.assertEquals("The DELTA_MODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getDELTA_MODE(),
                                dataQualityBCSContext.recFromDeltaCurrent.get(i).getDELTA_MODE());
                    }

                    break;
            }



        }
    }

    @Given("^Get the (.*) of BCS Core data from person transform_file Tables$")
    public void getRandKeyFromPersonDiffTransFile(String numberOfRecords) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core Person Diff of current and previous Transform File Tables....");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PERSON_KEY_DIFF_TRANS_FILE, numberOfRecords);
        List<Map<?, ?>> randomPersonIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomPersonIds.stream().map(m -> (Integer) m.get("UKEY")).map(String::valueOf).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the Data from the Difference of Current and Previous person transform_file Tables$")
    public void getRecPersonDiffOFTransFile(){
        Log.info("Get Records from Person Difference of Transform File Table");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_DIFF_TRANS_FILE, Joiner.on(",").join(Ids));
        dataQualityBCSContext.recFromPersonDiffOfTransformFile = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^We Get the records from person delta current table BCS core$")
    public void getRecPersonDeltaCurr(){
        Log.info("Get Records from Person Delta Current Table");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_DELTA_CURR, Joiner.on(",").join(Ids));
        dataQualityBCSContext.recFromPersonDeltaCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }


    @And ("^Compare the records of BCS Core person delta current and BCS person diff of Transform_File$")
    public void comparePersonCurrentandTransFile(){
        if (dataQualityBCSContext.recFromPersonDiffOfTransformFile.isEmpty()) {
            Log.info("No Data Found in Person Trans_File Diff....");
        } else {
            Log.info("Sorting the Ids to compare the records between Person Delta Current and Person Diff Transform File...");
            for (int i = 0; i < dataQualityBCSContext.recFromPersonDiffOfTransformFile.size(); i++) {
                dataQualityBCSContext.recFromPersonDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                dataQualityBCSContext.recFromPersonDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                Log.info("Person_trans_diff -> UKEY => " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY() +
                        "Person_Delta_Curr -> UKEY => " + dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getUKEY());
                if (dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY() != null ||
                        (dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getUKEY() != null)) {

                    Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY() + " is missing/not found in Person_trans_diff",
                            dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY(),
                            dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getUKEY());
                }

                Log.info("UKEY => " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY() +
                        " FIRSTNAME => Person_trans_diff =" + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getFIRSTNAME() +
                        " Person_Delta_Curr =" + dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getFIRSTNAME());

                if (dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getFIRSTNAME() != null ||
                        (dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getFIRSTNAME() != null)) {
                    Assert.assertEquals("The FIRSTNAME is incorrect for UKEY = " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY(),
                            dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getFIRSTNAME(),
                            dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getFIRSTNAME());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY() +
                        " FAMILYNAME => Person_trans_diff =" + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getFAMILYNAME() +
                        " Person_Delta_Curr =" + dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getFAMILYNAME());

                if (dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getFAMILYNAME() != null ||
                        (dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getFAMILYNAME() != null)) {
                    Assert.assertEquals("The FAMILYNAME is incorrect for UKEY = " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY(),
                            dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getFAMILYNAME(),
                            dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getFAMILYNAME());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY() +
                        " PEOPLEHUBID => Person_trans_diff =" + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getPEOPLEHUBID() +
                        " Person_Delta_Curr =" + dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getPEOPLEHUBID());

                if (dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getPEOPLEHUBID() != null ||
                        (dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getPEOPLEHUBID() != null)) {
                    Assert.assertEquals("The PEOPLEHUBID is incorrect for UKEY = " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY(),
                            dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getPEOPLEHUBID(),
                            dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getPEOPLEHUBID());
                }

                Log.info("UKEY => " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY() +
                        " EMAIL => Person_trans_diff =" + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getEMAIL() +
                        " Person_Delta_Curr =" + dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getEMAIL());

                if (dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getEMAIL() != null ||
                        (dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for UKEY = " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY(),
                            dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getEMAIL(),
                            dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getEMAIL());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getSOURCEREF() +
                        " DQ_ERR => Person_trans_diff =" + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getDQ_ERR() +
                        " Person_Delta_Curr =" + dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getDQ_ERR());

                if (dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getDQ_ERR() != null ||
                        (dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getDQ_ERR() != null)) {
                    Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY(),
                            dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getDQ_ERR(),
                            dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getDQ_ERR());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getSOURCEREF() +
                        " DELTA_MODE => Person_trans_diff =" + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getDELTA_MODE() +
                        " Person_Delta_Curr =" + dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getDELTA_MODE());

                if (dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getDELTA_MODE() != null ||
                        (dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getDELTA_MODE() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for UKEY = " + dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getUKEY(),
                            dataQualityBCSContext.recFromPersonDiffOfTransformFile.get(i).getDELTA_MODE(),
                            dataQualityBCSContext.recFromPersonDeltaCurrent.get(i).getDELTA_MODE());
                }

            }
        }

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
    }*//*




   @Then("^We Get the records from transform BCS Current History (.*)$")
   public void getRecFromt(String tableName){
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
*/
/*


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


    @Then("^We Get the records from transform BCS Transform_File (.*)$")
    public void getRecFromTransformFile(String tableName){
        Log.info("We get the records from Transform_File BCS Core table...");
        switch (tableName) {
            case "etl_accountable_product_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_identifier_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_product_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_relationship_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_identifier_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromTransformFile = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare the records of BCS Core current and BCS Transform_File (.*)$")
    public void compareCurrandTransFile(String targetTableName) {
        if (dataQualityBCSContext.recordsFromCurrent.isEmpty()) {
            Log.info("No Data Found in the Current Tables ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Current and transform_file tables...");
        }
        for (int i = 0; i < dataQualityBCSContext.recordsFromCurrent.size(); i++) {
            switch (targetTableName) {
                case "etl_accountable_product_transform_file_history_part":
                    Log.info("etl_accountable_product_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Acc_Prod_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "Acc_Prod_trans_file -> UKEY => " + dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getUKEY() != null)) {

                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in Acc_Prod_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " Acc_Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " ACCOUNTABLEPRODUCT => Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPRODUCT() +
                            " Acc_Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getACCOUNTABLEPRODUCT());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPRODUCT() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getACCOUNTABLEPRODUCT() != null)) {
                        Assert.assertEquals("The ACCOUNTABLEPRODUCT is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPRODUCT(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getACCOUNTABLEPRODUCT());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " ACCOUNTABLENAME => Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLENAME() +
                            " Acc_Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getACCOUNTABLENAME());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLENAME() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getACCOUNTABLENAME() != null)) {
                        Assert.assertEquals("The ACCOUNTABLENAME is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLENAME(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getACCOUNTABLENAME());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " ACCOUNTABLEPARENT => Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPARENT() +
                            " Acc_Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getACCOUNTABLEPARENT());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPARENT() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getACCOUNTABLEPARENT() != null)) {
                        Assert.assertEquals("The ACCOUNTABLEPARENT is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getACCOUNTABLEPARENT(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getACCOUNTABLEPARENT());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DQ_ERR => Acc_Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() +
                            " Acc_Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_manifestation_transform_file_history_part":
                    Log.info("etl_manifestation_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Manif_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "Manif_trans_file -> UKEY => " + dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getUKEY() != null)) {

                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in Manif_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " Manif_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " TITLE => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE() +
                            " Manif_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getTITLE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getTITLE() != null)) {
                        Assert.assertEquals("The TITLE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " intereditionflag => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getINTEREDITIONFLAG() +
                            " Manif_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getINTEREDITIONFLAG());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getINTEREDITIONFLAG() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getINTEREDITIONFLAG() != null)) {
                        Assert.assertEquals("The INTEREDITIONFLAG is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getINTEREDITIONFLAG(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getINTEREDITIONFLAG());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " firstpublisheddate => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getFIRSTPUBLISHEDDATE() +
                            " Manif_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getFIRSTPUBLISHEDDATE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getFIRSTPUBLISHEDDATE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getFIRSTPUBLISHEDDATE() != null)) {
                        Assert.assertEquals("The INTEREDITIONFLAG is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getFIRSTPUBLISHEDDATE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getFIRSTPUBLISHEDDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " binding => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getBINDING() +
                            " Manif_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getBINDING());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getBINDING() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getBINDING() != null)) {
                        Assert.assertEquals("The BINDING is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getBINDING(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getBINDING());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " MANIF_TYPE => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONTYPE() +
                            " Manif_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getMANIFESTATIONTYPE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONTYPE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getMANIFESTATIONTYPE() != null)) {
                        Assert.assertEquals("The MANIF_TYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONTYPE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getMANIFESTATIONTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " STATUS => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS() +
                            " Manif_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSTATUS());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSTATUS() != null)) {
                        Assert.assertEquals("The STATUS is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSTATUS());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " WORKID => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getWORKID() +
                            " Manif_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getWORKID());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKID() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getWORKID() != null)) {
                        Assert.assertEquals("The WORKID is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getWORKID(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getWORKID());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " last_pub_date => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getLASTPUBDATE() +
                            " Manif_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getLASTPUBDATE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKID() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getWORKID() != null)) {
                        Assert.assertEquals("The LASTPUBDATE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getLASTPUBDATE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getLASTPUBDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DQ_ERR => Manif_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() +
                            " Manif_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_manifestation_identifier_transform_file_history_part":
                    Log.info("etl_manifestation_identifier_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Manif_Ident_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "Manif_Ident_trans_file -> UKEY => " + dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in Manif_Ident_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => Manif_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " Manif_Ident_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " IDENTIFIER => Manif_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER() +
                            " Manif_Ident_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIER());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIER() != null)) {
                        Assert.assertEquals("The IDENTIFIER is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIER());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " IDENTIFIERTYPE => Manif_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE() +
                            " Manif_Ident_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIERTYPE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIERTYPE() != null)) {
                        Assert.assertEquals("The IDENTIFIERTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIERTYPE());
                    }
                    break;
                case "etl_product_transform_file_history_part":
                    Log.info("etl_product_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Prod_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "Prod_trans_file -> UKEY => " + dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in Prod_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " BINDINGCODE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getBINDINGCODE() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getBINDINGCODE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getBINDINGCODE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getBINDINGCODE() != null)) {
                        Assert.assertEquals("The BINDINGCODE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getBINDINGCODE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getBINDINGCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " NAME => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getNAME() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getNAME());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getNAME() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getNAME() != null)) {
                        Assert.assertEquals("The NAME is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getNAME(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getNAME());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SHORTTITLE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSHORTTITLE() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSHORTTITLE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSHORTTITLE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSHORTTITLE() != null)) {
                        Assert.assertEquals("The SHORTTITLE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSHORTTITLE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSHORTTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " LAUNCHDATE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getLAUNCHDATE() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getLAUNCHDATE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getLAUNCHDATE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getLAUNCHDATE() != null)) {
                        Assert.assertEquals("The LAUNCHDATE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getLAUNCHDATE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getLAUNCHDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " TAXCODE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getTAXCODE() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getTAXCODE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getTAXCODE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getTAXCODE() != null)) {
                        Assert.assertEquals("The TAXCODE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getTAXCODE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getTAXCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " STATUS => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSTATUS());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSTATUS() != null)) {
                        Assert.assertEquals("The STATUS is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUS(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSTATUS());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " MANIFESTATIONREF => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONREF() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getMANIFESTATIONREF());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getMANIFESTATIONREF() != null)) {
                        Assert.assertEquals("The MANIFESTATIONREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getMANIFESTATIONREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getMANIFESTATIONREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " WORKSOURCE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCE() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getWORKSOURCE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getWORKSOURCE() != null)) {
                        Assert.assertEquals("The WORKSOURCE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getWORKSOURCE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " WORKTYPE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getWORKTYPE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getWORKTYPE() != null)) {
                        Assert.assertEquals("The WORKTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getWORKTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SEPRATELYSALEINDICATOR => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSEPRATELYSALEINDICATOR() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSEPRATELYSALEINDICATOR());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSEPRATELYSALEINDICATOR() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSEPRATELYSALEINDICATOR() != null)) {
                        Assert.assertEquals("The SEPRATELYSALEINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSEPRATELYSALEINDICATOR(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSEPRATELYSALEINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " TRIALALLOWEDINDICATOR => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getTRIALALLOWEDINDICATOR() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getTRIALALLOWEDINDICATOR());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getTRIALALLOWEDINDICATOR() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getTRIALALLOWEDINDICATOR() != null)) {
                        Assert.assertEquals("The TRIALALLOWEDINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getTRIALALLOWEDINDICATOR(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getTRIALALLOWEDINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " FWORKSOURCEREF => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getFWORKSOURCEREF() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getFWORKSOURCEREF());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getFWORKSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getFWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The FWORKSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getFWORKSOURCEREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getFWORKSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " PRODUCTTYPE => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getPRODUCTTYPE() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getPRODUCTTYPE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getPRODUCTTYPE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getPRODUCTTYPE() != null)) {
                        Assert.assertEquals("The PRODUCTTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getPRODUCTTYPE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getPRODUCTTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " REVENUEMODEL => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getREVENUEMODEL() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getREVENUEMODEL());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getREVENUEMODEL() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getREVENUEMODEL() != null)) {
                        Assert.assertEquals("The REVENUEMODEL is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getREVENUEMODEL(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getREVENUEMODEL());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DQ_ERR => Prod_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() +
                            " Prod_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_work_person_role_transform_file_history_part":
                    Log.info("etl_work_person_role_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("PERS_ROLE_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "PERS_ROLE_trans_file -> UKEY => " + dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in PERS_ROLE_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " WORKSOURCEREF => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCEREF() +
                            " PERS_ROLE_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getWORKSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The WORKSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getWORKSOURCEREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getWORKSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " PERSONSOURCEREF => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getPERSONSOURCEREF() +
                            " PERS_ROLE_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getPERSONSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getPERSONSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getPERSONSOURCEREF() != null)) {
                        Assert.assertEquals("The PERSONSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getPERSONSOURCEREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getPERSONSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " ROLETYPE => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getROLETYPE() +
                            " PERS_ROLE_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getROLETYPE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getROLETYPE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getROLETYPE() != null)) {
                        Assert.assertEquals("The ROLETYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getROLETYPE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getROLETYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SEQUENCE => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSEQUENCE() +
                            " PERS_ROLE_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSEQUENCE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSEQUENCE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSEQUENCE() != null)) {
                        Assert.assertEquals("The SEQUENCE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSEQUENCE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSEQUENCE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DEDUPLICATOR => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDEDUPLICATOR() +
                            " PERS_ROLE_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getDEDUPLICATOR());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDEDUPLICATOR() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getDEDUPLICATOR() != null)) {
                        Assert.assertEquals("The DEDUPLICATOR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDEDUPLICATOR(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getDEDUPLICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DQ_ERR => PERS_ROLE_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() +
                            " PERS_ROLE_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DEDUPLICATOR is incorrect for DQ_ERR = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_work_relationship_transform_file_history_part":
                    Log.info("etl_work_relationship_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("WORK_RELAT_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "WORK_RELAT_trans_file -> UKEY => " + dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in WORK_RELAT_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " PARENTREF => WORK_RELAT_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getPARENTREF() +
                            " WORK_RELAT_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getPARENTREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getPARENTREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getPARENTREF() != null)) {
                        Assert.assertEquals("The PARENTREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getPARENTREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getPARENTREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " CHILDREF => WORK_RELAT_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getCHILDREF() +
                            " WORK_RELAT_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getCHILDREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getCHILDREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getCHILDREF() != null)) {
                        Assert.assertEquals("The CHILDREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getCHILDREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getCHILDREF());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " RELATIONTYPEREF => WORK_RELAT_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getRELATIONTYPEREF() +
                            " WORK_RELAT_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getRELATIONTYPEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getRELATIONTYPEREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getRELATIONTYPEREF() != null)) {
                        Assert.assertEquals("The RELATIONTYPEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getRELATIONTYPEREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getRELATIONTYPEREF());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " LASTUDATEDDATE => WORK_RELAT_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getLASTUDATEDDATE() +
                            " WORK_RELAT_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getLASTUDATEDDATE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getLASTUDATEDDATE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getLASTUDATEDDATE() != null)) {
                        Assert.assertEquals("The LASTUDATEDDATE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getLASTUDATEDDATE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getLASTUDATEDDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " DQ_ERR => WORK_RELAT_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() +
                            " WORK_RELAT_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_work_transform_file_history_part":
                    Log.info("etl_work_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("WORK_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "WORK_trans_file -> UKEY => " + dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in WORK_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " TITLE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getTITLE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getTITLE() != null)) {
                        Assert.assertEquals("The TITLE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getTITLE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SUBTITLE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSUBTITLE() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSUBTITLE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSUBTITLE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSUBTITLE() != null)) {
                        Assert.assertEquals("The SUBTITLE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSUBTITLE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSUBTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " VOLUMENO => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getVOLUMENO() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getVOLUMENO());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getVOLUMENO() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getVOLUMENO() != null)) {
                        Assert.assertEquals("The VOLUMENO is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getVOLUMENO(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getVOLUMENO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " COPYRIGHTYEAR => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getCOPYRIGHTYEAR() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getCOPYRIGHTYEAR());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getCOPYRIGHTYEAR() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getCOPYRIGHTYEAR() != null)) {
                        Assert.assertEquals("The COPYRIGHTYEAR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getCOPYRIGHTYEAR(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getCOPYRIGHTYEAR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " EDITIONNO => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getEDITIONNO() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getEDITIONNO());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getEDITIONNO() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getEDITIONNO() != null)) {
                        Assert.assertEquals("The EDITIONNO is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getEDITIONNO(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getEDITIONNO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " PMC => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getPMC() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getPMC());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getPMC() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getPMC() != null)) {
                        Assert.assertEquals("The PMC is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getPMC(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getPMC());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " WORKTYPE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getWORKTYPE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getWORKTYPE() != null)) {
                        Assert.assertEquals("The WORKTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getWORKTYPE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getWORKTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " STATUSCODE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUSCODE() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSTATUSCODE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUSCODE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSTATUSCODE() != null)) {
                        Assert.assertEquals("The STATUSCODE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSTATUSCODE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSTATUSCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " IMPRINTCODE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getIMPRINTCODE() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getIMPRINTCODE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getIMPRINTCODE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getIMPRINTCODE() != null)) {
                        Assert.assertEquals("The IMPRINTCODE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getIMPRINTCODE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getIMPRINTCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " TEOPCO => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getTEOPCO() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getTEOPCO());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getTEOPCO() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getTEOPCO() != null)) {
                        Assert.assertEquals("The TEOPCO is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getTEOPCO(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getTEOPCO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " OPCO => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getOPCO() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getOPCO());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getOPCO() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getOPCO() != null)) {
                        Assert.assertEquals("The OPCO is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getOPCO(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getOPCO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " RESPCENTRE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getRESPCENTRE() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getRESPCENTRE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getRESPCENTRE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getRESPCENTRE() != null)) {
                        Assert.assertEquals("The RESPCENTRE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getRESPCENTRE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getRESPCENTRE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " LANGUAGECODE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getLANGUAGECODE() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getLANGUAGECODE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getLANGUAGECODE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getLANGUAGECODE() != null)) {
                        Assert.assertEquals("The LANGUAGECODE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getLANGUAGECODE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getLANGUAGECODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " ELECTRORIGHTSINDICATOR => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getELECTRORIGHTSINDICATOR() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getELECTRORIGHTSINDICATOR());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getELECTRORIGHTSINDICATOR() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getELECTRORIGHTSINDICATOR() != null)) {
                        Assert.assertEquals("The ELECTRORIGHTSINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getELECTRORIGHTSINDICATOR(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getELECTRORIGHTSINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " FOAJOURNALTYPE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getFOAJOURNALTYPE() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getFOAJOURNALTYPE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getFOAJOURNALTYPE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getFOAJOURNALTYPE() != null)) {
                        Assert.assertEquals("The FOAJOURNALTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getFOAJOURNALTYPE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getFOAJOURNALTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " FSOCIETYOWNERSHIP => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getFSOCIETYOWNERSHIP() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getFSOCIETYOWNERSHIP());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getFSOCIETYOWNERSHIP() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getFSOCIETYOWNERSHIP() != null)) {
                        Assert.assertEquals("The FSOCIETYOWNERSHIP is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getFSOCIETYOWNERSHIP(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getFSOCIETYOWNERSHIP());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SUBSCRIPTIONTYPE => WORK_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSUBSCRIPTIONTYPE() +
                            " WORK_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSUBSCRIPTIONTYPE());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSUBSCRIPTIONTYPE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSUBSCRIPTIONTYPE() != null)) {
                        Assert.assertEquals("The SUBSCRIPTIONTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSUBSCRIPTIONTYPE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSUBSCRIPTIONTYPE());
                    }
                    break;
                case "etl_work_identifier_transform_file_history_part":
                    Log.info("etl_work_identifier_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Work_Ident_Curr -> UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            "Work_Ident_trans_file -> UKEY => " + dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() + " is missing/not found in Manif_Ident_Curr",
                                dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " SOURCEREF => Work_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                            " Work_Ident_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " IDENTIFIER => Work_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER() +
                            " Work_Ident_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIER());
                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIER() != null)) {
                        Assert.assertEquals("The IDENTIFIER is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIER(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIER());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                            " IDENTIFIERTYPE => Work_Ident_Curr =" + dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE() +
                            " Work_Ident_trans_file =" + dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIERTYPE());

                    if (dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE() != null ||
                            (dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIERTYPE() != null)) {
                        Assert.assertEquals("The IDENTIFIERTYPE is incorrect for UKEY = " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                dataQualityBCSContext.recordsFromCurrent.get(i).getIDENTIFIERTYPE(),
                                dataQualityBCSContext.recFromTransformFile.get(i).getIDENTIFIERTYPE());
                    }
                    break;
            }

        }
    }

    @Then("^We Get the records from Transform file Person of BCS Core$")
    public void getRecPersonTransFile(){
        Log.info("Get Records from Person Transform File Table");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_TRANS_FILE_DATA, Joiner.on(",").join(Ids));
        dataQualityBCSContext.recFromPersonTransFile = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And ("^Compare the records of BCS Core Person current and BCS Person Transform_File$")
    public void comparePersonCurrentandTransFile(){
        if (dataQualityBCSContext.recordsFromPersonCurrent.isEmpty()) {
            Log.info("No Data Found in Person Current....");
        } else {
            Log.info("Sorting the Ids to compare the records between Person Current and Person Transform File...");
            for (int i = 0; i < dataQualityBCSContext.recordsFromPersonCurrent.size(); i++) {
                dataQualityBCSContext.recordsFromPersonCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                dataQualityBCSContext.recFromPersonTransFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                Log.info("Person_Curr -> UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() +
                        "Person_trans_file -> UKEY => " + dataQualityBCSContext.recFromPersonTransFile.get(i).getUKEY());
                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() != null ||
                        (dataQualityBCSContext.recFromPersonTransFile.get(i).getUKEY() != null)) {

                    Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() + " is missing/not found in Person_Curr",
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recFromPersonTransFile.get(i).getUKEY());
                }

                Log.info("UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() +
                        " FIRSTNAME => Person_Curr =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFIRSTNAME() +
                        " Person_trans_file =" + dataQualityBCSContext.recFromPersonTransFile.get(i).getFIRSTNAME());

                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFIRSTNAME() != null ||
                        (dataQualityBCSContext.recFromPersonTransFile.get(i).getFIRSTNAME() != null)) {
                    Assert.assertEquals("The FIRSTNAME is incorrect for UKEY = " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFIRSTNAME(),
                            dataQualityBCSContext.recFromPersonTransFile.get(i).getFIRSTNAME());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() +
                        " FAMILYNAME => Person_Curr =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFAMILYNAME() +
                        " Person_trans_file =" + dataQualityBCSContext.recFromPersonTransFile.get(i).getFAMILYNAME());

                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFAMILYNAME() != null ||
                        (dataQualityBCSContext.recFromPersonTransFile.get(i).getFAMILYNAME() != null)) {
                    Assert.assertEquals("The FAMILYNAME is incorrect for UKEY = " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getFAMILYNAME(),
                            dataQualityBCSContext.recFromPersonTransFile.get(i).getFAMILYNAME());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() +
                        " PEOPLEHUBID => Person_Curr =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getPEOPLEHUBID() +
                        " Person_trans_file =" + dataQualityBCSContext.recFromPersonTransFile.get(i).getPEOPLEHUBID());

                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getPEOPLEHUBID() != null ||
                        (dataQualityBCSContext.recFromPersonTransFile.get(i).getPEOPLEHUBID() != null)) {
                    Assert.assertEquals("The PEOPLEHUBID is incorrect for UKEY = " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getPEOPLEHUBID(),
                            dataQualityBCSContext.recFromPersonTransFile.get(i).getPEOPLEHUBID());
                }

                Log.info("UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY() +
                        " EMAIL => Person_Curr =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getEMAIL() +
                        " Person_trans_file =" + dataQualityBCSContext.recFromPersonTransFile.get(i).getEMAIL());

                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getEMAIL() != null ||
                        (dataQualityBCSContext.recFromPersonTransFile.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for UKEY = " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getEMAIL(),
                            dataQualityBCSContext.recFromPersonTransFile.get(i).getEMAIL());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getSOURCEREF() +
                        " DQ_ERR => PErson_Curr =" + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getDQ_ERR() +
                        " Person_trans_file =" + dataQualityBCSContext.recFromPersonTransFile.get(i).getDQ_ERR());

                if (dataQualityBCSContext.recordsFromPersonCurrent.get(i).getDQ_ERR() != null ||
                        (dataQualityBCSContext.recFromPersonTransFile.get(i).getDQ_ERR() != null)) {
                    Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recordsFromPersonCurrent.get(i).getUKEY(),
                            dataQualityBCSContext.recordsFromPersonCurrent.get(i).getDQ_ERR(),
                            dataQualityBCSContext.recFromPersonTransFile.get(i).getDQ_ERR());
                }
            }
        }

    }




*//*
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




