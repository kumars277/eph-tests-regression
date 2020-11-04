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


public class BCS_ETLCore_LatestDataChecksSteps {

    public BCSETL_CoreAccessDLContext dataQualityBCSContext;
    private static String sql;
    private static List<String> Ids;
    private BCS_ETLCoreDataChecksSQL bcsCoreObj = new BCS_ETLCoreDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^Get the (.*) from diff of delta_current and current_hist tables (.*)$")
    public void getIdsFromDiffOfDeltaCurrAndCurrHist(String numberOfRecords, String tableName) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core Tables from Diff of Delta Current and Current Hist....");

        switch (tableName) {
            case "etl_transform_history_accountable_product_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_ACCPROD_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_manifestation_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_manifestation_identifier_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_IDENT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_product_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PRODUCT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_work_person_role_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_work_relationship_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_RELATION_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_work_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_work_identifier_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_IDENT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
       }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("UKEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }



    @When ("^Get the records from the diff of delta_current and current_hist tables (.*)$")
    public void getRecFromDiffDeltaCurrAndCurrHist(String tableName){
        Log.info("We get the records from Diff of Delta Current and Current Hist of BCS Core table...");
        switch (tableName) {
            case "etl_transform_history_accountable_product_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_manifestation_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_manifestation_identifier_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_product_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_person_role_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_relationship_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_identifier_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @When ("^Get the records from (.*) exclude table$")
    public void getRecFromDeltaCurrent(String tableName){
        Log.info("We get the records from Exclude delta BCS Core table...");
        switch (tableName) {
            case "etl_transform_history_accountable_product_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_manifestation_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_manifestation_identifier_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_product_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_person_role_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_relationship_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_DIFF_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_identifier_excl_delta":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromExclDelta = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare the records of Exclude with diff of delta_current and current_hist tables (.*)$")
    public void compareExclwithDiffOfDeltaAndCurrHist(String targetTableName) {
        if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Exclude delta and Diff of delta and current hist tables...");
        }
        for (int i = 0; i < dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.size(); i++) {
            switch (targetTableName) {
                case "etl_transform_history_accountable_product_excl_delta":
                    Log.info("etl_transform_history_accountable_product_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Acc_Prod_Diff_DeltaCurr_CurrHist -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            "Acc_Prod_Excl_Delta -> UKEY => " + dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getUKEY() != null)) {

                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() + " is missing/not found in Acc_Prod_Exclude",
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " SOURCEREF => Acc_Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() +
                            " Acc_Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " ACCOUNTABLEPRODUCT => Acc_Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getACCOUNTABLEPRODUCT() +
                            " Acc_Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getACCOUNTABLEPRODUCT());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getACCOUNTABLEPRODUCT() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getACCOUNTABLEPRODUCT() != null)) {
                        Assert.assertEquals("The ACCOUNTABLEPRODUCT is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getACCOUNTABLEPRODUCT(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getACCOUNTABLEPRODUCT());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " ACCOUNTABLENAME => Acc_Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getACCOUNTABLENAME() +
                            " Acc_Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getACCOUNTABLENAME());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getACCOUNTABLENAME() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getACCOUNTABLENAME() != null)) {
                        Assert.assertEquals("The ACCOUNTABLENAME is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getACCOUNTABLENAME(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getACCOUNTABLENAME());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " ACCOUNTABLEPARENT => Acc_Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getACCOUNTABLEPARENT() +
                            " Acc_Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getACCOUNTABLEPARENT());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getACCOUNTABLEPARENT() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getACCOUNTABLEPARENT() != null)) {
                        Assert.assertEquals("The ACCOUNTABLEPARENT is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getACCOUNTABLEPARENT(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getACCOUNTABLEPARENT());
                    }
                    break;
                case "etl_transform_history_manifestation_excl_delta":
                    Log.info("etl_transform_history_manifestation_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Manif_Diff_DeltaCurr_CurrHist -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            "Manif_Excl_Delta -> UKEY => " + dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getUKEY() != null)) {

                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() + " is missing/not found in Manif_DEXclude",
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " SOURCEREF => Manif_Diff_Exc_DeltaHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() +
                            " Manif_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " TITLE => Manif_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTITLE() +
                            " Manif_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getTITLE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTITLE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getTITLE() != null)) {
                        Assert.assertEquals("The TITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTITLE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " intereditionflag => Manif_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getINTEREDITIONFLAG() +
                            " Manif_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getINTEREDITIONFLAG());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getINTEREDITIONFLAG() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getINTEREDITIONFLAG() != null)) {
                        Assert.assertEquals("The INTEREDITIONFLAG is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getINTEREDITIONFLAG(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getINTEREDITIONFLAG());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " firstpublisheddate => Manif_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFIRSTPUBLISHEDDATE() +
                            " Manif_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getFIRSTPUBLISHEDDATE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFIRSTPUBLISHEDDATE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getFIRSTPUBLISHEDDATE() != null)) {
                        Assert.assertEquals("The INTEREDITIONFLAG is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFIRSTPUBLISHEDDATE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getFIRSTPUBLISHEDDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " binding => Manif_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getBINDING() +
                            " Manif_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getBINDING());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getBINDING() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getBINDING() != null)) {
                        Assert.assertEquals("The BINDING is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getBINDING(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getBINDING());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " MANIF_TYPE => Manif_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getMANIFESTATIONTYPE() +
                            " Manif_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getMANIFESTATIONTYPE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getMANIFESTATIONTYPE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getMANIFESTATIONTYPE() != null)) {
                        Assert.assertEquals("The MANIF_TYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getMANIFESTATIONTYPE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getMANIFESTATIONTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " STATUS => Manif_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSTATUS() +
                            " Manif_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSTATUS());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSTATUS() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSTATUS() != null)) {
                        Assert.assertEquals("The STATUS is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSTATUS(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSTATUS());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " WORKID => Manif_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKID() +
                            " Manif_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getWORKID());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKID() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getWORKID() != null)) {
                        Assert.assertEquals("The WORKID is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKID(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getWORKID());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " last_pub_date => Manif_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getLASTPUBDATE() +
                            " Manif_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getLASTPUBDATE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKID() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getWORKID() != null)) {
                        Assert.assertEquals("The LASTPUBDATE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getLASTPUBDATE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getLASTPUBDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " DQ_ERR => Manif_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR() +
                            " Manif_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR());
                    }

                    break;
                case "etl_transform_history_manifestation_identifier_excl_delta":
                    Log.info("etl_transform_history_manifestation_identifier_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Manif_Ident_Diff_DeltaCurr_CurrHist -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            "Manif_Ident_Excl_Delta -> UKEY => " + dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() + " is missing/not found in Manif_Ident_Excl_Delta",
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " SOURCEREF => Manif_Ident_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() +
                            " Manif_Ident_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " IDENTIFIER => Manif_Ident_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIER() +
                            " Manif_Ident_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIER());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIER() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIER() != null)) {
                        Assert.assertEquals("The IDENTIFIER is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIER(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIER());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " IDENTIFIERTYPE => Manif_Ident_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIERTYPE() +
                            " Manif_Ident_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIERTYPE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIERTYPE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIERTYPE() != null)) {
                        Assert.assertEquals("The IDENTIFIERTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIERTYPE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIERTYPE());
                    }

                    break;
                case "etl_transform_history_product_excl_delta":
                    Log.info("etl_transform_history_product_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Prod_Diff_DeltaCurr_CurrHist -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            "Prod_Excl_Delta -> UKEY => " + dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() + " is missing/not found in Prod_Excl_Delta",
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " SOURCEREF => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " BINDINGCODE => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getBINDINGCODE() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getBINDINGCODE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getBINDINGCODE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getBINDINGCODE() != null)) {
                        Assert.assertEquals("The BINDINGCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getBINDINGCODE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getBINDINGCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " NAME => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getNAME() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getNAME());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getNAME() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getNAME() != null)) {
                        Assert.assertEquals("The NAME is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getNAME(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getNAME());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " SHORTTITLE => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSHORTTITLE() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSHORTTITLE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSHORTTITLE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSHORTTITLE() != null)) {
                        Assert.assertEquals("The SHORTTITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSHORTTITLE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSHORTTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " LAUNCHDATE => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getLAUNCHDATE() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getLAUNCHDATE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getLAUNCHDATE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getLAUNCHDATE() != null)) {
                        Assert.assertEquals("The LAUNCHDATE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getLAUNCHDATE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getLAUNCHDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " TAXCODE => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTAXCODE() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getTAXCODE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTAXCODE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getTAXCODE() != null)) {
                        Assert.assertEquals("The TAXCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTAXCODE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getTAXCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " STATUS => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSTATUS() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSTATUS());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSTATUS() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSTATUS() != null)) {
                        Assert.assertEquals("The STATUS is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSTATUS(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSTATUS());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " MANIFESTATIONREF => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getMANIFESTATIONREF() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getMANIFESTATIONREF());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getMANIFESTATIONREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getMANIFESTATIONREF() != null)) {
                        Assert.assertEquals("The MANIFESTATIONREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getMANIFESTATIONREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getMANIFESTATIONREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " WORKSOURCE => Prod_Diff_Excl_DeltaHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKSOURCE() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getWORKSOURCE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKSOURCE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getWORKSOURCE() != null)) {
                        Assert.assertEquals("The WORKSOURCE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKSOURCE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getWORKSOURCE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " WORKTYPE => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKTYPE() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getWORKTYPE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKTYPE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getWORKTYPE() != null)) {
                        Assert.assertEquals("The WORKTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKTYPE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getWORKTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " SEPRATELYSALEINDICATOR => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSEPRATELYSALEINDICATOR() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSEPRATELYSALEINDICATOR());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSEPRATELYSALEINDICATOR() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSEPRATELYSALEINDICATOR() != null)) {
                        Assert.assertEquals("The SEPRATELYSALEINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSEPRATELYSALEINDICATOR(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSEPRATELYSALEINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " TRIALALLOWEDINDICATOR => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTRIALALLOWEDINDICATOR() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getTRIALALLOWEDINDICATOR());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTRIALALLOWEDINDICATOR() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getTRIALALLOWEDINDICATOR() != null)) {
                        Assert.assertEquals("The TRIALALLOWEDINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTRIALALLOWEDINDICATOR(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getTRIALALLOWEDINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " FWORKSOURCEREF => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFWORKSOURCEREF() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getFWORKSOURCEREF());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFWORKSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getFWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The FWORKSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFWORKSOURCEREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getFWORKSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " PRODUCTTYPE => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPRODUCTTYPE() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getPRODUCTTYPE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPRODUCTTYPE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getPRODUCTTYPE() != null)) {
                        Assert.assertEquals("The PRODUCTTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPRODUCTTYPE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getPRODUCTTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " REVENUEMODEL => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getREVENUEMODEL() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getREVENUEMODEL());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getREVENUEMODEL() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getREVENUEMODEL() != null)) {
                        Assert.assertEquals("The REVENUEMODEL is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getREVENUEMODEL(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getREVENUEMODEL());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " DQ_ERR => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " DQ_ERR => Prod_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDELTA_MODE() +
                            " Prod_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getDELTA_MODE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDELTA_MODE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getDELTA_MODE() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDELTA_MODE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getDELTA_MODE());
                    }
                    break;
                case "etl_transform_history_work_person_role_excl_delta":
                    Log.info("etl_transform_history_work_person_role_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("PERS_ROLE_Diff_DeltaCurr_CurrHist -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            "PERS_ROLE_Excl_Delta -> UKEY => " + dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() + " is missing/not found in PERS_ROLE_Excl_Delta",
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " WORKSOURCEREF => PERS_ROLE_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKSOURCEREF() +
                            " PERS_ROLE_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getWORKSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The WORKSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKSOURCEREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getWORKSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " PERSONSOURCEREF => PERS_ROLE_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPERSONSOURCEREF() +
                            " PERS_ROLE_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getPERSONSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPERSONSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getPERSONSOURCEREF() != null)) {
                        Assert.assertEquals("The PERSONSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPERSONSOURCEREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getPERSONSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " ROLETYPE => PERS_ROLE_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getROLETYPE() +
                            " PERS_ROLE_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getROLETYPE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getROLETYPE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getROLETYPE() != null)) {
                        Assert.assertEquals("The ROLETYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getROLETYPE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getROLETYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " SEQUENCE => PERS_ROLE_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSEQUENCE() +
                            " PERS_ROLE_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSEQUENCE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSEQUENCE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSEQUENCE() != null)) {
                        Assert.assertEquals("The SEQUENCE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSEQUENCE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSEQUENCE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " DEDUPLICATOR => PERS_ROLE_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDEDUPLICATOR() +
                            " PERS_ROLE_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getDEDUPLICATOR());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDEDUPLICATOR() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getDEDUPLICATOR() != null)) {
                        Assert.assertEquals("The DEDUPLICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDEDUPLICATOR(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getDEDUPLICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " DQ_ERR => PERS_ROLE_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR() +
                            " PERS_ROLE_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for U_KEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_transform_history_work_relationship_excl_delta":
                    Log.info("etl_transform_history_work_relationship_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("WORK_RELAT_Diff_DeltaCurr_CurrHist -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            "WORK_RELAT_Excl_Delta -> UKEY => " + dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() + " is missing/not found in WORK_RELAT_Excl_Delta",
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " PARENTREF => WORK_RELAT_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPARENTREF() +
                            " WORK_RELAT_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getPARENTREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPARENTREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getPARENTREF() != null)) {
                        Assert.assertEquals("The PARENTREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPARENTREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getPARENTREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " CHILDREF => WORK_RELAT_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getCHILDREF() +
                            " WORK_RELAT_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getCHILDREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getCHILDREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getCHILDREF() != null)) {
                        Assert.assertEquals("The CHILDREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getCHILDREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getCHILDREF());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " RELATIONTYPEREF => WORK_RELAT_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getRELATIONTYPEREF() +
                            " WORK_RELAT_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getRELATIONTYPEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getRELATIONTYPEREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getRELATIONTYPEREF() != null)) {
                        Assert.assertEquals("The RELATIONTYPEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getRELATIONTYPEREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getRELATIONTYPEREF());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " LASTUDATEDDATE => WORK_RELAT_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getLASTUDATEDDATE() +
                            " WORK_RELAT_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getLASTUDATEDDATE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getLASTUDATEDDATE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getLASTUDATEDDATE() != null)) {
                        Assert.assertEquals("The LASTUDATEDDATE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getLASTUDATEDDATE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getLASTUDATEDDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " DQ_ERR => WORK_RELAT_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR() +
                            " WORK_RELAT_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getDQ_ERR());
                    }

                break;
                case "etl_transform_history_work_excl_delta":
                    Log.info("etl_transform_history_work_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("WORK_Diff_DeltaCurr_CurrHist -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            "WORK_Excl_Delta -> UKEY => " + dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() + " is missing/not found in WORK_Excl_Delta",
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " SOURCEREF => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " TITLE => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTITLE() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getTITLE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTITLE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getTITLE() != null)) {
                        Assert.assertEquals("The TITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTITLE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " SUBTITLE => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSUBTITLE() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSUBTITLE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSUBTITLE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSUBTITLE() != null)) {
                        Assert.assertEquals("The SUBTITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSUBTITLE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSUBTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " VOLUMENO => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getVOLUMENO() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getVOLUMENO());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getVOLUMENO() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getVOLUMENO() != null)) {
                        Assert.assertEquals("The VOLUMENO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getVOLUMENO(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getVOLUMENO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " COPYRIGHTYEAR => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getCOPYRIGHTYEAR() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getCOPYRIGHTYEAR());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getCOPYRIGHTYEAR() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getCOPYRIGHTYEAR() != null)) {
                        Assert.assertEquals("The COPYRIGHTYEAR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getCOPYRIGHTYEAR(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getCOPYRIGHTYEAR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " EDITIONNO => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEDITIONNO() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getEDITIONNO());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEDITIONNO() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getEDITIONNO() != null)) {
                        Assert.assertEquals("The EDITIONNO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEDITIONNO(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getEDITIONNO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " PMC => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPMC() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getPMC());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPMC() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getPMC() != null)) {
                        Assert.assertEquals("The PMC is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getPMC(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getPMC());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " WORKTYPE => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKTYPE() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getWORKTYPE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKTYPE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getWORKTYPE() != null)) {
                        Assert.assertEquals("The WORKTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getWORKTYPE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getWORKTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " STATUSCODE => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSTATUSCODE() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSTATUSCODE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSTATUSCODE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSTATUSCODE() != null)) {
                        Assert.assertEquals("The STATUSCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSTATUSCODE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSTATUSCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " IMPRINTCODE => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIMPRINTCODE() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getIMPRINTCODE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIMPRINTCODE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getIMPRINTCODE() != null)) {
                        Assert.assertEquals("The IMPRINTCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIMPRINTCODE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getIMPRINTCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " TEOPCO => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTEOPCO() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getTEOPCO());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTEOPCO() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getTEOPCO() != null)) {
                        Assert.assertEquals("The TEOPCO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getTEOPCO(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getTEOPCO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " OPCO => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getOPCO() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getOPCO());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getOPCO() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getOPCO() != null)) {
                        Assert.assertEquals("The OPCO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getOPCO(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getOPCO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " RESPCENTRE => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getRESPCENTRE() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getRESPCENTRE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getRESPCENTRE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getRESPCENTRE() != null)) {
                        Assert.assertEquals("The RESPCENTRE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getRESPCENTRE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getRESPCENTRE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " LANGUAGECODE => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getLANGUAGECODE() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getLANGUAGECODE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getLANGUAGECODE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getLANGUAGECODE() != null)) {
                        Assert.assertEquals("The LANGUAGECODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getLANGUAGECODE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getLANGUAGECODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " ELECTRORIGHTSINDICATOR => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getELECTRORIGHTSINDICATOR() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getELECTRORIGHTSINDICATOR());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getELECTRORIGHTSINDICATOR() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getELECTRORIGHTSINDICATOR() != null)) {
                        Assert.assertEquals("The ELECTRORIGHTSINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getELECTRORIGHTSINDICATOR(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getELECTRORIGHTSINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " FOAJOURNALTYPE => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFOAJOURNALTYPE() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getFOAJOURNALTYPE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFOAJOURNALTYPE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getFOAJOURNALTYPE() != null)) {
                        Assert.assertEquals("The FOAJOURNALTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFOAJOURNALTYPE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getFOAJOURNALTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " FSOCIETYOWNERSHIP => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFSOCIETYOWNERSHIP() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getFSOCIETYOWNERSHIP());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFSOCIETYOWNERSHIP() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getFSOCIETYOWNERSHIP() != null)) {
                        Assert.assertEquals("The FSOCIETYOWNERSHIP is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getFSOCIETYOWNERSHIP(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getFSOCIETYOWNERSHIP());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " SUBSCRIPTIONTYPE => WORK_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSUBSCRIPTIONTYPE() +
                            " WORK_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSUBSCRIPTIONTYPE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSUBSCRIPTIONTYPE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSUBSCRIPTIONTYPE() != null)) {
                        Assert.assertEquals("The SUBSCRIPTIONTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSUBSCRIPTIONTYPE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSUBSCRIPTIONTYPE());
                    }

                   break;
                case "etl_transform_history_work_identifier_excl_delta":
                    Log.info("etl_transform_history_work_identifier_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Work_Ident_Diff_DeltaCurr_CurrHist -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            "Work_Ident_Excl_Delta -> UKEY => " + dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() + " is missing/not found in Manif_Ident_Curr",
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " SOURCEREF => Work_Ident_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() +
                            " Work_Ident_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " IDENTIFIER => Work_Ident_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIER() +
                            " Work_Ident_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIER());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIER() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIER() != null)) {
                        Assert.assertEquals("The IDENTIFIER is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIER(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIER());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                            " IDENTIFIERTYPE => Work_Ident_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIERTYPE() +
                            " Work_Ident_Excl_Delta =" + dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIERTYPE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIERTYPE() != null ||
                            (dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIERTYPE() != null)) {
                        Assert.assertEquals("The IDENTIFIERTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getIDENTIFIERTYPE(),
                                dataQualityBCSContext.recFromExclDelta.get(i).getIDENTIFIERTYPE());
                    }

                    break;
            }
       }
    }



    @Given("^Get the (.*) from diff of person delta_current and current_hist tables$")
    public void getRandKeyFromPersonDiffDeltaAndCurrHist(String numberOfRecords) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core Person Diff of Delta current and Current History Tables....");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PERSON_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
        List<Map<?, ?>> randomPersonIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomPersonIds.stream().map(m -> (Integer) m.get("UKEY")).map(String::valueOf).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the records from the diff of person delta_current and current_hist tables$")
    public void getRecPersonDiffOFDeltaAndCurrHist(){
        Log.info("Get Records from Person Diff of Delta current and Current History Tables");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_DIFF_DELTACURR_CURRHIST, Joiner.on(",").join(Ids));
        dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Get the records from person exclude BCS core table$")
    public void getRecPersonExclDelta(){
        Log.info("Get Records from Person Exclude Delta Table");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_EXCL_DELTA, Joiner.on(",").join(Ids));
        dataQualityBCSContext.recFromPersonExclDelta = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And ("^Compare the records of Exclude with diff of person delta_current and current_hist tables$")
    public void comparePersonExclAndDiffOfDeltaAndCurrHist(){
        if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.isEmpty()) {
            Log.info("No Data Found in  Diff of Delta and Current History table....");
        } else {
            Log.info("Sorting the Ids to compare the records between Person Exclude and Person Diff Delta and Current Hist...");
            for (int i = 0; i < dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.size(); i++) {
                dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                dataQualityBCSContext.recFromPersonExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                Log.info("Person_Diff_DeltaCurr_CurrHist -> UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY() +
                        "Person_Excl_Delta -> UKEY => " + dataQualityBCSContext.recFromPersonExclDelta.get(i).getUKEY());
                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY() != null ||
                        (dataQualityBCSContext.recFromPersonExclDelta.get(i).getUKEY() != null)) {

                    Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY() + " is missing/not found in Person_Excl_Delta",
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY(),
                            dataQualityBCSContext.recFromPersonExclDelta.get(i).getUKEY());
                }

                Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY() +
                        " FIRSTNAME => Person_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getFIRSTNAME() +
                        " Person_Excl_Delta =" + dataQualityBCSContext.recFromPersonExclDelta.get(i).getFIRSTNAME());

                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getFIRSTNAME() != null ||
                        (dataQualityBCSContext.recFromPersonExclDelta.get(i).getFIRSTNAME() != null)) {
                    Assert.assertEquals("The FIRSTNAME is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY(),
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getFIRSTNAME(),
                            dataQualityBCSContext.recFromPersonExclDelta.get(i).getFIRSTNAME());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY() +
                        " FAMILYNAME => Person_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getFAMILYNAME() +
                        " Person_Excl_Delta =" + dataQualityBCSContext.recFromPersonExclDelta.get(i).getFAMILYNAME());

                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getFAMILYNAME() != null ||
                        (dataQualityBCSContext.recFromPersonExclDelta.get(i).getFAMILYNAME() != null)) {
                    Assert.assertEquals("The FAMILYNAME is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY(),
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getFAMILYNAME(),
                            dataQualityBCSContext.recFromPersonExclDelta.get(i).getFAMILYNAME());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY() +
                        " PEOPLEHUBID => Person_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getPEOPLEHUBID() +
                        " Person_Excl_Delta =" + dataQualityBCSContext.recFromPersonExclDelta.get(i).getPEOPLEHUBID());

                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getPEOPLEHUBID() != null ||
                        (dataQualityBCSContext.recFromPersonExclDelta.get(i).getPEOPLEHUBID() != null)) {
                    Assert.assertEquals("The PEOPLEHUBID is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY(),
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getPEOPLEHUBID(),
                            dataQualityBCSContext.recFromPersonExclDelta.get(i).getPEOPLEHUBID());
                }

                Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY() +
                        " EMAIL => Person_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getEMAIL() +
                        " Person_Excl_Delta =" + dataQualityBCSContext.recFromPersonExclDelta.get(i).getEMAIL());

                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getEMAIL() != null ||
                        (dataQualityBCSContext.recFromPersonExclDelta.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY(),
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getEMAIL(),
                            dataQualityBCSContext.recFromPersonExclDelta.get(i).getEMAIL());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getSOURCEREF() +
                        " DQ_ERR => Person_Diff_DeltaCurr_CurrHist =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getDQ_ERR() +
                        " Person_Excl_Delta =" + dataQualityBCSContext.recFromPersonExclDelta.get(i).getDQ_ERR());

                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getDQ_ERR() != null ||
                        (dataQualityBCSContext.recFromPersonExclDelta.get(i).getDQ_ERR() != null)) {
                    Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY(),
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getDQ_ERR(),
                            dataQualityBCSContext.recFromPersonExclDelta.get(i).getDQ_ERR());
                }
            }
        }
    }


    @Given("^Get the (.*) from diff of delta_current and exclude_delta tables (.*)$")
    public void getIdsFromDiffOfDeltaCurrAndExcl(String numberOfRecords, String tableName) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core Tables from Diff of Delta Current and Exclude....");

        switch (tableName) {
            case "etl_transform_history_accountable_product_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_ACCPROD_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_manifestation_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_IDENT_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_product_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PRODUCT_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_work_person_role_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_work_relationship_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_RELATION_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_work_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_work_identifier_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_IDENT_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("UKEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }


    @When ("^Get the records from the diff of delta_current and exclude_delta tables (.*)$")
    public void getRecFromDiffDeltaCurrAndExcl(String tableName){
        Log.info("We get the records from Diff of Delta Current and Excl of BCS Core table...");
        switch (tableName) {
            case "etl_transform_history_accountable_product_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_DIFF_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_manifestation_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_DIFF_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_DIFF_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_product_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_DIFF_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_person_role_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_DIFF_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_relationship_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_DIFF_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_DIFF_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_identifier_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_DIFF_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromDiffOfDeltaAndExcl = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @When ("^Get the records from (.*) BCS core latest table$")
    public void getRecFromLatest(String tableName){
        Log.info("We get the records from Latest BCS Core table...");
        switch (tableName) {
            case "etl_transform_history_accountable_product_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_manifestation_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_product_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_person_role_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_relationship_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_work_identifier_latest":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_LATEST, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromLatest = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare the records of Latest with diff of delta_current and Exclude_Delta tables (.*)$")
    public void compareLatestwithDiffOfDeltaAndExcl(String targetTableName) {
        if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Latest and Diff of delta and exclude tables...");
        }
        for (int i = 0; i < dataQualityBCSContext.recFromDiffOfDeltaAndExcl.size(); i++) {
            switch (targetTableName) {
                case "etl_transform_history_accountable_product_latest":
                    Log.info("etl_transform_history_accountable_product_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Acc_Prod_Diff_DeltaCurr_Excl -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            "Acc_Prod_Latest -> UKEY => " + dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getUKEY() != null)) {

                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() + " is missing/not found in Acc_Prod_Latest",
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " SOURCEREF => Acc_Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() +
                            " Acc_Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " ACCOUNTABLEPRODUCT => Acc_Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getACCOUNTABLEPRODUCT() +
                            " Acc_Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getACCOUNTABLEPRODUCT());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getACCOUNTABLEPRODUCT() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getACCOUNTABLEPRODUCT() != null)) {
                        Assert.assertEquals("The ACCOUNTABLEPRODUCT is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getACCOUNTABLEPRODUCT(),
                                dataQualityBCSContext.recFromLatest.get(i).getACCOUNTABLEPRODUCT());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " ACCOUNTABLENAME => Acc_Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getACCOUNTABLENAME() +
                            " Acc_Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getACCOUNTABLENAME());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getACCOUNTABLENAME() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getACCOUNTABLENAME() != null)) {
                        Assert.assertEquals("The ACCOUNTABLENAME is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getACCOUNTABLENAME(),
                                dataQualityBCSContext.recFromLatest.get(i).getACCOUNTABLENAME());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " ACCOUNTABLEPARENT => Acc_Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getACCOUNTABLEPARENT() +
                            " Acc_Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getACCOUNTABLEPARENT());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getACCOUNTABLEPARENT() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getACCOUNTABLEPARENT() != null)) {
                        Assert.assertEquals("The ACCOUNTABLEPARENT is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getACCOUNTABLEPARENT(),
                                dataQualityBCSContext.recFromLatest.get(i).getACCOUNTABLEPARENT());
                    }
                    break;
                case "etl_transform_history_manifestation_latest":
                    Log.info("etl_transform_history_manifestation_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Manif_Diff_DeltaCurr_Excl -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            "Manif_Latest -> UKEY => " + dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getUKEY() != null)) {

                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() + " is missing/not found in Manif_DLatest",
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " SOURCEREF => Manif_Diff_Exc_DeltaHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() +
                            " Manif_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " TITLE => Manif_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTITLE() +
                            " Manif_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getTITLE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTITLE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getTITLE() != null)) {
                        Assert.assertEquals("The TITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTITLE(),
                                dataQualityBCSContext.recFromLatest.get(i).getTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " intereditionflag => Manif_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getINTEREDITIONFLAG() +
                            " Manif_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getINTEREDITIONFLAG());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getINTEREDITIONFLAG() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getINTEREDITIONFLAG() != null)) {
                        Assert.assertEquals("The INTEREDITIONFLAG is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getINTEREDITIONFLAG(),
                                dataQualityBCSContext.recFromLatest.get(i).getINTEREDITIONFLAG());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " firstpublisheddate => Manif_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFIRSTPUBLISHEDDATE() +
                            " Manif_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getFIRSTPUBLISHEDDATE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFIRSTPUBLISHEDDATE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getFIRSTPUBLISHEDDATE() != null)) {
                        Assert.assertEquals("The INTEREDITIONFLAG is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFIRSTPUBLISHEDDATE(),
                                dataQualityBCSContext.recFromLatest.get(i).getFIRSTPUBLISHEDDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " binding => Manif_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getBINDING() +
                            " Manif_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getBINDING());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getBINDING() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getBINDING() != null)) {
                        Assert.assertEquals("The BINDING is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getBINDING(),
                                dataQualityBCSContext.recFromLatest.get(i).getBINDING());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " MANIF_TYPE => Manif_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getMANIFESTATIONTYPE() +
                            " Manif_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getMANIFESTATIONTYPE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getMANIFESTATIONTYPE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getMANIFESTATIONTYPE() != null)) {
                        Assert.assertEquals("The MANIF_TYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getMANIFESTATIONTYPE(),
                                dataQualityBCSContext.recFromLatest.get(i).getMANIFESTATIONTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " STATUS => Manif_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSTATUS() +
                            " Manif_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSTATUS());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSTATUS() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSTATUS() != null)) {
                        Assert.assertEquals("The STATUS is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSTATUS(),
                                dataQualityBCSContext.recFromLatest.get(i).getSTATUS());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " WORKID => Manif_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKID() +
                            " Manif_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getWORKID());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKID() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getWORKID() != null)) {
                        Assert.assertEquals("The WORKID is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKID(),
                                dataQualityBCSContext.recFromLatest.get(i).getWORKID());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " last_pub_date => Manif_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getLASTPUBDATE() +
                            " Manif_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getLASTPUBDATE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKID() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getWORKID() != null)) {
                        Assert.assertEquals("The LASTPUBDATE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getLASTPUBDATE(),
                                dataQualityBCSContext.recFromLatest.get(i).getLASTPUBDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " DQ_ERR => Manif_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR() +
                            " Manif_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR());
                    }

                    break;
                case "etl_transform_history_manifestation_identifier_latest":
                    Log.info("etl_transform_history_manifestation_identifier_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Manif_Ident_Diff_DeltaCurr_Excl -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            "Manif_Ident_Latest -> UKEY => " + dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() + " is missing/not found in Manif_Ident_Latest",
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " SOURCEREF => Manif_Ident_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() +
                            " Manif_Ident_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " IDENTIFIER => Manif_Ident_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIER() +
                            " Manif_Ident_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIER());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIER() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIER() != null)) {
                        Assert.assertEquals("The IDENTIFIER is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIER(),
                                dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIER());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " IDENTIFIERTYPE => Manif_Ident_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIERTYPE() +
                            " Manif_Ident_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIERTYPE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIERTYPE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIERTYPE() != null)) {
                        Assert.assertEquals("The IDENTIFIERTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIERTYPE(),
                                dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIERTYPE());
                    }

                    break;
                case "etl_transform_history_product_latest":
                    Log.info("etl_transform_history_product_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Prod_Diff_DeltaCurr_Excl -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            "Prod_Latest -> UKEY => " + dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() + " is missing/not found in Prod_Latest",
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " SOURCEREF => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " BINDINGCODE => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getBINDINGCODE() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getBINDINGCODE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getBINDINGCODE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getBINDINGCODE() != null)) {
                        Assert.assertEquals("The BINDINGCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getBINDINGCODE(),
                                dataQualityBCSContext.recFromLatest.get(i).getBINDINGCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " NAME => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getNAME() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getNAME());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getNAME() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getNAME() != null)) {
                        Assert.assertEquals("The NAME is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getNAME(),
                                dataQualityBCSContext.recFromLatest.get(i).getNAME());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " SHORTTITLE => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSHORTTITLE() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSHORTTITLE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSHORTTITLE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSHORTTITLE() != null)) {
                        Assert.assertEquals("The SHORTTITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSHORTTITLE(),
                                dataQualityBCSContext.recFromLatest.get(i).getSHORTTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " LAUNCHDATE => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getLAUNCHDATE() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getLAUNCHDATE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getLAUNCHDATE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getLAUNCHDATE() != null)) {
                        Assert.assertEquals("The LAUNCHDATE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getLAUNCHDATE(),
                                dataQualityBCSContext.recFromLatest.get(i).getLAUNCHDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " TAXCODE => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTAXCODE() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getTAXCODE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTAXCODE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getTAXCODE() != null)) {
                        Assert.assertEquals("The TAXCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTAXCODE(),
                                dataQualityBCSContext.recFromLatest.get(i).getTAXCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " STATUS => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSTATUS() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSTATUS());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSTATUS() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSTATUS() != null)) {
                        Assert.assertEquals("The STATUS is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSTATUS(),
                                dataQualityBCSContext.recFromLatest.get(i).getSTATUS());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " MANIFESTATIONREF => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getMANIFESTATIONREF() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getMANIFESTATIONREF());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getMANIFESTATIONREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getMANIFESTATIONREF() != null)) {
                        Assert.assertEquals("The MANIFESTATIONREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getMANIFESTATIONREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getMANIFESTATIONREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " WORKSOURCE => Prod_Diff_LatestHist =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKSOURCE() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getWORKSOURCE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKSOURCE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getWORKSOURCE() != null)) {
                        Assert.assertEquals("The WORKSOURCE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKSOURCE(),
                                dataQualityBCSContext.recFromLatest.get(i).getWORKSOURCE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " WORKTYPE => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKTYPE() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getWORKTYPE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKTYPE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getWORKTYPE() != null)) {
                        Assert.assertEquals("The WORKTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKTYPE(),
                                dataQualityBCSContext.recFromLatest.get(i).getWORKTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " SEPRATELYSALEINDICATOR => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSEPRATELYSALEINDICATOR() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSEPRATELYSALEINDICATOR());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSEPRATELYSALEINDICATOR() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSEPRATELYSALEINDICATOR() != null)) {
                        Assert.assertEquals("The SEPRATELYSALEINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSEPRATELYSALEINDICATOR(),
                                dataQualityBCSContext.recFromLatest.get(i).getSEPRATELYSALEINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " TRIALALLOWEDINDICATOR => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTRIALALLOWEDINDICATOR() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getTRIALALLOWEDINDICATOR());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTRIALALLOWEDINDICATOR() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getTRIALALLOWEDINDICATOR() != null)) {
                        Assert.assertEquals("The TRIALALLOWEDINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTRIALALLOWEDINDICATOR(),
                                dataQualityBCSContext.recFromLatest.get(i).getTRIALALLOWEDINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " FWORKSOURCEREF => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFWORKSOURCEREF() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getFWORKSOURCEREF());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFWORKSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getFWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The FWORKSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFWORKSOURCEREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getFWORKSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " PRODUCTTYPE => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPRODUCTTYPE() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getPRODUCTTYPE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPRODUCTTYPE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getPRODUCTTYPE() != null)) {
                        Assert.assertEquals("The PRODUCTTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPRODUCTTYPE(),
                                dataQualityBCSContext.recFromLatest.get(i).getPRODUCTTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " REVENUEMODEL => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getREVENUEMODEL() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getREVENUEMODEL());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getREVENUEMODEL() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getREVENUEMODEL() != null)) {
                        Assert.assertEquals("The REVENUEMODEL is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getREVENUEMODEL(),
                                dataQualityBCSContext.recFromLatest.get(i).getREVENUEMODEL());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " DQ_ERR => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " DQ_ERR => Prod_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDELTA_MODE() +
                            " Prod_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getDELTA_MODE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDELTA_MODE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getDELTA_MODE() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDELTA_MODE(),
                                dataQualityBCSContext.recFromLatest.get(i).getDELTA_MODE());
                    }
                    break;
                case "etl_transform_history_work_person_role_latest":
                    Log.info("etl_transform_history_work_person_role_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("PERS_ROLE_Diff_DeltaCurr_Excl -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            "PERS_ROLE_Latest -> UKEY => " + dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() + " is missing/not found in PERS_ROLE_Latest",
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " WORKSOURCEREF => PERS_ROLE_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKSOURCEREF() +
                            " PERS_ROLE_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getWORKSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The WORKSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKSOURCEREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getWORKSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " PERSONSOURCEREF => PERS_ROLE_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPERSONSOURCEREF() +
                            " PERS_ROLE_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getPERSONSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPERSONSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getPERSONSOURCEREF() != null)) {
                        Assert.assertEquals("The PERSONSOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPERSONSOURCEREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getPERSONSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " ROLETYPE => PERS_ROLE_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getROLETYPE() +
                            " PERS_ROLE_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getROLETYPE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getROLETYPE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getROLETYPE() != null)) {
                        Assert.assertEquals("The ROLETYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getROLETYPE(),
                                dataQualityBCSContext.recFromLatest.get(i).getROLETYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " SEQUENCE => PERS_ROLE_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSEQUENCE() +
                            " PERS_ROLE_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSEQUENCE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSEQUENCE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSEQUENCE() != null)) {
                        Assert.assertEquals("The SEQUENCE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSEQUENCE(),
                                dataQualityBCSContext.recFromLatest.get(i).getSEQUENCE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " DEDUPLICATOR => PERS_ROLE_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDEDUPLICATOR() +
                            " PERS_ROLE_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getDEDUPLICATOR());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDEDUPLICATOR() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getDEDUPLICATOR() != null)) {
                        Assert.assertEquals("The DEDUPLICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDEDUPLICATOR(),
                                dataQualityBCSContext.recFromLatest.get(i).getDEDUPLICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " DQ_ERR => PERS_ROLE_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR() +
                            " PERS_ROLE_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for U_KEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR());
                    }
                    break;
                case "etl_transform_history_work_relationship_latest":
                    Log.info("etl_transform_history_work_relationship_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("WORK_RELAT_Diff_DeltaCurr_Excl -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            "WORK_RELAT_Latest -> UKEY => " + dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() + " is missing/not found in WORK_RELAT_Latest",
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " PARENTREF => WORK_RELAT_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPARENTREF() +
                            " WORK_RELAT_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getPARENTREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPARENTREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getPARENTREF() != null)) {
                        Assert.assertEquals("The PARENTREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPARENTREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getPARENTREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " CHILDREF => WORK_RELAT_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getCHILDREF() +
                            " WORK_RELAT_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getCHILDREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getCHILDREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getCHILDREF() != null)) {
                        Assert.assertEquals("The CHILDREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getCHILDREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getCHILDREF());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " RELATIONTYPEREF => WORK_RELAT_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getRELATIONTYPEREF() +
                            " WORK_RELAT_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getRELATIONTYPEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getRELATIONTYPEREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getRELATIONTYPEREF() != null)) {
                        Assert.assertEquals("The RELATIONTYPEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getRELATIONTYPEREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getRELATIONTYPEREF());
                    }
                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " LASTUDATEDDATE => WORK_RELAT_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getLASTUDATEDDATE() +
                            " WORK_RELAT_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getLASTUDATEDDATE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getLASTUDATEDDATE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getLASTUDATEDDATE() != null)) {
                        Assert.assertEquals("The LASTUDATEDDATE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getLASTUDATEDDATE(),
                                dataQualityBCSContext.recFromLatest.get(i).getLASTUDATEDDATE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " DQ_ERR => WORK_RELAT_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR() +
                            " WORK_RELAT_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getDQ_ERR(),
                                dataQualityBCSContext.recFromLatest.get(i).getDQ_ERR());
                    }

                    break;
                case "etl_transform_history_work_latest":
                    Log.info("etl_transform_history_work_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("WORK_Diff_DeltaCurr_Excl -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            "WORK_Latest -> UKEY => " + dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() + " is missing/not found in WORK_Latest",
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " SOURCEREF => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " TITLE => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTITLE() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getTITLE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTITLE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getTITLE() != null)) {
                        Assert.assertEquals("The TITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTITLE(),
                                dataQualityBCSContext.recFromLatest.get(i).getTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " SUBTITLE => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSUBTITLE() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSUBTITLE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSUBTITLE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSUBTITLE() != null)) {
                        Assert.assertEquals("The SUBTITLE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSUBTITLE(),
                                dataQualityBCSContext.recFromLatest.get(i).getSUBTITLE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " VOLUMENO => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getVOLUMENO() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getVOLUMENO());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getVOLUMENO() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getVOLUMENO() != null)) {
                        Assert.assertEquals("The VOLUMENO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getVOLUMENO(),
                                dataQualityBCSContext.recFromLatest.get(i).getVOLUMENO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " COPYRIGHTYEAR => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getCOPYRIGHTYEAR() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getCOPYRIGHTYEAR());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getCOPYRIGHTYEAR() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getCOPYRIGHTYEAR() != null)) {
                        Assert.assertEquals("The COPYRIGHTYEAR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getCOPYRIGHTYEAR(),
                                dataQualityBCSContext.recFromLatest.get(i).getCOPYRIGHTYEAR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " EDITIONNO => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getEDITIONNO() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getEDITIONNO());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getEDITIONNO() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getEDITIONNO() != null)) {
                        Assert.assertEquals("The EDITIONNO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getEDITIONNO(),
                                dataQualityBCSContext.recFromLatest.get(i).getEDITIONNO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " PMC => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPMC() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getPMC());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPMC() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getPMC() != null)) {
                        Assert.assertEquals("The PMC is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getPMC(),
                                dataQualityBCSContext.recFromLatest.get(i).getPMC());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " WORKTYPE => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKTYPE() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getWORKTYPE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKTYPE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getWORKTYPE() != null)) {
                        Assert.assertEquals("The WORKTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getWORKTYPE(),
                                dataQualityBCSContext.recFromLatest.get(i).getWORKTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " STATUSCODE => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSTATUSCODE() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSTATUSCODE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSTATUSCODE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSTATUSCODE() != null)) {
                        Assert.assertEquals("The STATUSCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSTATUSCODE(),
                                dataQualityBCSContext.recFromLatest.get(i).getSTATUSCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " IMPRINTCODE => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIMPRINTCODE() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getIMPRINTCODE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIMPRINTCODE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getIMPRINTCODE() != null)) {
                        Assert.assertEquals("The IMPRINTCODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIMPRINTCODE(),
                                dataQualityBCSContext.recFromLatest.get(i).getIMPRINTCODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " TEOPCO => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTEOPCO() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getTEOPCO());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTEOPCO() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getTEOPCO() != null)) {
                        Assert.assertEquals("The TEOPCO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getTEOPCO(),
                                dataQualityBCSContext.recFromLatest.get(i).getTEOPCO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " OPCO => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getOPCO() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getOPCO());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getOPCO() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getOPCO() != null)) {
                        Assert.assertEquals("The OPCO is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getOPCO(),
                                dataQualityBCSContext.recFromLatest.get(i).getOPCO());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " RESPCENTRE => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getRESPCENTRE() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getRESPCENTRE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getRESPCENTRE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getRESPCENTRE() != null)) {
                        Assert.assertEquals("The RESPCENTRE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getRESPCENTRE(),
                                dataQualityBCSContext.recFromLatest.get(i).getRESPCENTRE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " LANGUAGECODE => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getLANGUAGECODE() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getLANGUAGECODE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getLANGUAGECODE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getLANGUAGECODE() != null)) {
                        Assert.assertEquals("The LANGUAGECODE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getLANGUAGECODE(),
                                dataQualityBCSContext.recFromLatest.get(i).getLANGUAGECODE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " ELECTRORIGHTSINDICATOR => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getELECTRORIGHTSINDICATOR() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getELECTRORIGHTSINDICATOR());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getELECTRORIGHTSINDICATOR() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getELECTRORIGHTSINDICATOR() != null)) {
                        Assert.assertEquals("The ELECTRORIGHTSINDICATOR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getELECTRORIGHTSINDICATOR(),
                                dataQualityBCSContext.recFromLatest.get(i).getELECTRORIGHTSINDICATOR());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " FOAJOURNALTYPE => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFOAJOURNALTYPE() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getFOAJOURNALTYPE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFOAJOURNALTYPE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getFOAJOURNALTYPE() != null)) {
                        Assert.assertEquals("The FOAJOURNALTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFOAJOURNALTYPE(),
                                dataQualityBCSContext.recFromLatest.get(i).getFOAJOURNALTYPE());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " FSOCIETYOWNERSHIP => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFSOCIETYOWNERSHIP() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getFSOCIETYOWNERSHIP());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFSOCIETYOWNERSHIP() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getFSOCIETYOWNERSHIP() != null)) {
                        Assert.assertEquals("The FSOCIETYOWNERSHIP is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getFSOCIETYOWNERSHIP(),
                                dataQualityBCSContext.recFromLatest.get(i).getFSOCIETYOWNERSHIP());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " SUBSCRIPTIONTYPE => WORK_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSUBSCRIPTIONTYPE() +
                            " WORK_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSUBSCRIPTIONTYPE());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSUBSCRIPTIONTYPE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSUBSCRIPTIONTYPE() != null)) {
                        Assert.assertEquals("The SUBSCRIPTIONTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSUBSCRIPTIONTYPE(),
                                dataQualityBCSContext.recFromLatest.get(i).getSUBSCRIPTIONTYPE());
                    }

                    break;
                case "etl_transform_history_work_identifier_latest":
                    Log.info("etl_transform_history_work_identifier_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    Log.info("Work_Ident_Diff_DeltaCurr_Excl -> UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            "Work_Ident_Latest -> UKEY => " + dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getUKEY() != null)) {
                        Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() + " is missing/not found in Manif_Ident_Curr",
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromLatest.get(i).getUKEY());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " SOURCEREF => Work_Ident_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() +
                            " Work_Ident_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF() != null)) {
                        Assert.assertEquals("The SOURCEREF is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF(),
                                dataQualityBCSContext.recFromLatest.get(i).getSOURCEREF());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " IDENTIFIER => Work_Ident_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIER() +
                            " Work_Ident_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIER());
                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIER() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIER() != null)) {
                        Assert.assertEquals("The IDENTIFIER is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIER(),
                                dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIER());
                    }

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                            " IDENTIFIERTYPE => Work_Ident_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIERTYPE() +
                            " Work_Ident_Latest =" + dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIERTYPE());

                    if (dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIERTYPE() != null ||
                            (dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIERTYPE() != null)) {
                        Assert.assertEquals("The IDENTIFIERTYPE is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY(),
                                dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getIDENTIFIERTYPE(),
                                dataQualityBCSContext.recFromLatest.get(i).getIDENTIFIERTYPE());
                    }
                    break;
            }
        }
    }


    @Given("^Get the (.*) from diff of Person delta_current and Person exclude_delta tables$")
    public void getRandKeyFromPersonDiffDeltaAndExcl(String numberOfRecords) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core Person Diff of Delta current and Current History Tables....");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PERSON_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
        List<Map<?, ?>> randomPersonIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomPersonIds.stream().map(m -> (Integer) m.get("UKEY")).map(String::valueOf).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the records from the diff of Person delta_current and Person exclude_delta tables$")
    public void getRecPersonDiffOfDeltaAndExcl(){
        Log.info("Get Records from Person Diff of Delta current and Exclude Tables");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_DIFF_DELTACURR_EXCL, Joiner.on(",").join(Ids));
        dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Get the records from person Latest BCS core table$")
    public void getRecPersonLatest(){
        Log.info("Get Records from Person Latest Table");
        sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_LATEST, Joiner.on(",").join(Ids));
        dataQualityBCSContext.recFromPersonLatest = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And ("^Compare the records of Person Latest with diff of person delta_current and Exclude_delta tables$")
    public void comparePersonLatestAndDiffOfDeltaAndExcl(){
        if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.isEmpty()) {
            Log.info("No Data Found in Person Trans_File Diff....");
        } else {
            Log.info("Sorting the Ids to compare the records between Person Latest and Person Diff Delta and Excl...");
            for (int i = 0; i < dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.size(); i++) {
                dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                dataQualityBCSContext.recFromPersonLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                Log.info("Person_Diff_DeltaCurr_Excl -> UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY() +
                        "Person_Latest -> UKEY => " + dataQualityBCSContext.recFromPersonLatest.get(i).getUKEY());
                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY() != null ||
                        (dataQualityBCSContext.recFromPersonLatest.get(i).getUKEY() != null)) {

                    Assert.assertEquals("The UKEY is =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY() + " is missing/not found in Person_Latest",
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY(),
                            dataQualityBCSContext.recFromPersonLatest.get(i).getUKEY());
                }

                Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY() +
                        " FIRSTNAME => Person_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getFIRSTNAME() +
                        " Person_Latest =" + dataQualityBCSContext.recFromPersonLatest.get(i).getFIRSTNAME());

                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getFIRSTNAME() != null ||
                        (dataQualityBCSContext.recFromPersonLatest.get(i).getFIRSTNAME() != null)) {
                    Assert.assertEquals("The FIRSTNAME is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY(),
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getFIRSTNAME(),
                            dataQualityBCSContext.recFromPersonLatest.get(i).getFIRSTNAME());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY() +
                        " FAMILYNAME => Person_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getFAMILYNAME() +
                        " Person_Latest =" + dataQualityBCSContext.recFromPersonLatest.get(i).getFAMILYNAME());

                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getFAMILYNAME() != null ||
                        (dataQualityBCSContext.recFromPersonLatest.get(i).getFAMILYNAME() != null)) {
                    Assert.assertEquals("The FAMILYNAME is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY(),
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getFAMILYNAME(),
                            dataQualityBCSContext.recFromPersonLatest.get(i).getFAMILYNAME());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY() +
                        " PEOPLEHUBID => Person_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getPEOPLEHUBID() +
                        " Person_Latest =" + dataQualityBCSContext.recFromPersonLatest.get(i).getPEOPLEHUBID());

                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getPEOPLEHUBID() != null ||
                        (dataQualityBCSContext.recFromPersonLatest.get(i).getPEOPLEHUBID() != null)) {
                    Assert.assertEquals("The PEOPLEHUBID is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY(),
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getPEOPLEHUBID(),
                            dataQualityBCSContext.recFromPersonLatest.get(i).getPEOPLEHUBID());
                }

                Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY() +
                        " EMAIL => Person_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getEMAIL() +
                        " Person_Latest =" + dataQualityBCSContext.recFromPersonLatest.get(i).getEMAIL());

                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getEMAIL() != null ||
                        (dataQualityBCSContext.recFromPersonLatest.get(i).getEMAIL() != null)) {
                    Assert.assertEquals("The EMAIL is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY(),
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getEMAIL(),
                            dataQualityBCSContext.recFromPersonLatest.get(i).getEMAIL());
                }
                Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getSOURCEREF() +
                        " DQ_ERR => Person_Diff_DeltaCurr_Excl =" + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getDQ_ERR() +
                        " Person_Latest =" + dataQualityBCSContext.recFromPersonLatest.get(i).getDQ_ERR());

                if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getDQ_ERR() != null ||
                        (dataQualityBCSContext.recFromPersonLatest.get(i).getDQ_ERR() != null)) {
                    Assert.assertEquals("The DQ_ERR is incorrect for UKEY = " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY(),
                            dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getDQ_ERR(),
                            dataQualityBCSContext.recFromPersonLatest.get(i).getDQ_ERR());
                }
            }
        }
    }








}




