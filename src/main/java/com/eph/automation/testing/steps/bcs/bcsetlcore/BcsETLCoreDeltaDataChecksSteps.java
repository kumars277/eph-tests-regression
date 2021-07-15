package com.eph.automation.testing.steps.bcs.bcsetlcore;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSETL_CoreAccessDLContext;
import com.eph.automation.testing.models.dao.BCS.BCS_ETLCoreDLAccessObject;
import com.eph.automation.testing.services.db.BCS_ETLCoreSQL.BCS_ETLCoreDataChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class BcsETLCoreDeltaDataChecksSteps {

    private String sql;
    private List<String> ids;

    @Given("^Get the (.*) of BCS Core data from transform_file Tables (.*)$")
    public void getidsFromTransformFile(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for BCS Core Transform_File Tables....");

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
            case "etl_person_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PERSON_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            default:
               Log.info("No such tables found");
       }
        List<Map<?, ?>> randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("UKEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When ("^Get the Data from the Difference of Current and Previous transform_file Tables (.*)$")
    public void getRecFromDiffTransformFile(String tableName){
        Log.info("We get the records from Diff of Transform File BCS Core table...");
        switch (tableName) {
            case "etl_accountable_product_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_DIFF_TRANS_FILE, Joiner.on("','").join(ids));
                break;
            case "etl_manifestation_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_DIFF_TRANS_FILE, Joiner.on("','").join(ids));
                break;
            case "etl_manifestation_identifier_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_DIFF_TRANS_FILE, Joiner.on("','").join(ids));
                break;
            case "etl_product_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_DIFF_TRANS_FILE, Joiner.on("','").join(ids));
                break;
            case "etl_work_person_role_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_DIFF_TRANS_FILE, Joiner.on("','").join(ids));
                break;
            case "etl_work_relationship_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_DIFF_TRANS_FILE, Joiner.on("','").join(ids));
                break;
            case "etl_work_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_DIFF_TRANS_FILE, Joiner.on("','").join(ids));
                break;
            case "etl_work_identifier_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_DIFF_TRANS_FILE, Joiner.on("','").join(ids));
                break;
            case "etl_person_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_DIFF_TRANS_FILE, Joiner.on("','").join(ids));
                break;
            default:
                Log.info("No tables found");

        }
        BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When ("^We Get the records from delta current table BCS core (.*)$")
    public void getRecFromDeltaCurrent(String tableName){
        Log.info("We get the records from Delta Current BCS Core table...");
        switch (tableName) {
            case "etl_delta_current_accountable_product":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_DELTA_CURRENT, Joiner.on("','").join(ids));
                break;
            case "etl_delta_current_manifestation":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_DELTA_CURRENT, Joiner.on("','").join(ids));
                break;
            case "etl_delta_current_manifestation_identifier":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_DELTA_CURRENT, Joiner.on("','").join(ids));
                break;
            case "etl_delta_current_product":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_REC_DELTA_CURRENT, Joiner.on("','").join(ids));
                break;
            case "etl_delta_current_work_person_role":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_DELTA_CURRENT, Joiner.on("','").join(ids));
                break;
            case "etl_delta_current_work_relationship":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_DELTA_CURRENT, Joiner.on("','").join(ids));
                break;
            case "etl_delta_current_work":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_DIFF_DELTA_CURRENT, Joiner.on("','").join(ids));
                break;
            case "etl_delta_current_work_identifier":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_DELTA_CURRENT, Joiner.on("','").join(ids));
                break;
            case "etl_delta_current_person":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_DELTA_CURR, Joiner.on("','").join(ids));
                break;
            default:
                Log.info("No tables found");
        }
        BCSETL_CoreAccessDLContext.recFromDeltaCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare the records of BCS Core delta current and BCS diff of Transform_File (.*)$")
    public void compareCurrentandCurrHist(String targetTableName)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.isEmpty()) {
            Log.info("No Data Found in the Current Tables ....");
        } else {
            Log.info("Sorting the ids to compare the records between Delta Current and Diff of trans_file tables...");
                for (int i = 0; i < BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.size(); i++) {
                    switch (targetTableName) {
                    case "etl_delta_current_accountable_product":
                        Log.info("etl_delta_current_accountable_product Records:");
                        BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaCurrProd = {"getUKEY", "getSOURCEREF", "getACCOUNTABLEPRODUCT", "getACCOUNTABLENAME", "getACCOUNTABLEPARENT", "getDQ_ERR", "getDELTA_MODE"};
                            for (String strTemp : etlDeltaCurrProd) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() +
                                    " " + strTemp + " => Acc_Prod_Trans_file_Diff = " + method.invoke(objectToCompare1) +
                                    " Acc_Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                             if (method.invoke(objectToCompare1) != null || (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_manifestation":
                        Log.info("etl_delta_current_manifestation Records:");
                        BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaManifCurr = {"getUKEY", "getSOURCEREF", "getTITLE", "getINTEREDITIONFLAG", "getFIRSTPUBLISHEDDATE", "getBINDING", "getMANIFESTATIONTYPE", "getSTATUS"
                                , "getWORKID", "getLASTPUBDATE", "getDQ_ERR", "getDELTA_MODE"};
                        for (String strTemp : etlDeltaManifCurr) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() +
                                    " " + strTemp + " => Manif_Trans_File_Diff = " + method.invoke(objectToCompare1) +
                                    " Manif_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_manifestation_identifier":
                        Log.info("etl_delta_current_manifestation_identifier Records:");
                        BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaCurrManifIdent = {"getUKEY", "getSOURCEREF", "getIDENTIFIER", "getIDENTIFIERTYPE", "getDELTA_MODE"};
                        for (String strTemp : etlDeltaCurrManifIdent) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                    " " + strTemp + " => Manif_Ident_Trans_Diff = " + method.invoke(objectToCompare1) +
                                    " Manif_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_product":
                        Log.info("etl_delta_current_product Records:");
                        BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaCurrProds = {"getUKEY", "getSOURCEREF", "getBINDINGCODE", "getNAME", "getSHORTTITLE", "getLAUNCHDATE",
                                "getTAXCODE", "getSTATUS", "getMANIFESTATIONREF", "getWORKSOURCE", "getWORKTYPE", "getSEPRATELYSALEINDICATOR", "getTRIALALLOWEDINDICATOR", "getFWORKSOURCEREF",
                                "getPRODUCTTYPE", "getREVENUEMODEL", "getDQ_ERR", "getDELTA_MODE"};
                        for (String strTemp : etlDeltaCurrProds) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                    " " + strTemp + " => Prod_Trans_Diff = " + method.invoke(objectToCompare1) +
                                    " Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_work_person_role":
                        Log.info("etl_delta_current_work_person_role Records:");
                        BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaCurrWorkPersRole = {"getUKEY", "getWORKSOURCEREF", "getPERSONSOURCEREF", "getROLETYPE", "getSEQUENCE", "getDEDUPLICATOR", "getDQ_ERR", "getDELTA_MODE"};
                        for (String strTemp : etlDeltaCurrWorkPersRole) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                    " " + strTemp + " => PERS_ROLE_Diff_Tran_Diff = " + method.invoke(objectToCompare1) +
                                    " PERS_ROLE_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_work_relationship":
                        Log.info("etl_delta_current_work_relationship Records:");
                        BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlTransHisWorkRelPart = {"getUKEY", "getPARENTREF", "getCHILDREF", "getRELATIONTYPEREF", "getLASTUDATEDDATE", "getDQ_ERR", "getDELTA_MODE"};
                        for (String strTemp : etlTransHisWorkRelPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                    " " + strTemp + " => WORK_RELAT_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                    " WORK_RELAT_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_work":
                        Log.info("etl_delta_current_work Records:");
                        BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaCurrWork = {"getUKEY", "getSOURCEREF", "getTITLE", "getSUBTITLE", "getVOLUMENO", "getCOPYRIGHTYEAR", "getEDITIONNO", "getPMC", "getWORKTYPE", "getSTATUSCODE", "getIMPRINTCODE", "getTEOPCO", "getOPCO"
                                , "getRESPCENTRE", "getLANGUAGECODE", "getELECTRORIGHTSINDICATOR", "getFOAJOURNALTYPE", "getFSOCIETYOWNERSHIP", "getSUBSCRIPTIONTYPE", "getDELTA_MODE"};

                        for (String strTemp : etlDeltaCurrWork) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                    " " + strTemp + " => WORK_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                    " WORK_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_work_identifier":
                        Log.info("etl_delta_current_work_identifier Records:");
                        BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaCurrWorkIdentifier = {"getUKEY", "getSOURCEREF", "getIDENTIFIER", "getIDENTIFIERTYPE", "getDELTA_MODE"};
                        for (String strTemp : etlDeltaCurrWorkIdentifier) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                    " " + strTemp + " => Work_Ident_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                    " Work_Ident_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_person":
                        Log.info("etl_delta_current_person Records:");
                        BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                        String[] etlPersonCurrCol = {"getUKEY", "getSOURCEREF", "getFIRSTNAME", "getFAMILYNAME", "getPEOPLEHUBID", "getEMAIL", "getDQ_ERR", "getDELTA_MODE"};
                        for (String strTemp : etlPersonCurrCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                    " " + strTemp + " => Person_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                    " Person_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                        default:
                            Log.info("Tables not found");
                }
            }
        }
    }


//////////////////////////////////////////////////////////////////////////////

    @Given("^Get the (.*) of BCS Core data from delta_Current_hist Tables (.*)$")
    public void getidsFromDeltaHist(String numberOfRecords, String tableName) {
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for BCS Core Delta History Tables....");

        switch (tableName) {
            case "etl_delta_history_accountable_product_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_KEY_ACCPROD_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_manifestation_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_KEY_MANIF_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_manifestation_identifier_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_KEY_MANIF_IDENT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_product_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_KEY_PRODUCT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_work_person_role_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_KEY_WORK_PERS_ROLE_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_work_relationship_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_KEY_WORK_RELATION_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_work_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_KEY_WORK_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_work_identifier_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_KEY_WORK_IDENT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_person_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_KEY_PERS_DELTA_CURRENT_HIST, numberOfRecords);
                break;
                default:
                    Log.info("Table not found");

        }
        List<Map<?, ?>> randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("UKEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When ("^Get the Data from the Delta_Current_History Tables (.*)$")
    public void getRecFromDeltaHist(String tableName){
        Log.info("We get the records from Delta Hist of BCS Core table...");
        switch (tableName) {
            case "etl_delta_history_accountable_product_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(ids));
                break;
            case "etl_delta_history_manifestation_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(ids));
                break;
            case "etl_delta_history_manifestation_identifier_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(ids));
                break;
            case "etl_delta_history_product_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(ids));
                break;
            case "etl_delta_history_work_person_role_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(ids));
                break;
            case "etl_delta_history_work_relationship_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(ids));
                break;
            case "etl_delta_history_work_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_DIFF_DELTA_CURRENT_HIST, Joiner.on("','").join(ids));
                break;
            case "etl_delta_history_work_identifier_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_DIFF_DELTA_CURRENT_HIST, Joiner.on("','").join(ids));
                break;
            case "etl_delta_history_person_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_DELTA_CURR_HIST, Joiner.on("','").join(ids));
                break;
                default:
                    Log.info("Tables are not found");
        }

        BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare the records of BCS Core delta current and delta_Current_history (.*)$")
    public void compareDeltaCurrentandDletaHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.isEmpty()) {
            Log.info("No Data Found in the Delta Current Hist Tables ....");
        } else {
            for (int i = 0; i < BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.size(); i++) {
                switch (targetTableName) {
                    case "etl_delta_current_accountable_product":
                        Log.info("comparing delta_history and etl_delta_current_accountable_product Records:");
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaCurrAccProd = {"getUKEY", "getSOURCEREF", "getACCOUNTABLEPRODUCT", "getACCOUNTABLENAME", "getACCOUNTABLEPARENT", "getDQ_ERR", "getDELTA_MODE"};
                        for (String strTemp : etlDeltaCurrAccProd) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getSOURCEREF() +
                                    " " + strTemp + " => Acc_Prod_Delta_Hist = " + method.invoke(objectToCompare1) +
                                    " Acc_Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_manifestation":
                        Log.info("comparing delta_history and etl_delta_current_manifestation Records:");
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaCurrManif = {"getUKEY", "getSOURCEREF", "getTITLE", "getINTEREDITIONFLAG", "getFIRSTPUBLISHEDDATE", "getBINDING", "getMANIFESTATIONTYPE", "getSTATUS"
                                , "getWORKID", "getLASTPUBDATE", "getDQ_ERR", "getDELTA_MODE"};
                        for (String strTemp : etlDeltaCurrManif) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getSOURCEREF() +
                                    " " + strTemp + " => Manif_Delta_Hist = " + method.invoke(objectToCompare1) +
                                    " Manif_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_manifestation_identifier":
                        Log.info("comparing delta_history and etl_delta_current_manifestation_identifier Records:");
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaCurrManifIdent = {"getUKEY", "getSOURCEREF", "getIDENTIFIER", "getIDENTIFIERTYPE", "getDELTA_MODE"};
                        for (String strTemp : etlDeltaCurrManifIdent) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                    " " + strTemp + " => Manif_Ident_Delta_Hist = " + method.invoke(objectToCompare1) +
                                    " Manif_Ident_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_product":
                        Log.info("comparing delta_history and etl_delta_current_product Records:");
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaCurrProd = {"getUKEY", "getSOURCEREF", "getBINDINGCODE", "getNAME", "getSHORTTITLE", "getLAUNCHDATE",
                                "getTAXCODE", "getSTATUS", "getMANIFESTATIONREF", "getWORKSOURCE", "getWORKTYPE", "getSEPRATELYSALEINDICATOR", "getTRIALALLOWEDINDICATOR", "getFWORKSOURCEREF",
                                "getPRODUCTTYPE", "getREVENUEMODEL", "getDQ_ERR", "getDELTA_MODE"};
                        for (String strTemp : etlDeltaCurrProd) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                    " " + strTemp + " => Prod_Delta_Hist = " + method.invoke(objectToCompare1) +
                                    " Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_work_person_role":
                        Log.info("comparing delta_history and etl_delta_current_work_person_role Records:");
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaWorkCurrPersonRole = {"getUKEY", "getWORKSOURCEREF", "getPERSONSOURCEREF", "getROLETYPE", "getSEQUENCE", "getDEDUPLICATOR", "getDQ_ERR", "getDELTA_MODE"};
                        for (String strTemp : etlDeltaWorkCurrPersonRole) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                    " " + strTemp + " => PERS_ROLE_Delta_Hist = " + method.invoke(objectToCompare1) +
                                    " PERS_ROLE_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_work_relationship":
                        Log.info("comparing delta history and etl_delta_current_work_relationship Records:");
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlTransHisWorkRelation = {"getUKEY", "getPARENTREF", "getCHILDREF", "getRELATIONTYPEREF", "getLASTUDATEDDATE", "getDQ_ERR", "getDELTA_MODE"};
                        for (String strTemp : etlTransHisWorkRelation) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                    " " + strTemp + " => WORK_RELAT_Delta_Hist = " + method.invoke(objectToCompare1) +
                                    " WORK_RELAT_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Delta_Hist for UKEY:" + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_work":
                        Log.info("comparing delta history and etl_delta_current_work Records:");
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaWorkCurr = {"getUKEY", "getSOURCEREF", "getTITLE", "getSUBTITLE", "getVOLUMENO", "getCOPYRIGHTYEAR", "getEDITIONNO", "getPMC", "getWORKTYPE", "getSTATUSCODE", "getIMPRINTCODE", "getTEOPCO", "getOPCO"
                                , "getRESPCENTRE", "getLANGUAGECODE", "getELECTRORIGHTSINDICATOR", "getFOAJOURNALTYPE", "getFSOCIETYOWNERSHIP", "getSUBSCRIPTIONTYPE", "getDELTA_MODE"};

                        for (String strTemp : etlDeltaWorkCurr) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                    " " + strTemp + " => WORK_Delta_Curr_hist = " + method.invoke(objectToCompare1) +
                                    " WORK_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_work_identifier":
                        Log.info("comparing delta history and etl_delta_current_work_identifier Records:");
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etlDeltaCurrWorkIdendtifier = {"getUKEY", "getSOURCEREF", "getIDENTIFIER", "getIDENTIFIERTYPE", "getDELTA_MODE"};
                        for (String strTemp : etlDeltaCurrWorkIdendtifier) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                    " " + strTemp + " => Work_Ident_Delta_Curr_hist = " + method.invoke(objectToCompare1) +
                                    " Work_Ident_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Delta_Curr_hist for UKEY:" + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_delta_current_person":
                        Log.info("comparing delta history and etl_delta_current_person Records:");
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        BCSETL_CoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                        String[] etlPersonCurr = {"getUKEY", "getSOURCEREF", "getFIRSTNAME", "getFAMILYNAME", "getPEOPLEHUBID", "getEMAIL", "getDQ_ERR", "getDELTA_MODE"};
                        for (String strTemp : etlPersonCurr) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = BCSETL_CoreAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                    " " + strTemp + " => Person_Delta_curr_hist = " + method.invoke(objectToCompare1) +
                                    " Person_Delta_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Delta_Curr for UKEY:" + BCSETL_CoreAccessDLContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    default:
                        Log.info("Tables not found");
                }
            }
        }
    }

/////////////////////////////////////////////////////////////////////////////////////////////

}




