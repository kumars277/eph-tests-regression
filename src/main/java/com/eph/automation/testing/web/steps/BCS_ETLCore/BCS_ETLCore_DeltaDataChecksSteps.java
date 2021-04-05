package com.eph.automation.testing.web.steps.BCS_ETLCore;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSETL_CoreAccessDLContext;
import com.eph.automation.testing.models.dao.BCS_ETL.BCS_ETLCoreDLAccessObject;
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
            case "etl_person_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PERSON_KEY_DIFF_TRANS_FILE, numberOfRecords);
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
            case "etl_person_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
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
            case "etl_delta_current_person":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_DELTA_CURR, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromDeltaCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare the records of BCS Core delta current and BCS diff of Transform_File (.*)$")
    public void compareCurrentandCurrHist(String targetTableName)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
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

                    String[] etl_delta_current_accountable_product = {"getUKEY","getSOURCEREF","getACCOUNTABLEPRODUCT","getACCOUNTABLENAME","getACCOUNTABLEPARENT","getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_accountable_product) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() +
                                " " + strTemp + " => Acc_Prod_Trans_file_Diff = " + method.invoke(objectToCompare1) +
                                " Acc_Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "etl_delta_current_manifestation":
                    Log.info("etl_delta_current_manifestation Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_manifestation = {"getUKEY","getSOURCEREF","getTITLE","getINTEREDITIONFLAG","getFIRSTPUBLISHEDDATE","getBINDING","getMANIFESTATIONTYPE","getSTATUS"
                            ,"getWORKID","getLASTPUBDATE","getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_manifestation) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getSOURCEREF() +
                                " " + strTemp + " => Manif_Trans_File_Diff = " + method.invoke(objectToCompare1) +
                                " Manif_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "etl_delta_current_manifestation_identifier":
                    Log.info("etl_delta_current_manifestation_identifier Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_manifestation_identifier = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_manifestation_identifier) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                " " + strTemp + " => Manif_Ident_Trans_Diff = " + method.invoke(objectToCompare1) +
                                " Manif_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_product":
                    Log.info("etl_delta_current_product Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_product = {"getUKEY","getSOURCEREF","getBINDINGCODE","getNAME","getSHORTTITLE","getLAUNCHDATE",
                            "getTAXCODE","getSTATUS","getMANIFESTATIONREF","getWORKSOURCE","getWORKTYPE","getSEPRATELYSALEINDICATOR","getTRIALALLOWEDINDICATOR","getFWORKSOURCEREF",
                            "getPRODUCTTYPE","getREVENUEMODEL","getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_product) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                " " + strTemp + " => Prod_Trans_Diff = " + method.invoke(objectToCompare1) +
                                " Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_person_role":
                    Log.info("etl_delta_current_work_person_role Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_work_person_role = {"getUKEY","getWORKSOURCEREF","getPERSONSOURCEREF","getROLETYPE","getSEQUENCE","getDEDUPLICATOR","getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_work_person_role) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                " " + strTemp + " => PERS_ROLE_Diff_Tran_Diff = " + method.invoke(objectToCompare1) +
                                " PERS_ROLE_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_relationship":
                    Log.info("etl_delta_current_work_relationship Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_work_relationship_part = {"getUKEY","getPARENTREF","getCHILDREF","getRELATIONTYPEREF","getLASTUDATEDDATE","getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_transform_history_work_relationship_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                " " + strTemp + " => WORK_RELAT_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                " WORK_RELAT_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work":
                    Log.info("etl_delta_current_work Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_work = {"getUKEY","getSOURCEREF","getTITLE","getSUBTITLE","getVOLUMENO","getCOPYRIGHTYEAR","getEDITIONNO","getPMC","getWORKTYPE","getSTATUSCODE","getIMPRINTCODE","getTEOPCO","getOPCO"
                            ,"getRESPCENTRE","getLANGUAGECODE","getELECTRORIGHTSINDICATOR","getFOAJOURNALTYPE","getFSOCIETYOWNERSHIP","getSUBSCRIPTIONTYPE","getDELTA_MODE"};

                    for (String strTemp : etl_delta_current_work) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                " " + strTemp + " => WORK_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                " WORK_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_identifier":
                    Log.info("etl_delta_current_work_identifier Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_work_identifier = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_work_identifier) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                " " + strTemp + " => Work_Ident_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                " Work_Ident_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "etl_delta_current_person":
                    Log.info("etl_delta_current_person Records:");
                    dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    String[] etl_person_current_v_col = {"getUKEY", "getSOURCEREF", "getFIRSTNAME", "getFAMILYNAME", "getPEOPLEHUBID", "getEMAIL", "getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_person_current_v_col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                " " + strTemp + " => Person_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                " Person_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
            }
       }
    }


//////////////////////////////////////////////////////////////////////////////

    @Given("^Get the (.*) of BCS Core data from delta_Current_hist Tables (.*)$")
    public void getIdsFromDeltaHist(String numberOfRecords, String tableName) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core Delta History Tables....");

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

        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("UKEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When ("^Get the Data from the Delta_Current_History Tables (.*)$")
    public void getRecFromDeltaHist(String tableName){
        Log.info("We get the records from Delta Hist of BCS Core table...");
        switch (tableName) {
            case "etl_delta_history_accountable_product_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_manifestation_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_manifestation_identifier_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_product_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_work_person_role_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_work_relationship_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_work_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_DIFF_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_work_identifier_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_DIFF_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_person_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_DELTA_CURR_HIST, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromDeltaCurrentHist = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare the records of BCS Core delta current and delta_Current_history (.*)$")
    public void compareDeltaCurrentandDletaHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recFromDeltaCurrentHist.isEmpty()) {
            Log.info("No Data Found in the Delta Current Hist Tables ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Delta Current and Delta Hist tables...");
        }
        for (int i = 0; i < dataQualityBCSContext.recFromDeltaCurrentHist.size(); i++) {
            switch (targetTableName) {
                case "etl_delta_current_accountable_product":
                    Log.info("comparing delta_history and etl_delta_current_accountable_product Records:");
                    dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_accountable_product = {"getUKEY","getSOURCEREF","getACCOUNTABLEPRODUCT","getACCOUNTABLENAME","getACCOUNTABLEPARENT","getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_accountable_product) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getSOURCEREF() +
                                " " + strTemp + " => Acc_Prod_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " Acc_Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_manifestation":
                    Log.info("comparing delta_history and etl_delta_current_manifestation Records:");
                    dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_manifestation = {"getUKEY","getSOURCEREF","getTITLE","getINTEREDITIONFLAG","getFIRSTPUBLISHEDDATE","getBINDING","getMANIFESTATIONTYPE","getSTATUS"
                            ,"getWORKID","getLASTPUBDATE","getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_manifestation) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getSOURCEREF() +
                                " " + strTemp + " => Manif_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " Manif_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_manifestation_identifier":
                    Log.info("comparing delta_history and etl_delta_current_manifestation_identifier Records:");
                    dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_manifestation_identifier = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_manifestation_identifier) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                " " + strTemp + " => Manif_Ident_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " Manif_Ident_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_product":
                    Log.info("comparing delta_history and etl_delta_current_product Records:");
                    dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_product = {"getUKEY","getSOURCEREF","getBINDINGCODE","getNAME","getSHORTTITLE","getLAUNCHDATE",
                            "getTAXCODE","getSTATUS","getMANIFESTATIONREF","getWORKSOURCE","getWORKTYPE","getSEPRATELYSALEINDICATOR","getTRIALALLOWEDINDICATOR","getFWORKSOURCEREF",
                            "getPRODUCTTYPE","getREVENUEMODEL","getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_product) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                " " + strTemp + " => Prod_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_person_role":
                    Log.info("comparing delta_history and etl_delta_current_work_person_role Records:");
                    dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_work_person_role = {"getUKEY","getWORKSOURCEREF","getPERSONSOURCEREF","getROLETYPE","getSEQUENCE","getDEDUPLICATOR","getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_work_person_role) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                " " + strTemp + " => PERS_ROLE_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " PERS_ROLE_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_relationship":
                    Log.info("comparing delta history and etl_delta_current_work_relationship Records:");
                    dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_work_relationship_part = {"getUKEY","getPARENTREF","getCHILDREF","getRELATIONTYPEREF","getLASTUDATEDDATE","getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_transform_history_work_relationship_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                " " + strTemp + " => WORK_RELAT_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " WORK_RELAT_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Delta_Hist for UKEY:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work":
                    Log.info("comparing delta history and etl_delta_current_work Records:");
                    dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_work = {"getUKEY","getSOURCEREF","getTITLE","getSUBTITLE","getVOLUMENO","getCOPYRIGHTYEAR","getEDITIONNO","getPMC","getWORKTYPE","getSTATUSCODE","getIMPRINTCODE","getTEOPCO","getOPCO"
                            ,"getRESPCENTRE","getLANGUAGECODE","getELECTRORIGHTSINDICATOR","getFOAJOURNALTYPE","getFSOCIETYOWNERSHIP","getSUBSCRIPTIONTYPE","getDELTA_MODE"};

                    for (String strTemp : etl_delta_current_work) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                " " + strTemp + " => WORK_Delta_Curr_hist = " + method.invoke(objectToCompare1) +
                                " WORK_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_identifier":
                    Log.info("comparing delta history and etl_delta_current_work_identifier Records:");
                    dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_work_identifier = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE","getDELTA_MODE"};
                    for (String strTemp : etl_delta_current_work_identifier) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                " " + strTemp + " => Work_Ident_Delta_Curr_hist = " + method.invoke(objectToCompare1) +
                                " Work_Ident_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Delta_Curr_hist for UKEY:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_person":
                    Log.info("comparing delta history and etl_delta_current_person Records:");
                    dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    String[] etl_person_current_v_col = {"getUKEY", "getSOURCEREF", "getFIRSTNAME", "getFAMILYNAME", "getPEOPLEHUBID", "getEMAIL", "getDQ_ERR","getDELTA_MODE"};
                    for (String strTemp : etl_person_current_v_col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY() +
                                " " + strTemp + " => Person_Delta_curr_hist = " + method.invoke(objectToCompare1) +
                                " Person_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Delta_Curr for UKEY:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
            }
        }
    }

/////////////////////////////////////////////////////////////////////////////////////////////

}




