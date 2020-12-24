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

import java.lang.reflect.InvocationTargetException;
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
         //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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
    public void compareExclwithDiffOfDeltaAndCurrHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
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

                    String[] etl_transform_history_accountable_product_excl_delta = {"getUKEY","getSOURCEREF","getACCOUNTABLEPRODUCT","getACCOUNTABLENAME","getACCOUNTABLEPARENT"};
                    for (String strTemp : etl_transform_history_accountable_product_excl_delta) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() +
                                " " + strTemp + " => Acc_Prod_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " Acc_Prod_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Excl_Delta",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
               break;

                case "etl_transform_history_manifestation_excl_delta":
                    Log.info("etl_transform_history_manifestation_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_manifestation_excl_delta = {"getUKEY","getSOURCEREF","getTITLE","getINTEREDITIONFLAG","getFIRSTPUBLISHEDDATE","getBINDING","getMANIFESTATIONTYPE","getSTATUS"
                            ,"getWORKID","getLASTPUBDATE","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_manifestation_excl_delta) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getSOURCEREF() +
                                " " + strTemp + " => Manif_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " Manif_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Excl_Delta",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_manifestation_identifier_excl_delta":
                    Log.info("etl_transform_history_manifestation_identifier_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_manifestation_identifier_excl_delta = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE"};
                    for (String strTemp : etl_transform_history_manifestation_identifier_excl_delta) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                                " " + strTemp + " => Manif_Ident_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " Manif_Ident_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Ident_Excl_Delta",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_product_excl_delta":
                    Log.info("etl_transform_history_product_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_product_excl_delta = {"getUKEY","getSOURCEREF","getBINDINGCODE","getNAME","getSHORTTITLE","getLAUNCHDATE",
                            "getTAXCODE","getSTATUS","getMANIFESTATIONREF","getWORKSOURCE","getWORKTYPE","getSEPRATELYSALEINDICATOR","getTRIALALLOWEDINDICATOR","getFWORKSOURCEREF",
                            "getPRODUCTTYPE","getREVENUEMODEL","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_product_excl_delta) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                                " " + strTemp + " => Prod_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " Prod_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Excl_Delta",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_person_role_excl_delta":
                    Log.info("etl_transform_history_work_person_role_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_work_person_role_excl_delta = {"getUKEY","getWORKSOURCEREF","getPERSONSOURCEREF","getROLETYPE","getSEQUENCE","getDEDUPLICATOR","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_work_person_role_excl_delta) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                                " " + strTemp + " => PERS_ROLE_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " PERS_ROLE_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Excl_Delta",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_relationship_excl_delta":
                    Log.info("etl_transform_history_work_relationship_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_work_relationship_excl_delta = {"getUKEY","getPARENTREF","getCHILDREF","getRELATIONTYPEREF","getLASTUDATEDDATE","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_work_relationship_excl_delta) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                                " " + strTemp + " => WORK_RELAT_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " WORK_RELAT_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Excl_Delta",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                break;

                case "etl_transform_history_work_excl_delta":
                    Log.info("etl_transform_history_work_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_work = {"getUKEY","getSOURCEREF","getTITLE","getSUBTITLE","getVOLUMENO","getCOPYRIGHTYEAR","getEDITIONNO","getPMC","getWORKTYPE","getSTATUSCODE","getIMPRINTCODE","getTEOPCO","getOPCO"
                            ,"getRESPCENTRE","getLANGUAGECODE","getELECTRORIGHTSINDICATOR","getFOAJOURNALTYPE","getFSOCIETYOWNERSHIP","getSUBSCRIPTIONTYPE"};

                    for (String strTemp : etl_delta_current_work) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                                " " + strTemp + " => WORK_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " WORK_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Excl_Delta",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                   break;

                    case "etl_transform_history_work_identifier_excl_delta":
                    Log.info("etl_transform_history_work_identifier_excl_delta Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etl_delta_current_work_identifier = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE"};
                        for (String strTemp : etl_delta_current_work_identifier) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getUKEY() +
                                    " " + strTemp + " => Work_Ident_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                    " Work_Ident_Excl_Delta = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Excl_Delta",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
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
    public void comparePersonExclAndDiffOfDeltaAndCurrHist()throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.isEmpty()) {
            Log.info("No Data Found in  Diff of Delta and Current History table....");
        } else {
            Log.info("Sorting the Ids to compare the records between Person Exclude and Person Diff Delta and Current Hist...");
            for (int i = 0; i < dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.size(); i++) {
                dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                dataQualityBCSContext.recFromPersonExclDelta.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                String[] etl_person_current_v_col = {"getUKEY", "getSOURCEREF", "getFIRSTNAME", "getFAMILYNAME", "getPEOPLEHUBID", "getEMAIL", "getDQ_ERR"};
                for (String strTemp : etl_person_current_v_col) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;

                    BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i);
                    BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromPersonExclDelta.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndCurrHist.get(i).getUKEY() +
                            " " + strTemp + " => Person_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                            " Person_Excl_Delta = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Excl_Delta",
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

///////////////////////////////////////////////////////////////////////////////////////////////




    @Given("^Get the (.*) from diff of delta_current and exclude_delta tables (.*)$")
    public void getIdsFromDiffOfDeltaCurrAndExcl(String numberOfRecords, String tableName) {
         //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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
    public void compareLatestwithDiffOfDeltaAndExcl(String targetTableName)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
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

                    String[] etl_transform_history_accountable_product_latest = {"getUKEY","getSOURCEREF","getACCOUNTABLEPRODUCT","getACCOUNTABLENAME","getACCOUNTABLEPARENT"};
                    for (String strTemp : etl_transform_history_accountable_product_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() +
                                " " + strTemp + " => Acc_Prod_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " Acc_Prod_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Latest",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_manifestation_latest":
                    Log.info("etl_transform_history_manifestation_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_manifestation_latest = {"getUKEY","getSOURCEREF","getTITLE","getINTEREDITIONFLAG","getFIRSTPUBLISHEDDATE","getBINDING","getMANIFESTATIONTYPE","getSTATUS"
                            ,"getWORKID","getLASTPUBDATE","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_manifestation_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getSOURCEREF() +
                                " " + strTemp + " => Manif_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " Manif_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Latest",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                break;

                case "etl_transform_history_manifestation_identifier_latest":
                    Log.info("etl_transform_history_manifestation_identifier_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_manifestation_identifier_Latest = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE"};
                    for (String strTemp : etl_transform_history_manifestation_identifier_Latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                                " " + strTemp + " => Manif_Ident_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " Manif_Ident_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Ident_Latest",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_product_latest":
                    Log.info("etl_transform_history_product_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_product_latest = {"getUKEY","getSOURCEREF","getBINDINGCODE","getNAME","getSHORTTITLE","getLAUNCHDATE",
                            "getTAXCODE","getSTATUS","getMANIFESTATIONREF","getWORKSOURCE","getWORKTYPE","getSEPRATELYSALEINDICATOR","getTRIALALLOWEDINDICATOR","getFWORKSOURCEREF",
                            "getPRODUCTTYPE","getREVENUEMODEL","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_product_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                                " " + strTemp + " => Prod_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " Prod_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Latest",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_person_role_latest":
                    Log.info("etl_transform_history_work_person_role_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_work_person_role_latest = {"getUKEY","getWORKSOURCEREF","getPERSONSOURCEREF","getROLETYPE","getSEQUENCE","getDEDUPLICATOR","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_work_person_role_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                                " " + strTemp + " => PERS_ROLE_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " PERS_ROLE_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Latest",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

                    break;

                case "etl_transform_history_work_relationship_latest":
                    Log.info("etl_transform_history_work_relationship_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_work_relationship_latest = {"getUKEY","getPARENTREF","getCHILDREF","getRELATIONTYPEREF","getLASTUDATEDDATE","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_work_relationship_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                                " " + strTemp + " => WORK_RELAT_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " WORK_RELAT_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Latest",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_latest":
                    Log.info("etl_transform_history_work_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_delta_current_work = {"getUKEY","getSOURCEREF","getTITLE","getSUBTITLE","getVOLUMENO","getCOPYRIGHTYEAR","getEDITIONNO","getPMC","getWORKTYPE","getSTATUSCODE","getIMPRINTCODE","getTEOPCO","getOPCO"
                            ,"getRESPCENTRE","getLANGUAGECODE","getELECTRORIGHTSINDICATOR","getFOAJOURNALTYPE","getFSOCIETYOWNERSHIP","getSUBSCRIPTIONTYPE"};

                    for (String strTemp : etl_delta_current_work) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                                " " + strTemp + " => WORK_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " WORK_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Latest",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_identifier_latest":
                    Log.info("etl_transform_history_work_identifier_Latest Records:");
                    dataQualityBCSContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));


                    String[] etl_delta_current_work_identifier = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE"};
                    for (String strTemp : etl_delta_current_work_identifier) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recFromDiffOfDeltaAndExcl.get(i).getUKEY() +
                                " " + strTemp + " => Work_Ident_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " Work_Ident_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Latest",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
            }
        }
    }


    @Given("^Get the (.*) from diff of Person delta_current and Person exclude_delta tables$")
    public void getRandKeyFromPersonDiffDeltaAndExcl(String numberOfRecords) {
      //  numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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
    public void comparePersonLatestAndDiffOfDeltaAndExcl()throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.isEmpty()) {
            Log.info("No Data Found in Person Trans_File Diff....");
        } else {
            Log.info("Sorting the Ids to compare the records between Person Latest and Person Diff Delta and Excl...");
            for (int i = 0; i < dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.size(); i++) {
                dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                dataQualityBCSContext.recFromPersonLatest.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                String[] etl_person_current_v_col = {"getUKEY", "getSOURCEREF", "getFIRSTNAME", "getFAMILYNAME", "getPEOPLEHUBID", "getEMAIL", "getDQ_ERR"};
                for (String strTemp : etl_person_current_v_col) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;

                    BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i);
                    BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromPersonLatest.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfPersonDeltaAndExcl.get(i).getUKEY() +
                            " " + strTemp + " => Person_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                            " Person_Latest = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Latest",
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }


}




