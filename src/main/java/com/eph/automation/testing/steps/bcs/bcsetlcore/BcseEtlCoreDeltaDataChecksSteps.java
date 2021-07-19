package com.eph.automation.testing.steps.bcs.bcsetlcore;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BcsEtlCoreAccessDLContext;
import com.eph.automation.testing.models.dao.bcs.BcsEtlCoreDLAccessObject;
import com.eph.automation.testing.services.db.bcsetlcoresql.BcsEtlCoreDataChecksSql;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class BcseEtlCoreDeltaDataChecksSteps {

    private static String sql;
    private static List<String> ids;
    private static String noTablemsg = "no tables found";

    @Given("^Get the (.*) of BCS Core data from transform_file Tables (.*)$")
    public static void getidsFromTransformFile(String numberOfRecords, String tableName) {
         numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for bcs Core Transform_File Tables....");

        switch (tableName) {
            case "etl_accountable_product_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_ACCPROD_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_manifestation_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_manifestation_identifier_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_IDENT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_product_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_PRODUCT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_person_role_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_relationship_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_RELATION_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_identifier_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_IDENT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_person_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_PERSON_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
             default:
                 Log.info(noTablemsg);
       }
        List<Map<?, ?>> randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("uKey")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When ("^Get the Data from the Difference of Current and Previous transform_file Tables (.*)$")
    public static void getRecFromDiffTransformFile(String tableName){
        Log.info("We get the records from Diff of Transform File bcs Core table...");
        switch (tableName) {
            case "etl_accountable_product_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_DIFF_TRANS_FILE, String.join(("','"),ids));
                break;
            case "etl_manifestation_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_REC_DIFF_TRANS_FILE, String.join(("','"),ids));
                break;
            case "etl_manifestation_identifier_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_REC_DIFF_TRANS_FILE, String.join(("','"),ids));
                break;
            case "etl_product_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PRODUCT_DIFF_TRANS_FILE, String.join(("','"),ids));
                break;
            case "etl_work_person_role_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_ROLE_REC_DIFF_TRANS_FILE, String.join(("','"),ids));
                break;
            case "etl_work_relationship_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELATION_REC_DIFF_TRANS_FILE, String.join(("','"),ids));
                break;
            case "etl_work_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_REC_DIFF_TRANS_FILE, String.join(("','"),ids));
                break;
            case "etl_work_identifier_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_IDENT_REC_DIFF_TRANS_FILE, String.join(("','"),ids));
                break;
            case "etl_person_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_REC_DIFF_TRANS_FILE, String.join(("','"),ids));
                break;
            default:
                Log.info(noTablemsg);

        }
        BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When ("^We Get the records from delta current table BCS core (.*)$")
    public static void getRecFromDeltaCurrent(String tableName){
        Log.info("We get the records from Delta Current bcs Core table...");
        switch (tableName) {
            case "etl_delta_current_accountable_product":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_DELTA_CURRENT, String.join(("','"),ids));
                break;
            case "etl_delta_current_manifestation":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_REC_DELTA_CURRENT, String.join(("','"),ids));
                break;
            case "etl_delta_current_manifestation_identifier":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_REC_DELTA_CURRENT, String.join(("','"),ids));
                break;
            case "etl_delta_current_product":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PRODUCT_REC_DELTA_CURRENT, String.join(("','"),ids));
                break;
            case "etl_delta_current_work_person_role":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_ROLE_REC_DELTA_CURRENT, String.join(("','"),ids));
                break;
            case "etl_delta_current_work_relationship":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELATION_REC_DELTA_CURRENT, String.join(("','"),ids));
                break;
            case "etl_delta_current_work":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_REC_DIFF_DELTA_CURRENT, String.join(("','"),ids));
                break;
            case "etl_delta_current_work_identifier":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_IDENT_REC_DELTA_CURRENT, String.join(("','"),ids));
                break;
            case "etl_delta_current_person":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_REC_DELTA_CURR, String.join(("','"),ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        BcsEtlCoreAccessDLContext.recFromDeltaCurrent = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare the records of BCS Core delta current and BCS diff of Transform_File (.*)$")
    public static void compareCurrentandCurrHist(String targetTableName)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.isEmpty()) {
            Log.info("No Data Found in the Current Tables ....");
        } else {
        for (int i = 0; i < BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.size(); i++) {
            switch (targetTableName) {
                case "etl_delta_current_accountable_product":
                    Log.info("etl_delta_current_accountable_product Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurraccountableProduct = {"getuKey", "getsourceRef", "getaccountableProduct", "getaccountableName", "getaccountableParent", "getdqErr", "getdeltaMode"};
                    for (String strTemp : etlDeltaCurraccountableProduct) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getsourceRef() +
                                " " + strTemp + " => Acc_Prod_Trans_file_Diff = " + method.invoke(objectToCompare1) +
                                " Acc_Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Delta_Curr for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "etl_delta_current_manifestation":
                    Log.info("etl_delta_current_manifestation Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrManifestation = {"getuKey", "getsourceRef", "gettitle", "getintereditionFlag", "getfirstPublishedDate", "getbinding", "getmanifestationType", "getstatus"
                            , "getworkId", "getlastPubDate", "getdqErr", "getdeltaMode"};
                    for (String strTemp : etlDeltaCurrManifestation) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getsourceRef() +
                                " " + strTemp + " => Manif_Trans_File_Diff = " + method.invoke(objectToCompare1) +
                                " Manif_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "etl_delta_current_manifestation_identifier":
                    Log.info("etl_delta_current_manifestation_identifier Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrentManifestationIdentifier = {"getuKey", "getsourceRef", "getidentifier", "getidentifierType", "getdeltaMode"};
                    for (String strTemp : etlDeltaCurrentManifestationIdentifier) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey() +
                                " " + strTemp + " => Manif_Ident_Trans_Diff = " + method.invoke(objectToCompare1) +
                                " Manif_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_product":
                    Log.info("etl_delta_current_product Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrentProduct = {"getuKey", "getsourceRef", "getbindingCode", "getname", "getshortTitle", "getlaunchDate",
                            "gettaxCode", "getstatus", "getmanifestationRef", "getworkSource", "getworkType", "getsepratelySaleIndicator", "gettrialAllowedIndicator", "getfWorkSourceRef",
                            "getproductType", "getrevenueModel", "getdqErr", "getdeltaMode"};
                    for (String strTemp : etlDeltaCurrentProduct) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey() +
                                " " + strTemp + " => Prod_Trans_Diff = " + method.invoke(objectToCompare1) +
                                " Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Delta_Curr for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_person_role":
                    Log.info("etl_delta_current_work_person_role Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrentWorkPersonRole = {"getuKey", "getworkSourceRef", "getpersonSourceRef", "getroleType", "getsequence", "getdeDuplicator", "getdqErr", "getdeltaMode"};
                    for (String strTemp : etlDeltaCurrentWorkPersonRole) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey() +
                                " " + strTemp + " => PERS_ROLE_Diff_Tran_Diff = " + method.invoke(objectToCompare1) +
                                " PERS_ROLE_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Delta_Curr for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_relationship":
                    Log.info("etl_delta_current_work_relationship Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransformHistoryWorkRelationshiPart = {"getuKey", "getparentRef", "getchildRef", "getrelationTypeRef", "getlastUpdatedDate", "getdqErr", "getdeltaMode"};
                    for (String strTemp : etlTransformHistoryWorkRelationshiPart) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey() +
                                " " + strTemp + " => WORK_RELAT_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                " WORK_RELAT_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Delta_Curr for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work":
                    Log.info("etl_delta_current_work Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrentWork = {"getuKey", "getsourceRef", "gettitle", "getsubTitle", "getvolumeNo", "getcopyRightYear", "geteditionNo", "getpmc", "getworkType", "getstatusCODE", "getimprintCode", "getteopco", "getopco"
                            , "getrespCentre", "getlanguageCode", "getelectroRightIndicator", "getfoaJournalType", "getfSocietyOwnership", "getsubscriptionType", "getdeltaMode"};

                    for (String strTemp : etlDeltaCurrentWork) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey() +
                                " " + strTemp + " => WORK_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                " WORK_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Delta_Curr for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_identifier":
                    Log.info("etl_delta_current_work_identifier Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrentWorkIdentifier = {"getuKey", "getsourceRef", "getidentifier", "getidentifierType", "getdeltaMode"};
                    for (String strTemp : etlDeltaCurrentWorkIdentifier) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey() +
                                " " + strTemp + " => Work_Ident_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                " Work_Ident_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Delta_Curr for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "etl_delta_current_person":
                    Log.info("etl_delta_current_person Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));
                    String[] etlPersonCurrentCol = {"getuKey", "getsourceRef", "getfirstName", "getfamilyName", "getpeopleHubId", "getemail", "getdqErr", "getdeltaMode"};
                    for (String strTemp : etlPersonCurrentCol) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey() +
                                " " + strTemp + " => Person_Diff_Trans_File = " + method.invoke(objectToCompare1) +
                                " Person_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Delta_Curr for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfTransformFile.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                   default:
                       Log.info(noTablemsg);
            }

        }

        }
    }


//////////////////////////////////////////////////////////////////////////////

    @Given("^Get the (.*) of BCS Core data from delta_Current_hist Tables (.*)$")
    public static void getidsFromDeltaHist(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for bcs Core Delta History Tables....");

        switch (tableName) {
            case "etl_delta_history_accountable_product_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_KEY_ACCPROD_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_manifestation_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_KEY_MANIF_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_manifestation_identifier_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_KEY_MANIF_IDENT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_product_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_KEY_PRODUCT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_work_person_role_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_KEY_WORK_PERS_ROLE_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_work_relationship_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_KEY_WORK_RELATION_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_work_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_KEY_WORK_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_work_identifier_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_KEY_WORK_IDENT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_person_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_KEY_PERS_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            default:
                Log.info(noTablemsg);

        }
        List<Map<?, ?>> randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("uKey")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When ("^Get the Data from the Delta_Current_History Tables (.*)$")
    public static void getRecFromDeltaHist(String tableName){
        Log.info("We get the records from Delta Hist of bcs Core table...");
        switch (tableName) {
            case "etl_delta_history_accountable_product_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_DELTA_CURRENT_HIST, String.join(("','"),ids));
                break;
            case "etl_delta_history_manifestation_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_REC_DELTA_CURRENT_HIST, String.join(("','"),ids));
                break;
            case "etl_delta_history_manifestation_identifier_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_REC_DELTA_CURRENT_HIST, String.join(("','"),ids));
                break;
            case "etl_delta_history_product_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PRODUCT_REC_DELTA_CURRENT_HIST, String.join(("','"),ids));
                break;
            case "etl_delta_history_work_person_role_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_ROLE_REC_DELTA_CURRENT_HIST, String.join(("','"),ids));
                break;
            case "etl_delta_history_work_relationship_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELATION_REC_DELTA_CURRENT_HIST, String.join(("','"),ids));
                break;
            case "etl_delta_history_work_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_REC_DIFF_DELTA_CURRENT_HIST, String.join(("','"),ids));
                break;
            case "etl_delta_history_work_identifier_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_REC_DIFF_DELTA_CURRENT_HIST, String.join(("','"),ids));
                break;
            case "etl_delta_history_person_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_REC_DELTA_CURR_HIST, String.join(("','"),ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare the records of BCS Core delta current and delta_Current_history (.*)$")
    public static void compareDeltaCurrentandDletaHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.isEmpty()) {
            Log.info("No Data Found in the Delta Current Hist Tables ....");
        } else {
            Log.info("Sorting the ids to compare the records between Delta Current and Delta Hist tables...");
        for (int i = 0; i < BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.size(); i++) {
            switch (targetTableName) {
                case "etl_delta_current_accountable_product":
                    Log.info("comparing delta_history and etl_delta_current_accountable_product Records:");
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrAccoProduct = {"getuKey","getsourceRef","getaccountableProduct","getaccountableName","getaccountableParent","getdqErr","getdeltaMode"};
                    for (String strTemp : etlDeltaCurrAccoProduct) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getsourceRef() +
                                " " + strTemp + " => Acc_Prod_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " Acc_Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Delta_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_manifestation":
                    Log.info("comparing delta_history and etl_delta_current_manifestation Records:");
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrManif = {"getuKey","getsourceRef","gettitle","getintereditionFlag","getfirstPublishedDate","getbinding","getmanifestationType","getstatus"
                            ,"getworkId","getlastPubDate","getdqErr","getdeltaMode"};
                    for (String strTemp : etlDeltaCurrManif) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getsourceRef() +
                                " " + strTemp + " => Manif_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " Manif_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_manifestation_identifier":
                    Log.info("comparing delta_history and etl_delta_current_manifestation_identifier Records:");
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrentManifIdentifier = {"getuKey","getsourceRef","getidentifier","getidentifierType","getdeltaMode"};
                    for (String strTemp : etlDeltaCurrentManifIdentifier) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey() +
                                " " + strTemp + " => Manif_Ident_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " Manif_Ident_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_product":
                    Log.info("comparing delta_history and etl_delta_current_product Records:");
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrentProduct = {"getuKey","getsourceRef","getbindingCode","getname","getshortTitle","getlaunchDate",
                            "gettaxCode","getstatus","getmanifestationRef","getworkSource","getworkType","getsepratelySaleIndicator","gettrialAllowedIndicator","getfWorkSourceRef",
                            "getproductType","getrevenueModel","getdqErr","getdeltaMode"};
                    for (String strTemp : etlDeltaCurrentProduct) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey() +
                                " " + strTemp + " => Prod_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " Prod_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Delta_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_person_role":
                    Log.info("comparing delta_history and etl_delta_current_work_person_role Records:");
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrWorkPersonRole = {"getuKey","getworkSourceRef","getpersonSourceRef","getroleType","getsequence","getdeDuplicator","getdqErr","getdeltaMode"};
                    for (String strTemp : etlDeltaCurrWorkPersonRole) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey() +
                                " " + strTemp + " => PERS_ROLE_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " PERS_ROLE_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Delta_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_relationship":
                    Log.info("comparing delta history and etl_delta_current_work_relationship Records:");
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransformHistWorkRelationshiPart = {"getuKey","getparentRef","getchildRef","getrelationTypeRef","getlastUpdatedDate","getdqErr","getdeltaMode"};
                    for (String strTemp : etlTransformHistWorkRelationshiPart) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey() +
                                " " + strTemp + " => WORK_RELAT_Delta_Hist = " + method.invoke(objectToCompare1) +
                                " WORK_RELAT_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Delta_Hist for uKey:"+ BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work":
                    Log.info("comparing delta history and etl_delta_current_work Records:");
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrentWork = {"getuKey","getsourceRef","gettitle","getsubTitle","getvolumeNo","getcopyRightYear","geteditionNo","getpmc","getworkType","getstatusCODE","getimprintCode","getteopco","getopco"
                            ,"getrespCentre","getlanguageCode","getelectroRightIndicator","getfoaJournalType","getfSocietyOwnership","getsubscriptionType","getdeltaMode"};

                    for (String strTemp : etlDeltaCurrentWork) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey() +
                                " " + strTemp + " => WORK_Delta_Curr_hist = " + method.invoke(objectToCompare1) +
                                " WORK_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Delta_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_work_identifier":
                    Log.info("comparing delta history and etl_delta_current_work_identifier Records:");
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrentWorkIdentifier = {"getuKey","getsourceRef","getidentifier","getidentifierType","getdeltaMode"};
                    for (String strTemp : etlDeltaCurrentWorkIdentifier) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey() +
                                " " + strTemp + " => Work_Ident_Delta_Curr_hist = " + method.invoke(objectToCompare1) +
                                " Work_Ident_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Delta_Curr_hist for uKey:"+ BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_delta_current_person":
                    Log.info("comparing delta history and etl_delta_current_person Records:");
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));
                    String[] etlPersonCurrCol = {"getuKey", "getsourceRef", "getfirstName", "getfamilyName", "getpeopleHubId", "getemail", "getdqErr","getdeltaMode"};
                    for (String strTemp : etlPersonCurrCol) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromDeltaCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey() +
                                " " + strTemp + " => Person_Delta_curr_hist = " + method.invoke(objectToCompare1) +
                                " Person_Delta_Curr = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Delta_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recFromDeltaCurrentHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                    default:
                        Log.info(noTablemsg);
            }
        }
    }
    }

}




