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

import static com.eph.automation.testing.steps.GenericFunctions.setRandomCount;


public class BcsEtlCoreLatestDataChecksSteps {

    private static String sql;
    private static List<String> ids;
    private static String noTablemsg = "No such Tables found";

    @Given("^Get the (.*) from diff of delta_current and current_hist tables (.*)$")
    public static void getIdsFromDiffOfDeltaCurrAndCurrHist(String countOfRandomIds, String tableName) {
        /*String numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        if(numberOfRecords==null)numberOfRecords= countOfRandomIds;
        Log.info("numberOfRecords = " + numberOfRecords);*/

        String numberOfRecords = setRandomCount(countOfRandomIds);
        Log.info("Get random Ids for bcs Core Tables from Diff of Delta Current and Current Hist....");

        switch (tableName) {
            case "etl_transform_history_accountable_product_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_ACCPROD_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_manifestation_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_manifestation_identifier_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_IDENT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_product_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_PRODUCT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_work_person_role_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_work_relationship_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_RELATION_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_work_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_work_identifier_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_IDENT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
            case "etl_transform_history_person_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_PERSON_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
                break;
             default:
                 Log.info(noTablemsg);
       }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomIds.stream().map(m -> (String) m.get("uKey")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When ("^Get the records from the diff of delta_current and current_hist tables (.*)$")
    public static void getRecFromDiffDeltaCurrAndCurrHist(String tableName){
        Log.info("We get the records from Diff of Delta Current and Current Hist of bcs Core table...");
        switch (tableName) {
            case "etl_transform_history_accountable_product_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_manifestation_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_manifestation_identifier_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_product_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PRODUCT_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_work_person_role_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_ROLE_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_work_relationship_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELATION_REC_DIFF_DELTACURR_CURRHIST,String.join("','",ids));
                break;
            case "etl_transform_history_work_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_work_identifier_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_IDENT_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_person_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
                default:
                    Log.info(noTablemsg);
        }
        BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @When ("^Get the records from (.*) exclude table$")
    public static void getRecFromDeltaCurrent(String tableName){
        Log.info("We get the records from Exclude delta bcs Core table...");
        switch (tableName) {
            case "etl_transform_history_accountable_product_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_manifestation_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_manifestation_identifier_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_product_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PRODUCT_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_work_person_role_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_ROLE_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_work_relationship_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELATION_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_work_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_REC_DIFF_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_work_identifier_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_IDENT_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_person_excl_delta":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_REC_EXCL_DELTA, String.join("','",ids));
                break;
             default:
                 Log.info(noTablemsg);
        }
        BcsEtlCoreAccessDLContext.recFromExclDelta = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare the records of Exclude with diff of delta_current and current_hist tables (.*)$")
    public void compareExclwithDiffOfDeltaAndCurrHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if(BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.size(); i++) {
                switch (targetTableName) {
                    case "etl_transform_history_accountable_product_excl_delta":
                        Log.info("etl_transform_history_accountable_product_excl_delta Records:");
                        BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlTransHistAccProdExclDelta = {"getuKey","getsourceRef","getaccountableProduct","getaccountableName","getaccountableParent"};
                        for (String strTemp : etlTransHistAccProdExclDelta) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getsourceRef() +
                                    " " + strTemp + " => Acc_Prod_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                    " Acc_Prod_Excl_Delta = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Excl_Delta for uKey:"+ BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
               break;

                case "etl_transform_history_manifestation_excl_delta":
                    Log.info("etl_transform_history_manifestation_excl_delta Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransHistManifExclDelta = {"getuKey","getsourceRef","gettitle","getintereditionFlag","getfirstPublishedDate","getbinding","getmanifestationType","getstatus"
                            ,"getworkId","getlastPubDate","getdqErr"};
                    for (String strTemp : etlTransHistManifExclDelta) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getsourceRef() +
                                " " + strTemp + " => Manif_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " Manif_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Excl_Delta for uKey:"+ BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_manifestation_identifier_excl_delta":
                    Log.info("etl_transform_history_manifestation_identifier_excl_delta Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransHisManifIdentifExclDelta = {"getuKey","getsourceRef","getidentifier","getidentifierTYPE"};
                    for (String strTemp : etlTransHisManifIdentifExclDelta) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey() +
                                " " + strTemp + " => Manif_Ident_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " Manif_Ident_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Ident_Excl_Delta for uKey:"+ BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_product_excl_delta":
                    Log.info("etl_transform_history_product_excl_delta Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransformHistoryProductExclDelta = {"getuKey","getsourceRef","getbindingCode","getname","getshortTitle","getlaunchDate",
                            "gettaxCode","getstatus","getmanifestationRef","getworkSource","getworkType","getsepratelySaleIndicator","gettrialAllowedIndicator","getfWorkSourceRef",
                            "getproductType","getrevenueModel","getdqErr"};
                    for (String strTemp : etlTransformHistoryProductExclDelta) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey() +
                                " " + strTemp + " => Prod_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " Prod_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Excl_Delta for uKey:"+ BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_person_role_excl_delta":
                    Log.info("etl_transform_history_work_person_role_excl_delta Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransformHistoryWorkPersonRoleExclDelta = {"getuKey","getworkSourceRef","getpersonSourceRef","getroleType","getsequence","getdeDuplicator","getdqErr"};
                    for (String strTemp : etlTransformHistoryWorkPersonRoleExclDelta) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey() +
                                " " + strTemp + " => PERS_ROLE_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " PERS_ROLE_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Excl_Delta for uKey:"+ BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_relationship_excl_delta":
                    Log.info("etl_transform_history_work_relationship_excl_delta Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransformHistoryWorkRelationshipExclDelta = {"getuKey","getparentRef","getchildRef","getrelationTypeRef","getlastUpdatedDate","getdqErr"};
                    for (String strTemp : etlTransformHistoryWorkRelationshipExclDelta) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey() +
                                " " + strTemp + " => WORK_RELAT_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " WORK_RELAT_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Excl_Delta for uKey:"+ BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                break;

                case "etl_transform_history_work_excl_delta":
                    Log.info("etl_transform_history_work_excl_delta Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrentWork = {"getuKey","getsourceRef","gettitle","getsubTitle","getvolumeNo","getcopyRightYear","geteditionNo","getpmc","getworkType","getstatusCODE","getimprintCode","getteopco","getopco"
                            ,"getrespCentre","getlanguageCode","getelectroRightIndicator","getfoaJournalType","getfSocietyOwnership","getsubscriptionType"};

                    for (String strTemp : etlDeltaCurrentWork) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey() +
                                " " + strTemp + " => WORK_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " WORK_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Excl_Delta for uKey:"+ BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                   break;

                    case "etl_transform_history_work_identifier_excl_delta":
                    Log.info("etl_transform_history_work_identifier_excl_delta Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlDeltaCurrentWorkIdentifier = {"getuKey","getsourceRef","getidentifier","getidentifierTYPE"};
                        for (String strTemp : etlDeltaCurrentWorkIdentifier) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey() +
                                    " " + strTemp + " => Work_Ident_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                    " Work_Ident_Excl_Delta = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Excl_Delta for uKey:"+ BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                    break;
                case "etl_transform_history_person_excl_delta":
                    Log.info("etl_transform_history_person_excl_delta Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));
                    String[] etlPersonCurrentCol = {"getuKey", "getsourceRef", "getfirstName", "getfamilyName", "getpeopleHubId", "getemail", "getdqErr"};
                    for (String strTemp : etlPersonCurrentCol) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromExclDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey() +
                                " " + strTemp + " => Person_Diff_DeltaCurr_CurrHist = " + method.invoke(objectToCompare1) +
                                " Person_Excl_Delta = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Excl_Delta for uKey:"+ BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).getuKey(),
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

///////////////////////////////////////////////////////////////////////////////////////////////

    @Given("^Get the (.*) from sum of delta_current and exclude_delta tables (.*)$")
    public static void getIdsFromDiffOfDeltaCurrAndExcl(String countOfRandomIds, String tableName) {
        String numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        if(numberOfRecords==null)numberOfRecords= countOfRandomIds;
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for bcs Core Tables from Diff of Delta Current and Exclude....");
        switch (tableName) {
            case "etl_transform_history_accountable_product_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_ACCPROD_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_manifestation_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_IDENT_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_product_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_PRODUCT_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_work_person_role_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_work_relationship_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_RELATION_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_work_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_work_identifier_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_IDENT_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_person_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_PERSON_KEY_DIFF_DELTACURR_EXCL, numberOfRecords);
                break;
            default:
                Log.info(noTablemsg);

        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomIds.stream().map(m -> (String) m.get("uKey")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When ("^Get the records from the sum of delta_current and exclude_delta tables (.*)$")
    public static void getRecFromDiffDeltaCurrAndExcl(String tableName){
        Log.info("We get the records from Diff of Delta Current and Excl of bcs Core table...");
        switch (tableName) {
            case "etl_transform_history_accountable_product_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_DIFF_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_manifestation_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_REC_DIFF_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_REC_DIFF_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_product_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PRODUCT_DIFF_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_work_person_role_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_ROLE_REC_DIFF_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_work_relationship_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELATION_REC_DIFF_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_work_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_REC_DIFF_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_work_identifier_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_IDENT_REC_DIFF_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_person_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_REC_DIFF_DELTACURR_EXCL, String.join("','",ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When ("^Get the records from (.*) BCS core latest table$")
    public static void getRecFromLatest(String tableName){
        Log.info("We get the records from Latest bcs Core table...");
        switch (tableName) {
            case "etl_transform_history_accountable_product_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_manifestation_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_manifestation_identifier_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_product_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PRODUCT_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_work_person_role_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_ROLE_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_work_relationship_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELATION_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_work_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_work_identifier_latest":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_IDENT_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_person_latest" :
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_REC_LATEST, String.join("','",ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        BcsEtlCoreAccessDLContext.recFromLatest = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare the records of Latest with sum of delta_current and Exclude_Delta tables (.*)$")
    public void compareLatestwithDiffOfDeltaAndExcl(String targetTableName)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
        for (int i = 0; i < BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.size(); i++) {
            switch (targetTableName) {
                case "etl_transform_history_accountable_product_latest":
                    Log.info("etl_transform_history_accountable_product_Latest Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransformHistoryAccountableProductLatest = {"getuKey", "getsourceRef", "getaccountableProduct", "getaccountableName", "getaccountableParent"};
                    for (String strTemp : etlTransformHistoryAccountableProductLatest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getsourceRef() +
                                " " + strTemp + " => Acc_Prod_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " Acc_Prod_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Latest for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_manifestation_latest":
                    Log.info("etl_transform_history_manifestation_Latest Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransformHistoryManifestationLatest = {"getuKey", "getsourceRef", "gettitle", "getintereditionFlag", "getfirstPublishedDate", "getbinding", "getmanifestationType", "getstatus"
                            , "getworkId", "getlastPubDate", "getdqErr"};
                    for (String strTemp : etlTransformHistoryManifestationLatest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getsourceRef() +
                                " " + strTemp + " => Manif_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " Manif_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Latest for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_manifestation_identifier_latest":
                    Log.info("etl_transform_history_manifestation_identifier_Latest Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransformHistoryManifestationIdentifierLatest = {"getuKey", "getsourceRef", "getidentifier", "getidentifierType","getleadIndicator"};
                    for (String strTemp : etlTransformHistoryManifestationIdentifierLatest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey() +
                                " " + strTemp + " => Manif_Ident_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " Manif_Ident_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Ident_Latest for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_product_latest":
                    Log.info("etl_transform_history_product_Latest Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransformHistoryProductLatest = {"getuKey", "getsourceRef", "getbindingCode", "getname", "getshortTitle", "getlaunchDate",
                            "gettaxCode", "getstatus", "getmanifestationRef", "getworkSource", "getworkType", "getsepratelySaleIndicator", "gettrialAllowedIndicator", "getfWorkSourceRef",
                            "getproductType", "getrevenueModel", "getdqErr"};
                    for (String strTemp : etlTransformHistoryProductLatest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey() +
                                " " + strTemp + " => Prod_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " Prod_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Latest for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_person_role_latest":
                    Log.info("etl_transform_history_work_person_role_Latest Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransformHistoryWorkPersonRoleLatest = {"getuKey", "getworkSourceRef", "getpersonSourceRef", "getroleType", "getsequence", "getdeDuplicator", "getdqErr"};
                    for (String strTemp : etlTransformHistoryWorkPersonRoleLatest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey() +
                                " " + strTemp + " => PERS_ROLE_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " PERS_ROLE_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Latest for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

                    break;

                case "etl_transform_history_work_relationship_latest":
                    Log.info("etl_transform_history_work_relationship_Latest Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlTransformHistoryWorkRelationshipLatest = {"getuKey", "getparentRef", "getchildRef", "getrelationTypeRef", "getlastUpdatedDate", "getdqErr"};
                    for (String strTemp : etlTransformHistoryWorkRelationshipLatest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey() +
                                " " + strTemp + " => WORK_RELAT_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " WORK_RELAT_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Latest for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_latest":
                    Log.info("etl_transform_history_work_Latest Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                    String[] etlDeltaCurrentWork = {"getuKey", "getsourceRef", "gettitle", "getsubTitle", "getvolumeNo", "getcopyRightYear", "geteditionNo", "getpmc", "getworkType", "getstatusCODE", "getimprintCode", "getteopco", "getopco"
                            , "getrespCentre", "getlanguageCode", "getelectroRightIndicator", "getfoaJournalType", "getfSocietyOwnership", "getsubscriptionType"};

                    for (String strTemp : etlDeltaCurrentWork) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey() +
                                " " + strTemp + " => WORK_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " WORK_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Latest for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_identifier_latest":
                    Log.info("etl_transform_history_work_identifier_Latest Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));


                    String[] etlDeltaCurrentWorkIdentifier = {"getuKey", "getsourceRef", "getidentifier", "getidentifierType"};
                    for (String strTemp : etlDeltaCurrentWorkIdentifier) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey() +
                                " " + strTemp + " => Work_Ident_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " Work_Ident_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Latest for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_person_latest":
                    Log.info("etl_transform_history_person_latest Records:");
                    BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                    BcsEtlCoreAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));
                    String[] etlPersonCurrentCol = {"getuKey", "getsourceRef", "getfirstName", "getfamilyName", "getpeopleHubId", "getemail", "getdqErr"};
                    for (String strTemp : etlPersonCurrentCol) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i);
                        BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("uKey => " + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey() +
                                " " + strTemp + " => Person_Diff_DeltaCurr_Excl = " + method.invoke(objectToCompare1) +
                                " Person_Latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Latest for uKey:" + BcsEtlCoreAccessDLContext.recFromDiffOfDeltaAndExcl.get(i).getuKey(),
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
