package com.eph.automation.testing.steps.bcs.bcsetlcore;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BcsEtlCoreAccessDLContext;
import com.eph.automation.testing.models.dao.bcs.BcsEtlCoreDLAccessObject;
import com.eph.automation.testing.services.db.bcsetlcoresql.BcsEtlCoreDataChecksSql;
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


public class BcsEtlCoreDataChecksSteps {

    private static String sql;
    private static List<String> ids;
    private static String noTablemsg = "No such tables found";

    @Given("^Get the (.*) of BCS Core data from Inbound Tables (.*)$")
    public static void getRandomidsFromInound(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for bcs Core Inbound Tables....");
        List<Map<?, ?>> randomids;
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_ACCPROD_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_IDENT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_product_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_PRODUCT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WRK_PERS_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WRK_RELT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_identifier_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_IDENT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_person_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_PERSON_KEY_INBOUND, numberOfRecords);
                break;
            case "all_manifestation_statuses_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_STATUS_KEY_INBOUND, numberOfRecords);
                break;
            case "all_manifestation_pubdates_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_PUBDATES_KEY_INBOUND, numberOfRecords);
                break;
            default:
                Log.info(noTablemsg);
        }
        randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("sourceRef")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }


    @When("^Get the Data from the Inbound Tables (.*)$")
    public static void getIngestRecords(String tableName) {
        Log.info("We get the bcs Ingest records...");
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_work_identifier_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_IDENT_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_person_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELT_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_work_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_INBOUND_DATA, String.join("','",ids));
                break;
            case "all_manifestation_statuses_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ALL_MANIF_status_INBOUND_DATA, String.join("','",ids));
                break;
            case "all_manifestation_pubdates_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ALL_MANIF_PUBDATES_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_product_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PROD_INBOUND_DATA, String.join("','",ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        BcsEtlCoreAccessDLContext.recordsFromInboundData = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Data from the BCS Core Current Tables to compare Inbound Check (.*)$")
    public static void getDataforInboundCheck(String tableName){
        Log.info("We get the records from Current bcs Core table for Inbound Check...");
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_CURR_DATA, String.join("','",ids));
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_CURR_REC, String.join("','",ids));
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_CURR_DATA, String.join("','",ids));
                break;
            case "etl_product_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PRODUCT_CURR_REC, String.join("','",ids));
                break;

            case "etl_work_identifier_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_IDENT_CURR_DATA, String.join("','",ids));
                break;
            case "etl_person_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_CURR_DATA, String.join("','",ids));
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELATION_CURR_DATA, String.join("','",ids));
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_ROLE_CURR_REC, String.join("','",ids));
                break;
            case "etl_work_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_CURR_DATA, String.join("','",ids));
                break;
            case "all_manifestation_statuses_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_STATUSES_DATA, String.join("','",ids));
                break;
            case "all_manifestation_pubdates_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_PUBDATES_DATA, String.join("','",ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        BcsEtlCoreAccessDLContext.recordsFromCurrent = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare data of BCS Inbound and BCS Core (.*) tables are identical$")
    public void compareIngestandCurrent(String tableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BcsEtlCoreAccessDLContext.recordsFromInboundData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records between Ingest and current...");
            for (int i = 0; i < BcsEtlCoreAccessDLContext.recordsFromInboundData.size(); i++) {
                switch (tableName) {
                    case "etl_accountable_product_current_v":
                        Log.info("comparing inbound and etl_accountable_product_current_v records...");
                        BcsEtlCoreAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] allAccProdCol = {"getsourceRef","getaccountableProduct","getaccountableName","getaccountableParent","getdqErr"};
                        for (String strTemp : allAccProdCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Acc_Prod_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "all_manifestation_statuses_v":
                        Log.info("all_manifestation_statuses_v records.... ");
                        BcsEtlCoreAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getsourceRef)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getsourceRef));

                        String[] allManifStatusesCol = {"getsourceRef","getrefKeyProdPriority","getdeliveryStatusProdPriority","getdeltaStatusProdPriority","getrefKeyManifPriority","getdeliveryStatusManifPriority","getdeltaStatusManifPriority"};
                        for (String strTemp : allManifStatusesCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("sourceRef => " +  BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getsourceRef() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Manif_statuses = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_statuses for sourceRef:"+ BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getsourceRef(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                        break;
                    case "all_manifestation_pubdates_v":
                        Log.info("all_manifestation_pubdates_v records ");
                        BcsEtlCoreAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getsourceRef)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getsourceRef));

                        String[] allManifPubdatesCol = {"getsourceRef","getworkMasterProjNo","getminActualPubDate","getminPlannedPubDate"};
                        for (String strTemp : allManifPubdatesCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("sourceRef => " +  BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getsourceRef() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Manif_pubdates = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_statuses for sourceRef:"+ BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getsourceRef(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_person_current_v":
                        Log.info("comparing inbound and etl_person_current_v records...");
                        BcsEtlCoreAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlPersonCurrentCol = {"getuKey","getsourceRef","getfirstName","getfamilyName","getpeopleHubId","getemail","getdqErr"};
                        for (String strTemp : etlPersonCurrentCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Person_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_manifestation_identifier_current_v":
                        Log.info("comparing inbound and etl_manifestation_identifier_current_v Records:");
                        BcsEtlCoreAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlManifIdentifierCurrentCol = {"getuKey","getsourceRef","getidentifier","getidentifierType","getleadIndicator"};
                        for (String strTemp : etlManifIdentifierCurrentCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Manif_Ident_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Ident_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_work_identifier_current_v":
                        Log.info("comparing inbound and etl_work_identifier Records:");
                        BcsEtlCoreAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlWorkIdentifierCurrentCol = {"getuKey","getsourceRef","getidentifier","getidentifierType"};
                        for (String strTemp : etlWorkIdentifierCurrentCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Work_Ident_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_work_relationship_current_v":
                        Log.info("comparing inbound and etl_work_relationship Records:");
                        BcsEtlCoreAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlWorkRelationshipCurrentCol = {"getuKey","getparentRef","getchildRef","getrelationTypeRef","getdqErr"};
                        for (String strTemp : etlWorkRelationshipCurrentCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Work_Relation_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Relation_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_work_person_role_current_v":
                        Log.info("comparing inbound and etl_work_person_role_current_v Records:");
                        BcsEtlCoreAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlWorkPersonRoleCurrentCol = {"getuKey","getworkSourceRef","getpersonSourceRef","getroleType","getsequence","getdeDuplicator","getdqErr"};
                        for (String strTemp : etlWorkPersonRoleCurrentCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " PERS_ROLE_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_work_current_v":
                        Log.info("comparing inbound and etl_work_current_v Records:");
                        BcsEtlCoreAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlWorkCurrentCol = {"getuKey","getsourceRef","gettitle","getsubTitle","getvolumeNo","getcopyRightYear","geteditionNo","getpmc","getworkType","getstatusCODE","getimprintCode","getteopco","getopco"
                                ,"getrespCentre","getlanguageCode","getelectroRightIndicator","getfoaJournalType","getfSocietyOwnership","getsubscriptionType"};
                        for (String strTemp : etlWorkCurrentCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " WORK_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_current_v":
                        Log.info("comparing inbound and etl_manifestation_current_v Records:");
                        BcsEtlCoreAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlManifCurrentCol = {"getuKey","getsourceRef","gettitle","getintereditionFlag","getfirstPublishedDate","getbinding","getmanifestationType","getstatus","getworkId","getlastPubDate","getdqErr"};
                        for (String strTemp : etlManifCurrentCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Manif_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_product_current_v":
                        Log.info("comparing current and etl_product_current_v Records:");
                        BcsEtlCoreAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlTransformHistProdPart = {"getuKey","getsourceRef","getbindingCode","getname","getshortTitle","getlaunchDate",
                                "gettaxCode","getstatus","getmanifestationRef","getworkSource","getworkType","getsepratelySaleIndicator","gettrialAllowedIndicator","getfWorkSourceRef",
                                "getproductType","getrevenueModel","getdqErr"};
                        for (String strTemp : etlTransformHistProdPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " +  BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey() +
                                    " " + strTemp + " => Prod_Curr = " + method.invoke(objectToCompare1) +
                                    " Prod_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Curr for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromInboundData.get(i).getuKey(),
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

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Given("^Get the (.*) of BCS Core data from Current Tables (.*)$")
    public static void getRandomidsFromCurrent(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for bcs Core Current Tables....");

        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_ACCPROD_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_MANIF_IDENT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_product_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_PRODUCT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_PERS_ROLE_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_RELATION_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_identifier_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_WORK_IDENT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_person_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_RANDOM_PERSON_KEY_CURRENT, numberOfRecords);
                break;
            default:
                Log.info(noTablemsg);
        }
        List<Map<?, ?>> randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("u_key")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());

    }

    @Then("^Get the Data from the BCS Core Current Tables (.*)$")
    public static void getRecFromCurrent(String tableName){
        Log.info("We get the records from Current bcs Core table...");
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_CURR_REC, String.join("','",ids));
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_CURR_REC, String.join("','",ids));
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_CURR_REC, String.join("','",ids));
                break;
            case "etl_product_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PRODUCT_CURR_REC, String.join("','",ids));
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_ROLE_CURR_REC, String.join("','",ids));
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELATION_CURR_REC, String.join("','",ids));
                break;
            case "etl_work_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_CURR_REC, String.join("','",ids));
                break;
            case "etl_work_identifier_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_IDENT_CURR_REC, String.join("','",ids));
                break;
            case "etl_person_current_v":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_CURR_REC, String.join("','",ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        BcsEtlCoreAccessDLContext.recordsFromCurrent = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^We Get the records from transform BCS Current History (.*)$")
    public static void getRecFromCurrentHistory(String tableName){
        Log.info("We get the records from Current_History bcs Core table...");
        switch (tableName) {
            case "etl_transform_history_accountable_product_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_manifestation_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_manifestation_identifier_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_product_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PRODUCT_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_work_person_role_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_ROLE_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_work_relationship_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELATION_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_work_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_work_identifier_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_IDENT_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_person_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        BcsEtlCoreAccessDLContext.recFromCurrentHist = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^we compare the records of BCS Core current and BCS Current_History (.*)$")
    public void compareCurrentandCurrHist(String targetTableName)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BcsEtlCoreAccessDLContext.recordsFromCurrent.isEmpty()) {
            Log.info("No Data Found in the Current Tables ....");
        } else {
            for (int i = 0; i < BcsEtlCoreAccessDLContext.recordsFromCurrent.size(); i++) {
                switch (targetTableName) {
                    case "etl_transform_history_accountable_product_part":
                        Log.info("comparing current and etl_transform_history_accountable_product_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlTransformHistAccProdPart = {"getuKey", "getsourceRef", "getaccountableProduct", "getaccountableName", "getaccountableParent", "getdqErr"};
                        for (String strTemp : etlTransformHistAccProdPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getsourceRef() +
                                    " " + strTemp + " => Acc_Prod_Curr = " + method.invoke(objectToCompare1) +
                                    " Acc_Prod_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Curr_hist for uKey:" + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_transform_history_manifestation_part":
                        Log.info("comparing current and etl_transform_history_manifestation_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlTransformHistManifPart = {"getuKey", "getsourceRef", "gettitle", "getintereditionFlag", "getfirstPublishedDate", "getbinding", "getmanifestationType", "getstatus"
                                , "getworkId", "getlastPubDate", "getdqErr"};
                        for (String strTemp : etlTransformHistManifPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getsourceRef() +
                                    " " + strTemp + " => Manif_Curr = " + method.invoke(objectToCompare1) +
                                    " Manif_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Curr_hist uKey:" + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_transform_history_manifestation_identifier_part":
                        Log.info("comparing current and etl_transform_history_manifestation_identifier_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlTransformHistManifIdentifierPart = {"getuKey", "getsourceRef", "getidentifier", "getidentifierType","getleadIndicator"};
                        for (String strTemp : etlTransformHistManifIdentifierPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => Manif_Ident_Curr = " + method.invoke(objectToCompare1) +
                                    " Manif_Ident_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Ident_Curr_hist uKey:" + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_transform_history_product_part":
                        Log.info("comparing current and etl_transform_history_product_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlTransformHistProdPart = {"getuKey", "getsourceRef", "getbindingCode", "getname", "getshortTitle", "getlaunchDate",
                                "gettaxCode", "getstatus", "getmanifestationRef", "getworkSource", "getworkType", "getsepratelySaleIndicator", "gettrialAllowedIndicator", "getfWorkSourceRef",
                                "getproductType", "getrevenueModel", "getdqErr"};
                        for (String strTemp : etlTransformHistProdPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => Prod_Curr = " + method.invoke(objectToCompare1) +
                                    " Prod_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Curr_hist uKey:" + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_work_person_role_part":
                        Log.info("comparing current and etl_transform_history_work_person_role_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlTransformHistWorkPersRolePart = {"getuKey", "getworkSourceRef", "getpersonSourceRef", "getroleType", "getsequence", "getdeDuplicator", "getdqErr"};
                        for (String strTemp : etlTransformHistWorkPersRolePart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => PERS_ROLE_Curr = " + method.invoke(objectToCompare1) +
                                    " PERS_ROLE_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Curr_hist for uKey:" + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_transform_history_work_relationship_part":
                        Log.info("comparing current and etl_transform_history_work_relationship_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlTransformHistWorkRelationshiPart = {"getuKey", "getparentRef", "getchildRef", "getrelationTypeRef", "getlastUpdatedDate", "getdqErr"};
                        for (String strTemp : etlTransformHistWorkRelationshiPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => WORK_RELAT_Curr = " + method.invoke(objectToCompare1) +
                                    " WORK_RELAT_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Curr_hist for uKey:" + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_transform_history_work_part":
                        Log.info("comparing current and etl_transform_history_work_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlWorkCurrCol = {"getuKey", "getsourceRef", "gettitle", "getsubTitle", "getvolumeNo", "getcopyRightYear", "geteditionNo", "getpmc", "getworkType", "getstatusCODE", "getimprintCode", "getteopco", "getopco"
                                , "getrespCentre", "getlanguageCode", "getelectroRightIndicator", "getfoaJournalType", "getfSocietyOwnership", "getsubscriptionType"};
                        for (String strTemp : etlWorkCurrCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => WORK_Curr = " + method.invoke(objectToCompare1) +
                                    " WORK_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Curr_hist for uKey:" + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_work_identifier_part":
                        Log.info("comparing current and etl_transform_history_work_identifier_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlTransformHistoryWorkIdentifierPart = {"getuKey", "getsourceRef", "getidentifier", "getidentifierType"};
                        for (String strTemp : etlTransformHistoryWorkIdentifierPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => Work_Ident_Curr = " + method.invoke(objectToCompare1) +
                                    " Work_Ident_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Curr_hist for uKey:" + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_transform_history_person_part":
                        Log.info("comparing current and etl_transform_history_person_part records ...");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));
                        String[] etlPersonCurrCol = {"getuKey", "getsourceRef", "getfirstName", "getfamilyName", "getpeopleHubId", "getemail", "getdqErr"};
                        for (String strTemp : etlPersonCurrCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => Person_Curr = " + method.invoke(objectToCompare1) +
                                    " Person_Curr_Hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Curr_Hist for uKey:" + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
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

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Then("^We Get the records from transform BCS Transform_File (.*)$")
    public static void getRecFromTransformFile(String tableName){
        Log.info("We get the records from Transform_File bcs Core table...");
        switch (tableName) {
            case "etl_accountable_product_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_ACCPROD_REC_TRANS_FILE_DATA, String.join("','",ids));
                break;
            case "etl_manifestation_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_REC_TRANS_FILE_DATA, String.join("','",ids));
                break;
            case "etl_manifestation_identifier_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_MANIF_IDENT_REC_TRANS_FILE_DATA, String.join("','",ids));
                break;
            case "etl_product_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PRODUCT_REC_TRANS_FILE_DATA, String.join("','",ids));
                break;
            case "etl_work_person_role_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_PERS_ROLE_REC_TRANS_FILE_DATA, String.join("','",ids));
                break;
            case "etl_work_relationship_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_RELATION_REC_TRANS_FILE_DATA, String.join("','",ids));
                break;
            case "etl_work_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_REC_TRANS_FILE_DATA, String.join("','",ids));
                break;
            case "etl_work_identifier_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_WORK_IDENT_REC_TRANS_FILE_DATA, String.join("','",ids));
                break;
            case "etl_person_transform_file_history_part":
                sql = String.format(BcsEtlCoreDataChecksSql.GET_PERSON_REC_TRANS_FILE_DATA, String.join("','",ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        BcsEtlCoreAccessDLContext.recFromTransformFile = DBManager.getDBResultAsBeanList(sql, BcsEtlCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^we compare the records of BCS Core current and BCS Transform_File (.*)$")
    public void compareCurrandTransFile(String targetTableName)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BcsEtlCoreAccessDLContext.recordsFromCurrent.isEmpty()) {
            Log.info("No Data Found in the Current Tables ....");
        } else {
            for (int i = 0; i < BcsEtlCoreAccessDLContext.recordsFromCurrent.size(); i++) {
                switch (targetTableName) {
                    case "etl_accountable_product_transform_file_history_part":
                        Log.info("compare current and etl_accountable_product_transform_file_history_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlAccProdTransformFileHistPart = {"getuKey", "getsourceRef", "getaccountableProduct", "getaccountableName", "getaccountableParent", "getdqErr"};
                        for (String strTemp : etlAccProdTransformFileHistPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getsourceRef() +
                                    " " + strTemp + " => Acc_Prod_Curr = " + method.invoke(objectToCompare1) +
                                    " Acc_Prod_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_trans_file",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_manifestation_transform_file_history_part":
                        Log.info("compare current and etl_manifestation_transform_file_history_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlManifTransformFileHistPart = {"getuKey", "getsourceRef", "gettitle", "getintereditionFlag", "getfirstPublishedDate", "getbinding", "getmanifestationType", "getstatus"
                                , "getworkId", "getlastPubDate", "getdqErr"};
                        for (String strTemp : etlManifTransformFileHistPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getsourceRef() +
                                    " " + strTemp + " => Manif_Curr = " + method.invoke(objectToCompare1) +
                                    " Manif_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_trans_file for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_manifestation_identifier_transform_file_history_part":
                        Log.info("compare current and etl_manifestation_identifier_transform_file_history_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlManifIdentifierTransformFileHistPart = {"getuKey", "getsourceRef", "getidentifier", "getidentifierType","getleadIndicator"};
                        for (String strTemp : etlManifIdentifierTransformFileHistPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => Manif_Ident_Curr = " + method.invoke(objectToCompare1) +
                                    " Manif_Ident_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Ident_trans_file for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_product_transform_file_history_part":
                        Log.info("compare current and etl_product_transform_file_history_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlProdTransformFileHistPart = {"getuKey", "getsourceRef", "getbindingCode", "getname", "getshortTitle", "getlaunchDate",
                                "gettaxCode", "getstatus", "getmanifestationRef", "getworkSource", "getworkType", "getsepratelySaleIndicator", "gettrialAllowedIndicator", "getfWorkSourceRef",
                                "getproductType", "getrevenueModel", "getdqErr"};
                        for (String strTemp : etlProdTransformFileHistPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => Prod_Curr = " + method.invoke(objectToCompare1) +
                                    " Prod_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_trans_file for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_work_person_role_transform_file_history_part":
                        Log.info("compare current and etl_work_person_role_transform_file_history_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlWorkPersonRoleTransformFileHistPart = {"getuKey", "getworkSourceRef", "getpersonSourceRef", "getroleType", "getsequence", "getdeDuplicator", "getdqErr"};
                        for (String strTemp : etlWorkPersonRoleTransformFileHistPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => PERS_ROLE_Curr = " + method.invoke(objectToCompare1) +
                                    " PERS_ROLE_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_trans_file for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_work_relationship_transform_file_history_part":
                        Log.info("compare current and etl_work_relationship_transform_file_history_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlWorkRelationTransfFileHistPart = {"getuKey", "getparentRef", "getchildRef", "getrelationTypeRef", "getlastUpdatedDate", "getdqErr"};
                        for (String strTemp : etlWorkRelationTransfFileHistPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => WORK_RELAT_Curr = " + method.invoke(objectToCompare1) +
                                    " WORK_RELAT_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_trans_file for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_work_transform_file_history_part":
                        Log.info("etl_work_transform_file_history_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));

                        String[] etlWorkTransformFileHistPart = {"getuKey", "getsourceRef", "gettitle", "getsubTitle", "getvolumeNo", "getcopyRightYear", "geteditionNo", "getpmc", "getworkType", "getstatusCODE", "getimprintCode", "getteopco", "getopco"
                                , "getrespCentre", "getlanguageCode", "getelectroRightIndicator", "getfoaJournalType", "getfSocietyOwnership", "getsubscriptionType"};
                        for (String strTemp : etlWorkTransformFileHistPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => WORK_Curr = " + method.invoke(objectToCompare1) +
                                    " WORK_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_trans_file for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                        break;
                    case "etl_work_identifier_transform_file_history_part":
                        Log.info("etl_work_identifier_transform_file_history_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));
                        String[] etlWorkIdentifTransformFileHistPart = {"getuKey", "getsourceRef", "getidentifier", "getidentifierType"};
                        for (String strTemp : etlWorkIdentifTransformFileHistPart) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => Work_Ident_Curr = " + method.invoke(objectToCompare1) +
                                    " Work_Ident_Curr_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Curr_file for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_person_transform_file_history_part":
                        Log.info("etl_person_transform_file_history_part Records:");
                        BcsEtlCoreAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey)); //sort primarykey data in the lists
                        BcsEtlCoreAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlCoreDLAccessObject::getuKey));
                        String[] etlPersonCurrTransFileCol = {"getuKey", "getsourceRef", "getfirstName", "getfamilyName", "getpeopleHubId", "getemail", "getdqErr"};
                        for (String strTemp : etlPersonCurrTransFileCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlCoreDLAccessObject objectToCompare1 = BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlCoreDLAccessObject objectToCompare2 = BcsEtlCoreAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("uKey => " + BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey() +
                                    " " + strTemp + " => Person_Curr = " + method.invoke(objectToCompare1) +
                                    " Person_Curr_File = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Curr_File for uKey:"+ BcsEtlCoreAccessDLContext.recordsFromCurrent.get(i).getuKey(),
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
