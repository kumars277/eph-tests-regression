package com.eph.automation.testing.steps.bcs.bcsetlextended;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSETL_ExtendedAccessDLContext;
import com.eph.automation.testing.models.dao.bcs.BcsEtlExtendedDLAccessObject;
import com.eph.automation.testing.services.db.bcsetlextendedsql.BcsEtlExtendedDataChecksSql;
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


public class BcsEtlExtendedDataChecksSteps {


    private static String sql;
    private static List<String> ids;

    @Given("^Get the (.*) of BCS Extended data from Inbound Tables (.*)$")
    public static void getRandomIdsFromInound(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for bcs Extended Inbound Tables....");
        List<Map<?, ?>> randomIds;
        switch (tableName) {
            case "etl_availability_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_AVAILABILITY_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_manifestation_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_MANIF_EXT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_page_count_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_PAGE_COUNT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_url_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_URL_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_EXT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_subject_area_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_SUBJ_AREA_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_MANIF_RESTRICT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_product_prices_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_PROD_PRICE_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_person_role_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_PERS_ROLE_KEY_INBOUND, numberOfRecords);
                break;
            default:
                Log.info("NO such tables found");
        }
        randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomIds.stream().map(m -> (String) m.get("eprid")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }


    @When("^Get the Data from the BCS Extended Inbound Tables (.*)$")
    public static void getIngestRecords(String tableName) {
        Log.info("We get the bcs Ingest records...");
        switch (tableName) {
            case "etl_availability_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_AVAILABILITY_REC_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_manifestation_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_EXT_REC_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_page_count_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PAGE_COUNT_EXT_REC_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_url_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_URL_REC_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_work_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_EXT_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_work_subject_area_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_SUBJ_AREA_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_RESTRICT_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_product_prices_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PROD_PRICE_INBOUND_DATA, String.join("','",ids));
                break;
            case "etl_work_person_role_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_PERS_ROLE_INBOUND_DATA, String.join("','",ids));
                break;
            default:
                Log.info("No tables found");
        }

        BCSETL_ExtendedAccessDLContext.recordsFromInboundData = DBManager.getDBResultAsBeanList(sql, BcsEtlExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Data from the BCS Extended Current Tables (.*)$")
    public static void getDataforInboundCheck(String tableName) {
        Log.info("We get the records from Current bcs Extended table ...");
        switch (tableName) {
            case "etl_availability_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_AVAILABILITY_REC_CURR_DATA, String.join("','",ids));
                break;
            case "etl_manifestation_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_EXT_REC_CURR_DATA, String.join("','",ids));
                break;
            case "etl_page_count_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PAGE_COUNT_REC_CURR_DATA, String.join("','",ids));
                break;
            case "etl_url_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_URL_REC_CURR_DATA, String.join("','",ids));
                break;
            case "etl_work_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_EXT_REC_CURR_DATA, String.join("','",ids));
                break;
            case "etl_work_subject_area_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_SUBJ_AREA_REC_CURR_DATA, String.join("','",ids));
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_RESTRICT_REC_CURR_DATA, String.join("','",ids));
                break;
            case "etl_product_prices_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PROD_PRICE_REC_CURR_DATA, String.join("','",ids));
                break;
            case "etl_work_person_role_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_PERS_ROLE_REC_CURR_DATA, String.join("','",ids));
                break;
            default:
                Log.info("No tables found");
        }
        BCSETL_ExtendedAccessDLContext.recordsFromCurrent = DBManager.getDBResultAsBeanList(sql, BcsEtlExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare data of BCS Inbound and BCS Extended (.*) tables are identical$")
    public void compareIngestandBCSExtCurrent(String tableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BCSETL_ExtendedAccessDLContext.recordsFromInboundData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Ingest and bcs Ext current...");
            for (int i = 0; i < BCSETL_ExtendedAccessDLContext.recordsFromInboundData.size(); i++) {
                switch (tableName) {
                    case "etl_availability_extended_current_v":
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing inbound and etl_availability_extended_current_v records...");
                        String[] allAvailabilityCol = {"geteprid", "getmetadeleted", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getukey", "getsourceref", "getmodifiedon", "getapplication"};
                        for (String strTemp : allAvailabilityCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Availability_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_Curr for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_extended_current_v":
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing inbound and etl_manifestation_extended_current_v records...");
                        String[] allManifExtCol = {"geteprid", "getmetadeleted", "getmanifestationtype",
                                "getuktextbookind", "getustextbookind", "getusdiscountcode", "getusdiscountname", "getukey",
                                "getsourceref", "getmodifiedon", "getemeadiscountcode", "getemeadiscountname",
                                "gettrimsize", "getweight", "getcommcode", "getjournalprodsitecode",
                                "getjournalissuetrimsize", "getwarreference","getexporttowebind"};
                        for (String strTemp : allManifExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT__Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_Curr for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_page_count_extended_current_v":
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing inbound and etl_page_count_extended_current_v records...");
                        String[] allPagecountCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmodifiedon", "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount"};
                        for (String strTemp : allPagecountCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " PageCount_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_Curr for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_url_extended_current_v":
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing inbound and etl_url_extended_current_v records...");
                        String[] allUrlCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "geturltypecode", "geturltypecode", "getsource", "getname"};
                        for (String strTemp : allUrlCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " URL_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_Curr for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_extended_current_v":
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing inbound and etl_work_extended_current_v records...");
                        String[] allWorkExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoclong", "gettocshort", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor"};
                        for (String strTemp : allWorkExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " work_ext_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_Curr for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_subject_area_extended_current_v":
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing inbound and etl_work_subject_area_extended_current_v records...");
                        String[] allWorkSubjAreaExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority"};
                        for (String strTemp : allWorkSubjAreaExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_Curr for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_restrictions_extended_current_v":
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing inbound and etl_manifestation_restrictions_extended_current_v records...");
                        String[] allManifRestrictCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref",
                                "getmodifiedon", "getmetadeleted", "getrestrictioncode", "getrestrictionname"};
                        for (String strTemp : allManifRestrictCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_Curr for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_product_prices_extended_current_v":
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing inbound and etl_product_prices_extended_current_v records...");
                        String[] allProdPriceCol = {"geteprid", "getukey", "getproducttype", "getsourceref", "getmodifiedon", "getmetadeleted", "getpricecurrency", "getpriceamount",
                                "getpricestartdate", "getpriceenddate", "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity"};
                        for (String strTemp : allProdPriceCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " product_price_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in product_price_Curr for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_person_role_extended_current_v":
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));


                        Log.info("comparing inbound and etl_work_person_role_extended_current_v records...");
                        String[] allPersRoleCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "getcorereference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getpersonfirstname", "getpersonfamilyname", "getemailaddress", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt", "getsequence", "getgroupnumber", "getmetamodifiedon"};
                        for (String strTemp : allPersRoleCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " pers_role_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_Curr for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromInboundData.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    default:
                        Log.info("Tabels not found");
                }
            }
        }
    }
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Given("^Get the (.*) of BCS Extended data from Current Tables (.*)$")
    public static void getRandomIdsFromCurrent(String numberOfRecords, String tableName) {
      numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for bcs Extended Current Tables....");

        switch (tableName) {
            case "etl_availability_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_AVAILABILITY_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_manifestation_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_MANIF_EXT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_page_count_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_PAGE_COUNT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_url_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_URL_EXT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_EXT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_subject_area_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_SUBJ_AREA_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_MANIF_RESTRICT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_product_prices_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_PROD_PRICE_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_person_role_extended_current_v":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_PERSON_ROLE_KEY_CURRENT, numberOfRecords);
                break;
            default:
                Log.info("no tables found");
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomIds.stream().map(m -> (String) m.get("eprid")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());

    }

    @Then("^We Get the records from transform BCS Ext Current History (.*)$")
    public static void getRecFromCurrentHistory(String tableName) {
        Log.info("We get the records from Current_History bcs Extended table...");
        switch (tableName) {
            case "etl_transform_history_extended_availability_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_AVAILABILTYI_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_manifestation_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_EXT_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_page_count_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PAGE_COUNT_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_url_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_URL_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_EXT_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_subject_area_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_SUB_AREA_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_manifestation_restrictions_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_RESTRICT_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_product_prices_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PROD_PRICE_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_person_role_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_PERS_ROLE_REC_CURR_HIST_DATA, String.join("','",ids));
                break;
             default:
                 Log.info("No tables found");
        }
        BCSETL_ExtendedAccessDLContext.recFromCurrentHist = DBManager.getDBResultAsBeanList(sql, BcsEtlExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare the records of BCS Extended current and BCS Current_History (.*)$")
    public void compareCurrentandCurrHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BCSETL_ExtendedAccessDLContext.recordsFromCurrent.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Current and Current Hist...");
            for (int i = 0; i < BCSETL_ExtendedAccessDLContext.recordsFromCurrent.size(); i++) {
                switch (targetTableName) {
                    case "etl_transform_history_extended_availability_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_extended_availability_current and etl_transform_history_extended_availability_part records...");
                        String[] allAvailabilityCol = {"geteprid", "getmetadeleted", "getproducttype", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getukey", "getsourceref", "getapplication"};
                        for (String strTemp : allAvailabilityCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => Availability_Curr = " + method.invoke(objectToCompare1) +
                                    " Availability_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_Curr_hist for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_manifestation_extended_current_v and etl_transform_history_extended_manifestation_part records...");
                        String[] allManifExtCol = {"geteprid", "getmetadeleted", "getmanifestationtype",
                                "getuktextbookind", "getustextbookind", "getusdiscountcode", "getusdiscountname", "getukey",
                                "getsourceref", "getemeadiscountcode", "getemeadiscountname", "gettrimsize",
                                "getweight", "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize", "getwarreference","getexporttowebind"};
                        for (String strTemp : allManifExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => Manif_EXT_Curr = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_Curr_hist for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_page_count_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_page_count_extended_current_v and etl_transform_history_extended_page_count_part records...");
                        String[] allPagecountCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount"};
                        for (String strTemp : allPagecountCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => PageCount_Curr = " + method.invoke(objectToCompare1) +
                                    " PageCount_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_Curr_hist for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_url_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_url_extended_current_v and etl_transform_history_extended_url_part records...");
                        String[] allUrlCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmetadeleted", "geturltypecode", "geturltypecode", "getsource", "getname"};
                        for (String strTemp : allUrlCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => URL_Curr = " + method.invoke(objectToCompare1) +
                                    " URL_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_Curr_hist for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_work_part and etl_work_extended_current_v records...");
                        String[] allWorkExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoclong", "gettocshort", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor"};
                        for (String strTemp : allWorkExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => work_ext_Curr = " + method.invoke(objectToCompare1) +
                                    " work_ext_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_Curr_hist for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_subject_area_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_work_subject_area_part and etl_work_subject_area_extended_current_v records...");
                        String[] allWorkSubjAreaExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority"};
                        for (String strTemp : allWorkSubjAreaExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => work_subj_area_Curr = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_Curr_hist for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_restrictions_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_manifestation_restrictions_part and etl_manifestation_restrictions_extended_current_v records...");
                        String[] allManifRestrictCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmetadeleted", "getrestrictioncode", "getrestrictionname"};
                        for (String strTemp : allManifRestrictCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => manifestation_restrictions_Curr = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_Curr_hist for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_product_prices_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_product_prices_part and etl_product_prices_extended_current_v records...");
                        String[] allProdPriceCol = {"geteprid", "getukey", "getproducttype", "getsourceref", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate", "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity"};
                        for (String strTemp : allProdPriceCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => prod_price_Curr = " + method.invoke(objectToCompare1) +
                                    " prod_price_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_Curr_hist for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_person_role_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_work_person_role_part and etl_work_person_role_extended_current_v records...");
                        String[] allPersRoleCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmetadeleted", "getcorereference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getpersonfirstname", "getpersonfamilyname", "getemailaddress", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt", "getsequence", "getgroupnumber"};
                        for (String strTemp : allPersRoleCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => pers_role_Curr = " + method.invoke(objectToCompare1) +
                                    " pers_role_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_Curr_hist for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    default:
                        Log.info("No tables Found");
                }
            }
        }
    }

    @Then("^We Get the records from transform BCS Ext Transform_File (.*)$")
    public static void getRecFromTransformFile(String tableName) {
        Log.info("We get the records from Transform_File bcs Ext table...");
        switch (tableName) {
            case "etl_availability_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_AVAILABILTYI_REC_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_manifestation_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_EXT_REC_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_page_count_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PAGE_COUNT_REC_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_url_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_URL_REC_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_work_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_EXT_REC_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_work_subject_area_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_SUB_AREA_REC_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_manifestation_restrictions_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_RESTRICT_REC_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_product_prices_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PROD_PRICE_REC_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_work_person_role_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_PERS_ROLE_REC_TRANS_FILE, String.join("','",ids));
                break;
            default:
                Log.info("No Tables found");
        }
        BCSETL_ExtendedAccessDLContext.recFromTransformFile = DBManager.getDBResultAsBeanList(sql, BcsEtlExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare the records of BCS Extended current and BCS Transform_File (.*)$")
    public void compareCurrandTransFile(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BCSETL_ExtendedAccessDLContext.recordsFromCurrent.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Current and transform file...");
            for (int i = 0; i < BCSETL_ExtendedAccessDLContext.recordsFromCurrent.size(); i++) {
                switch (targetTableName) {
                    case "etl_availability_extended_transform_file_history_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_extended_availability_current and etl_availability_extended_transform_file_history_part records...");
                        String[] allAvailCol = {"geteprid", "getmetadeleted", "getproducttype", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getukey", "getsourceref", "getmodifiedon", "getapplication"};
                        for (String strTemp : allAvailCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => Availability_Curr = " + method.invoke(objectToCompare1) +
                                    " Availability_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_trans_file for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_extended_transform_file_history_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_manifestation_extended_current_v and etl_manifestation_extended_transform_file_history_part records...");
                        String[] allManifExtCol = {"geteprid", "getmetadeleted", "getmanifestationtype",
                                "getuktextbookind", "getustextbookind", "getusdiscountcode", "getusdiscountname",
                                "getukey", "getsourceref", "getmodifiedon", "getemeadiscountcode", "getemeadiscountname",
                                "gettrimsize", "getweight", "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize", "getwarreference","getexporttowebind"};
                        for (String strTemp : allManifExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => Manif_EXT_Curr = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_trans_file for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_page_count_extended_transform_file_history_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_page_count_extended_current_v and etl_page_count_extended_transform_file_history_part records...");
                        String[] allPageCountCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmodifiedon", "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount"};
                        for (String strTemp : allPageCountCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => PageCount_Curr = " + method.invoke(objectToCompare1) +
                                    " PageCount_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_trans_file for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_url_extended_transform_file_history_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_url_extended_current_v and etl_url_extended_transform_file_history_part records...");
                        String[] allUrlCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "geturltypecode", "geturltypecode", "getsource", "getname"};
                        for (String strTemp : allUrlCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => URL_Curr = " + method.invoke(objectToCompare1) +
                                    " URL_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_trans_file for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_extended_transform_file_history_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_work_extended_transform_file_history_part and etl_work_extended_current_v records...");
                        String[] allWorkExtended = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoclong", "gettocshort", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor"};
                        for (String strTemp : allWorkExtended) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => work_ext_Curr = " + method.invoke(objectToCompare1) +
                                    " work_ext_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_trans_file for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_subject_area_extended_transform_file_history_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_work_subject_area_extended_transform_file_history_part and etl_work_subject_area_extended_current_v records...");
                        String[] allWorkSubjAreaExt = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority"};
                        for (String strTemp : allWorkSubjAreaExt) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => work_subj_area_Curr = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_trans_file for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_restrictions_extended_transform_file_history_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_manifestation_restrictions_extended_transform_file_history_part and etl_manifestation_restrictions_extended_current_v records...");
                        String[] allManifRestrictCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmodifiedon", "getmetadeleted", "getrestrictioncode", "getrestrictionname"};
                        for (String strTemp : allManifRestrictCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => manifestation_restrictions_Curr = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_trans_File = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_trans_File for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_product_prices_extended_transform_file_history_part":
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_product_prices_extended_transform_file_history_part and etl_product_prices_extended_current_v records...");
                        String[] allProdPriceCol = {"geteprid", "getukey", "getproducttype", "getsourceref", "getmodifiedon", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate", "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity"};
                        for (String strTemp : allProdPriceCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => prod_price_Curr = " + method.invoke(objectToCompare1) +
                                    " prod_price_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_trans_file for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_person_role_extended_transform_file_history_part":
                          Log.info("comparing etl_work_person_role_extended_transform_file_history_part and etl_work_person_role_extended_current_v records...");
                        String[] allPersRoleCol = {"geteprid", "getukey", "getworktype", "getsourceref", /*"getmodifiedon",*/ "getmetadeleted", "getcorereference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getpersonfirstname", "getpersonfamilyname", "getemailaddress", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt", "getsequence", "getgroupnumber"/*, "getmetamodifiedon"*/};
                        for (String strTemp : allPersRoleCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid() +
                                    " " + strTemp + " => pers_role_Curr = " + method.invoke(objectToCompare1) +
                                    " pers_role_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_Curr_hist for eprid:"+BCSETL_ExtendedAccessDLContext.recordsFromCurrent.get(i).geteprid(),
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
}
