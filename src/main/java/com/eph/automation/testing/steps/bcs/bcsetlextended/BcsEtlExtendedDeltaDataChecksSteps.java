package com.eph.automation.testing.steps.bcs.bcsetlextended;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSETL_ExtendedAccessDLContext;
import com.eph.automation.testing.models.dao.bcs.BcsEtlExtendedDLAccessObject;
import com.eph.automation.testing.services.db.bcsetlextendedsql.BcsEtlExtendedDataChecksSql;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;
import org.junit.Assert;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;



public class BcsEtlExtendedDeltaDataChecksSteps {

    private static String sql;
    private static List<String> ids;
    private static String noTable= "No Tables Found";

    @Given("^Get the (.*) of BCS Extended data from transform_file Tables (.*)$")
    public static void getIdsFromTransformFile(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for bcs Extended Transform_File Tables....");

        switch (tableName) {
            case "etl_availability_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_AVAILABILITY_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_manifestation_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_MANIF_EXT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_page_count_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_PAGE_COUNT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_url_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_URL_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_EXT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_subject_area_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_SUB_AREA_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_manifestation_restrictions_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_MANIF_RESTRICT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_product_prices_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_PROD_PRICE_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_person_role_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
             default:
                 Log.info(noTable);
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomIds.stream().map(m -> (String) m.get("ukey")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^Get the Data from the Difference of Current and Previous timestamp from transform_file Tables (.*)$")
    public static void getRecFromDiffTransformFile(String tableName) {
        Log.info("We get the records from Diff of Transform File bcs Extended table...");
        switch (tableName) {
            case "etl_availability_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_AVAILABILTIY_REC_DIFF_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_manifestation_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_EXT_REC_DIFF_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_page_count_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PAGE_COUNT_REC_DIFF_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_url_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_URL_DIFF_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_work_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_EXT_REC_DIFF_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_work_subject_area_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_SUBJ_AREA_REC_DIFF_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_manifestation_restrictions_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_RESTRICT_REC_DIFF_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_product_prices_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PROD_PRICE_REC_DIFF_TRANS_FILE, String.join("','",ids));
                break;
            case "etl_work_person_role_extended_transform_file_history_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PERSON_ROLE_REC_DIFF_TRANS_FILE, String.join("','",ids));
                break;
             default:
                 Log.info(noTable);

        }
        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile = DBManager.getDBResultAsBeanList(sql, BcsEtlExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When("^Get the Data from the BCS Extended delta current (.*)$")
    public static void getRecFromDeltaCurrentBCSExt(String tableName) {
        Log.info("We get the records from Delta Current bcs Extended table...");
        switch (tableName) {
            case "etl_delta_current_extended_availability":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_AVAILABILTIY_REC_DELTA_CURRENT, String.join("','",ids));
                break;
            case "etl_delta_current_extended_manifestation":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_EXT_REC_DELTA_CURRENT, String.join("','",ids));
                break;
            case "etl_delta_current_extended_page_count":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PAGE_COUNT_REC_DELTA_CURRENT, String.join("','",ids));
                break;
            case "etl_delta_current_extended_url":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_URL_REC_DELTA_CURRENT, String.join("','",ids));
                break;
            case "etl_delta_current_extended_work":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_EXT_REC_DELTA_CURRENT, String.join("','",ids));
                break;
            case "etl_delta_current_extended_work_subject_area":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_SUB_AREA_REC_DELTA_CURRENT, String.join("','",ids));
                break;
            case "etl_delta_current_extended_manifestation_restrictions":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_RESTRICT_REC_DIFF_DELTA_CURRENT, String.join("','",ids));
                break;
            case "etl_delta_current_extended_product_prices":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PROD_PRICE_REC_DELTA_CURRENT, String.join("','",ids));
                break;
            case "etl_delta_current_extended_work_person_role":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_PERS_ROLE_REC_DELTA_CURR, String.join("','",ids));
                break;
             default:
                 Log.info(noTable);
        }
        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent = DBManager.getDBResultAsBeanList(sql, BcsEtlExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare the records of BCS Extended delta current and BCS diff of Transform_File (.*)$")
    public void compareCurrentandCurrHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between delta Current and diff of transform file...");
            for (int i = 0; i < BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.size(); i++) {
                switch (targetTableName) {
                    case "etl_delta_current_extended_availability":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_delta_current_extended_availability and etl_availability_extended_transform_file_history_part records...");
                        String[] allAvailabilityCol = {"geteprid", "getmetadeleted", "getproducttype", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getukey", "getsourceref", "getmodifiedon", "getapplication", "getdeltamode"};
                        for (String strTemp : allAvailabilityCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid() +
                                    " " + strTemp + " => Availability_trans_file = " + method.invoke(objectToCompare1) +
                                    " Availability_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_delta_curr for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_manifestation":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_delta_current_extended_manifestation and etl_manifestation_extended_transform_file_history_part records...");
                        String[] allManifExtCol = {"geteprid", "getmetadeleted", "getmanifestationtype", "getuktextbookind", "getustextbookind", "getusdiscountcode", "getusdiscountname",
                                "getukey", "getsourceref", "getmodifiedon", "getemeadiscountcode", "getemeadiscountname", "gettrimsize", "getweight",
                                "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize", "getwarreference","getexporttowebind", "getdeltamode"};
                        for (String strTemp : allManifExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid() +
                                    " " + strTemp + " => Manif_EXT_trans_file = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_delta_curr eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_page_count":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_delta_current_extended_page_count and etl_page_count_extended_transform_file_history_part records...");
                        String[] allPagecountCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmodifiedon", "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount", "getdeltamode"};
                        for (String strTemp : allPagecountCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid() +
                                    " " + strTemp + " => PageCount_trans_file = " + method.invoke(objectToCompare1) +
                                    " PageCount_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_delta_curr eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_url":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_delta_current_extended_url and etl_url_extended_transform_file_history_part records...");
                        String[] allUrlCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "geturltypecode", "geturltypecode", "getsource", "getname", "getdeltamode"};
                        for (String strTemp : allUrlCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid() +
                                    " " + strTemp + " => URL_trans_file = " + method.invoke(objectToCompare1) +
                                    " URL_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_delta_curr eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_work_extended_transform_file_history_part and etl_delta_current_extended_work records...");
                        String[] allWorkExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoclong", "gettocshort", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor", "getdeltamode"};
                        for (String strTemp : allWorkExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid() +
                                    " " + strTemp + " => work_ext_tran_file = " + method.invoke(objectToCompare1) +
                                    " work_ext_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_delta_curr eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work_subject_area":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_work_subject_area_extended_transform_file_history_part and etl_delta_current_extended_work_subject_area records...");
                        String[] allWorkSubjAreaExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority", "getdeltamode"};
                        for (String strTemp : allWorkSubjAreaExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid() +
                                    " " + strTemp + " => work_subj_area_tran_file = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_delta_curr eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_manifestation_restrictions":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_manifestation_restrictions_extended_transform_file_history_part and etl_delta_current_extended_manifestation_restrictions records...");
                        String[] allManifRestrictCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmodifiedon", "getmetadeleted", "getrestrictioncode", "getrestrictionname", "getdeltamode"};
                        for (String strTemp : allManifRestrictCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid() +
                                    " " + strTemp + " => manifestation_restrict_tran_file = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_delta_curr eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_product_prices":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_product_prices_extended_transform_file_history_part and etl_delta_current_extended_product_prices records...");
                        String[] allProdPriceCol = {"geteprid", "getukey", "getproducttype", "getsourceref", "getmodifiedon", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate"
                                , "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity", "getdeltamode"};
                        for (String strTemp : allProdPriceCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid() +
                                    " " + strTemp + " => prod_price_trans_file = " + method.invoke(objectToCompare1) +
                                    " prod_price_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_delta_curr eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work_person_role":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_work_person_role_extended_transform_file_history_part and etl_delta_current_extended_work_person_role records...");
                        String[] allPersRoleCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "getcorereference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getpersonfirstname", "getpersonfamilyname", "getemailaddress", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt", "getsequence", "getgroupnumber", "getmetamodifiedon", "getdeltamode"};
                        for (String strTemp : allPersRoleCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid() +
                                    " " + strTemp + " => pers_role_trans_file = " + method.invoke(objectToCompare1) +
                                    " pers_role_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_delta_curr eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    default:
                        Log.info(noTable);
                }
            }
        }
    }


//////////////////////////////////////////////////////////////////////////////

    @Given("^Get the (.*) of BCS Extended data from delta_Current_hist Tables (.*)$")
    public static void getIdsFromDeltaHistBCSExt(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for bcs Extended Delta History Tables....");
        switch (tableName) {
            case "etl_delta_history_extended_availability_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_KEY_AVAILABILITY_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_manifestation_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_KEY_MANIF_EXT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_page_count_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_KEY_PAGE_COUNT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_url_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_KEY_URL_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_work_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_KEY_WORK_EXT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_work_subject_area_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_KEY_WORK_SUB_AREA_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_manifestation_restrictions_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_KEY_MANIF_RESTRICT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_product_prices_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_KEY_PROD_PRICE_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_work_person_role_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_KEY_WORK_PERS_ROLE_DELTA_CURRENT_HIST, numberOfRecords);
                break;
             default:
                 Log.info(noTable);
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Log.info(sql);
        ids = randomIds.stream().map(m -> (String) m.get("ukey")).collect(Collectors.toList());
        Log.info(ids.toString());
    }

    @When("^Get the Data for the BCS Extended Delta_Current_History Tables (.*)$")
    public static void getRecFromDeltaHistBCSExt(String tableName) {
        Log.info("We get the records from Delta Hist of bcs extended table...");
        switch (tableName) {
            case "etl_delta_history_extended_availability_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_AVAILABILITY_REC_DELTA_CURRENT_HIST, String.join("','",ids));
                break;
            case "etl_delta_history_extended_manifestation_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_EXT_REC_DELTA_CURRENT_HIST, String.join("','",ids));
                break;
            case "etl_delta_history_extended_url_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_URL_REC_DELTA_CURRENT_HIST, String.join("','",ids));
                break;
            case "etl_delta_history_extended_work_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_EXT_REC_DELTA_CURRENT_HIST, String.join("','",ids));
                break;
            case "etl_delta_history_extended_work_subject_area_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_SUBJ_AREA_REC_DELTA_CURRENT_HIST, String.join("','",ids));
                break;
            case "etl_delta_history_extended_manifestation_restrictions_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_RESTRICT_REC_DELTA_CURRENT_HIST, String.join("','",ids));
                break;
            case "etl_delta_history_extended_product_prices_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PROD_PRICE_DIFF_DELTA_CURRENT_HIST, String.join("','",ids));
                break;
            case "etl_delta_history_extended_work_person_role_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_PERS_ROLE_REC_DIFF_DELTA_CURRENT_HIST, String.join("','",ids));
                break;
            case "etl_delta_history_extended_page_count_part":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PAGE_COUNT_REC_DELTA_CURR_HIST, String.join("','",ids));
                break;
            default:
                Log.info(noTable);
        }
        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist = DBManager.getDBResultAsBeanList(sql, BcsEtlExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare the records of BCS Extended delta current and delta_Current_history (.*)$")
    public void compareDeltCurrentandDeltaHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between delta Current and delta history...");
            for (int i = 0; i < BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.size(); i++) {
                switch (targetTableName) {
                    case "etl_delta_current_extended_availability":
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));

                        Log.info("comparing etl_delta_current_extended_availability and etl_availability_extended_delta_history_part records...");
                        String[] allAvailabilityCol = {"geteprid", "getmetadeleted", "getproducttype", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getukey", "getsourceref", "getmodifiedon", "getapplication", "getdeltamode"};
                        for (String strTemp : allAvailabilityCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid() +
                                    " " + strTemp + " => Availability_delta_history = " + method.invoke(objectToCompare1) +
                                    " Availability_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_delta_curr for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_manifestation":
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));

                        Log.info("comparing etl_delta_current_extended_manifestation and etl_manifestation_extended_delta_history_part records...");
                        String[] allManifExtCol = {"geteprid", "getmetadeleted", "getmanifestationtype", "getuktextbookind",
                                "getustextbookind", "getusdiscountcode", "getusdiscountname", "getukey", "getsourceref",
                                "getmodifiedon", "getemeadiscountcode", "getemeadiscountname", "gettrimsize", "getweight", "getcommcode",
                                "getjournalprodsitecode", "getjournalissuetrimsize", "getwarreference","getexporttowebind", "getdeltamode"};
                        for (String strTemp : allManifExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid() +
                                    " " + strTemp + " => Manif_EXT_delta_history = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_delta_curr for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_page_count":
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));

                        Log.info("comparing etl_delta_current_extended_page_count and etl_page_count_extended_delta_history_part records...");
                        String[] allPagecountCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmodifiedon", "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount", "getdeltamode"};
                        for (String strTemp : allPagecountCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid() +
                                    " " + strTemp + " => PageCount_delta_history = " + method.invoke(objectToCompare1) +
                                    " PageCount_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_delta_curr for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_url":
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));

                        Log.info("comparing etl_delta_current_extended_url and etl_url_extended_delta_history_part records...");
                        String[] allUrlCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "geturltypecode", "geturltypecode", "getsource", "getname", "getdeltamode"};
                        for (String strTemp : allUrlCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid() +
                                    " " + strTemp + " => URL_delta_history = " + method.invoke(objectToCompare1) +
                                    " URL_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_delta_curr for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work":
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));

                        Log.info("comparing etl_work_extended_delta_history_part and etl_delta_current_extended_work records...");
                        String[] allWorkExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoclong", "gettocshort", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor", "getdeltamode"};
                        for (String strTemp : allWorkExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid() +
                                    " " + strTemp + " => work_ext_delta_hist = " + method.invoke(objectToCompare1) +
                                    " work_ext_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_delta_curr for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work_subject_area":
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));

                        Log.info("comparing etl_work_subject_area_extended_delta_history_part and etl_delta_current_extended_work_subject_area records...");
                        String[] allWorkSubjAreaExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority", "getdeltamode"};
                        for (String strTemp : allWorkSubjAreaExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid() +
                                    " " + strTemp + " => work_subj_area_delta_hist = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_delta_curr for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_manifestation_restrictions":
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));

                        Log.info("comparing etl_manifestation_restrictions_extended_delta_history_part and etl_delta_current_extended_manifestation_restrictions records...");
                        String[] allManifRestrictCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmodifiedon", "getmetadeleted", "getrestrictioncode", "getrestrictionname", "getdeltamode"};
                        for (String strTemp : allManifRestrictCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid() +
                                    " " + strTemp + " => manifestation_restrict_delta_hist = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_delta_curr for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_product_prices":
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));

                        Log.info("comparing etl_product_prices_extended_delta_history_part and etl_delta_current_extended_product_prices records...");
                        String[] allProdPriceCol = {"geteprid", "getukey", "getproducttype", "getsourceref", "getmodifiedon", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate"
                                , "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity", "getdeltamode"};
                        for (String strTemp : allProdPriceCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid() +
                                    " " + strTemp + " => prod_price_delta_history = " + method.invoke(objectToCompare1) +
                                    " prod_price_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_delta_curr for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work_person_role":
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));

                        Log.info("comparing etl_work_person_role_extended_delta_history_part and etl_delta_current_extended_work_person_role records...");
                        String[] allPersRoleCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "getcorereference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getpersonfirstname", "getpersonfamilyname", "getemailaddress", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt", "getsequence", "getgroupnumber", "getmetamodifiedon", "getdeltamode"};
                        for (String strTemp : allPersRoleCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid() +
                                    " " + strTemp + " => pers_role_delta_history = " + method.invoke(objectToCompare1) +
                                    " pers_role_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_delta_curr for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDeltaCurrentHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                     default:
                         Log.info(noTable);
                }
            }
        }
    }


/////////////////////////////////////////////////////////////////////////////////////////////
    @Given("^Get the (.*) from diff of BCS Ext delta_current and current_hist tables (.*)$")
    public static void getIdsFromDiffOfDeltaCurrAndHistBCSExt (String numberOfRecords, String tableName){
    numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
    Log.info("numberOfRecords = " + numberOfRecords);
    Log.info("Get random Ids for bcs Ext Tables from Diff of Delta Current and Current Hist....");
    switch (tableName) {
        case "etl_transform_history_extended_availability_excl_delta":
            sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_AVAILABILITY_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_manifestation_excl_delta":
            sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_MANIF_EXT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_page_count_excl_delta":
            sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_PAGE_COUNT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_url_excl_delta":
            sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_URL_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_work_excl_delta":
            sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_EXT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_work_subject_area_excl_delta":
            sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_SUB_AREA_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
            sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_MANIF_RESTRICT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_product_prices_excl_delta":
            sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_PROD_PRICE_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_work_person_role_excl_delta":
            sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        default:
            Log.info(noTable);
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomIds.stream().map(m -> (String) m.get("eprid")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
        }

    @When ("^Get the BCS Ext records from the diff of delta_current and current_hist tables (.*)$")
    public static void getRecFromDiffDeltaCurrAndCurrHist(String tableName){
        Log.info("We get the records from Diff of Delta Current and Current Hist of bcs Ext table...");
        switch (tableName) {
            case "etl_transform_history_extended_availability_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_AVAILABILITY_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_manifestation_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_EXT_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_page_count_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PAGE_COUNT_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_url_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_URL_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_EXT_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_subject_area_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_SUBJ_AREA_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_RESTRICT_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_product_prices_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PROD_PRICE_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_person_role_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_PERS_ROLE_REC_DIFF_DELTACURR_CURRHIST, String.join("','",ids));
                break;
             default:
                 Log.info(noTable);
        }
        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist = DBManager.getDBResultAsBeanList(sql, BcsEtlExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When ("^Get the BCS Ext records from (.*) exclude table$")
    public static void getRecFromBCSExtDeltaCurrent(String tableName){
        Log.info("We get the records from bcs Exclude delta bcs Extended table...");
        switch (tableName) {
            case "etl_transform_history_extended_availability_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_AVAILABILITY_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_manifestation_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_EXT_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_page_count_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PAGE_COUNT_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_url_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_URL_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_EXT_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_subject_area_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_SUB_AREA_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_RESTRICT_REC_DIFF_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_product_prices_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PROD_PRICE_EXT_REC_EXCL_DELTA, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_person_role_excl_delta":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_PERS_ROLE_REC_EXCL_DELTA, String.join("','",ids));
                break;
            default:
                Log.info(noTable);
        }
        BCSETL_ExtendedAccessDLContext.recFromExclDelta = DBManager.getDBResultAsBeanList(sql, BcsEtlExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @And("^Compare the BCS Ext records of Exclude with diff of delta_current and current_hist tables (.*)$")
    public void compareBCSExtExclude(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records for bcs Exclude tables...");
            for (int i = 0; i < BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.size(); i++) {
                switch (targetTableName) {
                    case "etl_transform_history_extended_availability_excl_delta":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        Log.info("comparing etl_transform_history_extended_manifestation_excl_delta records...");
                        String[] allAvailabilityCol = {"geteprid", "getmetadeleted", "getproducttype", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getukey", "getsourceref", "getmodifiedon", "getapplication"};
                        for (String strTemp : allAvailabilityCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid() +
                                    " " + strTemp + " => Availability_Delta_currHist = " + method.invoke(objectToCompare1) +
                                    " Availability_Excl = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_Excl for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_excl_delta":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        Log.info("comparing etl_transform_history_extended_manifestation_excl_delta records...");
                        String[] allManifExtCol = {"geteprid", "getmetadeleted", "getmanifestationtype", "getuktextbookind", "getustextbookind",
                                "getusdiscountcode", "getusdiscountname", "getukey", "getsourceref", "getmodifiedon", "getemeadiscountcode",
                                "getemeadiscountname", "gettrimsize", "getweight", "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize",
                                "getwarreference","getexporttowebind"};
                        for (String strTemp : allManifExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid() +
                                    " " + strTemp + " => Manif_EXT_Delta_CurrHist = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_Excl = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_Excl for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_page_count_excl_delta":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        Log.info("comparing etl_transform_history_extended_page_count_excl_delta records...");
                        String[] allPagecountCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmodifiedon",
                                "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount"};
                        for (String strTemp : allPagecountCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid() +
                                    " " + strTemp + " => PageCount_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " PageCount_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_exclude for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_url_excl_delta":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        Log.info("comparing etl_transform_history_extended_url_excl_delta records...");
                        String[] allUrlCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted",
                                "geturltypecode", "geturltypecode", "getsource", "getname"};
                        for (String strTemp : allUrlCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid() +
                                    " " + strTemp + " => URL_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " URL_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_exclude for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_excl_delta":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        Log.info("comparing etl_transform_history_extended_work_excl_delta records...");
                        String[] allWorkExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoclong", "gettocshort", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor"};
                        for (String strTemp : allWorkExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid() +
                                    " " + strTemp + " => work_ext_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " work_ext_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_currhist for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_subject_area_excl_delta":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        Log.info("comparing etl_transform_history_extended_work_subject_area_excl_delta records...");
                        String[] allWorkSubjAreaExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority"};
                        for (String strTemp : allWorkSubjAreaExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid() +
                                    " " + strTemp + " => work_subj_area_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_excl = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_delta_curr for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        Log.info("comparing etl_transform_history_extended_manifestation_restrictions_excl_delta records...");
                        String[] allManifRestrictCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmodifiedon", "getmetadeleted",
                                "getrestrictioncode", "getrestrictionname"};
                        for (String strTemp : allManifRestrictCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid() +
                                    " " + strTemp + " => manifestation_restrict_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_exclude for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_product_prices_excl_delta":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        Log.info("comparing etl_transform_history_extended_product_prices_excl_delta records...");
                        String[] allProdPriceCol = {"geteprid", "getukey", "getproducttype", "getsourceref", "getmodifiedon", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate"
                                , "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity"};
                        for (String strTemp : allProdPriceCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid() +
                                    " " + strTemp + " => prod_price_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " prod_price_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_exclude for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_person_role_excl_delta":
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromExclDelta.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        Log.info("comparing etl_transform_history_extended_work_person_role_excl_delta records...");
                        String[] allPersRoleCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "getcorereference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getpersonfirstname", "getpersonfamilyname", "getemailaddress", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt",
                                "getsequence", "getgroupnumber", "getmetamodifiedon"};
                        for (String strTemp : allPersRoleCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("ukey => " + BCSETL_ExtendedAccessDLContext.recFromDiffOfTransformFile.get(i).getukey() +
                                    " " + strTemp + " => pers_role_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " pers_role_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_exclude for eprid:"+BCSETL_ExtendedAccessDLContext.recFromDiffOfDeltaAndCurrHist.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    default:
                        Log.info(noTable);
                }
            }
        }
    }

    @Given("^Get the (.*) from sum of delta_current and exclude_delta for BCS Extended (.*)$")
    public static void  getIdsFromDiffOfDeltaCurrAndExcl(String numberOfRecords, String tableName) {
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for bcs Extended Tables from Diff of Delta Current and Exclude....");

        switch (tableName) {
            case "etl_transform_history_extended_availability_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_AVAILABILITY_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_manifestation_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_MANIF_EXT_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_page_count_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_PAGE_COUNT_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_url_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_URL_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_work_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_EXT_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_work_subject_area_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_SUB_AREA_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_manifestation_restrictions_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_MANIF_RESTRICT_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_product_prices_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_PROD_PRICE_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_work_person_role_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_RANDOM_WORK_PERS_ROLE_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
             default:
                 Log.info(noTable);

        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomIds.stream().map(m -> (String) m.get("eprid")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When ("^Get the records from the sum of delta_current and exclude_delta for BCS Extended (.*)$")
    public static void getRecFromDiffDeltaCurrAndExcl(String tableName){
        Log.info("We get the records from Diff of Delta Current and Excl of bcs Extended table...");
        switch (tableName) {
            case "etl_transform_history_extended_availability_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_AVAILABILITY_REC_SUM_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_extended_manifestation_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_EXT_REC_SUM_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_extended_page_count_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PAGE_COUNT_REC_SUM_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_extended_url_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_URL_SUM_REC_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_EXT_REC_SUM_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_subject_area_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_SUB_AREA_REC_SUM_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_extended_manifestation_restrictions_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_RESTRICT_REC_SUM_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_extended_product_prices_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PROD_PRICE_REC_SUM_DELTACURR_EXCL, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_person_role_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_PERSON_ROLE_REC_SUM_DELTACURR_EXCL, String.join("','",ids));
                break;
            default:
                Log.info(noTable);
        }
        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl = DBManager.getDBResultAsBeanList(sql, BcsEtlExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When ("^Get the records from (.*) BCS Extended latest table$")
    public static void getRecFromLatest(String tableName){
        Log.info("We get the records from Latest bcs Extended table...");
        switch (tableName) {
            case "etl_transform_history_extended_availability_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_AVAILABILITY_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_manifestation_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_EXT_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_page_count_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PAGE_COUNT_IDENT_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_url_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_URL_EXT_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WRK_EXT_LATEST_COUNT, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_subject_area_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_SUB_AREA_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_manifestation_restrictions_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_MANIF_RESTRICT_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_product_prices_latest":
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_PROD_PRICE_REC_LATEST, String.join("','",ids));
                break;
            case "etl_transform_history_extended_work_person_role_latest" :
                sql = String.format(BcsEtlExtendedDataChecksSql.GET_WORK_PERSON_ROLE_REC_LATEST, String.join("','",ids));
                break;
            default:
                Log.info(noTable);
        }
        BCSETL_ExtendedAccessDLContext.recFromLatest = DBManager.getDBResultAsBeanList(sql, BcsEtlExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }
    @And("^Compare the records of Latest with sum of delta_current and Exclude_Delta for BCS Extended (.*)$")
    public void compareBCSExtLatest(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records for bcs Latest tables...");
            for (int i = 0; i < BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.size(); i++) {
                switch (targetTableName) {
                    case "etl_transform_history_extended_availability_latest":
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));
                        Log.info("comparing etl_transform_history_extended_manifestation_latest_delta records...");
                        String[] allAvailabilityCol = {"geteprid", "getmetadeleted", "getproducttype", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getukey", "getsourceref", "getmodifiedon", "getapplication"};
                        for (String strTemp : allAvailabilityCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid() +
                                    " " + strTemp + " => Availability_Delta_Excl = " + method.invoke(objectToCompare1) +
                                    " Availability_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_latest for eprid:"+BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_latest":
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_manifestation_latest_delta records...");
                        String[] allManifExtCol = {"geteprid", "getmetadeleted", "getmanifestationtype", "getuktextbookind", "getustextbookind",
                                "getusdiscountcode", "getusdiscountname", "getukey", "getsourceref", "getmodifiedon", "getemeadiscountcode",
                                "getemeadiscountname", "gettrimsize", "getweight", "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize",
                                "getwarreference","getexporttowebind"};
                        for (String strTemp : allManifExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid() +
                                    " " + strTemp + " => Manif_EXT_Delta_Excl = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_Latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_Latest for eprid:"+BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_page_count_latest":
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_page_count_latest_delta records...");
                        String[] allPagecountCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmodifiedon",
                                "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount"};
                        for (String strTemp : allPagecountCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid() +
                                    " " + strTemp + " => PageCount_delta_excl = " + method.invoke(objectToCompare1) +
                                    " PageCount_Manif_EXT_Latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_latest for eprid:"+BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_url_latest":
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_url_latest_delta records...");
                        String[] allUrlCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted",
                                "geturltypecode", "geturltypecode", "getsource", "getname"};
                        for (String strTemp : allUrlCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid() +
                                    " " + strTemp + " => URL_delta_excl = " + method.invoke(objectToCompare1) +
                                    " URL_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_latest for eprid:"+BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_latest":
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_work_latest_delta records...");
                        String[] allWorkExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoclong", "gettocshort", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor"};
                        for (String strTemp : allWorkExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid() +
                                    " " + strTemp + " => work_ext_delta_excl = " + method.invoke(objectToCompare1) +
                                    " work_ext_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_latest for eprid:"+BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_subject_area_latest":
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_work_subject_area_latest_delta records...");
                        String[] allWorkSubjAreaExtCol = {"geteprid", "getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority"};
                        for (String strTemp : allWorkSubjAreaExtCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid() +
                                    " " + strTemp + " => work_subj_area_delta_excl = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_latest for eprid:"+BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_restrictions_latest":
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_manifestation_restrictions_latest_delta records...");
                        String[] allManifRestrictCol = {"geteprid", "getukey", "getmanifestationtype", "getsourceref", "getmodifiedon", "getmetadeleted",
                                "getrestrictioncode", "getrestrictionname"};
                        for (String strTemp : allManifRestrictCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid() +
                                    " " + strTemp + " => manifestation_restrict_delta_excl = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_latest for eprid:"+BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_product_prices_latest":
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::geteprid));
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_product_prices_latest_delta records...");
                        String[] allProdPriceCol = {"geteprid", "getukey", "getproducttype", "getsourceref", "getmodifiedon", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate"
                                , "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity"};
                        for (String strTemp : allProdPriceCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eprid => " + BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid() +
                                    " " + strTemp + " => prod_price_delta_excl = " + method.invoke(objectToCompare1) +
                                    " prod_price_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_latest for eprid:"+BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_person_role_latest":
                        BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey)); //sort primarykey data in the lists
                        BCSETL_ExtendedAccessDLContext.recFromLatest.sort(Comparator.comparing(BcsEtlExtendedDLAccessObject::getukey));

                        Log.info("comparing etl_transform_history_extended_work_person_role_latest_delta records...");
                        String[] allPersRoleCol = {"getukey", "getworktype", "getsourceref", "getmodifiedon", "getmetadeleted", "getcorereference",
                                "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename", "gettitle", "getpersonfirstname",
                                "getpersonfamilyname", "getemailaddress", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt",
                                "getsequence", "getgroupnumber", "getmetamodifiedon"};
                        for (String strTemp : allPersRoleCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BcsEtlExtendedDLAccessObject objectToCompare1 = BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i);
                            BcsEtlExtendedDLAccessObject objectToCompare2 = BCSETL_ExtendedAccessDLContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("ukey => " + BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).getukey() +
                                    " " + strTemp + " => pers_role_delta_excl = " + method.invoke(objectToCompare1) +
                                    " pers_role_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_latest for eprid:"+BCSETL_ExtendedAccessDLContext.recFromSumOfDeltaAndExcl.get(i).geteprid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                     default:
                         Log.info(noTable);
                }

            }
        }
    }






}





