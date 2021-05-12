package com.eph.automation.testing.web.steps.BCS_ETLExtended;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSETL_ExtendedAccessDLContext;
import com.eph.automation.testing.models.dao.BCS.BCS_ETLExtendedDLAccessObject;
import com.eph.automation.testing.services.db.BCS_ETLExtendedSQL.BCS_ETLExtendedDataChecksSQL;
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



public class BCS_ETLExtended_DeltaDataChecksSteps {

    public BCSETL_ExtendedAccessDLContext dataQualityBCSContext;
    private static String sql;
    private static List<String> Ids;

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^Get the (.*) of BCS Extended data from transform_file Tables (.*)$")
    public void getIdsFromTransformFile(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Extended Transform_File Tables....");

        switch (tableName) {
            case "etl_availability_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_AVAILABILITY_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_manifestation_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_MANIF_EXT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_page_count_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_PAGE_COUNT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_url_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_URL_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_EXT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_subject_area_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_SUB_AREA_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_manifestation_restrictions_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_MANIF_RESTRICT_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_product_prices_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_PROD_PRICE_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
            case "etl_work_person_role_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_TRANS_FILE, numberOfRecords);
                break;
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("UKEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the Data from the Difference of Current and Previous timestamp from transform_file Tables (.*)$")
    public void getRecFromDiffTransformFile(String tableName) {
        Log.info("We get the records from Diff of Transform File BCS Extended table...");
        switch (tableName) {
            case "etl_availability_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_AVAILABILTIY_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_EXT_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_page_count_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PAGE_COUNT_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_url_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_URL_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_work_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_EXT_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_work_subject_area_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_SUBJ_AREA_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_restrictions_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_RESTRICT_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_product_prices_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PROD_PRICE_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PERSON_ROLE_REC_DIFF_TRANS_FILE, Joiner.on("','").join(Ids));
                break;

        }
        dataQualityBCSContext.recFromDiffOfTransformFile = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When("^Get the Data from the BCS Extended delta current (.*)$")
    public void getRecFromDeltaCurrentBCSExt(String tableName) {
        Log.info("We get the records from Delta Current BCS Extended table...");
        switch (tableName) {
            case "etl_delta_current_extended_availability":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_AVAILABILTIY_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_extended_manifestation":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_EXT_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_extended_page_count":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PAGE_COUNT_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_extended_url":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_URL_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_extended_work":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_EXT_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_extended_work_subject_area":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_SUB_AREA_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_extended_manifestation_restrictions":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_RESTRICT_REC_DIFF_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_extended_product_prices":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PROD_PRICE_REC_DELTA_CURRENT, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_current_extended_work_person_role":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_PERS_ROLE_REC_DELTA_CURR, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromDeltaCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare the records of BCS Extended delta current and BCS diff of Transform_File (.*)$")
    public void compareCurrentandCurrHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recFromDiffOfTransformFile.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between delta Current and diff of transform file...");
            for (int i = 0; i < dataQualityBCSContext.recFromDiffOfTransformFile.size(); i++) {
                switch (targetTableName) {
                    case "etl_delta_current_extended_availability":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_delta_current_extended_availability and etl_availability_extended_transform_file_history_part records...");
                        String[] all_availability_Col = {"getEPRID", "getmetadeleted", "getPRODUCTTYPE", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getAPPLICATION", "getDELTA_MODE"};
                        for (String strTemp : all_availability_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID() +
                                    " " + strTemp + " => Availability_trans_file = " + method.invoke(objectToCompare1) +
                                    " Availability_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_delta_curr for EPRID:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_manifestation":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_delta_current_extended_manifestation and etl_manifestation_extended_transform_file_history_part records...");
                        String[] all_manif_ext_Col = {"getEPRID", "getmetadeleted", "getmanifestation_type", "getuktextbookind", "getustextbookind", "getusdiscountcode", "getusdiscountname",
                                "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getemeadiscountcode", "getemeadiscountname", "gettrimsize", "getweight",
                                "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize", "getwarreference","getexporttowebind", "getDELTA_MODE"};
                        for (String strTemp : all_manif_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID() +
                                    " " + strTemp + " => Manif_EXT_trans_file = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_delta_curr EPRID:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_page_count":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_delta_current_extended_page_count and etl_page_count_extended_transform_file_history_part records...");
                        String[] all_pagecount_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount", "getDELTA_MODE"};
                        for (String strTemp : all_pagecount_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID() +
                                    " " + strTemp + " => PageCount_trans_file = " + method.invoke(objectToCompare1) +
                                    " PageCount_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_delta_curr EPRID:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_url":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_delta_current_extended_url and etl_url_extended_transform_file_history_part records...");
                        String[] all_url_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "geturltypecode", "geturltypecode", "getsource", "getname", "getDELTA_MODE"};
                        for (String strTemp : all_url_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID() +
                                    " " + strTemp + " => URL_trans_file = " + method.invoke(objectToCompare1) +
                                    " URL_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_delta_curr EPRID:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_work_extended_transform_file_history_part and etl_delta_current_extended_work records...");
                        String[] all_work_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoc_long", "gettoc_short", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor", "getDELTA_MODE"};
                        for (String strTemp : all_work_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID() +
                                    " " + strTemp + " => work_ext_tran_file = " + method.invoke(objectToCompare1) +
                                    " work_ext_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_delta_curr EPRID:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work_subject_area":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_work_subject_area_extended_transform_file_history_part and etl_delta_current_extended_work_subject_area records...");
                        String[] all_work_subj_area_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority", "getDELTA_MODE"};
                        for (String strTemp : all_work_subj_area_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID() +
                                    " " + strTemp + " => work_subj_area_tran_file = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_delta_curr EPRID:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_manifestation_restrictions":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_manifestation_restrictions_extended_transform_file_history_part and etl_delta_current_extended_manifestation_restrictions records...");
                        String[] all_manif_restrict_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getrestrictioncode", "getrestrictionname", "getDELTA_MODE"};
                        for (String strTemp : all_manif_restrict_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID() +
                                    " " + strTemp + " => manifestation_restrict_tran_file = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_delta_curr EPRID:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_product_prices":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_product_prices_extended_transform_file_history_part and etl_delta_current_extended_product_prices records...");
                        String[] all_prod_price_Col = {"getEPRID", "getUKEY", "getPRODUCTTYPE", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate"
                                , "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity", "getDELTA_MODE"};
                        for (String strTemp : all_prod_price_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID() +
                                    " " + strTemp + " => prod_price_trans_file = " + method.invoke(objectToCompare1) +
                                    " prod_price_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_delta_curr EPRID:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work_person_role":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_work_person_role_extended_transform_file_history_part and etl_delta_current_extended_work_person_role records...");
                        String[] all_pers_role_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcore_reference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getperson_first_name", "getperson_family_name", "getemail_address", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt", "getsequence", "getgroupnumber", "getmetamodifiedon", "getDELTA_MODE"};
                        for (String strTemp : all_pers_role_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfTransformFile.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID() +
                                    " " + strTemp + " => pers_role_trans_file = " + method.invoke(objectToCompare1) +
                                    " pers_role_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_delta_curr EPRID:"+dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }


//////////////////////////////////////////////////////////////////////////////

    @Given("^Get the (.*) of BCS Extended data from delta_Current_hist Tables (.*)$")
    public void getIdsFromDeltaHistBCSExt(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Extended Delta History Tables....");
        switch (tableName) {
            case "etl_delta_history_extended_availability_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_KEY_AVAILABILITY_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_manifestation_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_KEY_MANIF_EXT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_page_count_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_KEY_PAGE_COUNT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_url_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_KEY_URL_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_work_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_KEY_WORK_EXT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_work_subject_area_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_KEY_WORK_SUB_AREA_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_manifestation_restrictions_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_KEY_MANIF_RESTRICT_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_product_prices_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_KEY_PROD_PRICE_DELTA_CURRENT_HIST, numberOfRecords);
                break;
            case "etl_delta_history_extended_work_person_role_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_KEY_WORK_PERS_ROLE_DELTA_CURRENT_HIST, numberOfRecords);
                break;
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Log.info(sql);
        Ids = randomIds.stream().map(m -> (String) m.get("UKEY")).collect(Collectors.toList());
        Log.info(Ids.toString());
    }

    @When("^Get the Data for the BCS Extended Delta_Current_History Tables (.*)$")
    public void getRecFromDeltaHistBCSExt(String tableName) {
        Log.info("We get the records from Delta Hist of BCS extended table...");
        switch (tableName) {
            case "etl_delta_history_extended_availability_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_AVAILABILITY_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_extended_manifestation_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_EXT_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_extended_url_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_URL_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_extended_work_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_EXT_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_extended_work_subject_area_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_SUBJ_AREA_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_extended_manifestation_restrictions_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_RESTRICT_REC_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_extended_product_prices_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PROD_PRICE_DIFF_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_extended_work_person_role_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_PERS_ROLE_REC_DIFF_DELTA_CURRENT_HIST, Joiner.on("','").join(Ids));
                break;
            case "etl_delta_history_extended_page_count_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PAGE_COUNT_REC_DELTA_CURR_HIST, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromDeltaCurrentHist = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare the records of BCS Extended delta current and delta_Current_history (.*)$")
    public void compareDeltCurrentandDeltaHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recFromDeltaCurrentHist.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between delta Current and delta history...");
            for (int i = 0; i < dataQualityBCSContext.recFromDeltaCurrentHist.size(); i++) {
                switch (targetTableName) {
                    case "etl_delta_current_extended_availability":
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));

                        Log.info("comparing etl_delta_current_extended_availability and etl_availability_extended_delta_history_part records...");
                        String[] all_availability_Col = {"getEPRID", "getmetadeleted", "getPRODUCTTYPE", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getAPPLICATION", "getDELTA_MODE"};
                        for (String strTemp : all_availability_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID() +
                                    " " + strTemp + " => Availability_delta_history = " + method.invoke(objectToCompare1) +
                                    " Availability_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_delta_curr for EPRID:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_manifestation":
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));

                        Log.info("comparing etl_delta_current_extended_manifestation and etl_manifestation_extended_delta_history_part records...");
                        String[] all_manif_ext_Col = {"getEPRID", "getmetadeleted", "getmanifestation_type", "getuktextbookind",
                                "getustextbookind", "getusdiscountcode", "getusdiscountname", "getUKEY", "getSOURCEREF",
                                "getMODIFIEDON", "getemeadiscountcode", "getemeadiscountname", "gettrimsize", "getweight", "getcommcode",
                                "getjournalprodsitecode", "getjournalissuetrimsize", "getwarreference","getexporttowebind", "getDELTA_MODE"};
                        for (String strTemp : all_manif_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID() +
                                    " " + strTemp + " => Manif_EXT_delta_history = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_delta_curr for EPRID:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_page_count":
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));

                        Log.info("comparing etl_delta_current_extended_page_count and etl_page_count_extended_delta_history_part records...");
                        String[] all_pagecount_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount", "getDELTA_MODE"};
                        for (String strTemp : all_pagecount_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID() +
                                    " " + strTemp + " => PageCount_delta_history = " + method.invoke(objectToCompare1) +
                                    " PageCount_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_delta_curr for EPRID:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_url":
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));

                        Log.info("comparing etl_delta_current_extended_url and etl_url_extended_delta_history_part records...");
                        String[] all_url_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "geturltypecode", "geturltypecode", "getsource", "getname", "getDELTA_MODE"};
                        for (String strTemp : all_url_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID() +
                                    " " + strTemp + " => URL_delta_history = " + method.invoke(objectToCompare1) +
                                    " URL_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_delta_curr for EPRID:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work":
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));

                        Log.info("comparing etl_work_extended_delta_history_part and etl_delta_current_extended_work records...");
                        String[] all_work_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoc_long", "gettoc_short", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor", "getDELTA_MODE"};
                        for (String strTemp : all_work_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID() +
                                    " " + strTemp + " => work_ext_delta_hist = " + method.invoke(objectToCompare1) +
                                    " work_ext_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_delta_curr for EPRID:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work_subject_area":
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));

                        Log.info("comparing etl_work_subject_area_extended_delta_history_part and etl_delta_current_extended_work_subject_area records...");
                        String[] all_work_subj_area_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority", "getDELTA_MODE"};
                        for (String strTemp : all_work_subj_area_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID() +
                                    " " + strTemp + " => work_subj_area_delta_hist = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_delta_curr for EPRID:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_manifestation_restrictions":
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));

                        Log.info("comparing etl_manifestation_restrictions_extended_delta_history_part and etl_delta_current_extended_manifestation_restrictions records...");
                        String[] all_manif_restrict_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getrestrictioncode", "getrestrictionname", "getDELTA_MODE"};
                        for (String strTemp : all_manif_restrict_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID() +
                                    " " + strTemp + " => manifestation_restrict_delta_hist = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_delta_curr for EPRID:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_product_prices":
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));

                        Log.info("comparing etl_product_prices_extended_delta_history_part and etl_delta_current_extended_product_prices records...");
                        String[] all_prod_price_Col = {"getEPRID", "getUKEY", "getPRODUCTTYPE", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate"
                                , "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity", "getDELTA_MODE"};
                        for (String strTemp : all_prod_price_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID() +
                                    " " + strTemp + " => prod_price_delta_history = " + method.invoke(objectToCompare1) +
                                    " prod_price_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_delta_curr for EPRID:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work_person_role":
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        dataQualityBCSContext.recFromDeltaCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));

                        Log.info("comparing etl_work_person_role_extended_delta_history_part and etl_delta_current_extended_work_person_role records...");
                        String[] all_pers_role_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcore_reference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getperson_first_name", "getperson_family_name", "getemail_address", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt", "getsequence", "getgroupnumber", "getmetamodifiedon", "getDELTA_MODE"};
                        for (String strTemp : all_pers_role_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDeltaCurrentHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromDeltaCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID() +
                                    " " + strTemp + " => pers_role_delta_history = " + method.invoke(objectToCompare1) +
                                    " pers_role_delta_curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_delta_curr for EPRID:"+dataQualityBCSContext.recFromDeltaCurrentHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }


/////////////////////////////////////////////////////////////////////////////////////////////
    @Given("^Get the (.*) from diff of BCS Ext delta_current and current_hist tables (.*)$")
    public void getIdsFromDiffOfDeltaCurrAndHistBCSExt (String numberOfRecords, String tableName){
    numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
    Log.info("numberOfRecords = " + numberOfRecords);
    Log.info("Get random Ids for BCS Ext Tables from Diff of Delta Current and Current Hist....");

    switch (tableName) {
        case "etl_transform_history_extended_availability_excl_delta":
            sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_AVAILABILITY_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_manifestation_excl_delta":
            sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_MANIF_EXT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_page_count_excl_delta":
            sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_PAGE_COUNT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_url_excl_delta":
            sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_URL_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_work_excl_delta":
            sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_EXT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_work_subject_area_excl_delta":
            sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_SUB_AREA_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
            sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_MANIF_RESTRICT_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_product_prices_excl_delta":
            sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_PROD_PRICE_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        case "etl_transform_history_extended_work_person_role_excl_delta":
            sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_DELTACURR_CURRHIST, numberOfRecords);
            break;
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("EPRID")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
        }

    @When ("^Get the BCS Ext records from the diff of delta_current and current_hist tables (.*)$")
    public void getRecFromDiffDeltaCurrAndCurrHist(String tableName){
        Log.info("We get the records from Diff of Delta Current and Current Hist of BCS Ext table...");
        switch (tableName) {
            case "etl_transform_history_extended_availability_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_AVAILABILITY_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_manifestation_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_EXT_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_page_count_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PAGE_COUNT_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_url_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_URL_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_EXT_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_subject_area_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_SUBJ_AREA_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_RESTRICT_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_product_prices_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PROD_PRICE_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_person_role_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_PERS_ROLE_REC_DIFF_DELTACURR_CURRHIST, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When ("^Get the BCS Ext records from (.*) exclude table$")
    public void getRecFromBCSExtDeltaCurrent(String tableName){
        Log.info("We get the records from BCS Exclude delta BCS Extended table...");
        switch (tableName) {
            case "etl_transform_history_extended_availability_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_AVAILABILITY_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_manifestation_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_EXT_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_page_count_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PAGE_COUNT_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_url_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_URL_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_EXT_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_subject_area_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_SUB_AREA_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_RESTRICT_REC_DIFF_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_product_prices_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PROD_PRICE_EXT_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_person_role_excl_delta":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_PERS_ROLE_REC_EXCL_DELTA, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromExclDelta = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @And("^Compare the BCS Ext records of Exclude with diff of delta_current and current_hist tables (.*)$")
    public void compareBCSExtExclude(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records for BCS Exclude tables...");
            for (int i = 0; i < dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.size(); i++) {
                switch (targetTableName) {
                    case "etl_transform_history_extended_availability_excl_delta":
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        Log.info("comparing etl_transform_history_extended_manifestation_excl_delta records...");
                        String[] all_availability_Col = {"getEPRID", "getmetadeleted", "getPRODUCTTYPE", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getAPPLICATION"};
                        for (String strTemp : all_availability_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID() +
                                    " " + strTemp + " => Availability_Delta_currHist = " + method.invoke(objectToCompare1) +
                                    " Availability_Excl = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_Excl for EPRID:"+dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_excl_delta":
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        Log.info("comparing etl_transform_history_extended_manifestation_excl_delta records...");
                        String[] all_manif_ext_Col = {"getEPRID", "getmetadeleted", "getmanifestation_type", "getuktextbookind", "getustextbookind",
                                "getusdiscountcode", "getusdiscountname", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getemeadiscountcode",
                                "getemeadiscountname", "gettrimsize", "getweight", "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize",
                                "getwarreference","getexporttowebind"};
                        for (String strTemp : all_manif_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID() +
                                    " " + strTemp + " => Manif_EXT_Delta_CurrHist = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_Excl = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_Excl for EPRID:"+dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_page_count_excl_delta":
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        Log.info("comparing etl_transform_history_extended_page_count_excl_delta records...");
                        String[] all_pagecount_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON",
                                "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount"};
                        for (String strTemp : all_pagecount_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID() +
                                    " " + strTemp + " => PageCount_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " PageCount_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_exclude for EPRID:"+dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_url_excl_delta":
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        Log.info("comparing etl_transform_history_extended_url_excl_delta records...");
                        String[] all_url_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted",
                                "geturltypecode", "geturltypecode", "getsource", "getname"};
                        for (String strTemp : all_url_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID() +
                                    " " + strTemp + " => URL_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " URL_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_exclude for EPRID:"+dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_excl_delta":
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        Log.info("comparing etl_transform_history_extended_work_excl_delta records...");
                        String[] all_work_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoc_long", "gettoc_short", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor"};
                        for (String strTemp : all_work_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID() +
                                    " " + strTemp + " => work_ext_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " work_ext_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_currhist for EPRID:"+dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_subject_area_excl_delta":
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        Log.info("comparing etl_transform_history_extended_work_subject_area_excl_delta records...");
                        String[] all_work_subj_area_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority"};
                        for (String strTemp : all_work_subj_area_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID() +
                                    " " + strTemp + " => work_subj_area_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_excl = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_delta_curr for EPRID:"+dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_restrictions_excl_delta":
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        Log.info("comparing etl_transform_history_extended_manifestation_restrictions_excl_delta records...");
                        String[] all_manif_restrict_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted",
                                "getrestrictioncode", "getrestrictionname"};
                        for (String strTemp : all_manif_restrict_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID() +
                                    " " + strTemp + " => manifestation_restrict_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_exclude for EPRID:"+dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_product_prices_excl_delta":
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        Log.info("comparing etl_transform_history_extended_product_prices_excl_delta records...");
                        String[] all_prod_price_Col = {"getEPRID", "getUKEY", "getPRODUCTTYPE", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate"
                                , "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity"};
                        for (String strTemp : all_prod_price_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID() +
                                    " " + strTemp + " => prod_price_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " prod_price_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_exclude for EPRID:"+dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_person_role_excl_delta":
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromExclDelta.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        Log.info("comparing etl_transform_history_extended_work_person_role_excl_delta records...");
                        String[] all_pers_role_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcore_reference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getperson_first_name", "getperson_family_name", "getemail_address", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt",
                                "getsequence", "getgroupnumber", "getmetamodifiedon"};
                        for (String strTemp : all_pers_role_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromExclDelta.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + dataQualityBCSContext.recFromDiffOfTransformFile.get(i).getUKEY() +
                                    " " + strTemp + " => pers_role_delta_currhist = " + method.invoke(objectToCompare1) +
                                    " pers_role_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_exclude for EPRID:"+dataQualityBCSContext.recFromDiffOfDeltaAndCurrHist.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }

    @Given("^Get the (.*) from sum of delta_current and exclude_delta for BCS Extended (.*)$")
    public void getIdsFromDiffOfDeltaCurrAndExcl(String numberOfRecords, String tableName) {
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Extended Tables from Diff of Delta Current and Exclude....");

        switch (tableName) {
            case "etl_transform_history_extended_availability_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_AVAILABILITY_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_manifestation_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_MANIF_EXT_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_page_count_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_PAGE_COUNT_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_url_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_URL_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_work_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_EXT_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_work_subject_area_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_SUB_AREA_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_manifestation_restrictions_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_MANIF_RESTRICT_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_product_prices_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_PROD_PRICE_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;
            case "etl_transform_history_extended_work_person_role_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_PERS_ROLE_KEY_SUM_DELTACURR_EXCL, numberOfRecords);
                break;

        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("EPRID")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When ("^Get the records from the sum of delta_current and exclude_delta for BCS Extended (.*)$")
    public void getRecFromDiffDeltaCurrAndExcl(String tableName){
        Log.info("We get the records from Diff of Delta Current and Excl of BCS Extended table...");
        switch (tableName) {
            case "etl_transform_history_extended_availability_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_AVAILABILITY_REC_SUM_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_manifestation_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_EXT_REC_SUM_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_page_count_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PAGE_COUNT_REC_SUM_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_url_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_URL_SUM_REC_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_EXT_REC_SUM_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_subject_area_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_SUB_AREA_REC_SUM_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_manifestation_restrictions_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_RESTRICT_REC_SUM_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_product_prices_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PROD_PRICE_REC_SUM_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_person_role_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_PERSON_ROLE_REC_SUM_DELTACURR_EXCL, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromSumOfDeltaAndExcl = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When ("^Get the records from (.*) BCS Extended latest table$")
    public void getRecFromLatest(String tableName){
        Log.info("We get the records from Latest BCS Extended table...");
        switch (tableName) {
            case "etl_transform_history_extended_availability_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_AVAILABILITY_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_manifestation_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_EXT_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_page_count_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PAGE_COUNT_IDENT_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_url_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_URL_EXT_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WRK_EXT_LATEST_COUNT, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_subject_area_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_SUB_AREA_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_manifestation_restrictions_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_RESTRICT_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_product_prices_latest":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PROD_PRICE_REC_LATEST, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_person_role_latest" :
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_PERSON_ROLE_REC_LATEST, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromLatest = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }
    @And("^Compare the records of Latest with sum of delta_current and Exclude_Delta for BCS Extended (.*)$")
    public void compareBCSExtLatest(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recFromSumOfDeltaAndExcl.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records for BCS Latest tables...");
            for (int i = 0; i < dataQualityBCSContext.recFromSumOfDeltaAndExcl.size(); i++) {
                switch (targetTableName) {
                    case "etl_transform_history_extended_availability_latest":
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));
                        Log.info("comparing etl_transform_history_extended_manifestation_latest_delta records...");
                        String[] all_availability_Col = {"getEPRID", "getmetadeleted", "getPRODUCTTYPE", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getAPPLICATION"};
                        for (String strTemp : all_availability_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID() +
                                    " " + strTemp + " => Availability_Delta_Excl = " + method.invoke(objectToCompare1) +
                                    " Availability_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_latest for EPRID:"+dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_latest":
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_manifestation_latest_delta records...");
                        String[] all_manif_ext_Col = {"getEPRID", "getmetadeleted", "getmanifestation_type", "getuktextbookind", "getustextbookind",
                                "getusdiscountcode", "getusdiscountname", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getemeadiscountcode",
                                "getemeadiscountname", "gettrimsize", "getweight", "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize",
                                "getwarreference","getexporttowebind"};
                        for (String strTemp : all_manif_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID() +
                                    " " + strTemp + " => Manif_EXT_Delta_Excl = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_Latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_Latest for EPRID:"+dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_page_count_latest":
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_page_count_latest_delta records...");
                        String[] all_pagecount_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON",
                                "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount"};
                        for (String strTemp : all_pagecount_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID() +
                                    " " + strTemp + " => PageCount_delta_excl = " + method.invoke(objectToCompare1) +
                                    " PageCount_Manif_EXT_Latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_latest for EPRID:"+dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_url_latest":
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_url_latest_delta records...");
                        String[] all_url_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted",
                                "geturltypecode", "geturltypecode", "getsource", "getname"};
                        for (String strTemp : all_url_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID() +
                                    " " + strTemp + " => URL_delta_excl = " + method.invoke(objectToCompare1) +
                                    " URL_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_latest for EPRID:"+dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_latest":
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_work_latest_delta records...");
                        String[] all_work_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoc_long", "gettoc_short", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor"};
                        for (String strTemp : all_work_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID() +
                                    " " + strTemp + " => work_ext_delta_excl = " + method.invoke(objectToCompare1) +
                                    " work_ext_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_latest for EPRID:"+dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_subject_area_latest":
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_work_subject_area_latest_delta records...");
                        String[] all_work_subj_area_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority"};
                        for (String strTemp : all_work_subj_area_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID() +
                                    " " + strTemp + " => work_subj_area_delta_excl = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_latest for EPRID:"+dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_restrictions_latest":
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_manifestation_restrictions_latest_delta records...");
                        String[] all_manif_restrict_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted",
                                "getrestrictioncode", "getrestrictionname"};
                        for (String strTemp : all_manif_restrict_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID() +
                                    " " + strTemp + " => manifestation_restrict_delta_excl = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_latest for EPRID:"+dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_product_prices_latest":
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_product_prices_latest_delta records...");
                        String[] all_prod_price_Col = {"getEPRID", "getUKEY", "getPRODUCTTYPE", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate"
                                , "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity"};
                        for (String strTemp : all_prod_price_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID() +
                                    " " + strTemp + " => prod_price_delta_excl = " + method.invoke(objectToCompare1) +
                                    " prod_price_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_latest for EPRID:"+dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_person_role_latest":
                       // dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        //dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recFromSumOfDeltaAndExcl.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromLatest.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_work_person_role_latest_delta records...");
                        String[] all_pers_role_Col = {"getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcore_reference",
                                "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename", "gettitle", "getperson_first_name",
                                "getperson_family_name", "getemail_address", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt",
                                "getsequence", "getgroupnumber", "getmetamodifiedon"};
                        for (String strTemp : all_pers_role_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromLatest.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " + dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getUKEY() +
                                    " " + strTemp + " => pers_role_delta_excl = " + method.invoke(objectToCompare1) +
                                    " pers_role_latest = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_latest for EPRID:"+dataQualityBCSContext.recFromSumOfDeltaAndExcl.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }

            }
        }
    }






}





