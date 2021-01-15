package com.eph.automation.testing.web.steps.BCS_ETLExtended;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSETL_ExtendedAccessDLContext;
import com.eph.automation.testing.models.dao.BCS_ETL.BCS_ETLExtendedDLAccessObject;
import com.eph.automation.testing.services.db.BCS_ETLExtendedSQL.BCS_ETLExtendedDataChecksSQL;
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



public class BCS_ETLExtended_DeltaDataChecksSteps {

    public BCSETL_ExtendedAccessDLContext dataQualityBCSContext;
    private static String sql;
    private static List<String> Ids;

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^Get the (.*) of BCS Extended data from transform_file Tables (.*)$")
    public void getIdsFromTransformFile(String numberOfRecords, String tableName) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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

    @When ("^Get the Data from the Difference of Current and Previous timestamp from transform_file Tables (.*)$")
    public void getRecFromDiffTransformFile(String tableName){
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

    @When ("^Compare the records of BCS Extended delta current and BCS diff of Transform_File (.*)$")
    public void getRecFromDeltaCurrent(String tableName){
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
        dataQualityBCSContext.recFromDeltaCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDataChecksSQL.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare the records of BCS Core delta current and BCS diff of Transform_File (.*)$")
    public void compareCurrentandCurrHist(String targetTableName)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recFromDiffOfTransformFile.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between delta Current and diff of transform file...");
            for (int i = 0; i < dataQualityBCSContext.recFromDiffOfTransformFile.size(); i++) {
                switch (targetTableName) {
                    case "etl_delta_current_extended_availability":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        Log.info("comparing etl_delta_current_extended_availability and etl_availability_extended_transform_file_history_part records...");
                        String[] all_availability_Col = {"getEPRID", "getmetadeleted", "getPRODUCTTYPE", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getAPPLICATION","getDELTA_MODE"};
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
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_delta_curr",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_manifestation":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        Log.info("comparing etl_delta_current_extended_manifestation and etl_manifestation_extended_transform_file_history_part records...");
                        String[] all_manif_ext_Col = {"getEPRID", "getmetadeleted", "getmanifestation_type", "getuktextbookind", "getustextbookind", "getusdiscountcode", "getusdiscountname", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getemeadiscountcode", "getemeadiscountname", "gettrimsize", "getweight", "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize", "getwarreference","getDELTA_MODE"};
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
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_delta_curr",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_page_count":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        Log.info("comparing etl_delta_current_extended_page_count and etl_page_count_extended_transform_file_history_part records...");
                        String[] all_pagecount_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount","getDELTA_MODE"};
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
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_delta_curr",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_url":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        Log.info("comparing etl_delta_current_extended_url and etl_url_extended_transform_file_history_part records...");
                        String[] all_url_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "geturltypecode", "geturltypecode", "getsource", "getname","getDELTA_MODE"};
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
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_delta_curr",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        Log.info("comparing etl_work_extended_transform_file_history_part and etl_delta_current_extended_work records...");
                        String[] all_work_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoc_long", "gettoc_short", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor","getDELTA_MODE"};
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
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_delta_curr",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work_subject_area":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        Log.info("comparing etl_work_subject_area_extended_transform_file_history_part and etl_delta_current_extended_work_subject_area records...");
                        String[] all_work_subj_area_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority","getDELTA_MODE"};
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
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_delta_curr",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_manifestation_restrictions":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        Log.info("comparing etl_manifestation_restrictions_extended_transform_file_history_part and etl_delta_current_extended_manifestation_restrictions records...");
                        String[] all_manif_restrict_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getrestrictioncode", "getrestrictionname","getDELTA_MODE"};
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
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_delta_curr",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_product_prices":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        Log.info("comparing etl_product_prices_extended_transform_file_history_part and etl_delta_current_extended_product_prices records...");
                        String[] all_prod_price_Col = {"getEPRID", "getUKEY", "getproduct_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate"
                                , "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity","getDELTA_MODE"};
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
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_delta_curr",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_delta_current_extended_work_person_role":
                        dataQualityBCSContext.recFromDiffOfTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromDeltaCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        Log.info("comparing etl_work_person_role_extended_transform_file_history_part and etl_delta_current_extended_work_person_role records...");
                        String[] all_pers_role_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcore_reference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getperson_first_name", "getperson_family_name", "getemail_address", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt", "getsequence", "getgroupnumber", "getmetamodifiedon","getDELTA_MODE"};
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
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_delta_curr",
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
/*
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Delta_Curr",
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr",
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Delta_Curr",
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Delta_Curr",
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Delta_Curr",
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Delta_Hist",
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Delta_Curr",
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Delta_Curr_hist",
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Delta_Curr",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
            }
        }
    } */

/////////////////////////////////////////////////////////////////////////////////////////////

}




