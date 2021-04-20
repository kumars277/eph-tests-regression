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


public class BCS_ETLExtendedDataChecksSteps {

    public BCSETL_ExtendedAccessDLContext dataQualityBCSContext;
    private static String sql;
    private static List<String> Ids;

    @Given("^Get the (.*) of BCS Extende data from Inbound Tables (.*)$")
    public void getRandomIdsFromInound(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Extended Inbound Tables....");
        List<Map<?, ?>> randomIds;
        switch (tableName) {
            case "etl_availability_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_AVAILABILITY_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_manifestation_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_MANIF_EXT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_page_count_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_PAGE_COUNT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_url_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_URL_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_EXT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_subject_area_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_SUBJ_AREA_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_MANIF_RESTRICT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_product_prices_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_PROD_PRICE_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_person_role_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_PERS_ROLE_KEY_INBOUND, numberOfRecords);
                break;
        }
        randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("EPRID")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }


    @When("^Get the Data from the BCS Extended Inbound Tables (.*)$")
    public void getIngestRecords(String tableName) {
        Log.info("We get the BCS Ingest records...");
        switch (tableName) {
            case "etl_availability_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_AVAILABILITY_REC_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_EXT_REC_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_page_count_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PAGE_COUNT_EXT_REC_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_url_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_URL_REC_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_EXT_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_subject_area_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_SUBJ_AREA_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_RESTRICT_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_product_prices_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PROD_PRICE_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_PERS_ROLE_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recordsFromInboundData = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Data from the BCS Extended Current Tables (.*)$")
    public void getDataforInboundCheck(String tableName) {
        Log.info("We get the records from Current BCS Extended table ...");
        switch (tableName) {
            case "etl_availability_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_AVAILABILITY_REC_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_EXT_REC_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_page_count_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PAGE_COUNT_REC_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_url_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_URL_REC_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_EXT_REC_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_subject_area_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_SUBJ_AREA_REC_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_RESTRICT_REC_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_product_prices_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PROD_PRICE_REC_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_PERS_ROLE_REC_CURR_DATA, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recordsFromCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare data of BCS Inbound and BCS Extended (.*) tables are identical$")
    public void compareIngestandBCSExtCurrent(String tableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recordsFromInboundData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Ingest and BCS Ext current...");
            for (int i = 0; i < dataQualityBCSContext.recordsFromInboundData.size(); i++) {
                switch (tableName) {
                    case "etl_availability_extended_current_v":
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing inbound and etl_availability_extended_current_v records...");
                        String[] all_availability_Col = {"getEPRID", "getmetadeleted", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getAPPLICATION"};
                        for (String strTemp : all_availability_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Availability_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_Curr for EPRID:"+dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_extended_current_v":
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing inbound and etl_manifestation_extended_current_v records...");
                        String[] all_manif_ext_Col = {"getEPRID", "getmetadeleted", "getmanifestation_type", "getuktextbookind", "getustextbookind", "getusdiscountcode", "getusdiscountname", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getemeadiscountcode", "getemeadiscountname", "gettrimsize", "getweight", "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize", "getwarreference"};
                        for (String strTemp : all_manif_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT__Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT__Curr for EPRID:"+dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_page_count_extended_current_v":
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing inbound and etl_page_count_extended_current_v records...");
                        String[] all_pagecount_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount"};
                        for (String strTemp : all_pagecount_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " PageCount_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_Curr for EPRID:"+dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_url_extended_current_v":
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing inbound and etl_url_extended_current_v records...");
                        String[] all_url_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "geturltypecode", "geturltypecode", "getsource", "getname"};
                        for (String strTemp : all_url_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " URL_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_Curr for EPRID:"+dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_extended_current_v":
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing inbound and etl_work_extended_current_v records...");
                        String[] all_work_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoc_long", "gettoc_short", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor"};
                        for (String strTemp : all_work_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " work_ext_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_Curr for EPRID:"+dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_subject_area_extended_current_v":
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing inbound and etl_work_subject_area_extended_current_v records...");
                        String[] all_work_subj_area_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority"};
                        for (String strTemp : all_work_subj_area_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_Curr for EPRID:"+dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_restrictions_extended_current_v":
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing inbound and etl_manifestation_restrictions_extended_current_v records...");
                        String[] all_manif_restrict_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF",
                                "getMODIFIEDON", "getmetadeleted", "getrestrictioncode", "getrestrictionname"};
                        for (String strTemp : all_manif_restrict_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_Curr for EPRID:"+dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_product_prices_extended_current_v":
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing inbound and etl_product_prices_extended_current_v records...");
                        String[] all_prod_price_Col = {"getEPRID", "getUKEY", "getPRODUCTTYPE", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpricecurrency", "getpriceamount",
                                "getpricestartdate", "getpriceenddate", "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity"};
                        for (String strTemp : all_prod_price_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " product_price_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in product_price_Curr for EPRID:"+dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_person_role_extended_current_v":
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing inbound and etl_work_person_role_extended_current_v records...");
                        String[] all_pers_role_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcore_reference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getperson_first_name", "getperson_family_name", "getemail_address", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt", "getsequence", "getgroupnumber", "getmetamodifiedon"};
                        for (String strTemp : all_pers_role_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " pers_role_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_Curr for EPRID:"+dataQualityBCSContext.recordsFromInboundData.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Given("^Get the (.*) of BCS Extended data from Current Tables (.*)$")
    public void getRandomIdsFromCurrent(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Extended Current Tables....");

        switch (tableName) {
            case "etl_availability_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_AVAILABILITY_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_manifestation_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_MANIF_EXT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_page_count_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_PAGE_COUNT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_url_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_URL_EXT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_EXT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_subject_area_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_SUBJ_AREA_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_manifestation_restrictions_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_MANIF_RESTRICT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_product_prices_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_PROD_PRICE_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_person_role_extended_current_v":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_RANDOM_WORK_PERSON_ROLE_KEY_CURRENT, numberOfRecords);
                break;
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("EPRID")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());

    }

    @Then("^We Get the records from transform BCS Ext Current History (.*)$")
    public void getRecFromCurrentHistory(String tableName) {
        Log.info("We get the records from Current_History BCS Extended table...");
        switch (tableName) {
            case "etl_transform_history_extended_availability_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_AVAILABILTYI_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_manifestation_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_EXT_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_page_count_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PAGE_COUNT_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_url_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_URL_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_EXT_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_subject_area_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_SUB_AREA_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_manifestation_restrictions_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_RESTRICT_CURR_HIST_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_product_prices_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PROD_PRICE_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_transform_history_extended_work_person_role_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_PERS_ROLE_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromCurrentHist = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare the records of BCS Extended current and BCS Current_History (.*)$")
    public void compareCurrentandCurrHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recordsFromCurrent.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Current and Current Hist...");
            for (int i = 0; i < dataQualityBCSContext.recordsFromCurrent.size(); i++) {
                switch (targetTableName) {
                    case "etl_transform_history_extended_availability_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_extended_availability_current and etl_transform_history_extended_availability_part records...");
                        String[] all_availability_Col = {"getEPRID", "getmetadeleted", "getPRODUCTTYPE", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getAPPLICATION"};
                        for (String strTemp : all_availability_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => Availability_Curr = " + method.invoke(objectToCompare1) +
                                    " Availability_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_Curr_hist for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_manifestation_extended_current_v and etl_transform_history_extended_manifestation_part records...");
                        String[] all_manif_ext_Col = {"getEPRID", "getmetadeleted", "getmanifestation_type", "getuktextbookind", "getustextbookind", "getusdiscountcode", "getusdiscountname", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getemeadiscountcode", "getemeadiscountname", "gettrimsize", "getweight", "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize", "getwarreference"};
                        for (String strTemp : all_manif_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => Manif_EXT_Curr = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_Curr_hist for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_page_count_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_page_count_extended_current_v and etl_transform_history_extended_page_count_part records...");
                        String[] all_pagecount_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount"};
                        for (String strTemp : all_pagecount_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => PageCount_Curr = " + method.invoke(objectToCompare1) +
                                    " PageCount_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_Curr_hist for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_url_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_url_extended_current_v and etl_transform_history_extended_url_part records...");
                        String[] all_url_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "geturltypecode", "geturltypecode", "getsource", "getname"};
                        for (String strTemp : all_url_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => URL_Curr = " + method.invoke(objectToCompare1) +
                                    " URL_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_Curr_hist for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_work_part and etl_work_extended_current_v records...");
                        String[] all_work_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoc_long", "gettoc_short", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor"};
                        for (String strTemp : all_work_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => work_ext_Curr = " + method.invoke(objectToCompare1) +
                                    " work_ext_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_Curr_hist for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_subject_area_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_work_subject_area_part and etl_work_subject_area_extended_current_v records...");
                        String[] all_work_subj_area_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority"};
                        for (String strTemp : all_work_subj_area_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => work_subj_area_Curr = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_Curr_hist for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_manifestation_restrictions_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_manifestation_restrictions_part and etl_manifestation_restrictions_extended_current_v records...");
                        String[] all_manif_restrict_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getrestrictioncode", "getrestrictionname"};
                        for (String strTemp : all_manif_restrict_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => manifestation_restrictions_Curr = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_Curr_hist for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_product_prices_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_product_prices_part and etl_product_prices_extended_current_v records...");
                        String[] all_prod_price_Col = {"getEPRID", "getUKEY", "getPRODUCTTYPE", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate", "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity"};
                        for (String strTemp : all_prod_price_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => prod_price_Curr = " + method.invoke(objectToCompare1) +
                                    " prod_price_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_Curr_hist for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_transform_history_extended_work_person_role_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_transform_history_extended_work_person_role_part and etl_work_person_role_extended_current_v records...");
                        String[] all_pers_role_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcore_reference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getperson_first_name", "getperson_family_name", "getemail_address", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt", "getsequence", "getgroupnumber", "getmetamodifiedon"};
                        for (String strTemp : all_pers_role_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => pers_role_Curr = " + method.invoke(objectToCompare1) +
                                    " pers_role_Curr_hist = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_Curr_hist for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Then("^We Get the records from transform BCS Ext Transform_File (.*)$")
    public void getRecFromTransformFile(String tableName) {
        Log.info("We get the records from Transform_File BCS Ext table...");
        switch (tableName) {
            case "etl_availability_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_AVAILABILTYI_REC_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_EXT_REC_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_page_count_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PAGE_COUNT_REC_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_url_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_URL_REC_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_work_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_EXT_REC_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_work_subject_area_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_SUB_AREA_REC_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_restrictions_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_MANIF_RESTRICT_REC_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_product_prices_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_PROD_PRICE_REC_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_extended_transform_file_history_part":
                sql = String.format(BCS_ETLExtendedDataChecksSQL.GET_WORK_PERS_ROLE_REC_TRANS_FILE, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromTransformFile = DBManager.getDBResultAsBeanList(sql, BCS_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare the records of BCS Extended current and BCS Transform_File (.*)$")
    public void compareCurrandTransFile(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recordsFromCurrent.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Current and transform file...");
            for (int i = 0; i < dataQualityBCSContext.recordsFromCurrent.size(); i++) {
                switch (targetTableName) {
                    case "etl_availability_extended_transform_file_history_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_extended_availability_current and etl_availability_extended_transform_file_history_part records...");
                        String[] all_availability_Col = {"getEPRID", "getmetadeleted", "getPRODUCTTYPE", "getstatus", "getpubdateactual", "getanzpubstatus", "getdeltaanswercodeuk", "getdeltaanswercodeus", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getAPPLICATION"};
                        for (String strTemp : all_availability_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => Availability_Curr = " + method.invoke(objectToCompare1) +
                                    " Availability_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Availability_trans_file for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_extended_transform_file_history_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_manifestation_extended_current_v and etl_manifestation_extended_transform_file_history_part records...");
                        String[] all_manif_ext_Col = {"getEPRID", "getmetadeleted", "getmanifestation_type", "getuktextbookind", "getustextbookind", "getusdiscountcode", "getusdiscountname", "getUKEY", "getSOURCEREF", "getMODIFIEDON", "getemeadiscountcode", "getemeadiscountname", "gettrimsize", "getweight", "getcommcode", "getjournalprodsitecode", "getjournalissuetrimsize", "getwarreference"};
                        for (String strTemp : all_manif_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => Manif_EXT_Curr = " + method.invoke(objectToCompare1) +
                                    " Manif_EXT_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_EXT_trans_file for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_page_count_extended_transform_file_history_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_page_count_extended_current_v and etl_page_count_extended_transform_file_history_part records...");
                        String[] all_pagecount_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpagecounttypecode", "getpagecounttypename", "getpagecount"};
                        for (String strTemp : all_pagecount_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => PageCount_Curr = " + method.invoke(objectToCompare1) +
                                    " PageCount_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PageCount_trans_file for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_url_extended_transform_file_history_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_url_extended_current_v and etl_url_extended_transform_file_history_part records...");
                        String[] all_url_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "geturltypecode", "geturltypecode", "getsource", "getname"};
                        for (String strTemp : all_url_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => URL_Curr = " + method.invoke(objectToCompare1) +
                                    " URL_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in URL_trans_file for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_extended_transform_file_history_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_work_extended_transform_file_history_part and etl_work_extended_current_v records...");
                        String[] all_work_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcompanygroup", "getimagefileref", "getworkmasterisbn", "gettextreftrade",
                                "getfeatures", "getawards", "gettoc_long", "gettoc_short", "getaudience", "getauthorbyline", "getdescription", "getsbu", "getprofitcentre", "getreview", "getjournalelscomind",
                                "getjournalaimsscope", "getddpeligibind", "getptsjournalind", "getauthorfeedbackind", "getdeltabusinessunit", "getprintername", "getprimarysitesystem", "getprimarysiteacronym", "getprimarysitesupportlevel",
                                "getfulfilmentsystem", "getfulfilmentjournalacronym", "getissueprodtypecode", "getcataloguevolumesqty", "getcatalogueissuesqty", "getcataloguevolumefrom",
                                "getcataloguevolumeto", "getrfissuesqty", "getrftotalpagesqty", "getrffvi", "getrflvi", "getjournalprevioustitle", "getjournalprimaryauthor"};
                        for (String strTemp : all_work_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => work_ext_Curr = " + method.invoke(objectToCompare1) +
                                    " work_ext_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_ext_trans_file for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_subject_area_extended_transform_file_history_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_work_subject_area_extended_transform_file_history_part and etl_work_subject_area_extended_current_v records...");
                        String[] all_work_subj_area_ext_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "gettypecode", "gettypedesc", "getsubjcode", "getsubjdesc",
                                "getpriority"};
                        for (String strTemp : all_work_subj_area_ext_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => work_subj_area_Curr = " + method.invoke(objectToCompare1) +
                                    " work_subj_area_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_subj_area_trans_file for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_restrictions_extended_transform_file_history_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_manifestation_restrictions_extended_transform_file_history_part and etl_manifestation_restrictions_extended_current_v records...");
                        String[] all_manif_restrict_Col = {"getEPRID", "getUKEY", "getmanifestation_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getrestrictioncode", "getrestrictionname"};
                        for (String strTemp : all_manif_restrict_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => manifestation_restrictions_Curr = " + method.invoke(objectToCompare1) +
                                    " manifestation_restrictions_trans_File = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in manifestation_restrictions_trans_File for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_product_prices_extended_transform_file_history_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_product_prices_extended_transform_file_history_part and etl_product_prices_extended_current_v records...");
                        String[] all_prod_price_Col = {"getEPRID", "getUKEY", "getPRODUCTTYPE", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getpricecurrency", "getpriceamount", "getpricestartdate", "getpriceenddate", "getpriceregion", "getpricecategory", "getpricecustomercategory", "getpricepurchasequantity"};
                        for (String strTemp : all_prod_price_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => prod_price_Curr = " + method.invoke(objectToCompare1) +
                                    " prod_price_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in prod_price_trans_file for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_person_role_extended_transform_file_history_part":
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getEPRID));
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLExtendedDLAccessObject::getUKEY));

                        Log.info("comparing etl_work_person_role_extended_transform_file_history_part and etl_work_person_role_extended_current_v records...");
                        String[] all_pers_role_Col = {"getEPRID", "getUKEY", "getwork_type", "getSOURCEREF", "getMODIFIEDON", "getmetadeleted", "getcore_reference", "getworksourceref", "getpersonsourceref", "getsource", "getroletype", "getrolename",
                                "gettitle", "getperson_first_name", "getperson_family_name", "getemail_address", "gethonours", "getaffiliation", "getimageurl", "getfootnotetxt", "getnotestxt", "getsequence", "getgroupnumber", "getmetamodifiedon"};
                        for (String strTemp : all_pers_role_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLExtendedDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                            BCS_ETLExtendedDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EPRID => " + dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID() +
                                    " " + strTemp + " => pers_role_Curr = " + method.invoke(objectToCompare1) +
                                    " pers_role_trans_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in pers_role_Curr_hist for EPRID:"+dataQualityBCSContext.recordsFromCurrent.get(i).getEPRID(),
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
