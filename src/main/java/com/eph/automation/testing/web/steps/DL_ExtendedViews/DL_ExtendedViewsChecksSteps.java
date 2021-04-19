package com.eph.automation.testing.web.steps.DL_ExtendedViews;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
//import com.eph.automation.testing.models.contexts.DL_CoreViewsAccessContext;
//import com.eph.automation.testing.models.dao.DL_CoreViews.DL_CoreViewsAccessObject;
import com.eph.automation.testing.models.contexts.DL_ExtViewsAccessContext;
import com.eph.automation.testing.models.dao.DL_ExtendedViews.DL_ExtendedViewsAccessObject;
import com.eph.automation.testing.services.db.DL_ExtViewSQL.DL_ExtendedViewChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DL_ExtendedViewsChecksSteps {
    private static String DLExtSQLViewCount;
    private static int DLExtViewCount;
    private static String DLAllExtSQLViewCount;
    private static int DLAllExtCount;

    public DL_ExtViewsAccessContext dataQualityDLExtViewContext;
    private static String sql;
    private static List<String> Ids;


    @Given("^Get the total count of DL Extended views (.*)$")
    public void getDLExtViewCount (String tableName) {
        switch (tableName){
            case "product_extended_availability":
                Log.info("Getting DL product_extended_availability Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_PROD_EXT_AVAILABILITY_COUNT;
                break;
            case "product_extended_pricing":
                Log.info("Getting DL product_extended_pricing Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_PROD_EXT_PRICING_COUNT;
                break;
            case "manifestation_extended_restriction":
                Log.info("Getting DL manifestation_extended_restriction Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_MANIF_EXT_RESTRICT_COUNT;
                break;
            case "manifestation_extended_page_count":
                Log.info("Getting DL manifestation_extended_page_count Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_MANIF_EXT_PAGE_COUNT;
                break;
            case "manifestation_extended":
                Log.info("Getting DL manifestation_extended Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_MANIF_EXT_COUNT;
                break;
            case "work_extended":
                Log.info("Getting DL work_extended Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_WORK_EXT_COUNT;
                break;
            case "work_extended_editorial_board":
                Log.info("Getting DL work_extended_editorial_board Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_WORK_EDIT_BOARD_EXT_COUNT;
                break;
            case "work_extended_metric":
                Log.info("Getting Dl work_extended_metric Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_WORK_EXT_METRIC_COUNT;
                break;
            case "work_extended_person_role":
                Log.info("Getting DL work_extended_person_role Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_WORK_EXT_PERS_ROLE_COUNT;
                break;
            case "work_extended_relationship_sibling":
                Log.info("Getting DL work_extended_relationship_sibling Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_WORK_EXT_RELATION_SIBLING_COUNT;
                break;
            case "work_extended_subject_area":
                Log.info("Getting DL work_extended_subject_area Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_WORK_EXT_SUBJ_AREA_COUNT;
                break;
            case "work_extended_url":
                Log.info("Getting DL work_extended_url Count...");
                DLExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_WORK_EXT_URL_COUNT;
                break;

        }
        Log.info(DLExtSQLViewCount);
        List<Map<String, Object>> DLExtViewTableCount = DBManager.getDBResultMap(DLExtSQLViewCount, Constants.AWS_URL);
        DLExtViewCount = ((Long) DLExtViewTableCount.get(0).get("Target_Count")).intValue();
    }

    @Given("^We know the total count of All Extended views (.*)$")
    public void getCountAllExtTables(String tableName){
        switch (tableName){
            case "product_extended_availability":
                Log.info("Getting DL All product_extended_availability Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_PROD_EXT_AVAILABILITY_COUNT;
                break;
            case "product_extended_pricing":
                Log.info("Getting DL All product_extended_pricing Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_PROD_EXT_PRICING_COUNT;
                break;
            case "manifestation_extended_restriction":
                Log.info("Getting DL All manifestation_extended_restriction Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_MANIF_EXT_RESTRICT_COUNT;
                break;
            case "manifestation_extended_page_count":
                Log.info("Getting DL All All manifestation_extended_page_count Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_MANIF_EXT_PAGE_COUNT;
                break;
            case "manifestation_extended":
                Log.info("Getting DL All manifestation_extended Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_MANIF_EXT_COUNT;
                break;
            case "work_extended":
                Log.info("Getting DL All work_extended Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_COUNT;
                break;
            case "work_extended_editorial_board":
                Log.info("Getting DL All work_extended_editorial_board Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WRK_EDIT_BOARD_VIEW_COUNT;
                break;
            case "work_extended_metric":
                Log.info("Getting Dl All work_extended_metric Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_METRIC_COUNT;
                break;
            case "work_extended_person_role":
                Log.info("Getting DL All work_extended_person_role Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_PERS_ROLE_COUNT;
                break;
            case "work_extended_relationship_sibling":
                Log.info("Getting DL All work_extended_relationship_sibling Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_RELATION_SIBLING_COUNT;
                break;
            case "work_extended_subject_area":
                Log.info("Getting DL All work_extended_subject_area Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_SUBJ_AREA_COUNT;
                break;
            case "work_extended_url":
                Log.info("Getting DL All work_extended_url Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_URL_COUNT;
                break;


        }
        Log.info(DLAllExtSQLViewCount);
        List<Map<String, Object>> DLAllTableCount = DBManager.getDBResultMap(DLAllExtSQLViewCount, Constants.AWS_URL);
        DLAllExtCount = ((Long) DLAllTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of All Ext Views with (.*) views are identical$")
    public void compareAllExtAndExtCounts(String tableName){
        Log.info("The count for view "+tableName+" => " + DLExtViewCount + " and in All Ext  => " + DLAllExtCount);
        Assert.assertEquals("The counts are not equal when compared with "+tableName+" and All Extended ", DLExtViewCount, DLAllExtCount);
    }

   @Given("^Get the (.*) from All Extended views (.*)$")
    public void getRandomIdsFromAllExtViews(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids from All Ext Views....");

        switch (tableName) {
            case "product_extended_availability":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_PROD_EXT_AVAILABILITY_RAND_ID, numberOfRecords);
                break;
            case "product_extended_pricing":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_PROD_EXT_PRICING_RAND_ID, numberOfRecords);
                break;
            case "manifestation_extended_restriction":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_MANIF_EXT_RESTRICT_RAND_ID, numberOfRecords);
                break;
            case "manifestation_extended_page_count":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_MANIF_EXT_PAGE_COUNT_RAND_ID, numberOfRecords);
                break;
            case "manifestation_extended":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_MANIF_EXT_RAND_ID, numberOfRecords);
                break;
            case "work_extended":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_RAND_ID, numberOfRecords);
                break;
            case "work_extended_editorial_board":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_EDITORIAL_RAND_ID, numberOfRecords);
                break;
            case "work_extended_metric":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_METRIC_RAND_ID, numberOfRecords);
                break;
            case "work_extended_person_role":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_PERS_ROLE_RAND_ID, numberOfRecords);
                break;
            case "work_extended_relationship_sibling":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_RELT_SIBLING_RAND_ID, numberOfRecords);
                break;
            case "work_extended_subject_area":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_SUBJ_AREA_RAND_ID, numberOfRecords);
                break;
            case "work_extended_url":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_URL_RAND_ID, numberOfRecords);
                break;
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("EPRID")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @Then("^Get the Records from the All Extended views (.*)$")
    public void getRecFromAllExtViews(String tableName){
        Log.info("We get the records from All Extended views...");
        switch (tableName) {
            case "product_extended_availability":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_PROD_EXT_AVAILABILITY_REC, Joiner.on("','").join(Ids));
                break;
            case "product_extended_pricing":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_PROD_PRICING_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_MANIF_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended_page_count":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_MANIF_PAGE_COUNT_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended_restriction":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_MANIF_RESTRICT_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_WORK_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_metric":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_WORK_METRIC_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_person_role":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_WORK_PERSON_ROLE_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_relationship_sibling":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_WORK_RELATIONSHIP_SIBLING_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_subject_area":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_WORK_SUB_AREA_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_url":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_WORK_URL_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_editorial_board":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_WORK_EDITORIAL_EXT_REC, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityDLExtViewContext.recordsFromAllExt = DBManager.getDBResultAsBeanList(sql, DL_ExtendedViewsAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Get the Records from the Extended Tables (.*)$")
    public void getRecFromExtTables(String tableName){
        Log.info("We get the records from Extended Tables...");
        switch (tableName) {
            case "product_extended_availability":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_PROD_EXT_AVAILABILITY_REC, Joiner.on("','").join(Ids));
                break;
            case "product_extended_pricing":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_PROD_PRICING_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_MANIF_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended_page_count":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_MANIF_PAGE_COUNT_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended_restriction":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_MANIF_RESTRICT_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_WORK_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_metric":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_WORK_METRIC_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_person_role":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_WORK_PERSON_ROLE_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_relationship_sibling":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_WORK_RELATIONSHIP_SIBLING_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_subject_area":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_WORK_SUB_AREA_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_url":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_WORK_URL_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_editorial_board":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_WORK_EDITORIAL_EXT_REC, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityDLExtViewContext.recordsFromExtTable = DBManager.getDBResultAsBeanList(sql, DL_ExtendedViewsAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare data of All Extended views with Extended Tables (.*) are identical$")
    public void compareExtTablesandAllExtViews(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityDLExtViewContext.recordsFromAllExt.isEmpty()) {
            Log.info("No Data Found in the All Ext Views ....");
        } else {
            Log.info("Sorting the Ids..");
        }
        for (int i = 0; i < dataQualityDLExtViewContext.recordsFromAllExt.size(); i++) {
            switch (targetTableName) {
                case "product_extended_availability":

                    Log.info("product_extended_availability Records:");
                    String[] all_prod_avail_Col = {"getepr_id", "getproduct_type", "getlast_updated_date", "getapplication_name", "getdelta_answer_code_uk",
                            "getdelta_answer_code_us","getpublication_status_anz","getavailability_format","getavailability_start_date","getavailability_status","getdelete_flag"};
                    for (String strTemp : all_prod_avail_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);


                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " =>  = All_Views_EXT" + method.invoke(objectToCompare1) +
                                " Ext_Tables  = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in Ext_Tables for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "product_extended_pricing":

                    Log.info("product_extended_pricing Records:");
                    //dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    //dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_prod_price_Col = {"getepr_id","getproduct_type", "getlast_updated_date", "getprice_currency", "getprice_amount",
                            "getprice_start_date","getprice_end_date","getprice_region","getprice_category","getprice_customer_category","getprice_purchase_quantity"};
                    for (String strTemp : all_prod_price_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " => All_Views_EXT = " + method.invoke(objectToCompare1) +
                                " Ext_Tables  = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in Ext Tables for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "manifestation_extended":

                    Log.info("manifestation_extended Records:");
                    //                    dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    //                   dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_manif_ext_Col = {"getepr_id","getlast_updated_date", "getmanifestation_type", "getuk_textbook_ind", "getus_textbook_ind", "getmanifestation_trim_text",
                            "getcommodity_code","getdiscount_code_emea","getdiscount_code_us","getmanifestation_weight","getjournal_issue_trim_size","getwar_reference","getdelete_flag"};
                    for (String strTemp : all_manif_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " => All_Views_EXT = " + method.invoke(objectToCompare1) +
                                " Ext_Tables = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in Ext Table for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "manifestation_extended_page_count":

                    Log.info("manifestation_extended_page_count Records:");
                    //  dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    // dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_manif_page_ext_Col = {"getepr_id","getmanifestation_type","getlast_updated_date", "getcount_type_code", "getcount_type_name", "getcount", "getdelete_flag"};
                    for (String strTemp : all_manif_page_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " => All_Views_EXT = " + method.invoke(objectToCompare1) +
                                " EXt_Tables = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in Ext_Tables for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "manifestation_extended_restriction":

                    Log.info("manifestation_extended_restriction Records:");
                    //dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    //dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_manif_restrict_ext_Col = {"getepr_id", "getmanifestation_type","getlast_updated_date", "getrestriction_code", "getrestriction_name","getdelete_flag"};
                    for (String strTemp : all_manif_restrict_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " => All_Views_EXT = " + method.invoke(objectToCompare1) +
                                " Ext_tables = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in Ext_tables for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended":
                    Log.info("work_extended Records:");
                    // dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    //dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));
                    String[] all_work_ext_Col = {"getepr_id", "getwork_type","getlast_updated_date", "getjournal_els_com_ind", "getjourns_aims_scope","getdelta_business_unit","getimage_file_ref","getmaster_isbn","getauthor_by_line_text",
                            "getkey_features","getproduct_awards","getproduct_long_desc","getproduct_short_desc","getreview_quotes","gettoc_long","gettoc_short","getaudience_text",
                            "getbook_sub_business_unit","getinternal_els_div","getprofit_centre","gettext_ref_trade","getprimary_site_system","getprimary_site_acronym","getprimary_site_support_level","getissue_prod_type_code",
                            "getcatalogue_volumes_qty","getcatalogue_issues_qty","getcatalogue_volume_from","getcatalogue_volume_to","getrf_issues_qty","getrf_total_pages_qty","getrf_fvi",
                            "getrf_lvi","getbusiness_unit_desc","getdelete_flag"};
                    for (String strTemp : all_work_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " => All_Views_EXT = " + method.invoke(objectToCompare1) +
                                " Ext_tables = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in Ext_tables for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_metric":
                    Log.info("work_extended_metric Records:");
                    dataQualityDLExtViewContext.recordsFromAllExt.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    dataQualityDLExtViewContext.recordsFromExtTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_work_metric_ext_Col = {"getepr_id", "getwork_type","getlast_updated_date", "getmetric_code", "getmetric_name","getmetric","getmetric_year","getmetric_url","getdelete_flag"};
                    for (String strTemp : all_work_metric_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " => All_Views_EXT = " + method.invoke(objectToCompare1) +
                                " Ext_Tables = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in Ext_Tables for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_person_role":

                    Log.info("work_extended_person_role Records:");

                    String[] all_work_pers_role_ext_Col = {"getepr_id", "getwork_type","getlast_updated_date", "getcore_work_person_role_id", "getrole_code","getrole_name","getsequence_number","getgroup_number","getfirst_name",
                            "getlast_name","getpeoplehub_id","getemail","gettitle","gethonours","getaffiliation","getimage_url","getfootnote_txt","getnotes_txt","getdelete_flag"};
                    for (String strTemp : all_work_pers_role_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " => All_Views_EXT = " + method.invoke(objectToCompare1) +
                                " Ext_table = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in Ext_table for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_relationship_sibling":

                    Log.info("work_extended_relationship_sibling Records:");
                    //  dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    // dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_work_relation_sibling_ext_Col = {"getepr_id", "getwork_type","getlast_updated_date","getrole_code","getrole_name","getsequence_number","getgroup_number","getfirst_name",
                            "getlast_name","getpeoplehub_id","getemail","gettitle","gethonours","getaffiliation","getimage_url","getfootnote_txt","getnotes_txt","getdelete_flag"};
                    for (String strTemp : all_work_relation_sibling_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " => All_Views_EXT = " + method.invoke(objectToCompare1) +
                                " Ext_table = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in Ext_table for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_subject_area":

                    Log.info("work_extended_subject_area Records:");
                    // dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    //dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_work_subj_area_ext_Col = {"getepr_id", "getwork_type","getlast_updated_date", "getcode", "getname","getpriority","gettype_code","gettype_name","getdelete_flag"};
                    for (String strTemp : all_work_subj_area_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " => All_Views_EXT = " + method.invoke(objectToCompare1) +
                                " Ext_tables = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_url":

                    Log.info("work_extended_url Records:");
                    // dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    // dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_work_url_ext_Col = {"getepr_id","getwork_type","getlast_updated_date", "geturl_type_code", "geturl_type_name","geturl","geturl_title","getdelete_flag"};
                    for (String strTemp : all_work_url_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " => All_Views_EXT = " + method.invoke(objectToCompare1) +
                                " Ext_tables = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in Ext_tables for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_editorial_board":

                    Log.info("work_extended_editorial_board Records:");
                    dataQualityDLExtViewContext.recordsFromAllExt.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    dataQualityDLExtViewContext.recordsFromExtTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_work_edit_ext_Col = {"getepr_id", "getwork_type","getlast_updated_date", "getrole_code", "getrole_name","getsequence_number","getgroup_number","getfirst_name","getlast_name" ,
                            "gettitle","gethonours","getaffiliation","getimage_url","getfootnote_txt","getnotes_txt","getdelete_flag"};
                    for (String strTemp : all_work_edit_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromAllExt.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromExtTable.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id() +
                                " " + strTemp + " => All_Views_EXT = " + method.invoke(objectToCompare1) +
                                " Ext_Tables = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in Ext_Tables for EPRID:" + dataQualityDLExtViewContext.recordsFromAllExt.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
            }
        }

    }

}










