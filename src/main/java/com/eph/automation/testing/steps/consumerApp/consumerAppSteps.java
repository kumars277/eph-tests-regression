package com.eph.automation.testing.steps.consumerApp;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.GDTablesDLSQLContext;
import com.eph.automation.testing.models.dao.consumerApp.consumerObjs;
import com.eph.automation.testing.services.db.consumerAppSQL.r12ChecksSQL;
import com.eph.automation.testing.services.db.consumerAppSQL.mercuryChecksSQL;
import com.eph.automation.testing.services.db.consumerAppSQL.exariChecksSQL;
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

public class consumerAppSteps {
    private static String consumTgtCountSQL;
    private static String consumerSourceCountSQL;
    private static int consumerSrcCnt;
    private static int consumerTrgtCnt;
    private static String sql;
    private static List<String> ids;

    @Given("^Get the total count of Full set (.*)$")
    public static void getconsumerSrcCnt (String tableNanem) {
        switch (tableNanem){
            case "r12_full_data_v":
                 consumerSourceCountSQL = r12ChecksSQL.GET_FULL_SET_COUNT_R12;
                break;
            case "drm_action_scripts_v":
                 consumerSourceCountSQL = r12ChecksSQL.GET_DRM_ACTION_SOURCE_COUNT;
                break;
            case "exari_view":
                consumerSourceCountSQL = exariChecksSQL.GET_EXARI_SOURCE_COUNT;
                break;
            case "extract_mercury_print_v":
                consumerSourceCountSQL = mercuryChecksSQL.GET_SOURCE_EPH_COUNT;
                break;

        }

        Log.info(consumerSourceCountSQL);
        List<Map<String, Object>> consumerSrcTableCnt = DBManager.getDBResultMap(consumerSourceCountSQL, Constants.AWS_URL);
        consumerSrcCnt = ((Long) consumerSrcTableCnt.get(0).get("Source_count")).intValue();
    }

    @Then("^We know the total count of (.*)$")
    public static void getconsumerTrgtCnt(String tableNanem) {
        switch (tableNanem){
            case "r12_full_data_v":
                consumTgtCountSQL = r12ChecksSQL.GET_TARGET_COUNT_R12;
                break;
            case "drm_action_scripts_v":
                consumTgtCountSQL = r12ChecksSQL.GET_DRM_ACTION_TARGET_COUNT;
                break;
            case "extract_mercury_print_v":
                consumTgtCountSQL = mercuryChecksSQL.GET_MERCURY_PRINT_COUNT;
                break;
            case "exari_view":
                consumTgtCountSQL = exariChecksSQL.GET_EXARI_TGT_COUNT;
                break;
        }

        Log.info(consumTgtCountSQL);
        List<Map<String, Object>> consumerTgtTableCnt = DBManager.getDBResultMap(consumTgtCountSQL, Constants.AWS_URL);
        consumerTrgtCnt = ((Long) consumerTgtTableCnt.get(0).get("Target_count")).intValue();
    }

    @And("^Compare count of Full load with (.*)$")
    public void comparesrcAndTrgtCounts(String table){
        Log.info("The count from source => " + consumerSrcCnt + " and in "+table+ "=> " + consumerTrgtCnt);
        Assert.assertEquals("The counts are not equal when compared for "+table, consumerTrgtCnt,consumerSrcCnt );
    }

    @Given("^We get the (.*) random ids (.*)$")
    public void getRandomIds(String numberOfRecords,String tableName){
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids....");
        List<Map<?, ?>> randomids;
        switch (tableName) {
            case "r12_full_data_v":
                sql = String.format(r12ChecksSQL.GET_RANDOM_EPR_ID_R12_FULL, numberOfRecords);
                break;
            case "drm_action_scripts_v":
                sql = String.format(r12ChecksSQL.GET_RANDOM_EPR_ID_DRM_ACTION, numberOfRecords);
                break;
            case "extract_mercury_print_v":
                sql = String.format(mercuryChecksSQL.GET_RANDOM_ID_MERCURY, numberOfRecords);
                break;
            case "exari_view":
                sql = String.format(exariChecksSQL.GET_RANDOM_ID_EXARI, numberOfRecords);
                break;
        }
        randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("randomIds")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^We Get the records from full set data (.*)$")
    public static void getFullRecSrc(String tableName) {
        Log.info("We get source records...");
        switch (tableName) {
            case "r12_full_data_v":
                sql = String.format(r12ChecksSQL.GET_FULL_SET_SOURCE_R12_DATA, String.join("','",ids));
                break;
            case "drm_action_scripts_v":
                sql = String.format(r12ChecksSQL.GET_SOURCE_DATA_DRM_ACTION_SCRIPT, String.join("','",ids));
                break;
            case "extract_mercury_print_v":
                sql = String.format(mercuryChecksSQL.GET_EPH_SOURCE_DATA, String.join("','",ids));
                break;
            case "exari_view":
                sql = String.format(exariChecksSQL.GET_SOURCE_REC_EXARI, String.join("','",ids));
                break;
        }
        GDTablesDLSQLContext.recsConsumersource = DBManager.getDBResultAsBeanList(sql, consumerObjs.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When("^We Get the records from the views of (.*)$")
    public static void getRecTarget(String tableName) {
        Log.info("We get Target records...");
        switch (tableName) {
            case "r12_full_data_v":
                sql = String.format(r12ChecksSQL.GET_FULL_SET_TARGET_R12_DATA, String.join("','",ids));
                break;
            case "drm_action_scripts_v":
                sql = String.format(r12ChecksSQL.GET_TARGET_DATA_DRM_ACTION_SCRIPT, String.join("','",ids));
                break;
            case "extract_mercury_print_v":
                sql = String.format(mercuryChecksSQL.GET_MERCURY_PRINT_RECS, String.join("','",ids));
                break;
            case "exari_view":
                sql = String.format(exariChecksSQL.GET_TARGET_REC_EXARI, String.join("','",ids));
                break;
        }
        GDTablesDLSQLContext.recsConsumertarget = DBManager.getDBResultAsBeanList(sql, consumerObjs.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^we compare records of full set and (.*)$")
    public void compareRecs(String tableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (GDTablesDLSQLContext.recsConsumersource.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records...");
            for (int i = 0; i < GDTablesDLSQLContext.recsConsumersource.size(); i++) {
                switch (tableName) {
                    case "r12_full_data_v":
                        Log.info("comparing inbound and r12_full_data_v records...");
                        GDTablesDLSQLContext.recsConsumersource.sort(Comparator.comparing(consumerObjs::geteph_work_id)); //sort primarykey data in the lists
                        GDTablesDLSQLContext.recsConsumertarget.sort(Comparator.comparing(consumerObjs::geteph_work_id));

                        String[] r12FullData = {"geteph_work_id", "getlast_updated_date", "getproduct_type","getproduct_description",
                                "getr12_alias","getcompany_code","getresponsibility_centre","getproduct_segment","getcreated_date","getjournal_acronym","getpmg","getpmc","getissn","getpublisher","getmanifestation_type","getmanifestation_status"};
                        for (String strTemp : r12FullData) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            consumerObjs objectToCompare1 = GDTablesDLSQLContext.recsConsumersource.get(i);
                            consumerObjs objectToCompare2 = GDTablesDLSQLContext.recsConsumertarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eph_work_id => " + GDTablesDLSQLContext.recsConsumersource.get(i).geteph_work_id() +
                                    " " + strTemp + " => source = " + method.invoke(objectToCompare1) +
                                    " r12_full_data_v = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in r12_full_data_v for eph_work_id:" + GDTablesDLSQLContext.recsConsumersource.get(i).geteph_work_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "drm_action_scripts_v":
                        Log.info("drm_action_scripts_v records.... ");
                        GDTablesDLSQLContext.recsConsumersource.sort(Comparator.comparing(consumerObjs::geteph_work_id)); //sort primarykey data in the lists
                        GDTablesDLSQLContext.recsConsumertarget.sort(Comparator.comparing(consumerObjs::geteph_work_id));

                        String[] drm_action_recs = {"getscript_group", "getaction", "getconsolidation",
                                "getchart_of_account_value", "getproduct_segment", "getaction_value_type",
                                "getvalue","geteph_work_id","getlast_updated_date"};
                        for (String strTemp : drm_action_recs) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            consumerObjs objectToCompare1 = GDTablesDLSQLContext.recsConsumersource.get(i);
                            consumerObjs objectToCompare2 = GDTablesDLSQLContext.recsConsumertarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eph_work_id => " + GDTablesDLSQLContext.recsConsumersource.get(i).geteph_work_id() +
                                    " " + strTemp + " => Source = " + method.invoke(objectToCompare1) +
                                    " drm_action_scripts_v = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in drm_action_scripts_v for eph_work_id:" + GDTablesDLSQLContext.recsConsumersource.get(i).geteph_work_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                        break;

                    case "extract_mercury_print_v":
                        Log.info("comparing eph vs mercury records...");
                        GDTablesDLSQLContext.recsConsumersource.sort(Comparator.comparing(consumerObjs::getproduct_id)); //sort primarykey data in the lists
                        GDTablesDLSQLContext.recsConsumertarget.sort(Comparator.comparing(consumerObjs::getproduct_id));

                        String[] mercuryData = {"getschemaversion", "getcreated", "getupdated", "getproduct_id",
                                "getproduct_full_name", "getproduct_name", "getwork_title", "getwork_type", "getwork_typerollup", "getwork_status", "getwork_statusrollup", "getmanifestation_id", "getjournal_number", "getjournal_acronym", "getoperating_company", "getproduct_type", "getissn", "getpublication_status", "getetax_code", "getfulfilment_system", "getwarehouse_primary", "getwarehouse_backissue", "getnumber_of_issues", "getnumber_of_issues", "getddp_eligible", "getpts_journal_indicator", "getyear_of_first_issue", "getfirst_issue_name", "getfirst_volume_name", "getdefault_start", "getshort_title", "getrc_code", "getpmc_code"};
                        for (String strTemp : mercuryData) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            consumerObjs objectToCompare1 = GDTablesDLSQLContext.recsConsumersource.get(i);
                            consumerObjs objectToCompare2 = GDTablesDLSQLContext.recsConsumertarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("product_id => " + GDTablesDLSQLContext.recsConsumersource.get(i).getproduct_id() +
                                    " " + strTemp + " => EPH = " + method.invoke(objectToCompare1) +
                                    " Mercury Print = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Mercury_Print for product_id:" + GDTablesDLSQLContext.recsConsumersource.get(i).getproduct_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "exari_view":
                        Log.info("comparing records for exari_view...");
                        GDTablesDLSQLContext.recsConsumersource.sort(Comparator.comparing(consumerObjs::getjournal_number)); //sort primarykey data in the lists
                        GDTablesDLSQLContext.recsConsumertarget.sort(Comparator.comparing(consumerObjs::getjournal_number));

                        String[] exari_view = {"getjorunal_number","getissn","getwork_title","getjournal_acronym","getpmg","getpmc","getownership_type","getpublisher",
                        "getpublisher_email","getpublishing_director","getpublishing_director_email","getbusiness_controller","getbusiness_controller_email","getsenior_vice_president","getsenior_vice_president_email","getoperating_company","getrc"};
                        for (String strTemp : exari_view) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            consumerObjs objectToCompare1 = GDTablesDLSQLContext.recsConsumersource.get(i);
                            consumerObjs objectToCompare2 = GDTablesDLSQLContext.recsConsumertarget.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("issn => " + GDTablesDLSQLContext.recsConsumersource.get(i).getissn() +
                                    " " + strTemp + " => source = " + method.invoke(objectToCompare1) +
                                    " exari_view = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in exari_view for journa_number:" + GDTablesDLSQLContext.recsConsumersource.get(i).getjournal_number(),
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
