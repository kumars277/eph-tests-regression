package com.eph.automation.testing.steps.consumerApp;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.GDTablesDLSQLContext;
import com.eph.automation.testing.models.dao.consumerApp.r12Objects;
import com.eph.automation.testing.services.db.consumerAppSQL.r12ChecksSQL;
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

public class r12ConsumerAppSteps {
    private static String r12CountSQL;
    private static String r12FullSetCountSQL;
    private static int r12FullSetCount;
    private static int r12trgtCount;
    private static String sql;
    private static List<String> ids;

    @Given("^Get the total count of Full set (.*)$")
    public static void getr12FullSetCount (String tableNanem) {
        switch (tableNanem){
            case "r12_full_data_v":
                 r12FullSetCountSQL = r12ChecksSQL.GET_FULL_SET_COUNT_R12;
                break;
            case "drm_action_scripts_v":
                 r12FullSetCountSQL = r12ChecksSQL.GET_DRM_ACTION_SOURCE_COUNT;
                break;
        }

        Log.info(r12FullSetCountSQL);
        List<Map<String, Object>> r12FullSrcTableCnt = DBManager.getDBResultMap(r12FullSetCountSQL, Constants.AWS_URL);
        r12FullSetCount = ((Long) r12FullSrcTableCnt.get(0).get("Source_count")).intValue();
    }

    @Then("^We know the total count of (.*)$")
    public static void getr12TrgtCount (String tableNanem) {
        switch (tableNanem){
            case "r12_full_data_v":
                r12CountSQL = r12ChecksSQL.GET_TARGET_COUNT_R12;
                break;
            case "drm_action_scripts_v":
                r12CountSQL = r12ChecksSQL.GET_DRM_ACTION_TARGET_COUNT;
                break;
        }

        Log.info(r12CountSQL);
        List<Map<String, Object>> r12FullTableCnt = DBManager.getDBResultMap(r12CountSQL, Constants.AWS_URL);
        r12trgtCount = ((Long) r12FullTableCnt.get(0).get("Target_count")).intValue();
    }

    @And("^we Compare count of Full load with (.*)$")
    public void comparesrcAndTrgtCounts(String table){
        Log.info("The count for table r12 => " + r12FullSetCount + " and in "+table+ "=> " + r12trgtCount);
        Assert.assertEquals("The counts are not equal when compared for "+table, r12FullSetCount,r12FullSetCount );
    }

    @Given ("^We get the (.*) random ids (.*)$")
    public void getRandomIds(String numberOfRecords,String tableName){
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids from R12 Tables....");
        List<Map<?, ?>> randomids;
        switch (tableName) {
            case "r12_full_data_v":
                sql = String.format(r12ChecksSQL.GET_RANDOM_EPR_ID_R12_FULL, numberOfRecords);
                break;
            case "drm_action_scripts_v":
                sql = String.format(r12ChecksSQL.GET_RANDOM_EPR_ID_DRM_ACTION, numberOfRecords);
                break;
        }
        randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("eph_work_id")).collect(Collectors.toList());
       // Log.info(sql);
        //Log.info(ids.toString());
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
        }
        GDTablesDLSQLContext.recsR12source = DBManager.getDBResultAsBeanList(sql, r12Objects.class, Constants.AWS_URL);
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
        }
        GDTablesDLSQLContext.recsR12target = DBManager.getDBResultAsBeanList(sql, r12Objects.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^we compare records of full set and (.*)$")
    public void compareRecs(String tableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (GDTablesDLSQLContext.recsR12source.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records...");
            for (int i = 0; i < GDTablesDLSQLContext.recsR12source.size(); i++) {
                switch (tableName) {
                    case "r12_full_data_v":
                        Log.info("comparing inbound and r12_full_data_v records...");
                        GDTablesDLSQLContext.recsR12source.sort(Comparator.comparing(r12Objects::geteph_work_id)); //sort primarykey data in the lists
                        GDTablesDLSQLContext.recsR12target.sort(Comparator.comparing(r12Objects::geteph_work_id));

                        String[] r12FullData = {"geteph_work_id", "getlast_updated_date", "getproduct_type","getproduct_description",
                                "getr12_alias","getcompany_code","getresponsibility_centre","getproduct_segment","getcreated_date","getjournal_acronym","getpmg","getpmc","getissn","getpublisher","getmanifestation_type","getmanifestation_status"};
                        for (String strTemp : r12FullData) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            r12Objects objectToCompare1 = GDTablesDLSQLContext.recsR12source.get(i);
                            r12Objects objectToCompare2 = GDTablesDLSQLContext.recsR12target.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eph_work_id => " + GDTablesDLSQLContext.recsR12source.get(i).geteph_work_id() +
                                    " " + strTemp + " => source = " + method.invoke(objectToCompare1) +
                                    " r12_full_data_v = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in r12_full_data_v for eph_work_id:" + GDTablesDLSQLContext.recsR12source.get(i).geteph_work_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "drm_action_scripts_v":
                        Log.info("drm_action_scripts_v records.... ");
                        GDTablesDLSQLContext.recsR12source.sort(Comparator.comparing(r12Objects::geteph_work_id)); //sort primarykey data in the lists
                        GDTablesDLSQLContext.recsR12target.sort(Comparator.comparing(r12Objects::geteph_work_id));

                        String[] drm_action_recs = {"getscript_group", "getaction", "getconsolidation",
                                "getchart_of_account_value", "getproduct_segment", "getaction_value_type",
                                "getvalue","geteph_work_id","getlast_updated_date"};
                        for (String strTemp : drm_action_recs) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            r12Objects objectToCompare1 = GDTablesDLSQLContext.recsR12source.get(i);
                            r12Objects objectToCompare2 = GDTablesDLSQLContext.recsR12target.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("eph_work_id => " + GDTablesDLSQLContext.recsR12source.get(i).geteph_work_id() +
                                    " " + strTemp + " => Source = " + method.invoke(objectToCompare1) +
                                    " drm_action_scripts_v = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in drm_action_scripts_v for eph_work_id:" + GDTablesDLSQLContext.recsR12source.get(i).geteph_work_id(),
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
