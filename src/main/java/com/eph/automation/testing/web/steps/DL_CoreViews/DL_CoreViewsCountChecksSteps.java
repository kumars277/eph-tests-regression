package com.eph.automation.testing.web.steps.DL_CoreViews;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.DL_CoreViewCountChecksSQL.DL_CoreViewCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class DL_CoreViewsCountChecksSteps {
    private static String DLCoreSQLViewCount;
    private static int DLCoreViewCount;
    private static String BCSJMCoreSQLCount;
    private static int BCSJMCoreCount;




    @Given("^Get the total count of DL Core views (.*)$")
    public void getDLCoreViewCount (String tableName) {
        switch (tableName){
            case "all_accountable_product_v":
                Log.info("Getting DL Core All Accountable Product View Count...");
                DLCoreSQLViewCount = DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_ACC_PROD_VIEW_COUNT;
                break;
            case "all_manifestation_identifiers_v":
                Log.info("Getting DL Core All Manifestation Identifier View Count...");
                DLCoreSQLViewCount = DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_MANIF_IDENT_VIEW_COUNT;
                break;
            case "all_manifestation_v":
                Log.info("Getting DL Core All Manifestation View Count...");
                DLCoreSQLViewCount = DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_MANIF_VIEW_COUNT;
                break;
            case "all_person_v":
                Log.info("Getting DL Core All Person View Count...");
                DLCoreSQLViewCount = DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_PERSON_VIEW_COUNT;
                break;
            case "all_product_v":
                Log.info("Getting DL Core All product view Count...");
                DLCoreSQLViewCount = DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_PRODUCT_VIEW_COUNT;
                break;
            case "all_work_identifier_v":
                Log.info("Getting DL Core all work Identifier view Count...");
                DLCoreSQLViewCount = DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_WRK_IDENT_VIEW_COUNT;
                break;
            case "all_work_person_role_v":
                Log.info("Getting DL Core All work Person Role view Count...");
                DLCoreSQLViewCount = DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_WRK_PERS_ROLE_VIEW_COUNT;
                break;
            case "all_work_relationship_v":
                Log.info("Getting Dl Core all work relationship view Count...");
                DLCoreSQLViewCount = DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_WRK_RELT_VIEW_COUNT;
                break;
            case "all_work_subject_areas_v":
                Log.info("Getting DL Core all work subject areas view Count...");
                DLCoreSQLViewCount = DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_WORK_SUB_AREA_VIEW_COUNT;
                break;
            case "all_work_v":
                Log.info("Getting DL all work view Count...");
                DLCoreSQLViewCount = DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_WORK_VIEW_COUNT;
                break;

        }
        Log.info(DLCoreSQLViewCount);
        List<Map<String, Object>> DLCoreViewTableCount = DBManager.getDBResultMap(DLCoreSQLViewCount, Constants.AWS_URL);
        DLCoreViewCount = ((Long) DLCoreViewTableCount.get(0).get("Target_Count")).intValue();
    }

    @Given("^We know the total count of BCS And JM Core tables (.*)$")
    public void getCountBCSAndJMTables(String tableName){
        switch (tableName){
            case "all_accountable_product_v":
                Log.info("Getting BCS And JM Core Accountable Product Count...");
                BCSJMCoreSQLCount = DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_ACC_PROD_COUNT;
                break;
            case "all_manifestation_identifiers_v":
                Log.info("Getting BCS And JM Core Manifestation Identifier Count...");
                BCSJMCoreSQLCount = DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_MANIF_IDENT_COUNT;
                break;
            case "all_manifestation_v":
                Log.info("Getting BCS And JM Core Manifestation Count...");
                BCSJMCoreSQLCount = DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_MANIF_COUNT;
                break;
            case "all_person_v":
                Log.info("Getting BCS And JM Core Person Count...");
                BCSJMCoreSQLCount = DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_PERSON_COUNT;
                break;
            case "all_product_v":
                Log.info("Getting BCS And JM Core product Count...");
                BCSJMCoreSQLCount = DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_PRODUCT_COUNT;
                break;
            case "all_work_identifier_v":
                Log.info("Getting BCS And JM Core work Identifier Count...");
                BCSJMCoreSQLCount = DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_IDENT_COUNT;
                break;
            case "all_work_person_role_v":
                Log.info("Getting BCS And JM Core work Person Role Count...");
                BCSJMCoreSQLCount = DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_PERS_ROLE_COUNT;
                break;
            case "all_work_relationship_v":
                Log.info("Getting BCS And JM Core work relationship Count...");
                BCSJMCoreSQLCount = DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_RELATION_COUNT;
                break;
            case "all_work_subject_areas_v":
                Log.info("Getting BCS And JM Core work subject areas Count...");
                BCSJMCoreSQLCount = DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_SUBJ_AREA_COUNT;
                break;
            case "all_work_v":
                Log.info("Getting BCS And JM work Count...");
                BCSJMCoreSQLCount = DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_COUNT;
                break;

        }
        Log.info(BCSJMCoreSQLCount);
        List<Map<String, Object>> BCSJMCoreTableCount = DBManager.getDBResultMap(BCSJMCoreSQLCount, Constants.AWS_URL);
        BCSJMCoreCount = ((Long) BCSJMCoreTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of BCS and JM Core with (.*) views are identical$")
    public void compareCoreAndInboundCounts(String tableName){
        Log.info("The count for view "+tableName+" => " + DLCoreViewCount + " and in BCS_JM core  => " + BCSJMCoreCount);
        Assert.assertEquals("The counts are not equal when compared with "+tableName+" and BCS_JM core ", DLCoreViewCount, BCSJMCoreCount);
    }






}
