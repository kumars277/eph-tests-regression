package com.eph.automation.testing.web.steps.DL_ExtendedViews;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.DL_ExtViewSQL.DL_ExtendedViewChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

//import com.eph.automation.testing.models.contexts.DL_CoreViewsAccessContext;
//import com.eph.automation.testing.models.dao.DL_CoreViews.DL_CoreViewsAccessObject;

public class DL_AllSourceExtViewsChecksSteps {
    private static String DLAllExtSQLViewCount;
    private static int DLAllExtViewCount;
    private static String DLSourceIngestExtSQLViewCount;
    private static int DLSourceIngestExtCount;

  //  public DL_CoreViewsAccessContext dataQualityDLCoreViewContext;
    private static String sql;
    private static List<String> Ids;


    @Given("^Get the total count of All Extended views (.*)$")
    public void getCountAllExtTables (String tableName) {
        switch (tableName){
            case "product_availability_extended_allsource_v":
                Log.info("Getting DL product_availability_extended_allsource_v Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_PROD_EXT_AVAILABILITY_VIEW_COUNT;
                break;
            case "product_extended_pricing_allsource_v":
                Log.info("Getting DL product_pricing_extended_allsource_v Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_PROD_EXT_PRICING_VIEW_COUNT;
                break;
            case "manifestation_extended_allsource_v":
                Log.info("Getting DL manifestation_extended_allsource_v Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_MANIF_EXT_VIEW_COUNT;
                break;
            case "manifestation_extended_page_count_allsource_v":
                Log.info("Getting DL manifestation_extended_page_count_allsource_v Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_MANIF_EXT_PAGE_VIEW_COUNT;
                break;
            case "manifestation_extended_restriction_allsource_v":
                Log.info("Getting DL manifestation_extended_restriction_allsource_v Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_MANIF_EXT_RESTRICT_VIEW_COUNT;
                break;
            case "work_extended_allsource_v":
                Log.info("Getting DL work_extended_allsource_v Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_VIEW_COUNT;
                break;
           case "work_extended_metric_allsource_v":
                Log.info("Getting DL work_extended_metrics_allsource_v Count...");
               DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_METRIC_VIEW_COUNT;
                break;
            case "work_extended_person_role_allsource_v":
                Log.info("Getting Dl work_extended_person_roles_allsource_v Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_PERS_ROLE_VIEW_COUNT;
                break;
            case "work_extended_relationship_sibling_allsource_v":
                Log.info("Getting DL work_extended_relationship_siblings_allsource_v Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_RELATION_SIBLING_VIEW_COUNT;
                break;
            case "work_extended_subject_area_allsource_v":
                Log.info("Getting DL work_extended_subject_areas_allsource_v Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_SUBJ_AREA_VIEW_COUNT;
                break;
            case "work_extended_url_allsource_v":
                Log.info("Getting DL work_extended_urls_allsource_v Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_URL_VIEW_COUNT;
                break;
            case "work_extended_editorial_board_allsource_v":
                Log.info("Getting DL work_extended_editorial_board_allsource_v Count...");
                DLAllExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_DL_ALL_WORK_EXT_EDIT_VIEW_COUNT;
                break;

        }
        Log.info(DLAllExtSQLViewCount);
        List<Map<String, Object>> DLAllExtViewTableCount = DBManager.getDBResultMap(DLAllExtSQLViewCount, Constants.AWS_URL);
        DLAllExtViewCount = ((Long) DLAllExtViewTableCount.get(0).get("Target_Count")).intValue();
    }

    @Given("^We know the total count of Source Ingestion (.*)$")
    public void getSourceIngestTables(String tableName){
        switch (tableName){
            case "product_availability_extended_allsource_v":
                Log.info("Getting sourceIngest for product_availability_extended_allsource_v Count...");
                DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_PROD_EXT_AVAILABILITY_COUNT;
                break;
            case "product_extended_pricing_allsource_v":
                Log.info("Getting DL All product_extended_pricing Count...");
                DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_PROD_PRICING_EXT_COUNT;
                break;
            case "manifestation_extended_restriction_allsource_v":
                Log.info("Getting DL All manifestation_extended_restriction Count...");
                DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_MANIF_RESTRICT_EXT_COUNT;
                break;
            case "manifestation_extended_page_count_allsource_v":
                Log.info("Getting DL All All manifestation_extended_page_count Count...");
                DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_MANIF_PAGE_EXT_COUNT;
                break;
            case "manifestation_extended_allsource_v":
                Log.info("Getting DL All manifestation_extended Count...");
                DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_MANIF_EXT_COUNT;
                break;
            case "work_extended_allsource_v":
                Log.info("Getting DL All work_extended Count...");
                DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_EXT_COUNT;
                break;
           case "work_extended_editorial_board_allsource_v":
                Log.info("Getting DL All work_extended_editorial_board Count...");
               DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_EDITORIAL_EXT_COUNT;
                break;
            case "work_extended_metric_allsource_v":
                Log.info("Getting Dl All work_extended_metric Count...");
                DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_METRIC_EXT_COUNT;
                break;
            case "work_extended_person_role_allsource_v":
                Log.info("Getting DL All work_extended_person_role Count...");
                DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_PERSON_ROLE_EXT_COUNT;
                break;
            case "work_extended_relationship_sibling_allsource_v":
                Log.info("Getting DL All work_extended_relationship_sibling Count...");
                DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_RELATIONSHIP_SIBLING_EXT_COUNT;
                break;
            case "work_extended_subject_area_allsource_v":
                Log.info("Getting DL All work_extended_subject_area Count...");
                DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_SUBJ_AREA_EXT_COUNT;
                break;
            case "work_extended_url_allsource_v":
                Log.info("Getting DL All work_extended_url Count...");
                DLSourceIngestExtSQLViewCount = DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_URL_EXT_COUNT;
                break;

        }
        Log.info(DLSourceIngestExtSQLViewCount);
        List<Map<String, Object>> DLSourceIngestExtTableCount = DBManager.getDBResultMap(DLSourceIngestExtSQLViewCount, Constants.AWS_URL);
        DLSourceIngestExtCount = ((Long) DLSourceIngestExtTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of All Extended Views with Source Ingestion (.*) are identical$")
    public void compareAllExtAndSourceIngestCounts(String tableName){
        Log.info("The count for All source view "+tableName+" => " + DLAllExtViewCount + " and in Source Ingest  => " + DLSourceIngestExtCount);
        Assert.assertEquals("The counts are not equal when compared with "+tableName+" and Source Ingest ", DLAllExtViewCount, DLSourceIngestExtCount);
    }

}










