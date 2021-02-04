package com.eph.automation.testing.web.steps.DL_ExtendedViews;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
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

import java.util.List;
import java.util.Map;



public class DL_AllSourceExtViewsChecksSteps {
    private static String DLAllExtSQLViewCount;
    private static int DLAllExtViewCount;
    private static String DLSourceIngestExtSQLViewCount;
    private static int DLSourceIngestExtCount;

    public DL_ExtViewsAccessContext dataQualityDLExtViewContext;
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

     @Given("^Get the (.*) from source ingestion Tables (.*)$")
    public void getRandomIdsFromSourceIngest(String numberOfRecords, String tableName) {
       // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids from source ingest latest Tables....");

        switch (tableName) {
            case "product_availability_extended_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_PROD_EXT_AVAILABILITY_RAND_ID, numberOfRecords);
                break;
            case "product_extended_pricing_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_PROD_PRICING_EXT_RAND_ID, numberOfRecords);
                break;
            case "manifestation_extended_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_MANIF_EXT_RAND_ID, numberOfRecords);
                break;
            case "manifestation_extended_page_count_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_MANIF_PAGE_EXT_RAND_ID, numberOfRecords);
                break;
            case "manifestation_extended_restriction_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_MANIF_RESTRICT_EXT_RAND_ID, numberOfRecords);
                break;
            case "work_extended_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_EXT_RAND_ID, numberOfRecords);
                break;
            case "work_extended_metric_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_METRIC_EXT_RAND_ID, numberOfRecords);
                break;
            case "work_extended_person_role_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_PERSON_ROLE_EXT_RAND_ID, numberOfRecords);
                break;
            case "work_extended_relationship_sibling_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_RELATIONSHIP_SIBLING_EXT_RAND_ID, numberOfRecords);
                break;
            case "work_extended_subject_area_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_SUBJ_AREA_EXT_RAND_ID, numberOfRecords);
                break;
            case "work_extended_url_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_URL_EXT_RAND_ID, numberOfRecords);
                break;
            case "work_extended_editorial_board_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_EDITORIAL_EXT_RAND_ID, numberOfRecords);
                break;
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("EPRID")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @Then("^Get the Records from the source ingestion Tables (.*)$")
    public void getRecFromSourceIngest(String tableName){
        Log.info("We get the records from source ingestion tables...");
        switch (tableName) {
            case "product_availability_extended_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_PROD_EXT_AVAILABILITY_REC, Joiner.on("','").join(Ids));
                break;
            case "product_extended_pricing_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_PROD_PRICING_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_MANIF_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended_page_count_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_MANIF_PAGE_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended_restriction_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_MANIF_RESTRICT_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_metric_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_METRIC_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_person_role_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_PERSON_ROLE_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_relationship_sibling_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_RELATIONSHIP_SIBLING_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_subject_area_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_SUBJ_AREA_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_url_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_URL_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_editorial_board_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_SOURCE_WORK_EDITORIAL_EXT_REC, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityDLExtViewContext.recordsFromSourceIngestTable = DBManager.getDBResultAsBeanList(sql, DL_ExtendedViewsAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Get the Records from the DL all source views (.*)$")
    public void getRecFromAllExtViews(String tableName){
        Log.info("We get the records from source ingestion tables...");
        switch (tableName) {
            case "product_availability_extended_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_PROD_EXT_AVAILABILITY_REC, Joiner.on("','").join(Ids));
                break;
            case "product_extended_pricing_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_PROD_PRICING_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_MANIF_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended_page_count_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_MANIF_PAGE_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "manifestation_extended_restriction_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_MANIF_RESTRICT_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_WORK_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_metric_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_WORK_METRIC_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_person_role_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_WORK_PERSON_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_relationship_sibling_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_WORK_RELATIONSHIP_SIBLING_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_subject_area_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_WORK_SUB_AREA_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_url_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_WORK_URL_EXT_REC, Joiner.on("','").join(Ids));
                break;
            case "work_extended_editorial_board_allsource_v":
                sql = String.format(DL_ExtendedViewChecksSQL.GET_ALL_VIEW_WORK_EDITORIAL_EXT_REC, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityDLExtViewContext.recordsFromAllExtViews = DBManager.getDBResultAsBeanList(sql, DL_ExtendedViewsAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }




}










