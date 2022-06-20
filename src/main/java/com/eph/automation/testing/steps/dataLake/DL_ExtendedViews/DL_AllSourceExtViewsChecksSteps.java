package com.eph.automation.testing.steps.dataLake.DL_ExtendedViews;


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

    @Given("^get total count of Source Ingestion (.*)$")
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

    @And("^Compare data of source ingestion with all source extended views (.*) are identical$")
    public void compareSourceIngestandAllExtViews(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityDLExtViewContext.recordsFromSourceIngestTable.isEmpty()) {
            Log.info("No Data Found in the Source Ingest Tables ....");
        } else {
            Log.info("Sorting the Ids to compare the records between DL Extend All views...");
        }
        for (int i = 0; i < dataQualityDLExtViewContext.recordsFromSourceIngestTable.size(); i++) {
            switch (targetTableName) {
                case "product_availability_extended_allsource_v":

                    Log.info("product_availability_extended_allsource_v Records:");
                    String[] all_prod_avail_Col = {"getepr_id", "getsource", "getproduct_type", "getlast_updated_date", "getapplication_name", "getdelta_answer_code_uk",
                            "getdelta_answer_code_us","getpublication_status_anz","getavailability_format","getavailability_start_date","getavailability_status","getdelete_flag"};
                    for (String strTemp : all_prod_avail_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                            DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                            DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            Log.info("EPRIDs => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                    " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                    " All_Views_EXT = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        /*if(dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getavailability_status()!=null) {

                            DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                            DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            Log.info("EPRIDs => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                    " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                    " All_Views_EXT = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }if(dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getdelta_answer_code_uk()==null){
                                DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                                DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);
                                method = objectToCompare1.getClass().getMethod(strTemp);
                                method2 = objectToCompare2.getClass().getMethod(strTemp);
                                Log.info("EPRIDs => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                        " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                        " All_Views_EXT = " + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));

                            }
                        }if(dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getdelta_answer_code_us()==null){
                            DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                            DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            Log.info("EPRIDs => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                    " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                    " All_Views_EXT = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));

                            }
                        }if(dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getpublication_status_anz()==null){
                            DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                            DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            Log.info("EPRIDs => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                    " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                    " All_Views_EXT = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));

                            }
                        }*/



                    }
                    break;
                case "product_extended_pricing_allsource_v":

                    Log.info("product_extended_pricing_allsource_v Records:");
                    //dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    //dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_prod_price_Col = {"getepr_id", "getsource", "getproduct_type", "getlast_updated_date", "getprice_currency", "getprice_amount",
                            "getprice_start_date","getprice_end_date","getprice_region","getprice_category","getprice_customer_category","getprice_purchase_quantity"};
                    for (String strTemp : all_prod_price_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                " All_Views_EXT = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "manifestation_extended_allsource_v":

                    Log.info("manifestation_extended_allsource_v Records:");
    //                    dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
 //                   dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_manif_ext_Col = {"getepr_id", "getsource","getlast_updated_date", "getmanifestation_type", "getuk_textbook_ind", "getus_textbook_ind", "getmanifestation_trim_text",
                            "getcommodity_code","getdiscount_code_emea","getdiscount_code_us"
                            ,"getmanifestation_weight","getjournal_issue_trim_size","getwar_reference",
                            "getexport_to_web_ind","getdelete_flag"};
                    for (String strTemp : all_manif_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRIDs => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                " All_Views_EXT = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "manifestation_extended_page_count_allsource_v":

                    Log.info("manifestation_extended_page_count_allsource_v Records:");
                  //  dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                   // dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_manif_page_ext_Col = {"getepr_id", "getsource", "getmanifestation_type","getlast_updated_date", "getcount_type_code", "getcount_type_name", "getcount", "getdelete_flag"};
                    for (String strTemp : all_manif_page_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                " All_Views_EXT = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "manifestation_extended_restriction_allsource_v":

                    Log.info("manifestation_extended_restriction_allsource_v Records:");
                    //dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    //dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_manif_restrict_ext_Col = {"getepr_id", "getsource", "getmanifestation_type","getlast_updated_date", "getrestriction_code", "getrestriction_name","getdelete_flag"};
                    for (String strTemp : all_manif_restrict_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                " All_Views_EXT = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_allsource_v":

                    Log.info("work_extended_allsource_v Records:");
                   // dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    //dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_work_ext_Col = {"getepr_id", "getsource", "getwork_type","getlast_updated_date", "getjournal_els_com_ind", "getjourns_aims_scope","getdelta_business_unit","getimage_file_ref","getmaster_isbn","getauthor_by_line_text",
                    "getkey_features","getproduct_awards","getproduct_long_desc","getproduct_short_desc","getreview_quotes","gettoc_long","gettoc_short","getaudience_text",
                    "getbook_sub_business_unit","getinternal_els_div","getprofit_centre","gettext_ref_trade","getprimary_site_system","getprimary_site_acronym","getprimary_site_support_level","getissue_prod_type_code",
                    "getcatalogue_volumes_qty","getcatalogue_issues_qty","getcatalogue_volume_from","getcatalogue_volume_to","getrf_issues_qty","getrf_total_pages_qty","getrf_fvi",
                    "getrf_lvi","getbusiness_unit_desc","getjournal_prod_site_code","getdelete_flag"};
                    for (String strTemp : all_work_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                " All_Views_EXT = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_metric_allsource_v":

                    Log.info("work_extended_metric_allsource_v Records:");
                    dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_work_metric_ext_Col = {"getepr_id", "getsource", "getwork_type","getlast_updated_date", "getmetric_code", "getmetric_name","getmetric","getmetric_year","getmetric_url","getdelete_flag"};
                    for (String strTemp : all_work_metric_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                " All_Views_EXT = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_person_role_allsource_v":

                    Log.info("work_extended_person_role_allsource_v Records:");
                   /* dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));
                    dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getrole_code)); //sort primarykey data in the lists
                    dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getrole_code));
*/
                    String[] all_work_pers_role_ext_Col = {"getepr_id", "getsource", "getwork_type","getlast_updated_date", "getcore_work_person_role_id", "getrole_code","getrole_name","getsequence_number","getgroup_number","getfirst_name",
                    "getlast_name","getpeoplehub_id","getemail","gettitle","gethonours","getaffiliation","getimage_url","getfootnote_txt","getnotes_txt","getdelete_flag"};
                    for (String strTemp : all_work_pers_role_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                " All_Views_EXT = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_relationship_sibling_allsource_v":

                    Log.info("work_extended_relationship_sibling_allsource_v Records:");
                  //  dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                   // dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_work_relation_sibling_ext_Col = {"getepr_id", "getsource", "getwork_type","getlast_updated_date", "getcore_work_person_role_id", "getrole_code","getrole_name","getsequence_number","getgroup_number","getfirst_name",
                            "getlast_name","getpeoplehub_id","getemail","gettitle","gethonours","getaffiliation","getimage_url","getfootnote_txt","getnotes_txt","getdelete_flag"};
                    for (String strTemp : all_work_relation_sibling_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                " All_Views_EXT = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_subject_area_allsource_v":

                    Log.info("work_extended_subject_area_allsource_v Records:");
                   // dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    //dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_work_subj_area_ext_Col = {"getepr_id", "getsource", "getwork_type","getlast_updated_date", "getcode", "getname","getpriority","gettype_code","gettype_name","getdelete_flag"};
                    for (String strTemp : all_work_subj_area_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                " All_Views_EXT = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_url_allsource_v":

                    Log.info("work_extended_url_allsource_v Records:");
                   // dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                   // dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_work_url_ext_Col = {"getepr_id", "getsource", "getwork_type","getlast_updated_date", "geturl_type_code", "geturl_type_name","geturl","geturl_title","getdelete_flag"};
                    for (String strTemp : all_work_url_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                " All_Views_EXT = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "work_extended_editorial_board_allsource_v":

                    Log.info("work_extended_editorial_board_allsource_v Records:");
                    dataQualityDLExtViewContext.recordsFromSourceIngestTable.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id)); //sort primarykey data in the lists
                    dataQualityDLExtViewContext.recordsFromAllExtViews.sort(Comparator.comparing(DL_ExtendedViewsAccessObject::getepr_id));

                    String[] all_work_edit_ext_Col = {"getepr_id", "getsource", "getwork_type","getlast_updated_date", "getrole_code", "getrole_name","getsequence_number","getgroup_number","getfirst_name","getlast_name" ,
                            "gettitle","gethonours","getaffiliation","getimage_url","getfootnote_txt","getnotes_txt","getdelete_flag"};
                    for (String strTemp : all_work_edit_ext_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_ExtendedViewsAccessObject objectToCompare1 = dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i);
                        DL_ExtendedViewsAccessObject objectToCompare2 = dataQualityDLExtViewContext.recordsFromAllExtViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EPRID => " + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id() +
                                " " + strTemp + " => SourceIngest = " + method.invoke(objectToCompare1) +
                                " All_Views_EXT = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " are mismatch/not found in All Views for EPRID:" + dataQualityDLExtViewContext.recordsFromSourceIngestTable.get(i).getepr_id(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
            }
        }

    }

}










