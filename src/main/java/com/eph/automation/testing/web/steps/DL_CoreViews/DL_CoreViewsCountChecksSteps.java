package com.eph.automation.testing.web.steps.DL_CoreViews;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.DL_CoreViewCountChecksSQL.DL_CoreViewCountChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import com.eph.automation.testing.models.contexts.DL_CoreViewsAccessContext;
import com.eph.automation.testing.models.dao.DL_CoreViews.DL_CoreViewsAccessObject;
import com.google.common.base.Joiner;
import java.lang.reflect.InvocationTargetException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Comparator;

public class DL_CoreViewsCountChecksSteps {
    private static String DLCoreSQLViewCount;
    private static int DLCoreViewCount;
    private static String BCSJMCoreSQLCount;
    private static int BCSJMCoreCount;

    public DL_CoreViewsAccessContext dataQualityDLCoreViewContext;
    private static String sql;
    private static List<String> Ids;


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


    @Given("^Get the (.*) from JM and BCS Core Tables (.*)$")
    public void getRandomIdsFromBCSJM(String numberOfRecords, String tableName) {
       // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids from JM and BCS Core Tables....");

        switch (tableName) {
            case "all_accountable_product_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_ACC_PROD_RAND_ID, numberOfRecords);
                break;
            case "all_manifestation_identifiers_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_MANIF_IDENT_RAND_ID, numberOfRecords);
                break;
            case "all_manifestation_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_MANIF_RAND_ID, numberOfRecords);
                break;
            case "all_person_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_PERSON_RAND_ID, numberOfRecords);
                break;
            case "all_product_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_PRODUCT_RAND_ID, numberOfRecords);
                break;
            case "all_work_identifier_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_IDENT_RAND_ID, numberOfRecords);
                break;
            case "all_work_person_role_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_PERS_ROLE_RAND_ID, numberOfRecords);
                break;
            case "all_work_relationship_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_RELATION_RAND_ID, numberOfRecords);
                break;
            case "all_work_subject_areas_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_SUBJ_AREA_RAND_ID, numberOfRecords);
                break;
            case "all_work_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_RAND_ID, numberOfRecords);
                break;
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("external_reference")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @Then("^Get the Records from the JM and BCS Core Tables (.*)$")
    public void getRecFromJMAndBCS(String tableName){
        Log.info("We get the records from JM and BCS Core table...");
        switch (tableName) {
            case "all_accountable_product_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_ACC_PROD_REC, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_identifiers_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_MANIF_IDENT_REC, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_MANIF_REC, Joiner.on("','").join(Ids));
                break;
            case "all_person_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_PERSON_REC, Joiner.on("','").join(Ids));
                break;
            case "all_product_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_PRODUCT_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_identifier_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_IDENT_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_person_role_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_PERS_ROLE_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_relationship_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_RELATION_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_subject_areas_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_SUBJ_AREA_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_BCS_JM_CORE_WORK_REC, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityDLCoreViewContext.recordsFromBCSJMData = DBManager.getDBResultAsBeanList(sql, DL_CoreViewsAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And ("^Get the Records from the DL core views (.*)$")
    public void     getRecFromAllViews(String tableName){
        Log.info("We get the records from JM and BCS Core table...");
        switch (tableName) {
            case "all_accountable_product_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_ACC_PROD_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_identifiers_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_MANIF_IDENT_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_MANIF_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_person_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_PERSON_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_product_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_PRODUCT_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_identifier_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_WRK_IDENT_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_person_role_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_WRK_PERS_ROLE_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_relationship_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_WRK_RELT_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_subject_areas_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_WORK_SUB_AREA_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_v":
                sql = String.format(DL_CoreViewCountChecksSQL.GET_DL_CORE_ALL_WORK_VIEW_REC, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityDLCoreViewContext.recordsFromAllViews = DBManager.getDBResultAsBeanList(sql, DL_CoreViewsAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare data of BCS and JM Core with DL Core views (.*) are identical$")
    public void compareCurrentandCurrHist(String targetTableName) {
        if (dataQualityDLCoreViewContext.recordsFromBCSJMData.isEmpty()) {
            Log.info("No Data Found in the BCS/JM Core Tables ....");
        } else {
            Log.info("Sorting the Ids to compare the records between DL core All views...");
        }
        for (int i = 0; i < dataQualityDLCoreViewContext.recordsFromBCSJMData.size(); i++) {
            switch (targetTableName) {
                case "all_accountable_product_v":
                    Log.info("all_accountable_product_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));
                    Log.info("BCS/JM_Acc_Prod -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            "All_Views_Acc_Prod -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE() != null)) {
                        Assert.assertEquals("The EXTERNALREFERENCE is =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() + " is missing/not found in All_Views_Acc_Prod",
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " WORKREFERENCE => BCS/JM_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKREFERENCE() +
                            " All_Views_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKREFERENCE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKREFERENCE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKREFERENCE() != null)) {
                        Assert.assertEquals("The WORKREFERENCE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKREFERENCE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " GL_PRODUCT_SEGMENT_CODE => BCS/JM_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGL_PRODUCT_SEGMENT_CODE() +
                            " All_Views_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGL_PRODUCT_SEGMENT_CODE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGL_PRODUCT_SEGMENT_CODE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGL_PRODUCT_SEGMENT_CODE() != null)) {
                        Assert.assertEquals("The GL_PRODUCT_SEGMENT_CODE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGL_PRODUCT_SEGMENT_CODE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGL_PRODUCT_SEGMENT_CODE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " GL_PRODUCT_SEGMENT_NAME => BCS/JM_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGL_PRODUCT_SEGMENT_NAME() +
                            " All_Views_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGL_PRODUCT_SEGMENT_NAME());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGL_PRODUCT_SEGMENT_NAME() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGL_PRODUCT_SEGMENT_NAME() != null)) {
                        Assert.assertEquals("The GL_PRODUCT_SEGMENT_NAME is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGL_PRODUCT_SEGMENT_NAME(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGL_PRODUCT_SEGMENT_NAME());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " GL_PRODUCT_SEGMENT_PARENT => BCS/JM_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGL_PRODUCT_SEGMENT_PARENT() +
                            " All_Views_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGL_PRODUCT_SEGMENT_PARENT());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGL_PRODUCT_SEGMENT_PARENT() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGL_PRODUCT_SEGMENT_PARENT() != null)) {
                        Assert.assertEquals("The GL_PRODUCT_SEGMENT_PARENT is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGL_PRODUCT_SEGMENT_PARENT(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGL_PRODUCT_SEGMENT_PARENT());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " DQ_ERR => BCS/JM_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() +
                            " All_Views_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The GL_PRODUCT_SEGMENT_PARENT is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " WORKSOURCEREF => BCS/JM_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF() +
                            " All_Views_Acc_Prod =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The WORKSOURCEREF is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF());
                    }


/*

                   dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] JMF_WorkColumnName = {"WORKSOURCEREF","DQ_ERR"};
                    for (String strTemp : JMF_WorkColumnName) {

                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);


                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => BCS/DL=" + method.invoke(objectToCompare1) +
                                " " + strTemp + " ALL Views=" + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

*/
                    break;

                    case "all_manifestation_identifiers_v":
                        Log.info("all_manifestation_identifiers_v Records:");
                        dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                        dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));
                        Log.info("BCS/JM_ManifIdent -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                "All_ManifIdent_Views -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                        if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() != null ||
                                (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE() != null)) {
                            Assert.assertEquals("The EXTERNALREFERENCE is =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() + " is missing/not found in All_ManifIdent_Views",
                                    dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                        }

                        Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " IDENTIFIER => BCS/JM_ManifIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getIDENTIFIER() +
                                " All_ManifIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getIDENTIFIER());

                        if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getIDENTIFIER() != null ||
                                (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getIDENTIFIER() != null)) {
                            Assert.assertEquals("The IDENTIFIER is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getIDENTIFIER(),
                                    dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getIDENTIFIER());
                        }

                        Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " EFFECTIVE_START_DATE => BCS/JM_ManifIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE() +
                                " All_ManifIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE());

                        if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE() != null ||
                                (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE() != null)) {
                            Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE(),
                                    dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE());
                        }

                        Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " EFFECTIVE_END_DATE => BCS/JM_ManifIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE() +
                                " All_ManifIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE());

                        if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE() != null ||
                                (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE() != null)) {
                            Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE(),
                                    dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE());
                        }

                        Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " F_TYPE     => BCS/JM_ManifIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() +
                                " All_ManifIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());

                        if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() != null ||
                                (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE() != null)) {
                            Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE(),
                                    dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());
                        }

                        Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " F_MANIFESTATION => BCS/JM_ManifIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_MANIFESTATION() +
                                " All_ManifIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_MANIFESTATION());

                        if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_MANIFESTATION() != null ||
                                (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_MANIFESTATION() != null)) {
                            Assert.assertEquals("The F_MANIFESTATION is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_MANIFESTATION(),
                                    dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_MANIFESTATION());
                        }

                        Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " MANIFESTATIONSOURCEREF => BCS/JM_ManifIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getMANIFESTATIONSOURCEREF() +
                                " All_ManifIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getMANIFESTATIONSOURCEREF());

                        if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getMANIFESTATIONSOURCEREF() != null ||
                                (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getMANIFESTATIONSOURCEREF() != null)) {
                            Assert.assertEquals("The MANIFESTATIONSOURCEREF is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getMANIFESTATIONSOURCEREF(),
                                    dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getMANIFESTATIONSOURCEREF());
                        }
                        break;

                case "all_manifestation_v":

                    Log.info("all_manifestation_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));
                    Log.info("BCS/JM_Manif -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            "All_Manif_Views -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE() != null)) {
                        Assert.assertEquals("The EXTERNALREFERENCE is =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() + " is missing/not found in All_Manif_Views",
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EPH_MANIF_ID => BCS/JM_Manif =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEPH_MANIF_ID() +
                            " All_Manif_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEPH_MANIF_ID());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEPH_MANIF_ID() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEPH_MANIF_ID() != null)) {
                        Assert.assertEquals("The EPH_MANIF_ID is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEPH_MANIF_ID(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEPH_MANIF_ID());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " MANIF_KEY_TITLE => BCS/JM_Manif =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getMANIF_KEY_TITLE() +
                            " All_Manif_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getMANIF_KEY_TITLE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getMANIF_KEY_TITLE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getMANIF_KEY_TITLE() != null)) {
                        Assert.assertEquals("The MANIF_KEY_TITLE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getMANIF_KEY_TITLE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getMANIF_KEY_TITLE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " INTEREDITIONFLAG => BCS/JM_Manif =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getINTEREDITIONFLAG() +
                            " All_Manif_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getINTEREDITIONFLAG());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getINTEREDITIONFLAG() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getINTEREDITIONFLAG() != null)) {
                        Assert.assertEquals("The INTEREDITIONFLAG is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getINTEREDITIONFLAG(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getINTEREDITIONFLAG());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " FIRSTPUBLISHEDDATE     => BCS/JM_Manif =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getFIRSTPUBLISHEDDATE() +
                            " All_Manif_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getFIRSTPUBLISHEDDATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getFIRSTPUBLISHEDDATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getFIRSTPUBLISHEDDATE() != null)) {
                        Assert.assertEquals("The FIRSTPUBLISHEDDATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getFIRSTPUBLISHEDDATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getFIRSTPUBLISHEDDATE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " LASTPUBDATE => BCS/JM_Manif =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getLASTPUBDATE() +
                            " All_Manif_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getLASTPUBDATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getLASTPUBDATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getLASTPUBDATE() != null)) {
                        Assert.assertEquals("The LASTPUBDATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getLASTPUBDATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getLASTPUBDATE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_TYPE => BCS/JM_Manif =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() +
                            " All_Manif_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE() != null)) {
                        Assert.assertEquals("The F_TYPE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " FORMAT_TYPE => BCS/JM_Manif =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getFORMAT_TYPE() +
                            " All_Manif_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getFORMAT_TYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getFORMAT_TYPE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getFORMAT_TYPE() != null)) {
                        Assert.assertEquals("The FORMAT_TYPE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getFORMAT_TYPE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getFORMAT_TYPE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getFORMAT_TYPE());
                    }
                    break;

                case "all_person_v":
                    Log.info("all_person_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    Log.info("BCS/JM_Person -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            "All_Person_Views -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE() != null)) {
                        Assert.assertEquals("The EXTERNALREFERENCE is =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() + " is missing/not found in All_Person_Views",
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " GIVENNAME => BCS/JM_Person =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGIVENNAME() +
                            " All_Person_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGIVENNAME());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGIVENNAME() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGIVENNAME() != null)) {
                        Assert.assertEquals("The GIVENNAME is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getGIVENNAME(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getGIVENNAME());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " FAMILYNAME => BCS/JM_Person =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getFAMILYNAME() +
                            " All_Person_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getFAMILYNAME());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getFAMILYNAME() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getFAMILYNAME() != null)) {
                        Assert.assertEquals("The FAMILYNAME is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getFAMILYNAME(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getFAMILYNAME());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " PEOPLEHUBID => BCS/JM_Person =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPEOPLEHUBID() +
                            " All_Person_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPEOPLEHUBID());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPEOPLEHUBID() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPEOPLEHUBID() != null)) {
                        Assert.assertEquals("The PEOPLEHUBID is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPEOPLEHUBID(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPEOPLEHUBID());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EMAIL => BCS/JM_Person =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEMAIL() +
                            " All_Person_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEMAIL());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEMAIL() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEMAIL() != null)) {
                        Assert.assertEquals("The EMAIL is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEMAIL(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEMAIL());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " DQ_ERR => BCS/JM_Person =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() +
                            " All_Person_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());
                    }
                    break;

                case "all_product_v":
                    Log.info("all_product_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    Log.info("BCS/JM_Prod -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            "All_Prod_Views -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE() != null)) {
                        Assert.assertEquals("The EXTERNALREFERENCE is =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() + " is missing/not found in All_Prod_Views",
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " NAME => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getNAME() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getNAME());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getNAME() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getNAME() != null)) {
                        Assert.assertEquals("The NAME is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getNAME(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getNAME());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " SHORTNAME => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSHORTNAME() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSHORTNAME());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSHORTNAME() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSHORTNAME() != null)) {
                        Assert.assertEquals("The SHORTNAME is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSHORTNAME(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSHORTNAME());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " SEPRATELYSALEINDICATOR => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSEPRATELYSALEINDICATOR() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSEPRATELYSALEINDICATOR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSEPRATELYSALEINDICATOR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSEPRATELYSALEINDICATOR() != null)) {
                        Assert.assertEquals("The SEPRATELYSALEINDICATOR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSEPRATELYSALEINDICATOR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSEPRATELYSALEINDICATOR());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " TRIALALLOWEDINDICATOR => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getTRIALALLOWEDINDICATOR() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getTRIALALLOWEDINDICATOR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getTRIALALLOWEDINDICATOR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getTRIALALLOWEDINDICATOR() != null)) {
                        Assert.assertEquals("The TRIALALLOWEDINDICATOR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getTRIALALLOWEDINDICATOR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getTRIALALLOWEDINDICATOR());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " RESTRICTEDSALEINDICATOR => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getRESTRICTEDSALEINDICATOR() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getRESTRICTEDSALEINDICATOR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getRESTRICTEDSALEINDICATOR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getRESTRICTEDSALEINDICATOR() != null)) {
                        Assert.assertEquals("The RESTRICTEDSALEINDICATOR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getRESTRICTEDSALEINDICATOR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getRESTRICTEDSALEINDICATOR());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " LAUNCHDATE => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getLAUNCHDATE() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getLAUNCHDATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getLAUNCHDATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getLAUNCHDATE() != null)) {
                        Assert.assertEquals("The LAUNCHDATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getLAUNCHDATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getLAUNCHDATE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " CONTENTFROMDATE => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCONTENTFROMDATE() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCONTENTFROMDATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCONTENTFROMDATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCONTENTFROMDATE() != null)) {
                        Assert.assertEquals("The CONTENTFROMDATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCONTENTFROMDATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCONTENTFROMDATE());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " CONTENTTODATE => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCONTENTTODATE() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCONTENTTODATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCONTENTTODATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCONTENTTODATE() != null)) {
                        Assert.assertEquals("The CONTENTTODATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCONTENTTODATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCONTENTTODATE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " CONTENTDATEOFFSET => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCONTENTDATEOFFSET() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCONTENTDATEOFFSET());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCONTENTDATEOFFSET() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCONTENTDATEOFFSET() != null)) {
                        Assert.assertEquals("The CONTENTDATEOFFSET is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCONTENTDATEOFFSET(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCONTENTDATEOFFSET());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_TYPE => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE() != null)) {
                        Assert.assertEquals("The F_TYPE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_STATUS => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_STATUS() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_STATUS() != null)) {
                        Assert.assertEquals("The F_STATUS is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_STATUS(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_STATUS());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_ACCOUNTABLEPRODUCT => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_ACCOUNTABLEPRODUCT() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_ACCOUNTABLEPRODUCT());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_ACCOUNTABLEPRODUCT() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_ACCOUNTABLEPRODUCT() != null)) {
                        Assert.assertEquals("The F_ACCOUNTABLEPRODUCT is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_ACCOUNTABLEPRODUCT(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_ACCOUNTABLEPRODUCT());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_TAXCODE => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TAXCODE() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TAXCODE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TAXCODE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TAXCODE() != null)) {
                        Assert.assertEquals("The F_TAXCODE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TAXCODE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TAXCODE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_REVENUEMODEL => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_REVENUEMODEL() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_REVENUEMODEL());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_REVENUEMODEL() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_REVENUEMODEL() != null)) {
                        Assert.assertEquals("The F_REVENUEMODEL is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_REVENUEMODEL(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_REVENUEMODEL());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_REVENUEACC => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_REVENUEACC() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_REVENUEACC());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_REVENUEACC() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_REVENUEACC() != null)) {
                        Assert.assertEquals("The F_REVENUEACC is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_REVENUEACC(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_REVENUEACC());
                    }


                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_WWORK => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_WWORK() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_WWORK());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_WWORK() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_WWORK() != null)) {
                        Assert.assertEquals("The F_WWORK is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_WWORK(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_WWORK());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " WORKREFERENCE => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKREFERENCE() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKREFERENCE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKREFERENCE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKREFERENCE() != null)) {
                        Assert.assertEquals("The WORKREFERENCE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKREFERENCE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_MANIFESTATION => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_MANIFESTATION() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_MANIFESTATION());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_MANIFESTATION() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_MANIFESTATION() != null)) {
                        Assert.assertEquals("The F_MANIFESTATION is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_MANIFESTATION(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_MANIFESTATION());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " MANIFESTATIONREF => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getMANIFESTATIONREF() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getMANIFESTATIONREF());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getMANIFESTATIONREF() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getMANIFESTATIONREF() != null)) {
                        Assert.assertEquals("The MANIFESTATIONREF is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getMANIFESTATIONREF(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getMANIFESTATIONREF());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " DQ_ERR => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " UPDATE_TYPE => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getUPDATE_TYPE() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getUPDATE_TYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getUPDATE_TYPE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getUPDATE_TYPE() != null)) {
                        Assert.assertEquals("The UPDATE_TYPE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getUPDATE_TYPE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getUPDATE_TYPE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " WORKROLLUPTYPE => BCS/JM_Prod =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKROLLUPTYPE() +
                            " All_Prod_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKROLLUPTYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKROLLUPTYPE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKROLLUPTYPE() != null)) {
                        Assert.assertEquals("The WORKROLLUPTYPE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKROLLUPTYPE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKROLLUPTYPE());
                    }
                    break;

                case "all_work_identifier_v":
                    Log.info("all_work_identifier_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    Log.info("BCS/JM_WorkIdent -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            "All_WorkIdent_Views -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE() != null)) {
                        Assert.assertEquals("The EXTERNALREFERENCE is =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() + " is missing/not found in All_WorkIdent_Views",
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " IDENTIFIER => BCS/JM_WorkIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getIDENTIFIER() +
                            " All_WorkIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getIDENTIFIER());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getIDENTIFIER() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getIDENTIFIER() != null)) {
                        Assert.assertEquals("The IDENTIFIER is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getIDENTIFIER(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getIDENTIFIER());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EFFECTIVE_START_DATE => BCS/JM_WorkIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE() +
                            " All_WorkIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE() != null)) {
                        Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EFFECTIVE_END_DATE => BCS/JM_WorkIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE() +
                            " All_WorkIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE() != null)) {
                        Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_TYPE => BCS/JM_WorkIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() +
                            " All_WorkIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE() != null)) {
                        Assert.assertEquals("The F_TYPE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_WWORK => BCS/JM_WorkIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() +
                            " All_WorkIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_WWORK() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_WWORK() != null)) {
                        Assert.assertEquals("The F_WWORK is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_WWORK(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_WWORK());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " WORKSOURCEREF => BCS/JM_WorkIdent =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF() +
                            " All_WorkIdent_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The WORKSOURCEREF is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF());
                    }
                    break;

                case "all_work_person_role_v":

                    Log.info("all_work_person_role_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    Log.info("BCS/JM_WorkPers -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            "All_WorkPerson_Views -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE() != null)) {
                        Assert.assertEquals("The EXTERNALREFERENCE is =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() + " is missing/not found in All_WorkPerson_Views",
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EFFECTIVE_START_DATE => BCS/JM_WorkPers =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE() +
                            " All_WorkPerson_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE() != null)) {
                        Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EFFECTIVE_END_DATE => BCS/JM_WorkPers =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE() +
                            " All_WorkPerson_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE() != null)) {
                        Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_ROLE => BCS/JM_WorkPers =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_ROLE() +
                            " All_WorkPerson_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_ROLE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_ROLE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_ROLE() != null)) {
                        Assert.assertEquals("The F_ROLE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_ROLE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_ROLE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_WWORK => BCS/JM_WorkPers =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_WWORK() +
                            " All_WorkPerson_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_WWORK());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_WWORK() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_WWORK() != null)) {
                        Assert.assertEquals("The F_WWORK is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_WWORK(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_WWORK());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " WORKSOURCEREF => BCS/JM_WorkPers =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF() +
                            " All_WorkPerson_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The WORKSOURCEREF is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " PERSONSOURCEREF => BCS/JM_WorkPers =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPERSONSOURCEREF() +
                            " All_WorkPerson_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPERSONSOURCEREF());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPERSONSOURCEREF() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPERSONSOURCEREF() != null)) {
                        Assert.assertEquals("The PERSONSOURCEREF is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPERSONSOURCEREF(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPERSONSOURCEREF());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " PERSONEMAIL => BCS/JM_WorkPers =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPERSONEMAIL() +
                            " All_WorkPerson_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPERSONEMAIL());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPERSONEMAIL() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPERSONEMAIL() != null)) {
                        Assert.assertEquals("The PERSONEMAIL is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPERSONEMAIL(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPERSONEMAIL());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " DQ_ERR => BCS/JM_WorkPers =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() +
                            " All_WorkPerson_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());
                    }
                    break;

                case "all_work_relationship_v":
                    Log.info("all_work_relationship_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    Log.info("BCS/JM_WorkRelat -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            "All_WorkRelat_Views -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE() != null)) {
                        Assert.assertEquals("The EXTERNALREFERENCE is =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() + " is missing/not found in All_WorkRelat_Views",
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EFFECTIVE_START_DATE => BCS/JM_WorkRelat =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE() +
                            " All_WorkRelat_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE() != null)) {
                        Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EFFECTIVE_END_DATE => BCS/JM_WorkRelat =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE() +
                            " All_WorkRelat_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE() != null)) {
                        Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " DQ_ERR => BCS/JM_WorkRelat =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() +
                            " All_WorkRelat_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " PARENTWORKSOURCEREF => BCS/JM_WorkRelat =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPARENTWORKSOURCEREF() +
                            " All_WorkRelat_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPARENTWORKSOURCEREF());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPARENTWORKSOURCEREF() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPARENTWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The PARENTWORKSOURCEREF is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPARENTWORKSOURCEREF(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPARENTWORKSOURCEREF());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " CHILDWORKSOURCEREF => BCS/JM_WorkRelat =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCHILDWORKSOURCEREF() +
                            " All_WorkRelat_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCHILDWORKSOURCEREF());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCHILDWORKSOURCEREF() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCHILDWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The CHILDWORKSOURCEREF is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCHILDWORKSOURCEREF(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCHILDWORKSOURCEREF());
                    }
                    break;
                case "all_work_subject_areas_v":
                    Log.info("all_work_subject_areas_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    Log.info("BCS/JM_WorkSubj -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            "All_WorkSubj_Views -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE() != null)) {
                        Assert.assertEquals("The EXTERNALREFERENCE is =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() + " is missing/not found in All_WorkSubj_Views",
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EFFECTIVE_START_DATE => BCS/JM_WorkSubj =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE() +
                            " All_WorkSubj_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE() != null)) {
                        Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_START_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_START_DATE());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EFFECTIVE_END_DATE => BCS/JM_WorkSubj =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE() +
                            " All_WorkSubj_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE() != null)) {
                        Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEFFECTIVE_END_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEFFECTIVE_END_DATE());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " DQ_ERR => BCS/JM_WorkSubj =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() +
                            " All_WorkSubj_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_SUBJECTAREA => BCS/JM_WorkSubj =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_SUBJECTAREA() +
                            " All_WorkSubj_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_SUBJECTAREA());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_SUBJECTAREA() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_SUBJECTAREA() != null)) {
                        Assert.assertEquals("The F_SUBJECTAREA is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_SUBJECTAREA(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_SUBJECTAREA());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_WWORK => BCS/JM_WorkSubj =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_WWORK() +
                            " All_WorkSubj_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_WWORK());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_WWORK() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_WWORK() != null)) {
                        Assert.assertEquals("The F_WWORK is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_WWORK(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_WWORK());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " WORKSOURCEREF => BCS/JM_WorkSubj =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF() +
                            " All_WorkSubj_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF() != null)) {
                        Assert.assertEquals("The WORKSOURCEREF is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSOURCEREF(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSOURCEREF());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " SUBJECTAREAREF => BCS/JM_WorkSubj =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSUBJECTAREAREF() +
                            " All_WorkSubj_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSUBJECTAREAREF());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSUBJECTAREAREF() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSUBJECTAREAREF() != null)) {
                        Assert.assertEquals("The SUBJECTAREAREF is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSUBJECTAREAREF(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSUBJECTAREAREF());
                    }
                    break;

                case "all_work_v":
                    Log.info("all_work_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    Log.info("BCS/JM_Core -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            "All_Work_Views -> EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE() != null)) {
                        Assert.assertEquals("The EXTERNALREFERENCE is =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() + " is missing/not found in All_Work_Views",
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEXTERNALREFERENCE());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EPR => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEPR() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEPR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEPR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEPR() != null)) {
                        Assert.assertEquals("The EPR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEPR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEPR());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " WORKTITLE => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKTITLE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKTITLE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKTITLE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKTITLE() != null)) {
                        Assert.assertEquals("The WORKTITLE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKTITLE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKTITLE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " WORKSUBTITLE => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSUBTITLE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSUBTITLE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSUBTITLE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSUBTITLE() != null)) {
                        Assert.assertEquals("The WORKSUBTITLE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getWORKSUBTITLE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getWORKSUBTITLE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " ELECTRORIGHTINDICATOR => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getELECTRORIGHTINDICATOR() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getELECTRORIGHTINDICATOR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getELECTRORIGHTINDICATOR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getELECTRORIGHTINDICATOR() != null)) {
                        Assert.assertEquals("The ELECTRORIGHTINDICATOR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getELECTRORIGHTINDICATOR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getELECTRORIGHTINDICATOR());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " VOLUME => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getVOLUME() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getVOLUME());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getVOLUME() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getVOLUME() != null)) {
                        Assert.assertEquals("The VOLUME is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getVOLUME(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getVOLUME());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " COPYRIGHTYEAR => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCOPYRIGHTYEAR() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCOPYRIGHTYEAR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCOPYRIGHTYEAR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCOPYRIGHTYEAR() != null)) {
                        Assert.assertEquals("The COPYRIGHTYEAR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getCOPYRIGHTYEAR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getCOPYRIGHTYEAR());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " EDITIONNO => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEDITIONNO() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEDITIONNO());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEDITIONNO() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEDITIONNO() != null)) {
                        Assert.assertEquals("The EDITIONNO is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEDITIONNO(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getEDITIONNO());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_PMC => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_PMC() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_PMC());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_PMC() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_PMC() != null)) {
                        Assert.assertEquals("The F_PMC is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_PMC(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_PMC());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_OA_JOURNAL_TYPE => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_OA_JOURNAL_TYPE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_OA_JOURNAL_TYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_OA_JOURNAL_TYPE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_OA_JOURNAL_TYPE() != null)) {
                        Assert.assertEquals("The F_OA_JOURNAL_TYPE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_OA_JOURNAL_TYPE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_OA_JOURNAL_TYPE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_TYPE  => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE() != null)) {
                        Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_STATUS => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_TYPE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_TYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_STATUS() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_STATUS() != null)) {
                        Assert.assertEquals("The F_STATUS is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_STATUS(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_STATUS());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_IMPRINT => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_IMPRINT() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_IMPRINT());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_IMPRINT() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_IMPRINT() != null)) {
                        Assert.assertEquals("The F_IMPRINT is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_IMPRINT(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_IMPRINT());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " F_SOCIETY_OWNERSHIP => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_SOCIETY_OWNERSHIP() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_SOCIETY_OWNERSHIP());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_SOCIETY_OWNERSHIP() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_SOCIETY_OWNERSHIP() != null)) {
                        Assert.assertEquals("The F_SOCIETY_OWNERSHIP is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getF_SOCIETY_OWNERSHIP(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getF_SOCIETY_OWNERSHIP());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " RESP_CENTRE => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getRESP_CENTRE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getRESP_CENTRE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getRESP_CENTRE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getRESP_CENTRE() != null)) {
                        Assert.assertEquals("The RESP_CENTRE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getRESP_CENTRE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getRESP_CENTRE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " OPCO => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getOPCO() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getOPCO());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getOPCO() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getOPCO() != null)) {
                        Assert.assertEquals("The OPCO is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getOPCO(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getOPCO());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " LANGUAGECODE => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getLANGUAGECODE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getLANGUAGECODE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getLANGUAGECODE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getLANGUAGECODE() != null)) {
                        Assert.assertEquals("The LANGUAGECODE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getLANGUAGECODE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getLANGUAGECODE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " SUBSCRIPTIONTYPE => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSUBSCRIPTIONTYPE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSUBSCRIPTIONTYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSUBSCRIPTIONTYPE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSUBSCRIPTIONTYPE() != null)) {
                        Assert.assertEquals("The SUBSCRIPTIONTYPE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getSUBSCRIPTIONTYPE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getSUBSCRIPTIONTYPE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " PLANNED_LAUNCH_DATE => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPLANNED_LAUNCH_DATE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPLANNED_LAUNCH_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPLANNED_LAUNCH_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPLANNED_LAUNCH_DATE() != null)) {
                        Assert.assertEquals("The PLANNED_LAUNCH_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPLANNED_LAUNCH_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPLANNED_LAUNCH_DATE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " ACTUAL_LAUNCH_DATE => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getACTUAL_LAUNCH_DATE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getACTUAL_LAUNCH_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getACTUAL_LAUNCH_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getACTUAL_LAUNCH_DATE() != null)) {
                        Assert.assertEquals("The ACTUAL_LAUNCH_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getACTUAL_LAUNCH_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getACTUAL_LAUNCH_DATE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " PLANNED_TERMINATION_DATE => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPLANNED_TERMINATION_DATE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPLANNED_TERMINATION_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPLANNED_TERMINATION_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPLANNED_TERMINATION_DATE() != null)) {
                        Assert.assertEquals("The PLANNED_TERMINATION_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getPLANNED_TERMINATION_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getPLANNED_TERMINATION_DATE());
                    }

                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " ACTUAL_TERMINATION_DATE => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getACTUAL_TERMINATION_DATE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getACTUAL_TERMINATION_DATE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getACTUAL_TERMINATION_DATE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getACTUAL_TERMINATION_DATE() != null)) {
                        Assert.assertEquals("The ACTUAL_TERMINATION_DATE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getACTUAL_TERMINATION_DATE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getACTUAL_TERMINATION_DATE());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " DQ_ERR => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR() != null)) {
                        Assert.assertEquals("The DQ_ERR is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getDQ_ERR(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getDQ_ERR());
                    }
                    Log.info("EXTERNALREFERENCE => " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                            " UPDATE_TYPE => BCS/JM_Core =" + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getUPDATE_TYPE() +
                            " All_Work_Views =" + dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getUPDATE_TYPE());

                    if (dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getUPDATE_TYPE() != null ||
                            (dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getUPDATE_TYPE() != null)) {
                        Assert.assertEquals("The UPDATE_TYPE is incorrect for EXTERNALREFERENCE = " + dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getUPDATE_TYPE(),
                                dataQualityDLCoreViewContext.recordsFromAllViews.get(i).getUPDATE_TYPE());
                    }
                    break;

            }

        }
    }
}










