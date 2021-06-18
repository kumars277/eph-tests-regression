package com.eph.automation.testing.steps.dataLake.DL_CoreViews;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.DL_CoreViewSQL.DL_CoreViewChecksSQL;
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

public class DL_CoreViewsChecksSteps {
    private static String DLCoreSQLViewCount;
    private static int DLCoreViewCount;
    private static String BCSJMCoreSQLCount;
    private static int BCSJMCoreCount;
    private static int BCSJMCExtRefFieldNullCount;
    private static String BCSJMExtRefNullVal;

    public DL_CoreViewsAccessContext dataQualityDLCoreViewContext;
    private static String sql;
    private static List<String> Ids;


    @Given("^Get the total count of DL Core views (.*)$")
    public void getDLCoreViewCount (String tableName) {
        switch (tableName){
            case "all_accountable_product_v":
                Log.info("Getting DL Core All Accountable Product View Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_ACC_PROD_VIEW_COUNT;
                break;
            case "all_manifestation_identifiers_v":
                Log.info("Getting DL Core All Manifestation Identifier View Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_MANIF_IDENT_VIEW_COUNT;
                break;
            case "all_manifestation_v":
                Log.info("Getting DL Core All Manifestation View Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_MANIF_VIEW_COUNT;
                break;
            case "all_person_v":
                Log.info("Getting DL Core All Person View Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_PERSON_VIEW_COUNT;
                break;
            case "all_product_v":
                Log.info("Getting DL Core All product view Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_PRODUCT_VIEW_COUNT;
                break;
            case "all_product_rel_package_v":
                Log.info("Getting DL Core All product Relation PAckage view Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_PRODUCT_REL_PKG_VIEW_COUNT;
                break;
            case "all_work_identifier_v":
                Log.info("Getting DL Core all work Identifier view Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WRK_IDENT_VIEW_COUNT;
                break;
            case "all_work_person_role_v":
                Log.info("Getting DL Core All work Person Role view Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WRK_PERS_ROLE_VIEW_COUNT;
                break;
            case "all_work_relationship_v":
                Log.info("Getting Dl Core all work relationship view Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WRK_RELT_VIEW_COUNT;
                break;
            case "all_work_subject_areas_v":
                Log.info("Getting DL Core all work subject areas view Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WORK_SUB_AREA_VIEW_COUNT;
                break;
            case "all_work_v":
                Log.info("Getting DL all work view Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WORK_VIEW_COUNT;
                break;
            case "all_work_legal_owner_v":
                Log.info("Getting DL all_work_legal_owner view Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WORK_LEGAL_OWNER_COUNT;
                break;

            case "all_work_access_model_v":
                Log.info("Getting DL all_work_access_model_v view Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WORK_ACCESS_MODEL_COUNT;
                break;
            case "all_work_business_model_v":
                Log.info("Getting DL all_work_business_model_v view Count...");
                DLCoreSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WORK_BUSINESS_MODEL_COUNT;
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
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_ACC_PROD_COUNT;
                break;
            case "all_manifestation_identifiers_v":
                Log.info("Getting BCS And JM Core Manifestation Identifier Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_MANIF_IDENT_COUNT;
                break;
            case "all_manifestation_v":
                Log.info("Getting BCS And JM Core Manifestation Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_MANIF_COUNT;
                break;
            case "all_person_v":
                Log.info("Getting BCS And JM Core Person Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_PERSON_COUNT;
                break;
            case "all_product_v":
                Log.info("Getting BCS And JM Core product Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_PRODUCT_COUNT;
                break;
            case "all_product_rel_package_v":
                Log.info("Getting BCS And JM Core product Relation PAckage view Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_PRODUCT_REL_PKG_VIEW_COUNT;
                break;
            case "all_work_identifier_v":
                Log.info("Getting BCS And JM Core work Identifier Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_IDENT_COUNT;
                break;
            case "all_work_person_role_v":
                Log.info("Getting BCS And JM Core work Person Role Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_PERS_ROLE_COUNT;
                break;
            case "all_work_relationship_v":
                Log.info("Getting BCS And JM Core work relationship Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_RELATION_COUNT;
                break;
            case "all_work_subject_areas_v":
                Log.info("Getting BCS And JM Core work subject areas Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_SUBJ_AREA_COUNT;
                break;
            case "all_work_v":
                Log.info("Getting BCS And JM work Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_COUNT;
                break;
            case "all_work_legal_owner_v":
                Log.info("Getting BCS And JM work legal owner Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_LEGAL_OWNER_COUNT;
                break;
            case "all_work_access_model_v":
                Log.info("Getting BCS And JM work_access_model_v Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_ACCESS_MODEL_COUNT;
                break;
            case "all_work_business_model_v":
                Log.info("Getting BCS And JM work_business_model_v Count...");
                BCSJMCoreSQLCount = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_BUSINESS_MODEL_COUNT;
                break;



        }
        Log.info(BCSJMCoreSQLCount);
        List<Map<String, Object>> BCSJMCoreTableCount = DBManager.getDBResultMap(BCSJMCoreSQLCount, Constants.AWS_URL);
        BCSJMCoreCount = ((Long) BCSJMCoreTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of BCS and JM Core with (.*) views are identical$")
    public void compareCoreAndInboundCounts(String tableName){
        Log.info("The count for all core "+tableName+" => " + DLCoreViewCount + " and in Source_Ingest => " + BCSJMCoreCount);
        Assert.assertEquals("The counts are not equal when compared with all core "+tableName+" and Source_Ingest core ", DLCoreViewCount, BCSJMCoreCount);
    }


    @Then("^Check whether externalReference field not holding any null value (.*)$")
    public void checkForNullVal(String tableName){
        switch (tableName){
            case "all_accountable_product_v":
                Log.info("Checking External Reference Field holds null values for all_accountable_product_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Acc_Prod_Ext_Ref_Null_count;
                break;
            case "all_manifestation_identifiers_v":
                Log.info("Checking External Reference Field holds null values for all_manifestation_identifiers_v ...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_manif_ident_Ext_Ref_Null_count;
                break;
            case "all_manifestation_v":
                Log.info("Checking External Reference Field holds null values for all_manifestation_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_manif_Ext_Ref_Null_count;
                break;
            case "all_person_v":
                Log.info("Checking External Reference Field holds null values for all_person_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Person_Ext_Ref_Null_count;
                break;
            case "all_product_v":
                Log.info("Checking External Reference Field holds null values for all_product_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Product_Ext_Ref_Null_count;
                break;
            case "all_product_rel_package_v":
                Log.info("Checking External Reference Field holds null values for all_product_rel_package_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Product_Rel_Pkg_Ext_Ref_Null_count;
                break;
            case "all_work_identifier_v":
                Log.info("Checking External Reference Field holds null values for all_work_identifier_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Work_identf_Ext_Ref_Null_count;
                break;
            case "all_work_person_role_v":
                Log.info("Checking External Reference Field holds null values for all_work_person_role_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Work_PERS_role_Ext_Ref_Null_count;
                break;
            case "all_work_relationship_v":
                Log.info("Checking External Reference Field holds null values for all_work_relationship_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Work_RELAtion_Ext_Ref_Null_count;
                break;
            case "all_work_subject_areas_v":
                Log.info("Checking External Reference Field holds null values for all_work_subject_areas_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Work_sub_area_Ext_Ref_Null_count;
                break;
            case "all_work_v":
                Log.info("Checking External Reference Field holds null values for all_work_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Work_Ext_Ref_Null_count;
                break;
            case "all_work_legal_owner_v":
                Log.info("Checking External Reference Field holds null values for all_work_legal_owner_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Work_legal_Ext_Ref_Null_count;
                break;
            case "all_work_access_model_v":
                Log.info("Checking External Reference Field holds null values for all_work_legal_owner_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Work_access_Ext_Ref_Null_count;
                break;
            case "all_work_business_model_v":
                Log.info("Checking External Reference Field holds null values for all_work_business_model_v...");
                BCSJMExtRefNullVal = DL_CoreViewChecksSQL.GET_BCS_JM_CORE_Work_business_Ext_Ref_Null_count;
                break;
        }
        Log.info(BCSJMExtRefNullVal);
        List<Map<String, Object>> BCSJMCoreTableExtRefNullCount = DBManager.getDBResultMap(BCSJMExtRefNullVal, Constants.AWS_URL);
        BCSJMCExtRefFieldNullCount = ((Long) BCSJMCoreTableExtRefNullCount.get(0).get("Null_COunt")).intValue();
    }

    @And ("^Compare count of externalReference field null value count is 0 (.*)$")
    public void checkNullvalue(String tableName){
        Assert.assertEquals("There are some Null value records in Ext_Reference field for "+tableName,0,BCSJMCExtRefFieldNullCount);
    }

    @Given("^Get the (.*) from JM and BCS Core Tables (.*)$")
    public void getRandomIdsFromBCSJM(String numberOfRecords, String tableName) {
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids from JM and BCS Core Tables....");

        switch (tableName) {
            case "all_accountable_product_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_ACC_PROD_RAND_ID, numberOfRecords);
                break;
            case "all_manifestation_identifiers_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_MANIF_IDENT_RAND_ID, numberOfRecords);
                break;
            case "all_manifestation_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_MANIF_RAND_ID, numberOfRecords);
                break;
            case "all_person_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_PERSON_RAND_ID, numberOfRecords);
                break;
            case "all_product_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_PRODUCT_RAND_ID, numberOfRecords);
                break;
            case "all_product_rel_package_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_PROD_REL_PKG_RAND_ID, numberOfRecords);
                break;
            case "all_work_identifier_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_IDENT_RAND_ID, numberOfRecords);
                break;
            case "all_work_person_role_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_PERS_ROLE_RAND_ID, numberOfRecords);
                break;
            case "all_work_relationship_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_RELATION_RAND_ID, numberOfRecords);
                break;
            case "all_work_subject_areas_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_SUBJ_AREA_RAND_ID, numberOfRecords);
                break;
            case "all_work_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_RAND_ID, numberOfRecords);
                break;
            case "all_work_legal_owner_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_LEGAL_OWNER_RAND_ID, numberOfRecords);
                break;
            case "all_work_access_model_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_ACCESS_MODEL_RAND_ID, numberOfRecords);
                break;
            case "all_work_business_model_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_BUSINESS_MODEL_RAND_ID, numberOfRecords);
                break;
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("id")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @Then("^Get the Records from the JM and BCS Core Tables (.*)$")
    public void getRecFromJMAndBCS(String tableName){
        Log.info("We get the records from JM and BCS Core table...");
        switch (tableName) {
            case "all_accountable_product_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_ACC_PROD_REC, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_identifiers_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_MANIF_IDENT_REC, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_MANIF_REC, Joiner.on("','").join(Ids));
                break;
            case "all_person_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_PERSON_REC, Joiner.on("','").join(Ids));
                break;
            case "all_product_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_PRODUCT_REC, Joiner.on("','").join(Ids));
                break;
            case "all_product_rel_package_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_PROD_REL_PKG_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_identifier_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_IDENT_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_person_role_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_PERS_ROLE_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_relationship_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_RELATION_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_subject_areas_v":
                Log.info(Ids.toString());
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_SUBJ_AREA_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_legal_owner_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_LEGAL_OWNER_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_access_model_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_ACCESS_MODEL_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_business_model_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_BUSINESS_MODEL_REC, Joiner.on("','").join(Ids));
                break;}
        dataQualityDLCoreViewContext.recordsFromBCSJMData = DBManager.getDBResultAsBeanList(sql, DL_CoreViewsAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And ("^Get the Records from the DL core views (.*)$")
    public void getRecFromAllViews(String tableName){
        Log.info("We get the records from JM and BCS Core table...");
        switch (tableName) {
            case "all_accountable_product_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_ACC_PROD_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_identifiers_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_MANIF_IDENT_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_MANIF_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_person_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_PERSON_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_product_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_PRODUCT_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_product_rel_package_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_PROD_REL_PKG_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_identifier_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WRK_IDENT_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_person_role_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WRK_PERS_ROLE_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_relationship_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WRK_RELT_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_subject_areas_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WORK_SUB_AREA_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WORK_VIEW_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_legal_owner_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WORK_LEGAL_OWNER_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_access_model_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WORK_ACCESS_MODEL_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_business_model_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WORK_BUSINESS_MODEL_REC, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityDLCoreViewContext.recordsFromAllViews = DBManager.getDBResultAsBeanList(sql, DL_CoreViewsAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare data of BCS and JM Core with DL Core views (.*) are identical$")
    public void compareCurrentandCurrHist(String targetTableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityDLCoreViewContext.recordsFromBCSJMData.isEmpty()) {
            Log.info("No Data Found in the BCS/JM Core Tables ....");
        } else {
            Log.info("Sorting the Ids to compare the records between DL core All views...");
        }
        for (int i = 0; i < dataQualityDLCoreViewContext.recordsFromBCSJMData.size(); i++) {
            switch (targetTableName) {
                case "all_accountable_product_v":

                    Log.info("all_accountable_product_v Records:");
                  //  dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                   // dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_accountable_prod_Col = {"getEXTERNALREFERENCE","getWORKREFERENCE","getGL_PRODUCT_SEGMENT_NAME","getGL_PRODUCT_SEGMENT_CODE","getGL_PRODUCT_SEGMENT_PARENT","getLASTUPDATEDDATE","getWORKSOURCEREF","getDELETEFLAG","getDQ_ERR","getSOURCESYSTEM"};
                    for (String strTemp : all_accountable_prod_Col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => Source_Ingest = " + method.invoke(objectToCompare1) +
                                 " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                    case "all_manifestation_identifiers_v":

                        Log.info("all_manifestation_identifiers_v Records:");
                        dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                        dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                        String[] all_manifestation_identifiers_col = {"getEXTERNALREFERENCE","getIDENTIFIER","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE","getF_TYPE","getF_MANIFESTATION","getMANIFESTATIONSOURCEREF",
                        "getLASTUPDATEDDATE","getDELETEFLAG","getSOURCESYSTEM","getSCENARIOCODE","getSCENARIONAME"};
                        for (String strTemp : all_manifestation_identifiers_col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                            DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                    " " + strTemp + " => Source_Ingest = " + method.invoke(objectToCompare1) +
                                    " All_Views_Core = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                case "all_manifestation_v":

                    Log.info("all_manifestation_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_manifestation_v = {"getEXTERNALREFERENCE","getEPH_MANIF_ID","getMANIF_KEY_TITLE","getMANIF_KEY_TITLE",
                            "getFIRSTPUBLISHEDDATE","getLASTPUBDATE","getF_TYPE","getFORMAT_TYPE","getWORKSOURCEREF","getEPH_WORK_ID","getDQ_ERR","getLASTUPDATEDDATE",
                    "getUPDATETYPE","getDELETEFLAG","getSOURCESYSTEM","getSCENARIOCODE","getSCENARIONAME"};
                    for (String strTemp : all_manifestation_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => Source_Ingest = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_person_v":
                    Log.info("all_person_v Records:");
                    //dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    //dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_person_v = {"getPERSONID","getEXTERNALREFERENCE","getGIVENNAME","getFAMILYNAME","getPEOPLEHUBID","getEMAIL","getDQ_ERR","getDELETEFLAG","getSOURCESYSTEM"};
                    for (String strTemp : all_person_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => Source_ingest = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_product_v":
                    Log.info("all_product_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_product_v = {"getEXTERNALREFERENCE","getNAME","getSHORTNAME","getSEPRATELYSALEINDICATOR","getTRIALALLOWEDINDICATOR","getRESTRICTEDSALEINDICATOR"
                            ,"getLAUNCHDATE","getCONTENTFROMDATE","getCONTENTTODATE","getCONTENTDATEOFFSET","getF_TYPE"
                            ,"getF_STATUS","getF_ACCOUNTABLEPRODUCT","getF_TAXCODE","getF_REVENUEMODEL","getF_REVENUEACC"
                            ,"getF_WWORK","getWORKREFERENCE","getF_MANIFESTATION","getMANIFESTATIONREF","getDQ_ERR","getUPDATE_TYPE","getWORKROLLUPTYPE","getDELETEFLAG","getSOURCESYSTEM","getSCENARIOCODE","getSCENARIONAME"};
                    for (String strTemp : all_product_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => Source_ingest = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_product_rel_package_v":
                    Log.info("all_product_rel_package_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_product_rel_package_v = {"getEXTERNALREFERENCE","getALLOCATION","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE","getFPACKAGEOWNER",
                    "getFCOMPONENT","getFRELATIONSHIPTYPE","getLASTUPDATEDDATE","getDELETEFLAG","getSOURCESYSTEM"};
                    for (String strTemp : all_product_rel_package_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => Source_ingest = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                    case "all_work_identifier_v":
                    Log.info("all_work_identifier_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_identifier_v = {"getEXTERNALREFERENCE","getIDENTIFIER","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE",
                            "getF_TYPE","getF_WWORK","getWORKSOURCEREF","getLASTUPDATEDDATE","getDELETEFLAG","getSOURCESYSTEM","getSCENARIOCODE","getSCENARIONAME"};
                    for (String strTemp : all_work_identifier_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => Source_ingest = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_work_person_role_v":

                    Log.info("all_work_person_role_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_person_role_v = {"getEXTERNALREFERENCE","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE","getF_ROLE"
                            ,"getF_WWORK","getWORKSOURCEREF","getPERSONSOURCEREF","getPERSONEMAIL","getDQ_ERR","getLASTUPDATEDDATE","getDELETEFLAG","getSOURCESYSTEM","getSCENARIOCODE","getSCENARIONAME"};
                    for (String strTemp : all_work_person_role_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => Source_ingest = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "all_work_relationship_v":
                    Log.info("all_work_relationship_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_relationship_v = {"getEXTERNALREFERENCE","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE","getDQ_ERR"
                            ,"getPARENTWORKSOURCEREF","getCHILDWORKSOURCEREF","getF_RELATIONTYPEREF","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE",
                            "getLASTUPDATEDDATE","getDELETEFLAG","getSOURCESYSTEM"};

                    for (String strTemp : all_work_relationship_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => BCS/DL = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_work_subject_areas_v":
                    Log.info("all_work_subject_areas_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_subject_areas_v = {"getEXTERNALREFERENCE","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE",
                            "getDQ_ERR","getF_SUBJECTAREA","getF_WWORK","getWORKSOURCEREF","getSUBJECTAREAREF","getDQ_ERR","getLASTUPDATEDDATE","getDELETEFLAG","getSOURCESYSTEM"};
                    for (String strTemp : all_work_subject_areas_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => BCS/DL = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_work_v":
                    Log.info("all_work_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_v = {"getEXTERNALREFERENCE","getEPR","getWORKTITLE","getWORKSUBTITLE","getELECTRORIGHTINDICATOR",
                            "getVOLUME","getCOPYRIGHTYEAR","getEDITIONNO","getF_PMC","getF_OA_JOURNAL_TYPE","getF_TYPE","getF_STATUS",
                            "getF_IMPRINT","getF_SOCIETY_OWNERSHIP","getRESP_CENTRE","getOPCO","getLANGUAGECODE","getSUBSCRIPTIONTYPE",
                            "getPLANNED_LAUNCH_DATE","getACTUAL_LAUNCH_DATE","getPLANNED_TERMINATION_DATE","getACTUAL_TERMINATION_DATE",
                            "getDQ_ERR","getUPDATE_TYPE","getLASTUPDATEDDATE","getDELETEFLAG","getSOURCESYSTEM","getSCENARIOCODE","getSCENARIONAME"};
                    for (String strTemp : all_work_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => Source_ingest = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_work_legal_owner_v":
                    Log.info("all_work_legal_owner_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_legal_owner_v = {"getEXTERNALREFERENCE","getlegal_owner_reference","getf_ownership_description","getWORKSOURCEREF","getDQ_ERR",
                            "getLASTUPDATEDDATE","getDELETEFLAG","getSOURCESYSTEM"};
                    for (String strTemp : all_work_legal_owner_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => Source_ingest = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_work_access_model_v":
                    Log.info("all_work_access_model_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_access_model_v = {"getEXTERNALREFERENCE","getWORKSOURCEREF","getaccess_model_code","getaccess_model_description","getDQ_ERR",
                            "getLASTUPDATEDDATE","getDELETEFLAG","getSOURCESYSTEM"};
                    for (String strTemp : all_work_access_model_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => Source_ingest = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_work_business_model_v":
                    Log.info("all_work_business_model_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_business_model_v = {"getEXTERNALREFERENCE","getWORKSOURCEREF","getbusiness_model_code","getbusiness_model_description","getDQ_ERR",
                            "getLASTUPDATEDDATE","getDELETEFLAG","getSOURCESYSTEM"};
                    for (String strTemp : all_work_business_model_v) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        DL_CoreViewsAccessObject objectToCompare1 = dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i);
                        DL_CoreViewsAccessObject objectToCompare2 = dataQualityDLCoreViewContext.recordsFromAllViews.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("EXTERNALREFERENCE => " +  dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE() +
                                " " + strTemp + " => Source_ingest = " + method.invoke(objectToCompare1) +
                                " All_Views_Core = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views for EXTERNALREFERENCE:"+dataQualityDLCoreViewContext.recordsFromBCSJMData.get(i).getEXTERNALREFERENCE(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
            }
        }
    }
}










