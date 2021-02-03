package com.eph.automation.testing.web.steps.DL_ExtendedViews;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
//import com.eph.automation.testing.models.contexts.DL_CoreViewsAccessContext;
//import com.eph.automation.testing.models.dao.DL_CoreViews.DL_CoreViewsAccessObject;
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

  //  public DL_CoreViewsAccessContext dataQualityDLCoreViewContext;
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
           /* case "work_extended_editorial_board":
                Log.info("Getting DL work_extended_editorial_board Count...");
                DLExtSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WRK_PERS_ROLE_VIEW_COUNT;
                break;*/
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
          /*  case "work_extended_editorial_board":
                Log.info("Getting DL All work_extended_editorial_board Count...");
                DLAllExtSQLViewCount = DL_CoreViewChecksSQL.GET_DL_CORE_ALL_WRK_PERS_ROLE_VIEW_COUNT;
                break;*/
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

  /*  @Given("^Get the (.*) from JM and BCS Core Tables (.*)$")
    public void getRandomIdsFromBCSJM(String numberOfRecords, String tableName) {
       // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_SUBJ_AREA_REC, Joiner.on("','").join(Ids));
                break;
            case "all_work_v":
                sql = String.format(DL_CoreViewChecksSQL.GET_BCS_JM_CORE_WORK_REC, Joiner.on("','").join(Ids));
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
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_accountable_prod_Col = {"getEXTERNALREFERENCE","getGL_PRODUCT_SEGMENT_CODE","getGL_PRODUCT_SEGMENT_PARENT","getWORKSOURCEREF","getDQ_ERR"};
                    for (String strTemp : all_accountable_prod_Col) {
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                    case "all_manifestation_identifiers_v":

                        Log.info("all_manifestation_identifiers_v Records:");
                        dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                        dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                        String[] all_manifestation_identifiers_col = {"getEXTERNALREFERENCE","getIDENTIFIER","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE","getF_TYPE","getF_MANIFESTATION","getMANIFESTATIONSOURCEREF"};
                        for (String strTemp : all_manifestation_identifiers_col) {
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
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                case "all_manifestation_v":

                    Log.info("all_manifestation_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_manifestation_v = {"getEXTERNALREFERENCE","getEPH_MANIF_ID","getMANIF_KEY_TITLE","getMANIF_KEY_TITLE","getFIRSTPUBLISHEDDATE","getLASTPUBDATE","getF_TYPE","getFORMAT_TYPE"};
                    for (String strTemp : all_manifestation_v) {
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_person_v":
                    Log.info("all_person_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_person_v = {"getEXTERNALREFERENCE","getGIVENNAME","getFAMILYNAME","getPEOPLEHUBID","getEMAIL","getDQ_ERR"};
                    for (String strTemp : all_person_v) {
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_product_v":
                    Log.info("all_product_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_product_v = {"getEXTERNALREFERENCE","getNAME","getSHORTNAME","getSEPRATELYSALEINDICATOR","getTRIALALLOWEDINDICATOR","getRESTRICTEDSALEINDICATOR","getLAUNCHDATE","getCONTENTFROMDATE","getCONTENTTODATE","getCONTENTDATEOFFSET","getF_TYPE","getF_STATUS","getF_ACCOUNTABLEPRODUCT","getF_TAXCODE","getF_REVENUEMODEL","getF_REVENUEACC","getF_WWORK","getWORKREFERENCE","getF_MANIFESTATION","getMANIFESTATIONREF","getDQ_ERR","getUPDATE_TYPE","getWORKROLLUPTYPE"};
                    for (String strTemp : all_product_v) {
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_work_identifier_v":
                    Log.info("all_work_identifier_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));


                    String[] all_work_identifier_v = {"getEXTERNALREFERENCE","getIDENTIFIER","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE","getF_TYPE","getF_WWORK","getWORKSOURCEREF"};
                    for (String strTemp : all_work_identifier_v) {
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_work_person_role_v":

                    Log.info("all_work_person_role_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_person_role_v = {"getEXTERNALREFERENCE","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE","getF_ROLE","getF_WWORK","getWORKSOURCEREF","getPERSONSOURCEREF","getPERSONEMAIL","getDQ_ERR"};
                    for (String strTemp : all_work_person_role_v) {
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "all_work_relationship_v":
                    Log.info("all_work_relationship_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_relationship_v = {"getEXTERNALREFERENCE","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE","getDQ_ERR","getPARENTWORKSOURCEREF","getCHILDWORKSOURCEREF"};
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_work_subject_areas_v":
                    Log.info("all_work_subject_areas_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_subject_areas_v = {"getEXTERNALREFERENCE","getEFFECTIVE_START_DATE","getEFFECTIVE_END_DATE","getDQ_ERR","getF_SUBJECTAREA","getF_WWORK","getWORKSOURCEREF","getSUBJECTAREAREF"};
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "all_work_v":
                    Log.info("all_work_v Records:");
                    dataQualityDLCoreViewContext.recordsFromBCSJMData.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE)); //sort primarykey data in the lists
                    dataQualityDLCoreViewContext.recordsFromAllViews.sort(Comparator.comparing(DL_CoreViewsAccessObject::getEXTERNALREFERENCE));

                    String[] all_work_v = {"getEXTERNALREFERENCE","getEPR","getWORKTITLE","getWORKSUBTITLE","getELECTRORIGHTINDICATOR","getVOLUME","getCOPYRIGHTYEAR","getEDITIONNO","getF_PMC","getF_OA_JOURNAL_TYPE","getF_TYPE","getF_STATUS","getF_IMPRINT","getF_SOCIETY_OWNERSHIP","getRESP_CENTRE","getOPCO","getLANGUAGECODE","getSUBSCRIPTIONTYPE","getPLANNED_LAUNCH_DATE","getACTUAL_LAUNCH_DATE","getPLANNED_TERMINATION_DATE","getACTUAL_TERMINATION_DATE","getDQ_ERR","getUPDATE_TYPE"};
                    for (String strTemp : all_work_v) {
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
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in All Views",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
            }
        }
    }*/
}










