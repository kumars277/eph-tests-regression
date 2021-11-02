package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.CK.CKAccessDLContext;
import com.eph.automation.testing.models.dao.CK.CKCurrentTablesDataObject;
import com.eph.automation.testing.models.dao.CK.CKInboundSourceTableDataObject;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKEtlCoreDataCheckSql;
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

public class CKETLCoreDataChecks {
    private static String sql;
    private static List<String> ids;

    @Given("^We get the (.*) random CK ids of (.*)$")
    public static void getRandomidsFromCurrent(String numberOfRecords, String Currenttablename) {
//        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for CK Current Tables....");
        List<Map<?, ?>> randomids;
        sql = String.format(CKEtlCoreDataCheckSql.GET_CK_TRANSFORM_CURRENT, Currenttablename, numberOfRecords);
        randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("U_KEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }


    @When("^We get the CK Current records from (.*)$")
    public static void getCurrentRecordData(String CurrenttableName) {
        Log.info("We get the bcs Ingest records...");
        switch (CurrenttableName) {
            case "ck_transform_current_package":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_TRANSFORM_CURRENT_PACKAGE, CurrenttableName, String.join("','", ids));
                break;
            case "ck_transform_current_subject_area":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_TRANSFORM_CURRENT_SUBJECT_AREA, CurrenttableName, String.join("','", ids));
                break;
            case "ck_transform_current_work":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_TRANSFORM_CURRENT_WORK, CurrenttableName, String.join("','", ids));
                break;
            case "ck_transform_current_package_work":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_TRANSFORM_CURRENT_PACKAGE_WORK, CurrenttableName, String.join("','", ids));
                break;
            case "ck_transform_current_work_subject_area":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_TRANSFORM_CURRENT_WORK_SUBJECT_AREA, CurrenttableName, String.join("','", ids));
                break;
            case "ck_transform_current_package_work_url":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_TRANSFORM_CURRENT_PACKAGE_WORK_URL, CurrenttableName, String.join("','", ids));
                break;
        }
        CKAccessDLContext.CKCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKCurrentTablesDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^We get the CK Inbound Source records from (.*)$")
    public static void getDataforInboundsOURCECheck(String InboundSourcetablename) {
        Log.info("We get the records from Current CK Inbound Source table for Inbound Check...");
        switch (InboundSourcetablename) {
            case "ck_package_form_v":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_PACKAGE_FORM_V, InboundSourcetablename, String.join("','", ids));
                break;
            case "ck_subject_area_form_v":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_SUBJECT_AREA_FORM_V, InboundSourcetablename, String.join("','", ids));
                break;
            case "ck_work_form_v":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_WORK_FORM_V, InboundSourcetablename, String.join("','", ids));
                break;
                case "ck_package_work_form_v":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_PACKAGE_WORK_FORM_V, InboundSourcetablename, String.join("','", ids));
                break;

            case "ck_work_subject_area_form_v":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_WORK_SUBJECT_AREA_FORM_V, InboundSourcetablename, String.join("','", ids));
                break;
            case "ck_package_work_url_form_v":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_PACKAGE_WORK_URL_FORM_V, InboundSourcetablename, String.join("','", ids));
                break;
        }
        CKAccessDLContext.CKInboundSourceTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKInboundSourceTableDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare CK records in Inbound Source and Current of (.*)$")
    public void compareInboundSourceandCurrent(String InboundSourcetablename) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (CKAccessDLContext.CKCurrentTableDataObjectList.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records between Inbound Source and current...");
            for (int i = 0; i < CKAccessDLContext.CKCurrentTableDataObjectList.size(); i++) {
                switch (InboundSourcetablename) {
                    case "ck_package_form_v":
                        Log.info("comparing inbound source and Current records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getu_key)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getu_key));

                        String[] TransformCurrPackageCol = {"getu_key", "geteph_package_id", "getck_live_date", "getpackage_acronym", "getpackage_name", "geteph_work_type", "geteph_work_hierarchy_code", "geteph_work_hierarchy_id", "getpackage_start_date", "getpackage_end_date"};
                        for (String strTemp : TransformCurrPackageCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_Key => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key() +
                                    " " + strTemp + " => Current = " + method.invoke(objectToCompare1) +
                                    " InboundSource = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in ck_transform_current_package for U_Key:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "ck_subject_area_form_v":
                        Log.info("comparing inbound source and Current records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getu_key)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getu_key));

                        String[] TransformCurrSubjectAreaCol = {"getu_key","getsubject_area_id","getck_live_date","getsubject_area_name"};
                        for (String strTemp : TransformCurrSubjectAreaCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_Key => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key() +
                                    " " + strTemp + " => Current = " + method.invoke(objectToCompare1) +
                                    " InboundSource = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in ck_transform_current_Subject_Area for U_Key:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "ck_work_form_v":
                        Log.info("comparing inbound source and Current records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getu_key)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getu_key));

                        String[] TransformCurrWorkCol = {"getu_key","geteph_work_id","getck_live_date","getcmms_issn_isbn","getck_title_start_date","getalternate_title","getsearch_tier","getfinance_tier","getyears_of_coverage","getpdf_suppression"};
                        for (String strTemp : TransformCurrWorkCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_Key => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key() +
                                    " " + strTemp + " => Current = " + method.invoke(objectToCompare1) +
                                    " InboundSource = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in ck_transform_current_Subject_Area for U_Key:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "ck_package_work_form_v":
                        Log.info("comparing inbound source and Current records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getu_key)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getu_key));

                        String[] TransformCurrPackageWorkCol = {"getu_key","geteph_package_id","geteph_work_id","getck_live_date"};
                        for (String strTemp : TransformCurrPackageWorkCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_Key => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key() +
                                    " " + strTemp + " => Current = " + method.invoke(objectToCompare1) +
                                    " InboundSource = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in ck_transform_current_Subject_Area for U_Key:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "ck_work_subject_area_form_v":
                        Log.info("comparing inbound source and Current records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getu_key)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getu_key));

                        String[] TransformCurrWorkSubjectAreaCol = {"getu_key","geteph_work_id","getsubject_area_id","getck_live_date"};
                        for (String strTemp : TransformCurrWorkSubjectAreaCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_Key => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key() +
                                    " " + strTemp + " => Current = " + method.invoke(objectToCompare1) +
                                    " InboundSource = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in ck_transform_current_Subject_Area for U_Key:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "ck_package_work_url_form_v":
                        Log.info("comparing inbound source and Current records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getu_key)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getu_key));

                        String[] TransformCurrPackageWorkUrlCol = {"getu_key","geteph_package_id","geteph_work_id","getdurable_url","getck_live_date"};
                        for (String strTemp : TransformCurrPackageWorkUrlCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_Key => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key() +
                                    " " + strTemp + " => Current = " + method.invoke(objectToCompare1) +
                                    " InboundSource = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in ck_transform_current_Subject_Area for U_Key:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key(),
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
