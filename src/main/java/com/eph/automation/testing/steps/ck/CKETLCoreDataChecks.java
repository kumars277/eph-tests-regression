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

    //    ETL Data Checks Delta Current to Delta History
    @Given("^We get the (.*) random CK Delta ids of (.*)$")
    public static void getRandomidsFromDeltaCurrent(String numberOfRecords, String Currenttablename) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for CK Current Tables....");
        List<Map<?, ?>> randomids;
        sql = String.format(CKEtlCoreDataCheckSql.GET_CK_DELTA_CURRENT, Currenttablename, numberOfRecords);
        randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("U_KEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^We get the CK Delta Current Records from (.*)$")
    public static void getCurrentRecordData(String DeltaCurrenttablename) {
        Log.info("We get the Delta Current records...");
        switch (DeltaCurrenttablename) {
            case "ck_delta_current_package_work":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_DELTA_CURRENT_PACKAGE_WORK, DeltaCurrenttablename, String.join("','", ids));
                break;
            case "ck_delta_current_work_subject_area":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_DELTA_CURRENT_WORK_SUBJECT_AREA, DeltaCurrenttablename, String.join("','", ids));
                break;
        }
        CKAccessDLContext.CKCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKCurrentTablesDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^We get the CK Delta History records from (.*)$")
    public static void getDataforDeltaHistoryCheck(String InboundSourcetablename) {
        Log.info("We get the records from Current CK Inbound Source table for Inbound Check...");
        switch (InboundSourcetablename) {
            case "ck_delta_history_package_work_part":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_DELTA_HISTORY_PACKAGE_WORK, InboundSourcetablename,String.join("','", ids), InboundSourcetablename);
                break;
            case "ck_delta_history_work_subject_area_part":
                sql = String.format(CKEtlCoreDataCheckSql.GET_CK_DELTA_HISTORY_WORK_SUBJECT_AREA, InboundSourcetablename, String.join("','", ids) , InboundSourcetablename);
                break;
        }
        CKAccessDLContext.CKInboundSourceTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKInboundSourceTableDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare CK records in Delta Current and Delta History of (.*)$")
    public void compareDeltaCurrentandDeltaHistory(String DeltaCurrenttablename) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (CKAccessDLContext.CKCurrentTableDataObjectList.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records between Delta History and Delta current...");
            for (int i = 0; i < CKAccessDLContext.CKCurrentTableDataObjectList.size(); i++) {
                switch (DeltaCurrenttablename) {
                    case "ck_delta_current_package_work":
                        Log.info("comparing inbound source and Current records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getu_key)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getu_key));

                        String[] DeltaCurrentPackageWorkCol = {"getu_key", "geteph_package_id"};
                        for (String strTemp : DeltaCurrentPackageWorkCol) {
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
                    case "ck_delta_current_work_subject_area":
                        Log.info("comparing inbound source and Current records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getu_key)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getu_key));

                        String[] DeltaCurrentWorkSubjectAreaCol = {"getu_key", "geteph_work_id", "getsubject_area_id", "gettype","getck_live_date"};
                        for (String strTemp : DeltaCurrentWorkSubjectAreaCol) {
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
                }
            }
        }
    }
}
