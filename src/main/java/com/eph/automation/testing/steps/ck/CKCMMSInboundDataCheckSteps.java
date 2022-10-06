package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.CK.CKAccessDLContext;
import com.eph.automation.testing.models.dao.CK.CKCurrentTablesDataObject;
import com.eph.automation.testing.models.dao.CK.CKInboundSourceTableDataObject;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKCMMSInboundDataCheckSQL;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKReportsDataChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CKCMMSInboundDataCheckSteps {
    private static String sql;
    private static List<String> ids;

    @Given("We get the (.*) random CMMS Inbound View ids of (.*)")
    public static void getRandomidsFromCMMSInboundView(String numberOfRecords, String CMMSInboundView) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for CK CMMSInbound View....");
        List<Map<?, ?>> randomids;
        switch (CMMSInboundView) {
            case "cmms_durable_url1_form_v":
                System.out.print("here");
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CMMS_INBOUND_URL1_VIEW_IDs, CMMSInboundView, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (String) m.get("u_key")).collect(Collectors.toList());
                break;
            case "cmms_durable_url2_form_v":
                System.out.print("here");
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CMMS_INBOUND_URL2_VIEW_IDs, CMMSInboundView, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (String) m.get("u_key")).collect(Collectors.toList());
                break;
            case "cmms_durable_url3_form_v":
                System.out.print("here");
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CMMS_INBOUND_URL3_VIEW_IDs, CMMSInboundView, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (String) m.get("u_key")).collect(Collectors.toList());
                break;
            case "cmms_durable_url_transform_v":
                System.out.print("here");
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CMMS_INBOUND_URL_TRANSFORM_VIEW_IDs, CMMSInboundView, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (String) m.get("u_key")).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("We get the CKCMMSInbound View Records from (.*)")
    public static void getCMMSInboundViewData(String CMMSInboundView) {
        Log.info("We get the CMMSInbound View records...");
        switch (CMMSInboundView) {
            case "cmms_durable_url1_form_v":
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CMMS_INBOUND_URL1_VIEW_Data, CMMSInboundView, String.join("','", ids));
                break;
            case "cmms_durable_url2_form_v":
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CMMS_INBOUND_URL2_VIEW_DATA, CMMSInboundView, String.join("','", ids));
                break;
            case "cmms_durable_url3_form_v":
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CMMS_INBOUND_URL3_VIEW_DATA, CMMSInboundView, String.join("','", ids));
                break;
            case "cmms_durable_url_transform_v":
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CMMS_INBOUND_URL_TRANSFORM_VIEW_DATA, CMMSInboundView, String.join("','", ids));
                break;

        }
        CKAccessDLContext.CKInboundSourceTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKInboundSourceTableDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("We get the CKCMMSInbound Query records from (.*)")
    public static void getDataforCMMSInboundTableCheck(String CMMSInboundView) {
        switch (CMMSInboundView) {
            case "cmms_durable_url1_form_v":
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CKCMMS_INBOUND_URL1_QUERY_DATA, String.join("','", ids));
                break;
            case "cmms_durable_url2_form_v":
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CKCMMS_INBOUND_URL2_QUERY_DATA, String.join("','", ids));
                break;
            case "cmms_durable_url3_form_v":
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CKCMMS_INBOUND_URL3_QUERY_DATA, String.join("','", ids));
                break;
            case "cmms_durable_url_transform_v":
                sql = String.format(CKCMMSInboundDataCheckSQL.GET_CKCMMS_INBOUND_URL_TRANSFORM_QUERY_DATA, String.join("','", ids));
                break;

        }
        CKAccessDLContext.CKCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKCurrentTablesDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("Compare records in CKCMMSInbound View and CMMSInbound query of (.*)")
    public void compareCMMSInboundViewandQuery(String CMMSInboundView) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (CKAccessDLContext.CKCurrentTableDataObjectList.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records between View Table and Query table...");
            for (int i = 0; i < CKAccessDLContext.CKCurrentTableDataObjectList.size(); i++) {
                switch (CMMSInboundView) {
                    case "cmms_durable_url1_form_v":
                        Log.info("comparing Table and View records...");
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getcmms_issn_isbn));
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getcmms_issn_isbn));

                        String[] WorkflowTableauCol = {"getinbound_type","geteph_work_id","getuncorrected_cmms_issn_isbn","getcmms_issn_isbn","getcmms_site_name","getdurable_url","getu_key"};

                        for (String strTemp : WorkflowTableauCol) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("cmms_issn_isbn => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getcmms_issn_isbn() +
                                    " " + strTemp + " => View = " + method.invoke(objectToCompare1) +
                                    " Query = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in cmms_durable_url1_form_v  for workId:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getcmms_issn_isbn(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }

                        }
                        break;
                    case "cmms_durable_url2_form_v":
                        Log.info("comparing Table and View records...");
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getcmms_issn_isbn));
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getcmms_issn_isbn));

                        String[] WorkflowTableauCol1 = {"geteph_package_id","geteph_work_id","getcmms_issn_isbn","getdurable_url","getu_key"};

                        for (String strTemp : WorkflowTableauCol1) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("cmms_issn_isbn => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getcmms_issn_isbn() +
                                    " " + strTemp + " => View = " + method.invoke(objectToCompare1) +
                                    " Query = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in cmms_durable_url2_form_v  for workId:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getcmms_issn_isbn(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }

                        }
                        break;
                    case "cmms_durable_url3_form_v":
                        Log.info("comparing Table and View records...");
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getu_key));
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getu_key));

                        String[] WorkflowTableauCol2 = {"geteph_package_id","geteph_work_id","getdurable_url","getu_key"};

                        for (String strTemp : WorkflowTableauCol2) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("u_key => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key() +
                                    " " + strTemp + " => View = " + method.invoke(objectToCompare1) +
                                    " Query = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in cmms_durable_url3_form_v  for workId:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }

                        }
                        break;
                    case "cmms_durable_url_transform_v":
                        Log.info("comparing Table and View records...");
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getu_key));
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getu_key));

                        String[] WorkflowTableauCol3 = {"geteph_package_id","geteph_work_id","getdurable_url","getu_key"};

                        for (String strTemp : WorkflowTableauCol3) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("u_key => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key() +
                                    " " + strTemp + " => View = " + method.invoke(objectToCompare1) +
                                    " Query = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in cmms_durable_url3_form_v  for workId:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getu_key(),
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
