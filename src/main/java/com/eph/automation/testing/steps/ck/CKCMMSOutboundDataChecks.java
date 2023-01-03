package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.CK.CKAccessDLContext;
import com.eph.automation.testing.models.dao.CK.CKCurrentTablesDataObject;
import com.eph.automation.testing.models.dao.CK.CKInboundSourceTableDataObject;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKCMMSExportDataChecksSQL;
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

public class CKCMMSOutboundDataChecks {
    private static String sql;
    private static List<String> ids;


    //    CMMS_Outbound Data Checks
    @Given("^We get the (.*) random CK CMMS View ids of (.*)$")
    public static void getRandomidsFromCMMSView(String numberOfRecords, String DPPCMMSView) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for CK CMMS View....");
        List<Map<?, ?>> randomids;
        switch (DPPCMMSView){
            case "cmms_workflow_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORKFLOW_VIEW_IDs, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (Integer) m.get("U_KEY")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "cmms_work1_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORK1_VIEW_IDs, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (String) m.get("U_KEY")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "cmms_work2_identifiers_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORK2_IDENTIFIERS_VIEW_IDs, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (String) m.get("U_KEY")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "cmms_work3_subject_areas_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORK3_SUBJECT_AREAS_VIEW_IDs, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (Integer) m.get("U_KEY")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "cmms_package1_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_PACKAGE1_VIEW_IDs, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (String) m.get("U_KEY")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "cmms_package2_works_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_PACKAGE2_WORKS_VIEW_IDs, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (String) m.get("U_KEY")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^Get the Inbound Records for (.*)$")
    public static void getCMMSViewRecordData(String DPPCMMSTable) {
        Log.info("We get the CMMS View records...");
        switch (DPPCMMSTable) {
            case "cmms_workflow_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORKFLOW_VIEW,String.join(",", ids));
                break;
            case "cmms_work1_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORK1_VIEW, String.join("','", ids));
                break;
            case "cmms_work2_identifiers_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORK2_IDENTIFIERS_VIEW, String.join("','", ids));
                break;
            case "cmms_work3_subject_areas_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORK3_SUBJECT_AREAS_VIEW, String.join(",", ids));
                break;
            case "cmms_package1_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_PACKAGE1_VIEW, String.join("','", ids));
                break;
            case "cmms_package2_works_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_PACKAGE2_WORKS_VIEW, String.join("','", ids));
                break;
        }
        CKAccessDLContext.CKCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKCurrentTablesDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^We get the records from the views of (.*)$")
    public static void getCMMSTableRecordData(String InboundSourcetablename) {
        Log.info("We get the records from Current CK Inbound Source table for Inbound Check...");
        switch (InboundSourcetablename) {
            case "cmms_workflow":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORKFLOW_TABLE, InboundSourcetablename, String.join(",", ids));
                break;
            case "cmms_work1_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORK1, InboundSourcetablename, String.join("','", ids));
                break;
            case "cmms_work2_identifiers_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORK2_IDENTIFIERS, InboundSourcetablename, String.join("','", ids));
                break;
            case "cmms_work3_subject_areas_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_WORK3_SUBJECT_AREAS, InboundSourcetablename, String.join(",", ids));
                break;
            case "cmms_package1_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_PACKAGE1, InboundSourcetablename, String.join("','", ids));
                break;
            case "cmms_package2_works_v":
                sql = String.format(CKCMMSExportDataChecksSQL.GET_CK_CMMS_PACKAGE2_WORKS, InboundSourcetablename, String.join("','", ids));
                break;
        }
        CKAccessDLContext.CKInboundSourceTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKInboundSourceTableDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare records between Inbound and the view of (.*)$")
    public void compareCMMSViewandTable(String DPPCMMSTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (CKAccessDLContext.CKCurrentTableDataObjectList.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records between CMMS View and Table...");
            for (int i = 0; i < CKAccessDLContext.CKCurrentTableDataObjectList.size(); i++) {
                switch (DPPCMMSTable) {
                    case "cmms_workflow_v":
                        Log.info("comparing CMMS Workflow View and Table records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getworkflowId)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getworkflowId));

                        String[] CMMSWorkflowCol = {"getworkflowId", "getworkflowType", "getworkflowCutoffDate", "getworkflowLiveDate"};
                        for (String strTemp : CMMSWorkflowCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("workflowId => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkflowId() +
                                    " " + strTemp + " => View = " + method.invoke(objectToCompare1) +
                                    " Table = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in CMMS WORKFLOW for workflowId:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkflowId(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "cmms_work1_v":
                        Log.info("comparing CMMS Work1 View and Table records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getworkId)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getworkId));

                        String[] CMMSWork1Col = {"getworkId","getworkTitle","getalternateTitle","getplannedLaunchDate","getactualLaunchDate","getworkStatus","getworkType","getpmg","geteditionNumber","getcopyrightYear","getplannedDiscontinuedate","getactualDiscontinueDate","getcurrentIdentifier","getfinanceTier","getsearchTier","getpdfSuppression","gettopicPages","getjournalBackfilesOnly","getjournalBackfileYears","getjournalAip"};
                        String[] CMMSWork1Col2 = {"getworkId","getworkTitle","getalternateTitle","getplannedLaunchDate","getactualLaunchDate","getworkStatus","getworkType","getPMG","geteditionNumber","getcopyrightYear","getplannedDiscontinuedate","getactualDiscontinueDate","getcurrentIdentifier","getfinanceTier","getsearchTier","getpdfSuppression","gettopicPages","getjournalBackfilesOnly","getjournalBackfileYears","getjournalAip"};
                        int a = 0;
                        for (String strTemp : CMMSWork1Col) {
                            String strTemp2 = CMMSWork1Col2[a];
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp2);

                            Log.info("workId => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkId() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " View = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in CMMS Work1 for workId:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkId(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            a ++;
                        }
                        break;
                    case "cmms_work2_identifiers_v":
                        Log.info("comparing CMMS Work2 Identifiers View and Table records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getworkId)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getworkId));

                        String[] CMMSWork2IdentifiersCol = {"getworkId","getidentifier","getenddate"};
                        for (String strTemp : CMMSWork2IdentifiersCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("workId => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkId() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " View = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in CMMS Work2 Identifiers for workId:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkId(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "cmms_work3_subject_areas_v":
                        Log.info("comparing CMMS Work3 Subject Areas View and Table records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getsubjectareaid)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getsubjectareaid));

                        String[] CMMSWork3SubjectAreasCol = {"getworkId","getsubjectareaid","getsubjectareaname","getenddate"};
                        for (String strTemp : CMMSWork3SubjectAreasCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("subjectareaid => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getsubjectareaid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " View = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in CMMS Work3 SubjectAreaID for subjectareaid:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getsubjectareaid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "cmms_package1_v":
                        Log.info("comparing CMMS Package View and Table records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getpackageid)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getpackageid));

                        String[] CMMSPackage1Col = {"getpackageid","getpackagesitecode","getpackageacronym","getpackagetitle","getpackagestatus","getpackagetype","getcmmssitename"};
                        for (String strTemp : CMMSPackage1Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("packageid => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getpackageid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " View = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in CMMS Package1 for subjectareaid:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getpackageid(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "cmms_package2_works_v":
                        Log.info("comparing CMMS Package2 Works View and Table records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getpackageid)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getpackageid));

                        String[] CMMSPackage2WorkCol = {"getworkId","getpackageid","getenddate","getdurableurl","getworkflowLiveDate"};
                        for (String strTemp : CMMSPackage2WorkCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("packageid => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getpackageid() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " View = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in CMMS Package1 for subjectareaid:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getpackageid(),
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
