package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.CK.CKAccessDLContext;
import com.eph.automation.testing.models.dao.CK.CKCurrentTablesDataObject;
import com.eph.automation.testing.models.dao.CK.CKInboundSourceTableDataObject;
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

public class CKReportsDataChecksSteps {
    private static String sql;
    private static List<String> ids,pids;
    private static List<String> wids;
    //    CK Reports Data Checks
    @Given("^We get the (.*) randomIds for (.*)$")
    public static void getRandomidsFromReportsView(String numberOfRecords, String DPPReportsView) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for CK Reports Table....");
        List<Map<?, ?>> randomids;
        switch (DPPReportsView) {
            case "ck_workflow_tableau":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_TABLEAU_VIEW_IDs, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (String) m.get("Work_ID")).collect(Collectors.toList());
                break;
            case "ck_workflow_control_p1":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_CONTROL_P1_VIEW_IDs, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (BigDecimal) m.get("workflow_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "ck_workflow_control_p2":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_CONTROL_P2_VIEW_IDs, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (BigDecimal) m.get("workflow_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "ck_workflow_tableau_package_works":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_TABLEAU_PACKAGE_WORKS_VIEW_IDs, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (String) m.get("work_id")).map(String::valueOf).collect(Collectors.toList());
                 break;
            case "ck_transaction_workflow":
                sql = String.format(CKReportsDataChecksSQL.GET_TRANSACTION_WORKFLOW_VIEW_IDs, numberOfRecords);
                randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                ids = randomids.stream().map(m -> (String) m.get("Work_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^We get the Records from Inbound (.*)$")
    public static void getReportsViewData(String DPPReportsView) {
        Log.info("We get the CK Reports View records...");
        switch (DPPReportsView) {
            case "ck_workflow_tableau":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_TABLEAU_VIEW_Data, String.join("','", ids));
                break;
           case "ck_workflow_control_p1":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_CONTROL_P1_VIEW_Data, String.join(",", ids));
                break;
            case "ck_workflow_control_p2":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_CONTROL_P2_VIEW_Data, String.join(",", ids));
                break;
            case "ck_workflow_tableau_package_works":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_TABLEAU_PACKAGE_WORKS_VIEW_Data, String.join("','", ids));
                break;
            case "ck_transaction_workflow":
                sql = String.format(CKReportsDataChecksSQL.GET_TRANSACTION_WORKFLOW_VIEW_Data, String.join("','", ids));
                break;
        }
        CKAccessDLContext.CKInboundSourceTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKInboundSourceTableDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^We get the Reports Table records from (.*)$")
    public static void getDataforReportsTableCheck(String DPPReportsTable) {
        switch (DPPReportsTable) {
            case "ck_workflow_tableau":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_TABLEAU_TABLE_Data, String.join("','", ids));
                break;
           case "ck_workflow_control_p1":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_CONTROL_P1_TABLE_Data, String.join(",", ids));
                break;
             case "ck_workflow_control_p2":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_CONTROL_P2_TABLE_Data, String.join(",", ids));
                break;
            case "ck_workflow_tableau_package_works":
                sql = String.format(CKReportsDataChecksSQL.GET_WORKFLOW_TABLEAU_PACKAGE_WORKS_TABLE_Data,String.join("','", ids));
                break;
            case "ck_transaction_workflow":
                sql = String.format(CKReportsDataChecksSQL.GET_TRANSACTION_WORKFLOW_TABLE_Data, String.join("','", ids));
                break;
        }
        CKAccessDLContext.CKCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKCurrentTablesDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare records in Reports View and Table of (.*)$")
    public void compareReportsViewandTable(String DPPReportsTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (CKAccessDLContext.CKCurrentTableDataObjectList.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records between Reports Table and View current...");
            for (int i = 0; i < CKAccessDLContext.CKCurrentTableDataObjectList.size(); i++) {
                switch (DPPReportsTable) {
                    case "ck_workflow_tableau":
                        Log.info("comparing Inbound and Table records...");
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getworkId));
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getworkId));

                        String[] WorkflowTableauCol = {"getpackage_title","getissn_l","getck_package_type", "getck_package_description", "getpackage_status", "getpackage_status_roll_up", "getpackage_code", "getck_site_code", "getck_site_name", "getsub_platform_code", "getsub_platform_name", "getpackage_id", "getpackage_work_id", "getpkg_wk_effective_start_date", "getpkg_wk_effective_end_date", "getdurable_url", "getdurable_url_live_date", "getworkId", "getwork_title", "getlead_isbn", "getedition_number", "getcopyright_year", "getactual_launch_date", "getwork_type_group", "getpackage_component_status", "getpackage_component_status_roll_up", "getimprint", "getpmc", "getpmc_description", "getpmg", "getpmg_description", "getlanguage", "getspecialty_name", "getauthor", "getauthor_sequence", "geteditor", "geteditor_seq", "getworkflow_live_date", "getworkflow_id", "getck_alternate_title", "getsearch_tier", "getfinance_tier", "getjournal_backfile_years", "getpdfSuppression", "getvideo", "gettopic_pages", "getjournal_backfiles_only", "getaip", "getprevious_work_id", "getpreviuos_lead_isbn", "getnext_work_id", "getnext_lead_isbn", "getownership", "getfirst_pub_date"};
                        String[] WorkflowTableauCol2 = {"getPackage_Title","getISSN_L", "getCK_Package_Type", "getCK_Package_Description", "getPackage_Status", "getPackage_Status_Roll_Up", "getPackage_Code", "getCK_Site_Code", "getCK_Site_Name", "getSub_platform_code", "getSub_Platform_Name", "getPackage_ID", "getPackage_Work_ID", "getPkg_wk_effective_start_date", "getPkg_wk_effective_end_date", "getdurable_url", "getDurable_URL_Live_Date", "getworkId", "getWork_Title", "getLead_ISBN", "getEdition_Number", "getCopyright_Year", "getActual_Launch_Date", "getWork_Type_Group", "getPackage_Component_Status", "getPackage_Component_Status_Roll_up", "getImprint", "getPMC", "getPMC_Description", "getPMG", "getPMG_Description", "getLanguage", "getSpecialty_Name", "getAuthor", "getAuthor_Sequence", "getEditor", "getEditor_Seq", "getWorkflow_Live_Date", "getWorkflow_Id", "getCK_Alternate_Title", "getSearch_Tier", "getFinance_Tier", "getJournal_Backfile_Years", "getPDF_Suppression", "getvideo", "getTopic_Pages", "getJournal_Backfiles_Only", "getAIP", "getPrevious_Work_ID", "getPreviuos_Lead_ISBN", "getNext_Work_ID", "getNext_Lead_ISBN", "getOwnership", "getFirst_Pub_Date"};
                        int a = 0;
                        for (String strTemp : WorkflowTableauCol) {
                            String strTemp2 = WorkflowTableauCol2[a];
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp2);

                            Log.info("workId => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkId() +
                                    " " + strTemp + " => Table = " + method.invoke(objectToCompare1) +
                                    " View = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not matching in ck_workflow_tableau for workId:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkId(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            a++;
                        }
                        break;
                    case "ck_workflow_control_p1":
                        Log.info("comparing Table and View records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getworkflowId)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getworkflowId));

                        String[] CKWorkflowControlp1col = {"getworkflowId","getkey_snapshot","getworkflowType","getworkflowLiveDate","getworkflow_year_month"};
                        for (String strTemp : CKWorkflowControlp1col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("workflowId => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkflowId() +
                                    " " + strTemp + " => Table = " + method.invoke(objectToCompare1) +
                                    " View = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not matching in ck_workflow_control_p1 for workflowId:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkflow_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "ck_workflow_control_p2":
                        Log.info("comparing Table and View records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getworkflowId)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getworkflowId));

                        String[] CKWorkflowControlp2col = {"getworkflowId","getkey_snapshot","getworkflowType","getworkflowLiveDate","getworkflow_year_month","getinclude_flag"};
                        for (String strTemp : CKWorkflowControlp2col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("workflowId => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkflowId() +
                                    " " + strTemp + " => Table = " + method.invoke(objectToCompare1) +
                                    " View = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not matching in ck_workflow_control_p2 for workflowID:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkflow_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "ck_workflow_tableau_package_works":
                        Log.info("comparing Table and View records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getworkId)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getworkId));

                        String[] CKWorkflowTableauPackageWorkscol = {"getworkflowType","getworkflowLiveDate","getworkflowId","getsnapshot_ts","getpackage_id","getpackage_code","getpackage_title","getck_site_code","getck_site_name","getsub_platform_code","getsub_platform_name","getck_package_type","getworkId","getwork_type_group","getwork_title","getck_alternate_title","getissn_l","getlead_isbn","geteditionNumber","getauthor","getauthor_sequence","geteditor","geteditor_seq","getspecialty_name","getfinanceTier","getsearchTier","getprevious_work_id","getpreviuos_lead_isbn","getnext_work_id","getnext_lead_isbn","getownership","getfirst_pub_date"};
                        String[] CKWorkflowTableauPackageWorkscol2 = {"getworkflowType","getworkflowLiveDate","getworkflowId","getsnapshot_ts","getPackage_ID","getPackage_Code","getPackage_Title","getCK_Site_Code","getCK_Site_Name","getSub_platform_code","getSub_Platform_Name","getCK_Package_Type","getworkId","getWork_Type_Group","getWork_Title","getCK_Alternate_Title","getISSN_L","getLead_ISBN","geteditionNumber","getAuthor","getAuthor_Sequence","getEditor","getEditor_Seq","getSpecialty_Name","getfinanceTier","getsearchTier","getPrevious_Work_ID","getPreviuos_Lead_ISBN","getNext_Work_ID","getNext_Lead_ISBN","getOwnership","getFirst_Pub_Date"};
                        int b = 0;
                        for (String strTemp : CKWorkflowTableauPackageWorkscol) {
                            String strTemp2 = CKWorkflowTableauPackageWorkscol2[b];
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp2);

                            Log.info("workflowId => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkflowId() +
                                    " " + strTemp + " => Table = " + method.invoke(objectToCompare1) +
                                    " View = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not matching in ck_workflow_tableau_package_works for workId:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkId(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            b++;
                        }
                        break;
                    case "ck_transaction_workflow":
                        Log.info("comparing Table and View records...");
                        CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getworkId)); //sort U_key data in the lists
                        CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getworkId));

                        String[] CKTransactionWorkflowcol = {"getworkflowId","getworkflow_month","getworkflow_year","getcutoff_date","getworkflow_live_date","getworkflow_type","getworkflow_status","gettransaction_audit_id","getworkId","getaction","getdescription","getactioned_by","getactioned_on","getreason","getcomment","getisemergency_update","getworkflow_code","gettransaction_field_change_id","getfield","getfrom_value","getto_value","getwork_type_group","getissn_l","getlead_isbn","getpackage_id","getpackage_title","getpackage_code","getck_site_code","getck_site_name","getsub_platform_code","getsub_platform_name","getck_package_type"};
                        String[] CKTransactionWorkflowcol2 = {"getworkflowId","getWorkflow_Month","getWorkflow_Year","getCutoff_Date","getworkflow_live_date","getworkflow_type","getWorkflow_status","getTransaction_Audit_ID","getworkId","getAction","getDescription","getActioned_By","getActioned_On","getReason","getComment","getIsEmergency_Update","getWorkflow_Code","getTransaction_Field_Change_ID","getField","getfrom_value","getTo_Value","getWork_Type_Group","getISSN_L","getLead_ISBN","getPackage_ID","getPackage_Title","getPackage_Code","getCK_Site_Code","getCK_Site_Name","getSub_platform_code","getSub_Platform_Name","getCK_Package_Type"};
                        int c = 0;
                        for (String strTemp : CKTransactionWorkflowcol) {
                            String strTemp2 = CKTransactionWorkflowcol2[c];
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                            CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp2);

                            Log.info("workflowId => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkflowId() +
                                    " " + strTemp + " => Table = " + method.invoke(objectToCompare1) +
                                    " View = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not matching in ck_transaction_workflow for workId:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getworkId(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            c++;
                        }
                        break;
                }
            }
        }
    }
}
