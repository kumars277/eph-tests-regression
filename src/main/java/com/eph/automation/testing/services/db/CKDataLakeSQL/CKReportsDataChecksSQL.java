package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKReportsDataChecksSQL {
    //  Reports Data IDs SQL
    public static String GET_WORKFLOW_TABLEAU_VIEW_IDs = "select Work_ID as u_key from " + GetCKDLDB.getCKCMMSDataBase() + ".%s limit %s";
    public static String GET_WORKFLOW_CONTROL_P1_VIEW_IDs = "select workflow_id as u_key from " + GetCKDLDB.getCKCMMSDataBase() + ".%s limit %s";
    public static String GET_WORKFLOW_CONTROL_P2_VIEW_IDs = "select workflow_id as u_key from " + GetCKDLDB.getCKCMMSDataBase() + ".%s limit %s";
    public static String GET_WORKFLOW_TABLEAU_PACKAGE_WORKS_VIEW_IDs = "select work_id as u_key from " + GetCKDLDB.getCKCMMSDataBase() + ".%s limit %s";
    public static String GET_TRANSACTION_WORKFLOW_VIEW_IDs = "select Work_ID as u_key from " + GetCKDLDB.getCKCMMSDataBase() + ".%s limit %s";

    // Reports View data SQL
    public static String GET_WORKFLOW_TABLEAU_VIEW_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where Work_ID in ('%s') order by Work_ID, Package_Title, Specialty_Name, CK_Site_Name, CK_Title_Start_Date";
    public static String GET_WORKFLOW_CONTROL_P1_VIEW_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where workflow_id in (%s)";
    public static String GET_WORKFLOW_CONTROL_P2_VIEW_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where workflow_id in (%s)";
    public static String GET_WORKFLOW_TABLEAU_PACKAGE_WORKS_VIEW_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where work_id in ('%s') order by work_id, workflow_live_date, package_id, specialty_name";
    public static String GET_TRANSACTION_WORKFLOW_VIEW_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where Work_ID in ('%s') order by work_id, workflow_status, transaction_field_change_id, package_id, to_value";

    // Reports Table data SQL
    public static String GET_WORKFLOW_TABLEAU_TABLE_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where work_id in ('%s') order by work_id, Package_Title, specialty_name, ck_site_name, ck_title_start_date";
    public static String GET_WORKFLOW_CONTROL_P1_TABLE_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where workflow_id in (%s)";
    public static String GET_WORKFLOW_CONTROL_P2_TABLE_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where workflow_id in (%s)";
    public static String GET_WORKFLOW_TABLEAU_PACKAGE_WORKS_TABLE_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where work_id in ('%s') order by work_id, workflow_live_date, package_id, specialty_name";
    public static String GET_TRANSACTION_WORKFLOW_TABLE_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where Work_ID in ('%s') order by work_id, workflow_status, transaction_field_change_id, package_id, to_value";
}
