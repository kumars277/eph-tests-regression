package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKCMMSExportDataChecksSQL {
    //     GET IDs
    public static String GET_CK_CMMS_WORKFLOW_VIEW_IDs = "select workflowid as U_KEY from " + GetCKDLDB.getCKCMMSDataBase() + ".%s limit %s";
    public static String GET_CK_CMMS_WORK1_VIEW_IDs = "select currentIdentifier as U_KEY from " + GetCKDLDB.getCKCMMSDataBase() + ".%s limit %s";
    public static String GET_CK_CMMS_WORK2_IDENTIFIERS_VIEW_IDs = "select identifier as U_KEY from " + GetCKDLDB.getCKCMMSDataBase() + ".%s limit %s";
    public static String GET_CK_CMMS_WORK3_SUBJECT_AREAS_VIEW_IDs = "select subjectAreaId as U_KEY from " + GetCKDLDB.getCKCMMSDataBase() + ".%s limit %s";
    public static String GET_CK_CMMS_PACKAGE1_VIEW_IDs = "select packageId as U_KEY from " + GetCKDLDB.getCKCMMSDataBase() + ".%s limit %s";
    public static String GET_CK_CMMS_PACKAGE2_WORKS_VIEW_IDs = "select workId as U_KEY from " + GetCKDLDB.getCKCMMSDataBase() + ".%s limit %s";

    //     GET View Records
    public static String GET_CK_CMMS_WORKFLOW_VIEW = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where workflowid in (%s)";
    public static String GET_CK_CMMS_WORK1_VIEW = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where currentIdentifier in ('%s')";
    public static String GET_CK_CMMS_WORK2_IDENTIFIERS_VIEW = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where identifier in ('%s') order by workId desc, identifier desc ";
    public static String GET_CK_CMMS_WORK3_SUBJECT_AREAS_VIEW = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where subjectAreaId in (%s) order by enddate desc, workid desc ";
    public static String GET_CK_CMMS_PACKAGE1_VIEW = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where packageId in ('%s') order by packageID desc";
    public static String GET_CK_CMMS_PACKAGE2_WORKS_VIEW = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where workId in ('%s') order by workId desc, durableurl desc, workflowLiveDate desc";

    //    GET Table Records
    public static String GET_CK_CMMS_WORKFLOW_TABLE = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where workflowid in (%s)";
    public static String GET_CK_CMMS_WORK1 = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where currentIdentifier in ('%s')";
    public static String GET_CK_CMMS_WORK2_IDENTIFIERS = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where identifier in ('%s') order by workId desc, identifier desc";
    public static String GET_CK_CMMS_WORK3_SUBJECT_AREAS = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where subjectAreaId in (%s) order by enddate desc, workid desc";
    public static String GET_CK_CMMS_PACKAGE1 = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where packageId in ('%s') order by packageID desc";
    public static String GET_CK_CMMS_PACKAGE2_WORKS = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where workId in ('%s') order by workId desc, durableurl desc, workflowLiveDate desc";
}
