package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKEtlCoreDataCheckSql {
    //    GET Current U_Keys
    public static String GET_CK_TRANSFORM_CURRENT = "select u_key as U_KEY from " + GetCKDLDB.getCKDataBase() + ".%s limit %s";
    public static String GET_CK_DELTA_CURRENT = "select u_key as U_KEY from " + GetCKDLDB.getCKDataBase() + ".%s limit %s";


    //    GET Current Data
    public static String GET_CK_TRANSFORM_CURRENT_PACKAGE = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";
    public static String GET_CK_TRANSFORM_CURRENT_SUBJECT_AREA = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";
    public static String GET_CK_TRANSFORM_CURRENT_WORK = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";
    public static String GET_CK_TRANSFORM_CURRENT_PACKAGE_WORK = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";
    public static String GET_CK_TRANSFORM_CURRENT_WORK_SUBJECT_AREA = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";
    public static String GET_CK_TRANSFORM_CURRENT_PACKAGE_WORK_URL = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";

    //    GET InboundSource Data
    public static String GET_CK_PACKAGE_FORM_V = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";
    public static String GET_CK_SUBJECT_AREA_FORM_V = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";
    public static String GET_CK_WORK_FORM_V = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";
    public static String GET_CK_PACKAGE_WORK_FORM_V = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";
    public static String GET_CK_WORK_SUBJECT_AREA_FORM_V = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";
    public static String GET_CK_PACKAGE_WORK_URL_FORM_V = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s')";

    //   GET Delta Current Data
    public static String GET_CK_DELTA_CURRENT_PACKAGE_WORK = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s') order by u_key desc";
    public static String GET_CK_DELTA_CURRENT_WORK_SUBJECT_AREA = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s') order by u_key desc";

//    GET Delta History Data
    public static String GET_CK_DELTA_HISTORY_PACKAGE_WORK = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s') and insert_timestamp in (select insert_timestamp from " + GetCKDLDB.getCKDataBase() + ".%s order by insert_timestamp desc limit 1) order by u_key desc ";
    public static String GET_CK_DELTA_HISTORY_WORK_SUBJECT_AREA = "select * from " + GetCKDLDB.getCKDataBase() + ".%s where u_key in ('%s') and insert_timestamp in (select insert_timestamp from " + GetCKDLDB.getCKDataBase() + ".%s order by insert_timestamp desc limit 1) order by u_key desc";

}
