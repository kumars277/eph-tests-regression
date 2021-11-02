package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKEtlCoreDataCheckSql {
//    GET Current U_Keys
    public static String GET_CK_TRANSFORM_CURRENT = "select u_key as U_KEY from "+GetCKDLDB.getCKDataBase()+".%s limit %s";

//    GET Current Data
    public static String GET_CK_TRANSFORM_CURRENT_PACKAGE = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";
    public static String GET_CK_TRANSFORM_CURRENT_SUBJECT_AREA = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";
    public static String GET_CK_TRANSFORM_CURRENT_WORK = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";
    public static String GET_CK_TRANSFORM_CURRENT_PACKAGE_WORK = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";
    public static String GET_CK_TRANSFORM_CURRENT_WORK_SUBJECT_AREA = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";
    public static String GET_CK_TRANSFORM_CURRENT_PACKAGE_WORK_URL = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";

//    GET InboundSource Data
    public static String GET_CK_PACKAGE_FORM_V = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";
    public static String GET_CK_SUBJECT_AREA_FORM_V = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";
    public static String GET_CK_WORK_FORM_V = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";
    public static String GET_CK_PACKAGE_WORK_FORM_V = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";
    public static String GET_CK_WORK_SUBJECT_AREA_FORM_V = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";
    public static String GET_CK_PACKAGE_WORK_URL_FORM_V = "select * from "+GetCKDLDB.getCKDataBase()+".%s where u_key in ('%s')";
}
