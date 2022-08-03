package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKReportsCountChecksSQL {
    //  Reports View Count SQL
    public static String GET_Reports_VIEW_COUNT = "select count(*) as COUNT from "+GetCKDLDB.getCKCMMSDataBase()+".%s";
    //  Reports Table Count SQL
    public static String GET_Reports_Table_COUNT = "select count(*) as COUNT from "+GetCKDLDB.getCKCMMSDataBase()+".%s";
}
