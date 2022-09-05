package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKLongListDataChecksSQL {
    //  LongList Data IDs SQL
    public static String GET_LongList_VIEW_IDs = "select master_isbn as U_KEY from "+GetCKDLDB.getCKCMMSDataBase()+".%s limit %s";
    //  LongList View Data SQL
    public static String GET_LongList_VIEW_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where master_isbn in ('%s')";
    //  LongList Table Data SQL
    public static String GET_LongList_TABLE_Data = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where master_isbn in ('%s')";

}
