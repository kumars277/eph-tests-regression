package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKLongListCountChecksSQL {
    //  LongList View Count SQL
    public static String GET_LongList_VIEW_COUNT = "select count(*) as COUNT from "+GetCKDLDB.getCKCMMSDataBase()+".%s";
    //  LongList Table Count SQL
    public static String GET_LongList_Table_COUNT = "select count(*) as COUNT from "+GetCKDLDB.getCKCMMSDataBase()+".%s";

}
