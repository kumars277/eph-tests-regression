package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKCMMSOutboundCountCheckSQL {
    //  CMMS View Count SQL
    public static String GET_CMMS_VIEW_COUNT = "select count(*) as COUNT from "+GetCKDLDB.getCKCMMSDataBase()+".%s";

    //  CMMS Table Count SQL
    public static String GET_CMMS_Table_COUNT = "select count(*) as COUNT from "+GetCKDLDB.getCKCMMSDataBase()+".%s";
}
