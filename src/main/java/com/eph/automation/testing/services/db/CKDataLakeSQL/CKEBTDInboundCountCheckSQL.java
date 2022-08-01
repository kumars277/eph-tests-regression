package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKEBTDInboundCountCheckSQL {
    //  EBTD View Count SQL
    public static String GET_EBTD_Inbound_VIEW_COUNT = "select count(*) as COUNT from "+GetCKDLDB.getCKEBTDDataBase()+".%s";
    //  EBTD Table Count SQL
    public static String GET_EBTD_Inbound_Table_COUNT = "select count(*) as COUNT from "+GetCKDLDB.getCKEBTDDataBase()+".%s";
}
