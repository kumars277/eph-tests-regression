package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKEBTDInboundDataChecksSQL {
    //  EBTD Inbound Data IDs SQL
    public static String GET_EBTD_Inbound_VIEW_IDs = "select master_isbn as U_KEY from "+GetCKDLDB.getCKEBTDDataBase()+".%s limit %s";
    //  EBTD Inbound View Data SQL
    public static String GET_EBTD_Inbound_VIEW_Data = "select * from " + GetCKDLDB.getCKEBTDDataBase() + ".%s where master_isbn in ('%s')";
    //  EBTD Inbound View Data SQL
    public static String GET_EBTD_Inbound_TABLE_Data = "select * from " + GetCKDLDB.getCKEBTDDataBase() + ".%s where master_isbn in ('%s')";

}
