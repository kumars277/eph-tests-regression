package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKETLCountChecksSQL {
//  Inbound Source Count SQL
    public static String GET_CK_Inbound_Source_v = "select count(*) as COUNT from "+GetCKDLDB.getCKDataBase()+".%s where inbound_ts in (select distinct inbound_ts from "+GetCKDLDB.getCKDataBase()+".%s order by inbound_ts desc limit 1)";
//  Current Count SQL
    public static String GET_CK_Current = "select count(*) as COUNT from "+GetCKDLDB.getCKDataBase()+".%s";

//  Delta History Count SQL
    public static String GET_CK_Delta_History = "select count(*) as COUNT from "+GetCKDLDB.getCKDataBase()+".%s where insert_timestamp in (select distinct insert_timestamp from "+GetCKDLDB.getCKDataBase()+".%s order by insert_timestamp desc limit 1)";

//  Delta Current Count SQL
    public static String GET_CK_Delta_Current = "select count(*) as COUNT from "+GetCKDLDB.getCKDataBase()+".%s";

}
