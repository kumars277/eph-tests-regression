package com.eph.automation.testing.services.db.PROMISDataLakeSQL;

import com.eph.automation.testing.configuration.Constants;

public class PRMtoDataLakeTableCountChecksSQL {

    public static String GET_PRMAUTPUBT_COUNT_ORACLE = "select count(*) as Total_Count from PRM.PRMAUTPUBT";

    public static String GET_PRMAUTPUBT_COUNT_DL = "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmautpubt_part\n" +
            "where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmautpubt_part order by inbound_ts desc limit 1)";

    public static String GET_PRMCLSCODT_COUNT_ORACLE = "select count(*) as Total_Count from PRM.PRMCLSCODT";

    public static String GET_PRMCLSCODT_COUNT_DL = "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmclscodt_part\n" +
            "where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmclscodt_part order by inbound_ts desc limit 1)";

    public static String GET_PRMCLST_COUNT_ORACLE = "select count(*) as Total_Count from PRM.PRMCLST";

    public static String GET_PRMCLST_COUNT_DL = "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmclst_part\n" +
            "where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmclst_part order by inbound_ts desc limit 1)";

    public static String GET_PRMLONDEST_COUNT_ORACLE = "select count(*) as Total_Count from PRM.PRMLONDEST";

    public static String GET_PRMLONDEST_COUNT_DL = "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmlondest_part\n" +
            "where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmlondest_part order by inbound_ts desc limit 1)";

    public static String GET_PRMPRICEST_COUNT_ORACLE = "select count(*) as Total_Count from PRM.PRMPRICEST";

    public static String GET_PRMPRICEST_COUNT_DL = "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpricest_part\n" +
            "where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpricest_part order by inbound_ts desc limit 1)";

    public static String GET_PRMPUBINFT_COUNT_ORACLE = "select count(*) as Total_Count from PRM.PRMPUBINFT";

    public static String GET_PRMPUBINFT_COUNT_DL = "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_part\n" +
            "where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_part order by inbound_ts desc limit 1)";

    public static String GET_PRMPUBRELT_COUNT_ORACLE = "select count(*) as Total_Count from PRM.PRMPUBRELT";

    public static String GET_PRMPUBRELT_COUNT_DL = "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubrelt_part\n" +
            "where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubrelt_part order by inbound_ts desc limit 1)";

}




