package com.eph.automation.testing.services.db.PROMISDataLakeSQL;

import com.eph.automation.testing.configuration.Constants;

public class PRMtoDataLakeTableCountChecksSQL {

    public static String GET_PRMAUTPUBT_COUNT_ORACLE = "select count(*) as Total_Count from PRM.PRMAUTPUBT";

    public static String GET_PRMAUTPUBT_COUNT_DL = "select count(*) as Total_Count from "+Constants.PRM_AWS_SCHEMA+".promis_prmautpubt_part";



}




