package com.eph.automation.testing.services.db.sql;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class DataQualityChecksSQL {

    public static String GET_TOTAL_COUNT_BY_TABLE_NAME = "select count(*) as TOTAL_COUNT " +
            " FROM SCHEMA_PARAM.TABLE_PARAM";

}
