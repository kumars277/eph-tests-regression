package com.eph.automation.testing.services.db.sql;

public class GeneratingRandomSQL {
    public static String generatingValue="SELECT \n" +
            "    \"PARAM1\" as random_value\n" +
            "FROM\n" +
            "    ephsit.ephsit_talend_owner.stg_10_pmx_wwork\n" +
            "where \"PARAM1\" is not null \n" +
            "and \"WORK_TYPE\"='PARAM2' \n" +
            "ORDER BY RANDOM()\n" +
            "LIMIT 1;";
}
