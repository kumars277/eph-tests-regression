package com.eph.automation.testing.services.db.sql;

public class GeneratingRandomSQL {
    public static String generatingValue="SELECT \n" +
            "    \"PARAM1\" as random_value\n" +
            "FROM\n" +
            "    ephsit.ephsit_talend_owner.stg_pmx_wwork\n" +
            "where \"PARAM1\" is not null \n" +
            "ORDER BY RANDOM()\n" +
            "LIMIT 1;";
}
