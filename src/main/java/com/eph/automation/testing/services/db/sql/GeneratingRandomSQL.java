package com.eph.automation.testing.services.db.sql;

public class GeneratingRandomSQL {
    public static String generatingValue="SELECT \n" +
            "    PMX_SOURCE_REFERENCE as random_value\n" +
            "FROM\n" +
            "    ephsit_talend_owner.stg_10_pmx_wwork_dq\n" +
            "where \"dq_err\" = 'N' \n" +
            "ORDER BY RANDOM()\n" +
            "LIMIT 1;";
}
