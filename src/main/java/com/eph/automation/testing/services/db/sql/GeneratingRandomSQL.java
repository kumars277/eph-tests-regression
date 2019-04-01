package com.eph.automation.testing.services.db.sql;

public class GeneratingRandomSQL {
    public static String generatingValue="SELECT dq.PMX_SOURCE_REFERENCE as random_value\n" +
   " FROM ephsit_talend_owner.stg_10_pmx_wwork_dq dq\n" +
            " join semarchy_eph_mdm.sa_wwork sa on dq.pmx_source_reference=cast(sa.pmx_source_reference as numeric)\n" +
            "where dq.\"dq_err\" = 'N' and sa.b_error_status is null\n" +
            "ORDER BY RANDOM()\n" +
            " LIMIT 1;";
}
