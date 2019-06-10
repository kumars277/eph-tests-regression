package com.eph.automation.testing.services.db.sql;

public class GeneratingRandomSQL {
    public static String generatingValue="SELECT dq.pmx_source_reference as random_value\n" +
   " FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq dq\n" +
            " join semarchy_eph_mdm.sa_wwork sa on dq.pmx_source_reference=cast(sa.external_reference as numeric)\n" +
            "join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork st on dq.pmx_source_reference=st.\"PRODUCT_WORK_ID\"\n"+
            "where dq.\"dq_err\" = 'N' and sa.b_error_status is null\n" +
            "ORDER BY RANDOM()\n" +
            " LIMIT PARAM1;";
}
