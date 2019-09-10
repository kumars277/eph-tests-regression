package com.eph.automation.testing.services.db.sql;

public class GeneratingRandomSQL {
    public static String generatingValue=
            //old
//            "SELECT dq.pmx_source_reference as random_value\n" +
//   " FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq dq\n" +
//            " join semarchy_eph_mdm.sa_wwork sa on dq.pmx_source_reference=cast(sa.external_reference as numeric)\n" +
//            "join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork st on dq.pmx_source_reference=st.\"PRODUCT_WORK_ID\"\n"+
//            "where dq.\"dq_err\" = 'N' and sa.b_error_status is null\n" +
//            "ORDER BY RANDOM()\n" +
//            " LIMIT PARAM1;";
            "select pmx_source_reference as random_value  FROM stg_10_pmx_wwork_dq ww\n" +
                    "left join semarchy_eph_mdm.gd_wwork gw on ww.pmx_source_reference::varchar = gw.external_reference::varchar\n" +
                    "left join \n" +
                    "      (select distinct s.product_work_id, a.external_reference, a.accountable_product_id \n" +
                    "       from stg_10_pmx_accountable_product_dq s join (select distinct * from semarchy_eph_mdm.sa_accountable_product) a on\n" +
                    "      s.pmx_source_reference = a.external_reference where s.dq_err != 'Y') ap on ww.pmx_source_reference::varchar = ap.product_work_id::varchar\n" +
                    "WHERE ww.dq_err != 'Y'\n" +
                    "ORDER BY RANDOM()\n" +
                    " LIMIT PARAM1;";

    public static String generatingValueDelta =
            "select pmx_source_reference as random_value  FROM stg_10_pmx_wwork_dq ww\n" +
                    "left join semarchy_eph_mdm.gd_wwork gw on ww.pmx_source_reference::varchar = gw.external_reference::varchar\n" +
                    "left join \n" +
                    "      (select distinct s.product_work_id, a.external_reference, a.accountable_product_id \n" +
                    "       from stg_10_pmx_accountable_product_dq s join (select distinct * from semarchy_eph_mdm.sa_accountable_product) a on\n" +
                    "      s.pmx_source_reference = a.external_reference where s.dq_err != 'Y') ap on ww.pmx_source_reference::varchar = ap.product_work_id::varchar\n" +
                    "WHERE ww.dq_err != 'Y'\n" +
                    "--and (TO_TIMESTAMP(ww.work_updated,'YYYYMMDDHH24MI') > TO_TIMESTAMP('{LAST_REFRESH_VALUE}','YYYYMMDDHH24MI'))\n" +
                    "and (TO_TIMESTAMP(ww.work_updated,'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI'))\n" +
                    "ORDER BY RANDOM()\n" +
                    " LIMIT PARAM1;";
}
