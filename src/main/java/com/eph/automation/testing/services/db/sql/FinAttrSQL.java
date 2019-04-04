package com.eph.automation.testing.services.db.sql;

public class FinAttrSQL {
    public static String gettingSourceRef="SELECT dq.PMX_SOURCE_REFERENCE as random_value\n" +
            " FROM ephsit_talend_owner.stg_10_pmx_wwork_dq dq\n" +
            " join semarchy_eph_mdm.sa_work_financial_attribs sa on dq.pmx_source_reference=cast(sa.work_fin_attribs_id as numeric)\n" +
            "join ephsit_talend_owner.stg_10_pmx_wwork st on dq.pmx_source_reference=st.\"PRODUCT_WORK_ID\"\n"+
            "where dq.\"dq_err\" = 'N' and sa.b_error_status is null and st.\"WORK_TYPE\"='PARAM1'\n" +
            "ORDER BY RANDOM()\n" +
            " LIMIT 1;";

    public static String gettingWorkID="SELECT work_id as work_id from semarchy_eph_mdm.sa_wwork where pmx_source_reference='PARAM1'";

    public static String GET_STG_DQ_WORKS_DATA ="SELECT \n" +
            "  opco as opco\n" +
            "  ,resp_centre as resp_centre\n" +
            "  FROM ephsit_talend_owner.stg_10_pmx_wwork_dq \n"+
            "  WHERE pmx_source_reference='PARAM1'";

    public static String GET_SA_FinAttr_DATA ="SELECT \n" +
            "  sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,work_fin_attribs_id AS fin_attribs_id -- Title\n" +
            "  ,f_gl_company AS gl_company -- Subtitle\n" +
            "  ,f_gl_cost_resp_centre as cost_resp_centre\n" +
            "  ,f_gl_revenue_resp_centre as revenue_resp_centre\n" +
            "  ,f_wwork as work_id\n" +
            "  FROM ephsit.semarchy_eph_mdm.sa_work_financial_attribs sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_financial_attribs join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n"+
            "  AND f_wwork='PARAM1'";

    public static String GET_GD_FinnAttr_DATA ="SELECT \n" +
            "  sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,work_fin_attribs_id AS fin_attribs_id -- Title\n" +
            "  ,f_gl_company AS gl_company -- Subtitle\n" +
            "  ,f_gl_cost_resp_centre as cost_resp_centre\n" +
            "  ,f_gl_revenue_resp_centre as revenue_resp_centre\n" +
            "  ,f_wwork as work_id\n" +
            "  FROM ephsit.semarchy_eph_mdm.gd_work_financial_attribs sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_work_financial_attribs join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )\n"+
            "  AND f_wwork='PARAM1'";

    public static String GET_FinnAttr_ID = "select numeric_id as fin_attribs_id from ephsit_talend_owner.map_sourceref_2_numericid " +
            "where source_ref = 'PARAM1'";
}
