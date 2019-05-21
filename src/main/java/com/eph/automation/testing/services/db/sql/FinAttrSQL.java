package com.eph.automation.testing.services.db.sql;

public class FinAttrSQL {
    public static String gettingSourceRef="SELECT PMX_SOURCE_REFERENCE as random_value\n" +
            " FROM ephsit_talend_owner.stg_10_pmx_wwork_dq \n" +
            "where \"dq_err\" = 'N'\n" +
            "and opco is  not null and resp_centre is not null\n" +
            "ORDER BY RANDOM()\n" +
            " LIMIT PARAM1;";

    public static String gettingWorkID="SELECT work_id as work_id from semarchy_eph_mdm.sa_wwork where pmx_source_reference \n" +
            "in ('%s') ORDER BY pmx_source_reference";

    public static String GET_STG_DQ_WORKS_DATA ="SELECT \n" +
            " PMX_SOURCE_REFERENCE as PMX_SOURCE_REFERENCE " +
            "  ,opco as opco\n" +
            "  ,resp_centre as resp_centre\n" +
            "  FROM ephsit_talend_owner.stg_10_pmx_wwork_dq \n"+
            "  WHERE pmx_source_reference in ('%s') ORDER BY resp_centre,opco,PMX_SOURCE_REFERENCE";

    public static String GET_SA_FinAttr_DATA ="SELECT \n" +
            "  sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,work_fin_attribs_id AS fin_attribs_id -- Title\n" +
            "  ,f_gl_company AS gl_company -- Subtitle\n" +
            "  ,f_gl_cost_resp_centre as cost_resp_centre\n" +
            "  ,f_gl_revenue_resp_centre as revenue_resp_centre\n" +
            "  ,f_wwork as work_id\n" +
            "  FROM semarchy_eph_mdm.sa_work_financial_attribs sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_financial_attribs join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n"+
            "  AND f_wwork ='PARAM1'";

    public static String GET_SA_FinAttr_DATA_All ="SELECT \n" +
            "  sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,work_fin_attribs_id AS fin_attribs_id -- Title\n" +
            "  ,f_gl_company AS gl_company -- Subtitle\n" +
            "  ,f_gl_cost_resp_centre as cost_resp_centre\n" +
            "  ,f_gl_revenue_resp_centre as revenue_resp_centre\n" +
            "  ,f_wwork as work_id\n" +
            "  FROM semarchy_eph_mdm.sa_work_financial_attribs sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_financial_attribs join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n" +
            "and b_error_status is null\n"+
            "  AND f_wwork in ('%s') order by fin_attribs_id";

    public static String GET_GD_FinnAttr_DATA ="SELECT \n" +
            "  sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,work_fin_attribs_id AS fin_attribs_id -- Title\n" +
            "  ,f_gl_company AS gl_company -- Subtitle\n" +
            "  ,f_gl_cost_resp_centre as cost_resp_centre\n" +
            "  ,f_gl_revenue_resp_centre as revenue_resp_centre\n" +
            "  ,f_wwork as work_id\n" +
            "  FROM semarchy_eph_mdm.gd_work_financial_attribs sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_work_financial_attribs join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )\n"+
            "  AND f_wwork in ('%s') order by fin_attribs_id";

    public static String GET_FinnAttr_ID = "select numeric_id as fin_attribs_id from ephsit_talend_owner.map_sourceref_2_numericid " +
            "where source_ref = 'PARAM1'";

    public static String Get_work_id = "select eph_id as workID from ephsit_talend_owner.map_sourceref_2_ephid " +
            "where ref_type = 'WORK' and source_ref='PARAM1'";

    public static String Get_SA_count = "select count (*) as saCount " +
            "FROM semarchy_eph_mdm.sa_work_financial_attribs sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_financial_attribs join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )";


    public static String Get_GD_count = "select count (*) as gdCount " +
            "  FROM semarchy_eph_mdm.gd_work_financial_attribs gd\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_work_financial_attribs join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )";

    public static String Get_AE_count = "select count (distinct work_fin_attribs_id ) as aeCount " +
            "  FROM semarchy_eph_mdm.ae_work_financial_attribs ae\n"+
            " where ae.b_batchid =  (select max (ae.b_batchid) from\n" +
            "semarchy_eph_mdm.ae_work_financial_attribs ae join \n"+
            "semarchy_eph_mdm.gd_event e on ae.b_batchid = e.b_batchid\n"+
            "where  e.f_event_type = 'PMX'\n"+
            "and e.workflow_id = 'talend'\n"+
            "AND e.f_event_type = 'PMX'\n"+
            "and e.f_workflow_source = 'PMX' )";


    public static String PMX_STG_DQ_WORKS_COUNT_NoErr = "select count (*) as dqCount from ephsit_talend_owner.stg_10_pmx_wwork_dq where dq_err='N' and opco is not null and resp_centre is not null";
}
