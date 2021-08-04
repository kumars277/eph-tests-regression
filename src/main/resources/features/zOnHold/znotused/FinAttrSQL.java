package features.zOnHold.znotused;

import com.eph.automation.testing.services.db.sql.GetEPHDBUser;

public class FinAttrSQL {
    public static String gettingSourceRef="SELECT PMX_SOURCE_REFERENCE as random_value\n" +
            " FROM "+ GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq \n" +
            "where \"dq_err\" = 'N'\n" +
            "and opco is  not null and resp_centre is not null\n" +
            "ORDER BY RANDOM()\n" +
            " LIMIT PARAM1;";

    public static String gettingSourceRefDelta="SELECT PMX_SOURCE_REFERENCE as random_value\n" +
            "from " +GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork ww \n" +
            "join " +GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq dq on dq.PMX_SOURCE_REFERENCE=ww.\"PRODUCT_WORK_ID\"\n" +
            "left join semarchy_eph_mdm.gd_wwork gdw on gdw.external_reference = dq.pmx_source_reference::text\n" +
            "left join semarchy_eph_mdm.gd_work_financial_attribs wfa on wfa.f_wwork = gdw.work_id\n" +
            "where \"F_OPCO_R12\" is not null and \"F_RESPONSIBILITY_CENTRE\" is not null \n" +
            "and TO_DATE(\"UPDATED\",'YYYYMMDDHH24MI') > TO_DATE('PARAM2','YYYYMMDDHH24MI')\n" +
            "and dq.dq_err!='Y'\n" +
            "and (wfa.f_gl_company = dq.opco and wfa.f_gl_cost_resp_centre = dq.resp_centre and wfa.f_gl_revenue_resp_centre  = dq.resp_centre) \n" +
            "and wfa.effective_end_date  is  null\n" +
            "and wfa.b_batchid in (select max(b_batchid) from semarchy_eph_mdm.gd_event where description = 'PMX Talend Load' and workflow_id = 'talend' and f_workflow_source = 'PMX')\n" +
            "ORDER BY RANDOM()"+
            " LIMIT PARAM1;";

    public static String gettingWorkID="SELECT work_id as work_id from semarchy_eph_mdm.sa_wwork where external_reference \n" +
            "in ('%s') ORDER BY external_reference";

    public static String GET_STG_DQ_WORKS_DATA ="SELECT \n" +
            " PMX_SOURCE_REFERENCE as PMX_SOURCE_REFERENCE " +
            "  ,opco as opco\n" +
            "  ,resp_centre as resp_centre\n" +
            "  FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq \n"+
            "  WHERE pmx_source_reference in ('%s') ORDER BY resp_centre,opco,PMX_SOURCE_REFERENCE";

    public static String GET_SA_FinAttr_DATA ="SELECT \n" +
            "  sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,work_fin_attribs_id AS fin_attribs_id -- Title\n" +
            "  ,f_gl_company AS gl_company -- Subtitle\n" +
            "  ,f_gl_cost_resp_centre as cost_resp_centre\n" +
            "  ,f_gl_revenue_resp_centre as revenue_resp_centre\n" +
            "  ,f_wwork as work_id\n" +
            "  ,external_reference as external_reference\n" +
            "  FROM semarchy_eph_mdm.sa_work_financial_attribs sa\n"+
            " where f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.sa_event\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n"+
            "and effective_end_date is null\n"+
            "  and f_wwork ='PARAM1'";

    public static String GET_SA_FinAttr_DATA_All ="SELECT \n" +
            "  sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,work_fin_attribs_id AS fin_attribs_id -- Title\n" +
            "  ,f_gl_company AS gl_company -- Subtitle\n" +
            "  ,f_gl_cost_resp_centre as cost_resp_centre\n" +
            "  ,f_gl_revenue_resp_centre as revenue_resp_centre\n" +
            "  ,f_wwork as work_id\n" +
            "  ,external_reference as external_reference\n" +
            "  FROM semarchy_eph_mdm.sa_work_financial_attribs sa\n"+
            " where f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.sa_event\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n" +
            "and b_error_status is null\n"+
            "and effective_end_date is null\n"+
            "  and f_wwork in ('%s') order by fin_attribs_id";

    public static String GET_GD_FinnAttr_DATA ="SELECT \n" +
            "  sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,work_fin_attribs_id AS fin_attribs_id -- Title\n" +
            "  ,f_gl_company AS gl_company -- Subtitle\n" +
            "  ,f_gl_cost_resp_centre as cost_resp_centre\n" +
            "  ,f_gl_revenue_resp_centre as revenue_resp_centre\n" +
            "  ,f_wwork as work_id\n" +
            "  ,external_reference as external_reference" +
            "  FROM semarchy_eph_mdm.gd_work_financial_attribs sa\n"+
            " where f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.gd_event\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )\n"+
            "and effective_end_date is null"+
            "  and f_wwork in ('%s') order by fin_attribs_id";

    public static String GET_FinnAttr_ID = "select numeric_id as fin_attribs_id from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid " +
            "where source_ref = 'PARAM1'";

    public static String Get_work_id = "select eph_id as workID from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid " +
            "where ref_type = 'WORK' and source_ref='PARAM1'";

    public static String Get_SA_count = "select count (*) as saCount " +
            "FROM semarchy_eph_mdm.sa_work_financial_attribs sa\n"+
            " where f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.sa_event\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n"+
            "and sa.effective_end_date is null";


    public static String Get_GD_count = "select count (*) as gdCount " +
            "  FROM semarchy_eph_mdm.gd_work_financial_attribs gd\n"+
            " where f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.gd_event\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )\n"+
            "and gd.effective_end_date is null";

    public static String Get_AE_count = "select count (distinct ae.work_fin_attribs_id ) as aeCount \n" +
            "  FROM semarchy_eph_mdm.ae_work_financial_attribs ae\n" +
            "  join semarchy_eph_mdm.sa_work_financial_attribs sa on sa.work_fin_attribs_id=ae.work_fin_attribs_id\n" +
            " where ae.b_batchid =  (select max (b_batchid) from\n" +
            "semarchy_eph_mdm.gd_event e\n" +
            "where  e.f_event_type = 'PMX'\n" +
            "and e.workflow_id = 'talend'\n" +
            "AND e.f_event_type = 'PMX'\n" +
            "and e.f_workflow_source = 'PMX' )\n" +
            "and sa.effective_end_date is null";


    public static String PMX_STG_DQ_WORKS_COUNT_NoErr =

            //old
//            "select count(*) as dqCount\n" +
//            "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork ww \n" +
//            "join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq dq on dq.PMX_SOURCE_REFERENCE=ww.\"PRODUCT_WORK_ID\"\n" +
//            "left join semarchy_eph_mdm.gd_wwork gdw on gdw.external_reference = dq.pmx_source_reference::text\n" +
//            "left join semarchy_eph_mdm.gd_work_financial_attribs wfa on wfa.f_wwork = gdw.work_id\n" +
//            "where \"F_OPCO_R12\" is not null and \"F_RESPONSIBILITY_CENTRE\" is not null \n" +
////            "and TO_DATE(\"UPDATED\",'YYYYMMDDHH24MI') > TO_DATE('PARAM1','YYYYMMDDHH24MI')\n" +
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')\n" +
//            "and dq.dq_err!='Y'\n" +
//            "and (wfa.f_gl_company = dq.opco and wfa.f_gl_cost_resp_centre = dq.resp_centre and wfa.f_gl_revenue_resp_centre  = dq.resp_centre) \n" +
//            "and wfa.effective_end_date  is  null\n" +
//            "and wfa.b_batchid in (select max(b_batchid) from semarchy_eph_mdm.gd_event where description = 'PMX Talend Load' and workflow_id = 'talend' and f_workflow_source = 'PMX')\n";
    "select count(*) as dqCount\n" +
            "from " +GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq s\n" +
            "--left join (\n" +
            "--      select \n" +
            "--             f.f_gl_company as company\n" +
            "--            ,f.f_gl_cost_resp_centre as cost_rc\n" +
            "--            ,f.f_gl_revenue_resp_centre as rev_rc\n" +
            "--            ,w.external_reference\n" +
            "--      from semarchy_eph_mdm.gd_work_financial_attribs f\n" +
            "--      join semarchy_eph_mdm.gd_wwork w on f.f_wwork = w.work_id\n" +
            "--      where f.effective_end_date is null) as g on s.pmx_source_reference::varchar = g.external_reference\n" +
            "left join (select distinct external_reference, work_fin_attribs_id from semarchy_eph_mdm.sa_work_financial_attribs) a on concat(s.pmx_source_reference,s.opco,s.resp_centre)::varchar = a.external_reference\n" +
            "where dq_err != 'Y'\n" +
            "and resp_centre is not null\n" +
            "and opco is not null\n" +
            "--and not (\n" +
            "--         s.opco = coalesce(g.company,'')\n" +
            "--    and  s.resp_centre = coalesce(g.cost_rc,'')\n" +
            "--    and  s.resp_centre = coalesce(g.rev_rc,''))\n" +
            "and (TO_TIMESTAMP(s.work_updated,'YYYYMMDDHH24MI') > TO_TIMESTAMP('%s','YYYYMMDDHH24MI'))";

    public static String PMX_STG_DQ_WORKS_COUNT_NoErr_Full = "select count(*) as dqCount\n" +
            "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq s\n" +
            "--left join (\n" +
            "--      select \n" +
            "--             f.f_gl_company as company\n" +
            "--            ,f.f_gl_cost_resp_centre as cost_rc\n" +
            "--            ,f.f_gl_revenue_resp_centre as rev_rc\n" +
            "--            ,w.external_reference\n" +
            "--      from semarchy_eph_mdm.gd_work_financial_attribs f\n" +
            "--      join semarchy_eph_mdm.gd_wwork w on f.f_wwork = w.work_id\n" +
            "--      where f.effective_end_date is null) as g on s.pmx_source_reference::varchar = g.external_reference\n" +
            "left join (select distinct external_reference, work_fin_attribs_id from semarchy_eph_mdm.sa_work_financial_attribs) a on concat(s.pmx_source_reference,s.opco,s.resp_centre)::varchar = a.external_reference\n" +
            "where dq_err != 'Y'\n" +
            "and resp_centre is not null\n" +
            "and opco is not null\n" +
            "--and not (\n" +
            "--         s.opco = coalesce(g.company,'')\n" +
            "--    and  s.resp_centre = coalesce(g.cost_rc,'')\n" +
            "--    and  s.resp_centre = coalesce(g.rev_rc,''))";

    public static String GET_GD_FinnAttr_DATA_End_Date ="SELECT \n" +
            "  sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,work_fin_attribs_id AS fin_attribs_id -- Title\n" +
            "  ,f_gl_company AS gl_company -- Subtitle\n" +
            "  ,f_gl_cost_resp_centre as cost_resp_centre\n" +
            "  ,f_gl_revenue_resp_centre as revenue_resp_centre\n" +
            "  ,f_wwork as work_id\n" +
            "  ,sa.external_reference as pmx_source_reference\n" +
            "  FROM semarchy_eph_mdm.gd_work_financial_attribs sa\n"+
            "  join semarchy_eph_mdm.gd_wwork ww on sa.f_wwork = ww.work_id\n"+
            " where sa.f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_work_financial_attribs join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )\n"+
            "  AND effective_end_date is not null";

    public static String GET_STG_DQ_WORKS_DATA_Delta ="SELECT \n" +
            " PMX_SOURCE_REFERENCE as PMX_SOURCE_REFERENCE " +
            "  ,opco as opco\n" +
            "  ,resp_centre as resp_centre\n" +
            "  FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq \n"+
            "  WHERE pmx_source_reference='%s'";
}
