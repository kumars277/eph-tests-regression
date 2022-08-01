package com.eph.automation.testing.services.db.consumerAppSQL;


import com.eph.automation.testing.services.db.PROMISDataLakeSQL.GetPRMDLDBUser;

public class r12ChecksSQL {
     private r12ChecksSQL() {
         throw new IllegalStateException("Utility class");
     }

       public static String GET_FULL_SET_COUNT_R12 =
              "select count(*) as Source_count from(\n" +
                      "WITH\n" +
                      "  work_data AS (\n" +
                      "   SELECT\n" +
                      "     w.work_id work_epr\n" +
                      "   , w.work_title\n" +
                      "   , wt.l_description work_type\n" +
                      "   , wt.roll_up_type work_super_type\n" +
                      "   , ws.l_description work_status\n" +
                      "   , ws.roll_up_status work_super_status\n" +
                      "   , w.f_pmc\n" +
                      "   , w.f_accountable_product\n" +
                      "   , w.b_upddate b_upddate\n" +
                      "   , w.b_credate b_credate\n" +
                      "   FROM\n" +
                      "     (("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n" +
                      "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                      "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                      ") \n" +
                      ", manifestation_data AS (\n" +
                      "   SELECT\n" +
                      "     m.manifestation_id\n" +
                      "   , m.f_wwork\n" +
                      "   , mt.l_description manifestation_type\n" +
                      "   , m.b_upddate b_upddate\n" +
                      "   , m.b_credate b_credate\n" +
                      "   , ms.l_description manifestation_status\n" +
                      "   FROM\n" +
                      "     (("+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation m\n" +
                      "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_type mt ON (m.f_type = mt.code))\n" +
                      "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_status ms ON (ms.code = m.f_status))\n" +
                      ") \n" +
                      ", work_ids AS (\n" +
                      "   SELECT\n" +
                      "     f_wwork\n" +
                      "   , max(b_upddate) b_upddate\n" +
                      "   , max((CASE WHEN (f_type = 'ELSEVIER JOURNAL NUMBER') THEN identifier ELSE null END)) journal_number\n" +
                      "   , max((CASE WHEN (f_type = 'JOURNAL ACRONYM') THEN identifier ELSE null END)) journal_acronym\n" +
                      "   , max((CASE WHEN (f_type = 'ISSN-L') THEN identifier ELSE null END)) issn_l\n" +
                      "   , max(b_credate) b_credate\n" +
                      "   FROM\n" +
                      "     (\n" +
                      "      SELECT\n" +
                      "        f_wwork\n" +
                      "      , b_upddate b_upddate\n" +
                      "      , f_type\n" +
                      "      , identifier\n" +
                      "      , b_credate\n" +
                      "      FROM\n" +
                      "        "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier\n" +
                      "      WHERE ((f_type IN ('ELSEVIER JOURNAL NUMBER', 'JOURNAL ACRONYM', 'ISSN-L')) AND (effective_end_date IS NULL))\n" +
                      "   ) \n" +
                      "   GROUP BY f_wwork\n" +
                      ") \n" +
                      ", manifestation_ids AS (\n" +
                      "   SELECT\n" +
                      "     f_manifestation\n" +
                      "   , max(b_upddate) b_upddate\n" +
                      "   , max((CASE WHEN (f_type = 'ISSN') THEN identifier ELSE null END)) issn\n" +
                      "   , max((CASE WHEN (f_type = 'ISBN') THEN identifier ELSE null END)) isbn\n" +
                      "   , max(b_credate) b_credate\n" +
                      "   FROM\n" +
                      "     (\n" +
                      "      SELECT\n" +
                      "        f_manifestation\n" +
                      "      , b_upddate b_upddate\n" +
                      "      , f_type\n" +
                      "      , identifier\n" +
                      "      , b_credate\n" +
                      "      FROM\n" +
                      "        "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier\n" +
                      "      WHERE ((f_type IN ('ISSN', 'ISBN')) AND (effective_end_date IS NULL))\n" +
                      "   ) \n" +
                      "   GROUP BY f_manifestation\n" +
                      ") \n" +
                      ", financial_attribs AS (\n" +
                      "   SELECT\n" +
                      "     f_wwork\n" +
                      "   , f_gl_company company_code\n" +
                      "   , f_gl_cost_resp_centre responsibility_centre\n" +
                      "   , b_upddate b_upddate\n" +
                      "   , b_credate b_credate\n" +
                      "   FROM\n" +
                      "     "+GetPRMDLDBUser.getProdDataBase()+".gd_work_financial_attribs\n" +
                      "   WHERE (effective_end_date IS NULL)\n" +
                      ") \n" +
                      ", person AS (\n" +
                      "   SELECT\n" +
                      "     p.given_name\n" +
                      "   , p.family_name\n" +
                      "   , wp.f_wwork\n" +
                      "   , greatest(p.b_upddate, wp.b_upddate) b_upddate\n" +
                      "   , greatest(p.b_credate, wp.b_credate) b_credate\n" +
                      "   FROM\n" +
                      "     ("+GetPRMDLDBUser.getProdDataBase()+".gd_person p\n" +
                      "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_person_role wp ON (p.person_id = wp.f_person))\n" +
                      "   WHERE ((wp.f_role = 'PU') AND (wp.effective_end_date IS NULL))\n" +
                      ") \n" +
                      "SELECT\n" +
                      "  wd.work_epr eph_work_id\n" +
                      ", (CASE WHEN ((((((((ap.b_upddate IS NULL) AND (wd.b_upddate IS NULL)) AND (md.b_upddate IS NULL)) AND (wi.b_upddate IS NULL)) AND (mi.b_upddate IS NULL)) AND (fa.b_upddate IS NULL)) AND (pmc.b_upddate IS NULL)) AND (per.b_upddate IS NULL)) THEN greatest(COALESCE(CAST(ap.b_credate AS varchar), '0'), COALESCE(CAST(wd.b_credate AS varchar), '0'), COALESCE(CAST(md.b_credate AS varchar), '0'), COALESCE(CAST(wi.b_credate AS varchar), '0'), COALESCE(CAST(mi.b_credate AS varchar), '0'), COALESCE(CAST(fa.b_credate AS varchar), '0'), COALESCE(CAST(pmc.b_credate AS varchar), '0'), COALESCE(CAST(per.b_credate AS varchar), '0')) ELSE greatest(COALESCE(CAST(ap.b_upddate AS varchar), '0'), COALESCE(CAST(wd.b_upddate AS varchar), '0'), COALESCE(CAST(md.b_upddate AS varchar), '0'), COALESCE(CAST(wi.b_upddate AS varchar), '0'), COALESCE(CAST(mi.b_upddate AS varchar), '0'), COALESCE(CAST(fa.b_upddate AS varchar), '0'), COALESCE(CAST(pmc.b_upddate AS varchar), '0'), COALESCE(CAST(per.b_upddate AS varchar), '0')) END) last_updated_date\n" +
                      ", wd.work_super_type product_type\n" +
                      ", wd.work_title product_description\n" +
                      ", (CASE WHEN (wd.work_super_type = 'Journal') THEN ap.gl_product_segment_code WHEN ((wd.work_super_type = 'Book') AND (mi.isbn IS NOT NULL)) THEN mi.isbn ELSE null END) r12_alias\n" +
                      ", fa.company_code\n" +
                      ", fa.responsibility_centre\n" +
                      ", ap.gl_product_segment_code product_segment\n" +
                      ", wd.b_credate created_date\n" +
                      ", wi.journal_acronym\n" +
                      ", pmc.f_pmg pmg\n" +
                      ", wd.f_pmc pmc\n" +
                      ", mi.issn\n" +
                      ", concat(per.given_name, ' ', per.family_name) publisher\n" +
                      ", md.manifestation_type\n" +
                      ", md.manifestation_status\n" +
                      "FROM\n" +
                      "  (((((((work_data wd\n" +
                      "LEFT JOIN manifestation_data md ON (wd.work_epr = md.f_wwork))\n" +
                      "LEFT JOIN work_ids wi ON (wd.work_epr = wi.f_wwork))\n" +
                      "LEFT JOIN manifestation_ids mi ON (md.manifestation_id = mi.f_manifestation))\n" +
                      "LEFT JOIN financial_attribs fa ON (wd.work_epr = fa.f_wwork))\n" +
                      "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmc pmc ON (wd.f_pmc = pmc.code))\n" +
                      "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (wd.f_accountable_product = ap.accountable_product_id))\n" +
                      "LEFT JOIN person per ON (per.f_wwork = wd.work_epr))\n" +
                      ")";
     public static String GET_TARGET_COUNT_R12=
             "select count(*) as Target_count from "+GetPRMDLDBUser.getProdDataBase()+".r12_full_data_v";

     public static String GET_DRM_ACTION_TARGET_COUNT=
             "select count(*) as Target_count from "+GetPRMDLDBUser.getProdDataBase()+".drm_action_scripts_v";

     public static String GET_DRM_ACTION_SOURCE_COUNT=
    "select count(*) as Source_count from(\n"+
            "SELECT\n"+
            "  base.script_group\n"+
            ", base.action\n"+
            ", base.consolidation\n"+
            ", base.chart_of_account_value\n"+
            ", base.product_segment\n"+
            ", base.action_value_type\n"+
            ", base.value\n"+
            ", base.eph_work_id\n"+
            ", base.last_updated_date\n"+
            "FROM\n"+
            "  (\n"+
            "   SELECT\n"+
            "     'Group 1' script_group\n"+
            "   , 'Add' action\n"+
            "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
            "   , 'R12 Product' chart_of_account_value\n"+
            "   , ap.gl_product_segment_code product_segment\n"+
            "   , 'R12 Product' action_value_type\n"+
            "   , 'TRUE' value\n"+
            "   , w.work_id eph_work_id\n"+
            "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
            "   FROM\n"+
            "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
            "   WHERE (wt.roll_up_type = 'Journal')\n"+
            "UNION    SELECT\n"+
            "     'Group 2' script_group\n"+
            "   , 'ChangeProp' action\n"+
            "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
            "   , 'R12 Product' chart_of_account_value\n"+
            "   , ap.gl_product_segment_code product_segment\n"+
            "   , 'Allow Budgeting' action_value_type\n"+
            "   , 'TRUE' value\n"+
            "   , w.work_id eph_work_id\n"+
            "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
            "   FROM\n"+
            "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
            "   WHERE (wt.roll_up_type = 'Journal')\n"+
            "UNION    SELECT\n"+
            "     'Group 3' script_group\n"+
            "   , 'ChangeProp' action\n"+
            "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
            "   , 'R12 Product' chart_of_account_value\n"+
            "   , ap.gl_product_segment_code product_segment\n"+
            "   , 'Allow Posting' action_value_type\n"+
            "   , 'TRUE' value\n"+
            "   , w.work_id eph_work_id\n"+
            "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
            "   FROM\n"+
            "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
            "   WHERE (wt.roll_up_type = 'Journal')\n"+
            "UNION    SELECT\n"+
            "     'Group 4' script_group\n"+
            "   , 'ChangeProp' action\n"+
            "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
            "   , 'R12 Product' chart_of_account_value\n"+
            "   , ap.gl_product_segment_code product_segment\n"+
            "   , 'Description' action_value_type\n"+
            "   , replace(replace(w.work_title, chr(13), ''), chr(10), ' ') value\n"+
            "   , w.work_id eph_work_id\n"+
            "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
            "   FROM\n"+
            "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
            "   WHERE (wt.roll_up_type = 'Journal')\n"+
            "UNION    SELECT\n"+
            "     'Group 5' script_group\n"+
            "   , 'ChangeProp' action\n"+
            "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
            "   , 'R12 Product' chart_of_account_value\n"+
            "   , ap.gl_product_segment_code product_segment\n"+
            "   , 'GLProductType' action_value_type\n"+
            "   , 'Journal' value\n"+
            "   , w.work_id eph_work_id\n"+
            "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
            "   FROM\n"+
            "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
            "   WHERE (wt.roll_up_type = 'Journal')\n"+
            "UNION    SELECT\n"+
            "     'Group 6' script_group\n"+
            "   , 'Move' action\n"+
            "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
            "   , 'R12 Product' chart_of_account_value\n"+
            "   , ap.gl_product_segment_code product_segment\n"+
            "   , ap.f_gl_product_segment_parent action_value_type\n"+
            "   , null value\n"+
            "   , w.work_id eph_work_id\n"+
            "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
            "   FROM\n"+
            "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
            "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
            "   WHERE (wt.roll_up_type = 'Journal')\n"+
            ")  base\n"+
            "ORDER BY base.script_group ASC, base.chart_of_account_value ASC\n"+
            ")";

    public static String GET_RANDOM_EPR_ID_R12_FULL =
            "select distinct eph_work_id as randomIds from(\n" +
                    "WITH\n" +
                    "  work_data AS (\n" +
                    "   SELECT\n" +
                    "     w.work_id work_epr\n" +
                    "   , w.work_title\n" +
                    "   , wt.l_description work_type\n" +
                    "   , wt.roll_up_type work_super_type\n" +
                    "   , ws.l_description work_status\n" +
                    "   , ws.roll_up_status work_super_status\n" +
                    "   , w.f_pmc\n" +
                    "   , w.f_accountable_product\n" +
                    "   , w.b_upddate b_upddate\n" +
                    "   , w.b_credate b_credate\n" +
                    "   FROM\n" +
                    "     (("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                    ") \n" +
                    ", manifestation_data AS (\n" +
                    "   SELECT\n" +
                    "     m.manifestation_id\n" +
                    "   , m.f_wwork\n" +
                    "   , mt.l_description manifestation_type\n" +
                    "   , m.b_upddate b_upddate\n" +
                    "   , m.b_credate b_credate\n" +
                    "   , ms.l_description manifestation_status\n" +
                    "   FROM\n" +
                    "     (("+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation m\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_type mt ON (m.f_type = mt.code))\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_status ms ON (ms.code = m.f_status))\n" +
                    ") \n" +
                    ", work_ids AS (\n" +
                    "   SELECT\n" +
                    "     f_wwork\n" +
                    "   , max(b_upddate) b_upddate\n" +
                    "   , max((CASE WHEN (f_type = 'ELSEVIER JOURNAL NUMBER') THEN identifier ELSE null END)) journal_number\n" +
                    "   , max((CASE WHEN (f_type = 'JOURNAL ACRONYM') THEN identifier ELSE null END)) journal_acronym\n" +
                    "   , max((CASE WHEN (f_type = 'ISSN-L') THEN identifier ELSE null END)) issn_l\n" +
                    "   , max(b_credate) b_credate\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT\n" +
                    "        f_wwork\n" +
                    "      , b_upddate b_upddate\n" +
                    "      , f_type\n" +
                    "      , identifier\n" +
                    "      , b_credate\n" +
                    "      FROM\n" +
                    "        "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier\n" +
                    "      WHERE ((f_type IN ('ELSEVIER JOURNAL NUMBER', 'JOURNAL ACRONYM', 'ISSN-L')) AND (effective_end_date IS NULL))\n" +
                    "   ) \n" +
                    "   GROUP BY f_wwork\n" +
                    ") \n" +
                    ", manifestation_ids AS (\n" +
                    "   SELECT\n" +
                    "     f_manifestation\n" +
                    "   , max(b_upddate) b_upddate\n" +
                    "   , max((CASE WHEN (f_type = 'ISSN') THEN identifier ELSE null END)) issn\n" +
                    "   , max((CASE WHEN (f_type = 'ISBN') THEN identifier ELSE null END)) isbn\n" +
                    "   , max(b_credate) b_credate\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT\n" +
                    "        f_manifestation\n" +
                    "      , b_upddate b_upddate\n" +
                    "      , f_type\n" +
                    "      , identifier\n" +
                    "      , b_credate\n" +
                    "      FROM\n" +
                    "        "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier\n" +
                    "      WHERE ((f_type IN ('ISSN', 'ISBN')) AND (effective_end_date IS NULL))\n" +
                    "   ) \n" +
                    "   GROUP BY f_manifestation\n" +
                    ") \n" +
                    ", financial_attribs AS (\n" +
                    "   SELECT\n" +
                    "     f_wwork\n" +
                    "   , f_gl_company company_code\n" +
                    "   , f_gl_cost_resp_centre responsibility_centre\n" +
                    "   , b_upddate b_upddate\n" +
                    "   , b_credate b_credate\n" +
                    "   FROM\n" +
                    "     "+GetPRMDLDBUser.getProdDataBase()+".gd_work_financial_attribs\n" +
                    "   WHERE (effective_end_date IS NULL)\n" +
                    ") \n" +
                    ", person AS (\n" +
                    "   SELECT\n" +
                    "     p.given_name\n" +
                    "   , p.family_name\n" +
                    "   , wp.f_wwork\n" +
                    "   , greatest(p.b_upddate, wp.b_upddate) b_upddate\n" +
                    "   , greatest(p.b_credate, wp.b_credate) b_credate\n" +
                    "   FROM\n" +
                    "     ("+GetPRMDLDBUser.getProdDataBase()+".gd_person p\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_person_role wp ON (p.person_id = wp.f_person))\n" +
                    "   WHERE ((wp.f_role = 'PU') AND (wp.effective_end_date IS NULL))\n" +
                    ") \n" +
                    "SELECT\n" +
                    "  wd.work_epr eph_work_id\n" +
                    ", (CASE WHEN ((((((((ap.b_upddate IS NULL) AND (wd.b_upddate IS NULL)) AND (md.b_upddate IS NULL)) AND (wi.b_upddate IS NULL)) AND (mi.b_upddate IS NULL)) AND (fa.b_upddate IS NULL)) AND (pmc.b_upddate IS NULL)) AND (per.b_upddate IS NULL)) THEN greatest(COALESCE(CAST(ap.b_credate AS varchar), '0'), COALESCE(CAST(wd.b_credate AS varchar), '0'), COALESCE(CAST(md.b_credate AS varchar), '0'), COALESCE(CAST(wi.b_credate AS varchar), '0'), COALESCE(CAST(mi.b_credate AS varchar), '0'), COALESCE(CAST(fa.b_credate AS varchar), '0'), COALESCE(CAST(pmc.b_credate AS varchar), '0'), COALESCE(CAST(per.b_credate AS varchar), '0')) ELSE greatest(COALESCE(CAST(ap.b_upddate AS varchar), '0'), COALESCE(CAST(wd.b_upddate AS varchar), '0'), COALESCE(CAST(md.b_upddate AS varchar), '0'), COALESCE(CAST(wi.b_upddate AS varchar), '0'), COALESCE(CAST(mi.b_upddate AS varchar), '0'), COALESCE(CAST(fa.b_upddate AS varchar), '0'), COALESCE(CAST(pmc.b_upddate AS varchar), '0'), COALESCE(CAST(per.b_upddate AS varchar), '0')) END) last_updated_date\n" +
                    ", wd.work_super_type product_type\n" +
                    ", wd.work_title product_description\n" +
                    ", (CASE WHEN (wd.work_super_type = 'Journal') THEN ap.gl_product_segment_code WHEN ((wd.work_super_type = 'Book') AND (mi.isbn IS NOT NULL)) THEN mi.isbn ELSE null END) r12_alias\n" +
                    ", fa.company_code\n" +
                    ", fa.responsibility_centre\n" +
                    ", ap.gl_product_segment_code product_segment\n" +
                    ", wd.b_credate created_date\n" +
                    ", wi.journal_acronym\n" +
                    ", pmc.f_pmg pmg\n" +
                    ", wd.f_pmc pmc\n" +
                    ", mi.issn\n" +
                    ", concat(per.given_name, ' ', per.family_name) publisher\n" +
                    ", md.manifestation_type\n" +
                    ", md.manifestation_status\n" +
                    "FROM\n" +
                    "  (((((((work_data wd\n" +
                    "LEFT JOIN manifestation_data md ON (wd.work_epr = md.f_wwork))\n" +
                    "LEFT JOIN work_ids wi ON (wd.work_epr = wi.f_wwork))\n" +
                    "LEFT JOIN manifestation_ids mi ON (md.manifestation_id = mi.f_manifestation))\n" +
                    "LEFT JOIN financial_attribs fa ON (wd.work_epr = fa.f_wwork))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmc pmc ON (wd.f_pmc = pmc.code))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (wd.f_accountable_product = ap.accountable_product_id))\n" +
                    "LEFT JOIN person per ON (per.f_wwork = wd.work_epr))\n" +
                    "order by rand()) limit %s";

    public static String GET_RANDOM_EPR_ID_DRM_ACTION=
            "select distinct eph_work_id as randomIds from(\n"+
                    "SELECT\n"+
                    "  base.script_group\n"+
                    ", base.action\n"+
                    ", base.consolidation\n"+
                    ", base.chart_of_account_value\n"+
                    ", base.product_segment\n"+
                    ", base.action_value_type\n"+
                    ", base.value\n"+
                    ", base.eph_work_id\n"+
                    ", base.last_updated_date\n"+
                    "FROM\n"+
                    "  (\n"+
                    "   SELECT\n"+
                    "     'Group 1' script_group\n"+
                    "   , 'Add' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , 'R12 Product' action_value_type\n"+
                    "   , 'TRUE' value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    "UNION    SELECT\n"+
                    "     'Group 2' script_group\n"+
                    "   , 'ChangeProp' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , 'Allow Budgeting' action_value_type\n"+
                    "   , 'TRUE' value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    "UNION    SELECT\n"+
                    "     'Group 3' script_group\n"+
                    "   , 'ChangeProp' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , 'Allow Posting' action_value_type\n"+
                    "   , 'TRUE' value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    "UNION    SELECT\n"+
                    "     'Group 4' script_group\n"+
                    "   , 'ChangeProp' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , 'Description' action_value_type\n"+
                    "   , replace(replace(w.work_title, chr(13), ''), chr(10), ' ') value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    "UNION    SELECT\n"+
                    "     'Group 5' script_group\n"+
                    "   , 'ChangeProp' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , 'GLProductType' action_value_type\n"+
                    "   , 'Journal' value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    "UNION    SELECT\n"+
                    "     'Group 6' script_group\n"+
                    "   , 'Move' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , ap.f_gl_product_segment_parent action_value_type\n"+
                    "   , null value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    ")  base\n"+
                    "ORDER BY base.script_group ASC, base.chart_of_account_value ASC,rand()\n"+
                    ")limit %s";


    public static String GET_FULL_SET_SOURCE_R12_DATA =
            "select eph_work_id as eph_work_id" +
                    ",last_updated_date as last_updated_date" +
                    ",product_type as product_type" +
                    ",product_description as product_description" +
                    ",r12_alias as r12_alias" +
                    ",company_code as company_code" +
                    ",responsibility_centre as responsibility_centre" +
                    ",product_segment as product_segment" +
                    ",created_date as created_date" +
                    ",journal_acronym as journal_acronym" +
                    ",pmg as pmg" +
                    ",pmc as pmc" +
                    ",issn as issn" +
                    ",publisher as publisher" +
                    ",manifestation_type as manifestation_type" +
                    ",manifestation_status as manifestation_status" +
                    " from(\n" +
                    "WITH\n" +
                    "  work_data AS (\n" +
                    "   SELECT\n" +
                    "     w.work_id work_epr\n" +
                    "   , w.work_title\n" +
                    "   , wt.l_description work_type\n" +
                    "   , wt.roll_up_type work_super_type\n" +
                    "   , ws.l_description work_status\n" +
                    "   , ws.roll_up_status work_super_status\n" +
                    "   , w.f_pmc\n" +
                    "   , w.f_accountable_product\n" +
                    "   , w.b_upddate b_upddate\n" +
                    "   , w.b_credate b_credate\n" +
                    "   FROM\n" +
                    "     (("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                    ") \n" +
                    ", manifestation_data AS (\n" +
                    "   SELECT\n" +
                    "     m.manifestation_id\n" +
                    "   , m.f_wwork\n" +
                    "   , mt.l_description manifestation_type\n" +
                    "   , m.b_upddate b_upddate\n" +
                    "   , m.b_credate b_credate\n" +
                    "   , ms.l_description manifestation_status\n" +
                    "   FROM\n" +
                    "     (("+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation m\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_type mt ON (m.f_type = mt.code))\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_status ms ON (ms.code = m.f_status))\n" +
                    ") \n" +
                    ", work_ids AS (\n" +
                    "   SELECT\n" +
                    "     f_wwork\n" +
                    "   , max(b_upddate) b_upddate\n" +
                    "   , max((CASE WHEN (f_type = 'ELSEVIER JOURNAL NUMBER') THEN identifier ELSE null END)) journal_number\n" +
                    "   , max((CASE WHEN (f_type = 'JOURNAL ACRONYM') THEN identifier ELSE null END)) journal_acronym\n" +
                    "   , max((CASE WHEN (f_type = 'ISSN-L') THEN identifier ELSE null END)) issn_l\n" +
                    "   , max(b_credate) b_credate\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT\n" +
                    "        f_wwork\n" +
                    "      , b_upddate b_upddate\n" +
                    "      , f_type\n" +
                    "      , identifier\n" +
                    "      , b_credate\n" +
                    "      FROM\n" +
                    "        "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier\n" +
                    "      WHERE ((f_type IN ('ELSEVIER JOURNAL NUMBER', 'JOURNAL ACRONYM', 'ISSN-L')) AND (effective_end_date IS NULL))\n" +
                    "   ) \n" +
                    "   GROUP BY f_wwork\n" +
                    ") \n" +
                    ", manifestation_ids AS (\n" +
                    "   SELECT\n" +
                    "     f_manifestation\n" +
                    "   , max(b_upddate) b_upddate\n" +
                    "   , max((CASE WHEN (f_type = 'ISSN') THEN identifier ELSE null END)) issn\n" +
                    "   , max((CASE WHEN (f_type = 'ISBN') THEN identifier ELSE null END)) isbn\n" +
                    "   , max(b_credate) b_credate\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT\n" +
                    "        f_manifestation\n" +
                    "      , b_upddate b_upddate\n" +
                    "      , f_type\n" +
                    "      , identifier\n" +
                    "      , b_credate\n" +
                    "      FROM\n" +
                    "        "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier\n" +
                    "      WHERE ((f_type IN ('ISSN', 'ISBN')) AND (effective_end_date IS NULL))\n" +
                    "   ) \n" +
                    "   GROUP BY f_manifestation\n" +
                    ") \n" +
                    ", financial_attribs AS (\n" +
                    "   SELECT\n" +
                    "     f_wwork\n" +
                    "   , f_gl_company company_code\n" +
                    "   , f_gl_cost_resp_centre responsibility_centre\n" +
                    "   , b_upddate b_upddate\n" +
                    "   , b_credate b_credate\n" +
                    "   FROM\n" +
                    "     "+GetPRMDLDBUser.getProdDataBase()+".gd_work_financial_attribs\n" +
                    "   WHERE (effective_end_date IS NULL)\n" +
                    ") \n" +
                    ", person AS (\n" +
                    "   SELECT\n" +
                    "     p.given_name\n" +
                    "   , p.family_name\n" +
                    "   , wp.f_wwork\n" +
                    "   , greatest(p.b_upddate, wp.b_upddate) b_upddate\n" +
                    "   , greatest(p.b_credate, wp.b_credate) b_credate\n" +
                    "   FROM\n" +
                    "     ("+GetPRMDLDBUser.getProdDataBase()+".gd_person p\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_person_role wp ON (p.person_id = wp.f_person))\n" +
                    "   WHERE ((wp.f_role = 'PU') AND (wp.effective_end_date IS NULL))\n" +
                    ") \n" +
                    "SELECT\n" +
                    "  wd.work_epr eph_work_id\n" +
                    ", (CASE WHEN ((((((((ap.b_upddate IS NULL) AND (wd.b_upddate IS NULL)) AND (md.b_upddate IS NULL)) AND (wi.b_upddate IS NULL)) AND (mi.b_upddate IS NULL)) AND (fa.b_upddate IS NULL)) AND (pmc.b_upddate IS NULL)) AND (per.b_upddate IS NULL)) THEN greatest(COALESCE(CAST(ap.b_credate AS varchar), '0'), COALESCE(CAST(wd.b_credate AS varchar), '0'), COALESCE(CAST(md.b_credate AS varchar), '0'), COALESCE(CAST(wi.b_credate AS varchar), '0'), COALESCE(CAST(mi.b_credate AS varchar), '0'), COALESCE(CAST(fa.b_credate AS varchar), '0'), COALESCE(CAST(pmc.b_credate AS varchar), '0'), COALESCE(CAST(per.b_credate AS varchar), '0')) ELSE greatest(COALESCE(CAST(ap.b_upddate AS varchar), '0'), COALESCE(CAST(wd.b_upddate AS varchar), '0'), COALESCE(CAST(md.b_upddate AS varchar), '0'), COALESCE(CAST(wi.b_upddate AS varchar), '0'), COALESCE(CAST(mi.b_upddate AS varchar), '0'), COALESCE(CAST(fa.b_upddate AS varchar), '0'), COALESCE(CAST(pmc.b_upddate AS varchar), '0'), COALESCE(CAST(per.b_upddate AS varchar), '0')) END) last_updated_date\n" +
                    ", wd.work_super_type product_type\n" +
                    ", wd.work_title product_description\n" +
                    ", (CASE WHEN (wd.work_super_type = 'Journal') THEN ap.gl_product_segment_code WHEN ((wd.work_super_type = 'Book') AND (mi.isbn IS NOT NULL)) THEN mi.isbn ELSE null END) r12_alias\n" +
                    ", fa.company_code\n" +
                    ", fa.responsibility_centre\n" +
                    ", ap.gl_product_segment_code product_segment\n" +
                    ", wd.b_credate created_date\n" +
                    ", wi.journal_acronym\n" +
                    ", pmc.f_pmg pmg\n" +
                    ", wd.f_pmc pmc\n" +
                    ", mi.issn\n" +
                    ", concat(per.given_name, ' ', per.family_name) publisher\n" +
                    ", md.manifestation_type\n" +
                    ", md.manifestation_status\n" +
                    "FROM\n" +
                    "  (((((((work_data wd\n" +
                    "LEFT JOIN manifestation_data md ON (wd.work_epr = md.f_wwork))\n" +
                    "LEFT JOIN work_ids wi ON (wd.work_epr = wi.f_wwork))\n" +
                    "LEFT JOIN manifestation_ids mi ON (md.manifestation_id = mi.f_manifestation))\n" +
                    "LEFT JOIN financial_attribs fa ON (wd.work_epr = fa.f_wwork))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmc pmc ON (wd.f_pmc = pmc.code))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (wd.f_accountable_product = ap.accountable_product_id))\n" +
                    "LEFT JOIN person per ON (per.f_wwork = wd.work_epr))\n" +
                    ") where eph_work_id in('%s') order by eph_work_id desc," +
                    "product_type desc,product_description desc, responsibility_centre desc,issn desc,manifestation_type desc," +
                    "r12_alias desc,manifestation_status desc ";

    public static String GET_SOURCE_DATA_DRM_ACTION_SCRIPT=
            "select eph_work_id as eph_work_id" +
                    ",script_group as script_group" +
                    ",action as action" +
                    ",consolidation as consolidation" +
                    ",chart_of_account_value as chart_of_account_value" +
                    ",product_segment as product_segment" +
                    ",action_value_type as action_value_type" +
                    ",value as value" +
                    ",eph_work_id as eph_work_id" +
                    ",last_updated_date as last_updated_date" +
                    " from(\n"+
                    "SELECT\n"+
                    "  base.script_group\n"+
                    ", base.action\n"+
                    ", base.consolidation\n"+
                    ", base.chart_of_account_value\n"+
                    ", base.product_segment\n"+
                    ", base.action_value_type\n"+
                    ", base.value\n"+
                    ", base.eph_work_id\n"+
                    ", base.last_updated_date\n"+
                    "FROM\n"+
                    "  (\n"+
                    "   SELECT\n"+
                    "     'Group 1' script_group\n"+
                    "   , 'Add' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , 'R12 Product' action_value_type\n"+
                    "   , 'TRUE' value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    "UNION    SELECT\n"+
                    "     'Group 2' script_group\n"+
                    "   , 'ChangeProp' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , 'Allow Budgeting' action_value_type\n"+
                    "   , 'TRUE' value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    "UNION    SELECT\n"+
                    "     'Group 3' script_group\n"+
                    "   , 'ChangeProp' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , 'Allow Posting' action_value_type\n"+
                    "   , 'TRUE' value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    "UNION    SELECT\n"+
                    "     'Group 4' script_group\n"+
                    "   , 'ChangeProp' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , 'Description' action_value_type\n"+
                    "   , replace(replace(w.work_title, chr(13), ''), chr(10), ' ') value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    "UNION    SELECT\n"+
                    "     'Group 5' script_group\n"+
                    "   , 'ChangeProp' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , 'GLProductType' action_value_type\n"+
                    "   , 'Journal' value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    "UNION    SELECT\n"+
                    "     'Group 6' script_group\n"+
                    "   , 'Move' action\n"+
                    "   , 'Consolidated_WFPandPBF_Fin' consolidation\n"+
                    "   , 'R12 Product' chart_of_account_value\n"+
                    "   , ap.gl_product_segment_code product_segment\n"+
                    "   , ap.f_gl_product_segment_parent action_value_type\n"+
                    "   , null value\n"+
                    "   , w.work_id eph_work_id\n"+
                    "   , greatest(COALESCE(w.b_upddate, w.b_credate), COALESCE(ap.b_upddate, ap.b_credate)) last_updated_date\n"+
                    "   FROM\n"+
                    "     ((("+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi ON ((w.work_id = wi.f_wwork) AND (wi.f_type = 'ELSEVIER JOURNAL NUMBER')))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n"+
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_accountable_product ap ON (w.f_accountable_product = ap.accountable_product_id))\n"+
                    "   WHERE (wt.roll_up_type = 'Journal')\n"+
                    ")  base\n"+
                    " where eph_work_id in ('%s') ORDER BY eph_work_id desc, " +
                    "product_segment desc,script_group desc,action desc,action_value_type desc\n"+
                    ")";

    public static String GET_TARGET_DATA_DRM_ACTION_SCRIPT=
            "select eph_work_id as eph_work_id" +
                    ",script_group as script_group" +
                    ",action as action" +
                    ",consolidation as consolidation" +
                    ",chart_of_account_value as chart_of_account_value" +
                    ",product_segment as product_segment" +
                    ",action_value_type as action_value_type" +
                    ",value as value" +
                    ",last_updated_date as last_updated_date" +
                    " from "+GetPRMDLDBUser.getProdDataBase()+".drm_action_scripts_v where eph_work_id in ('%s') " +
                    "order by eph_work_id desc, product_segment desc,script_group desc,action desc,action_value_type desc";

    public static String GET_FULL_SET_TARGET_R12_DATA =
            "select eph_work_id as eph_work_id" +
                    ",last_updated_date as last_updated_date" +
                    ",product_type as product_type" +
                    ",product_description as product_description" +
                    ",r12_alias as r12_alias" +
                    ",company_code as company_code" +
                    ",responsibility_centre as responsibility_centre" +
                    ",product_segment as product_segment" +
                    ",created_date as created_date" +
                    ",journal_acronym as journal_acronym" +
                    ",pmg as pmg" +
                    ",pmc as pmc" +
                    ",issn as issn" +
                    ",publisher as publisher" +
                    ",manifestation_type as manifestation_type" +
                    ",manifestation_status as manifestation_status" +
                    " from "+GetPRMDLDBUser.getProdDataBase()+".r12_full_data_v where eph_work_id in('%s') " +
                    "order by eph_work_id desc,product_type desc,product_description desc, responsibility_centre desc, " +
                    "issn desc, manifestation_type desc,r12_alias desc,manifestation_status desc ";

}




