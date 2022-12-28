package com.eph.automation.testing.services.db.CKDataLakeSQL;

import com.eph.automation.testing.services.db.bcsetlcoresql.GetBcsEtlCoreDLDBUser;

public class CKReportsCountChecksSQL {
    //  Reports View Count SQL
    public static String GET_WorkFlow_Tableu_INBOUND_COUNT =
            "WITH\n" +
                    "  packages AS (\n" +
                    "   SELECT\n" +
                    "     ww.work_id pkg_epr\n" +
                    "   , ww.work_title pkg_title\n" +
                    "   , wty.code pkg_type\n" +
                    "   , wty.l_description pkg_type_name\n" +
                    "   , wty.roll_up_type pkg_type_roll_up\n" +
                    "   , wst.code pkg_status\n" +
                    "   , wst.l_description pkg_status_name\n" +
                    "   , wst.roll_up_status pkg_status_roll_up\n" +
                    "   , wid.identifier pkg_acronym\n" +
                    "   , wh.hl3_code pkg_hl3_code\n" +
                    "   , wh.hl3_name pkg_hl3_name\n" +
                    "   , wh.hl3_level pkg_hl3_level\n" +
                    "   , wh.hl2_code pkg_hl2_code\n" +
                    "   , wh.hl2_name pkg_hl2_name\n" +
                    "   , wh.hl2_level pkg_hl2_level\n" +
                    "   , wh.hl1_code pkg_hl1_code\n" +
                    "   , wh.hl1_name pkg_hl1_name\n" +
                    "   , wh.hl1_level pkg_hl1_level\n" +
                    "   FROM\n" +
                    "     (((("+GetCKDLDB.getProductDataBase()+".gd_wwork ww\n" +
                    "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_work_type wty ON ((wty.code = ww.f_type) AND (wty.roll_up_type = 'CK')))\n" +
                    "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_work_status wst ON (wst.code = ww.f_status))\n" +
                    "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_identifier wid ON (((wid.f_wwork = ww.work_id) AND (wid.f_type = 'PACKAGE ACRONYM')) AND (wid.effective_end_date IS NULL)))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        whl.f_wwork hl_epr\n" +
                    "      , whc.code hl3_code\n" +
                    "      , whc.name hl3_name\n" +
                    "      , whc.hierarchy_level hl3_level\n" +
                    "      , whp.code hl2_code\n" +
                    "      , whp.name hl2_name\n" +
                    "      , whp.hierarchy_level hl2_level\n" +
                    "      , whpp.code hl1_code\n" +
                    "      , whpp.name hl1_name\n" +
                    "      , whpp.hierarchy_level hl1_level\n" +
                    "      FROM\n" +
                    "        ((("+GetCKDLDB.getProductDataBase()+".gd_work_work_hchy_link whl\n" +
                    "      INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_hierarchy whc ON ((whl.f_work_hierarchy = whc.work_hierarchy_id) AND (whc.f_type = 'CK')))\n" +
                    "      LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_hierarchy whp ON (whc.f_parent_work_hierarchy = whp.work_hierarchy_id))\n" +
                    "      LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_hierarchy whpp ON (whp.f_parent_work_hierarchy = whpp.work_hierarchy_id))\n" +
                    "   )  wh ON (ww.work_id = wh.hl_epr))\n" +
                    ") \n" +
                    ", package_component_relations AS (\n" +
                    "   SELECT\n" +
                    "     pkg.f_package_owner pkgr_owner_epr\n" +
                    "   , pkg.f_component pkgr_component_epr\n" +
                    "   , pkg.effective_start_date pkgr_rel_start_date\n" +
                    "   , pkg.effective_end_date pkgr_rel_end_date\n" +
                    "   , pkgwu.durable_url pkgr_rel_durable_url\n" +
                    "   , pkg.effective_start_date pkgr_rel_durable_url_live_date\n" +
                    "   FROM\n" +
                    "     ((("+GetCKDLDB.getProductDataBase()+".gd_work_rel_package pkg\n" +
                    "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_wwork pwo ON ((pwo.work_id = pkg.f_package_owner) AND (pkg.effective_end_date IS NULL)))\n" +
                    "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_work_type pcwty ON ((pcwty.code = pwo.f_type) AND (pcwty.roll_up_type = 'CK')))\n" +
                    "   LEFT JOIN "+GetCKDLDB.getCKDataBase()+".cmms_transform_current_durable_url pkgwu ON (((pkgwu.eph_package_id = pkg.f_package_owner) AND (pkgwu.eph_work_id = pkg.f_component)) AND (pkg.effective_end_date IS NULL)))\n" +
                    ") \n" +
                    ", package_components AS (\n" +
                    "   SELECT\n" +
                    "     piwo.work_id pco_epr\n" +
                    "   , piwo.work_title pco_title\n" +
                    "   , piissnl.identifier pco_issnl\n" +
                    "   , pilisbn.identifier pco_lead_isbn\n" +
                    "   , piwo.edition_number pco_edition_number\n" +
                    "   , piwo.copyright_year pco_copyright_year\n" +
                    "   , piwo.actual_launch_date pco_actual_launch_date\n" +
                    "   , pity.code pco_type\n" +
                    "   , pity.l_description pco_type_name\n" +
                    "   , pity.roll_up_type pco_type_roll_up\n" +
                    "   , pist.code pco_status\n" +
                    "   , pist.l_description pco_status_name\n" +
                    "   , pist.roll_up_status pco_status_roll_up\n" +
                    "   , piimp.code pco_imprint\n" +
                    "   , piimp.l_description pco_imprint_name\n" +
                    "   , pipmc.code pco_pmc\n" +
                    "   , pipmc.l_description pco_pmc_name\n" +
                    "   , pipmg.code pco_pmg\n" +
                    "   , pipmg.l_description pco_pmg_name\n" +
                    "   , piln.code pco_language\n" +
                    "   , piln.l_description pco_language_name\n" +
                    "   , pisub.name pco_specialty\n" +
                    "   , pisub.effective_start_date pco_specialty_live_date\n" +
                    "   , piperseq.author pco_author\n" +
                    "   , piperseq.sequence pco_sequence\n" +
                    "   , piedtseq.editor pco_editor\n" +
                    "   , piedtseq.editor_seq pco_editor_seq\n" +
                    "   , pickbpm.ck_title_start_date pco_ck_title_start_date\n" +
                    "   , pickbpm.ck_live_date pco_ck_item_live_date\n" +
                    "   , pickbpm.workflowid pco_ck_workflow_id\n" +
                    "   , pickbpm.alternate_title pco_ck_alternate_title\n" +
                    "   , pickbpm.search_tier pco_ck_search_tier\n" +
                    "   , pickbpm.finance_tier pco_ck_finance_tier\n" +
                    "   , pickbpm.years_of_coverage pco_journal_backfile_years\n" +
                    "   , pickbpm.pdf_suppression pco_ck_pdf_suppression\n" +
                    "   , pickbpm.video pco_video\n" +
                    "   , pickbpm.topic_pages pco_topic_pages\n" +
                    "   , pickbpm.journal_backfiles_only pco_journal_backfiles_only\n" +
                    "   , pickbpm.aip pco_aip\n" +
                    "   , piedi.prev_work_id pco_prev_work_id\n" +
                    "   , piedi.prev_lead_isbn pco_prev_lead_isbn\n" +
                    "   , piedi.next_work_id pco_next_work_id\n" +
                    "   , piedi.next_lead_isbn pco_next_lead_isbn\n" +
                    "   , picopd.ownership pco_ownership\n" +
                    "   , picopd.firstpubdate pco_firstpubdate\n" +
                    "   FROM\n" +
                    "     ((((((((((((((("+GetCKDLDB.getProductDataBase()+".gd_wwork piwo\n" +
                    "   INNER JOIN (\n" +
                    "      SELECT DISTINCT pkg.f_component pkgr_component_epr\n" +
                    "      FROM\n" +
                    "        (("+GetCKDLDB.getProductDataBase()+".gd_work_rel_package pkg\n" +
                    "      INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_wwork pwo ON ((pwo.work_id = pkg.f_package_owner) AND (pkg.effective_end_date IS NULL)))\n" +
                    "      INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_work_type pcwty ON ((pcwty.code = pwo.f_type) AND (pcwty.roll_up_type = 'CK')))\n" +
                    "   )  piid ON (piwo.work_id = piid.pkgr_component_epr))\n" +
                    "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_work_type pity ON (pity.code = piwo.f_type))\n" +
                    "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_work_status pist ON (pist.code = piwo.f_status))\n" +
                    "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_imprint piimp ON (piimp.code = piwo.f_imprint))\n" +
                    "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_pmc pipmc ON (pipmc.code = piwo.f_pmc))\n" +
                    "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_pmg pipmg ON (pipmg.code = pipmc.f_pmg))\n" +
                    "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_language piln ON (piln.code = piwo.f_llanguage))\n" +
                    "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_identifier piissnl ON (((piwo.work_id = piissnl.f_wwork) AND (piissnl.f_type = 'ISSN-L')) AND (piissnl.effective_end_date IS NULL)))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        gdw.work_id epr\n" +
                    "      , gdmid.identifier\n" +
                    "      FROM\n" +
                    "        (("+GetCKDLDB.getProductDataBase()+".gd_wwork gdw\n" +
                    "      INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation gdma ON (gdw.work_id = gdma.f_wwork))\n" +
                    "      INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation_identifier gdmid ON ((((gdma.manifestation_id = gdmid.f_manifestation) AND (gdmid.f_type = 'ISBN')) AND (gdmid.lead_indicator = true)) AND (gdmid.effective_end_date IS NULL)))\n" +
                    "   )  pilisbn ON (piwo.work_id = pilisbn.epr))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        pisal.f_wwork epr\n" +
                    "      , pasa.name\n" +
                    "      , pisal.effective_start_date\n" +
                    "      FROM\n" +
                    "        ("+GetCKDLDB.getProductDataBase()+".gd_work_subject_area_link pisal\n" +
                    "      INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_subject_area pasa ON (((pisal.f_subject_area = pasa.subject_area_id) AND (pasa.f_type = 'CK')) AND (pisal.effective_end_date IS NULL)))\n" +
                    "   )  pisub ON (piwo.work_id = pisub.epr))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        pickitm.*\n" +
                    "      , CAST(piwflw.workflow_id AS decimal(38,0)) workflowid\n" +
                    "      FROM\n" +
                    "        ("+GetCKDLDB.getCKDataBase()+".ck_transform_current_work pickitm\n" +
                    "      INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_current_workflow piwflw ON (pickitm.ck_live_date = TRY(CAST(piwflw.ck_live_date AS date))))\n" +
                    "   )  pickbpm ON (piwo.work_id = pickbpm.eph_work_id))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        wpr.eprid eprid\n" +
                    "      , \"rtrim\"(\"concat\"((CASE WHEN (wpr.person_first_name IS NULL) THEN '' WHEN (\"lower\"(wpr.person_first_name) IN ('unknown', 'author')) THEN '' ELSE wpr.person_first_name END), ' ', (CASE WHEN (wpr.person_family_name IS NULL) THEN '' WHEN (\"lower\"(wpr.person_family_name) IN ('unknown', 'author')) THEN '' ELSE wpr.person_family_name END))) author\n" +
                    "      , CAST(wpr.sequence AS decimal(38,0)) sequence\n" +
                    "      FROM\n" +
                    "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_person_role_extended_current_v wpr\n" +
                    "      WHERE ((wpr.roletype = 'AU') AND (wpr.metadeleted = false))\n" +
                    "      GROUP BY wpr.eprid, wpr.person_first_name, wpr.person_family_name, wpr.sequence\n" +
                    "   )  piperseq ON (piperseq.eprid = piwo.work_id))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        wpr.eprid eprid\n" +
                    "      , \"rtrim\"(\"concat\"((CASE WHEN (wpr.person_first_name IS NULL) THEN '' WHEN (\"lower\"(wpr.person_first_name) IN ('unknown', 'editor')) THEN '' ELSE wpr.person_first_name END), ' ', (CASE WHEN (wpr.person_family_name IS NULL) THEN '' WHEN (\"lower\"(wpr.person_family_name) IN ('unknown', 'editor')) THEN '' ELSE wpr.person_family_name END))) editor\n" +
                    "      , CAST(wpr.sequence AS decimal(38,0)) editor_seq\n" +
                    "      FROM\n" +
                    "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_person_role_extended_current_v wpr\n" +
                    "      WHERE ((wpr.roletype = 'ED') AND (wpr.metadeleted = false))\n" +
                    "      GROUP BY wpr.eprid, wpr.person_first_name, wpr.person_family_name, wpr.sequence\n" +
                    "   )  piedtseq ON (piedtseq.eprid = piwo.work_id))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        prev_work_id\n" +
                    "      , prev_lead_isbn\n" +
                    "      , curr_work_id\n" +
                    "      , next_work_id\n" +
                    "      , next_lead_isbn\n" +
                    "      FROM\n" +
                    "        (\n" +
                    "         SELECT\n" +
                    "           w1.work_id prev_work_id\n" +
                    "         , mi1.identifier prev_lead_isbn\n" +
                    "         , w.work_id curr_work_id\n" +
                    "         , w2.work_id next_work_id\n" +
                    "         , mi2.identifier next_lead_isbn\n" +
                    "         FROM\n" +
                    "           (((((((((("+GetCKDLDB.getProductDataBase()+".gd_wwork w\n" +
                    "         INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation m ON ((m.f_wwork = w.work_id) AND (m.f_status <> 'NVM')))\n" +
                    "         INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation_identifier mi ON (((((mi.f_manifestation = m.manifestation_id) AND (mi.f_type = 'ISBN')) AND (mi.effective_end_date IS NULL)) AND (mi.identifier IS NOT NULL)) AND (mi.lead_indicator = true)))\n" +
                    "         LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_relationship wr1 ON (((wr1.f_child = w.work_id) AND (wr1.f_relationship_type = 'EDI')) AND (wr1.effective_end_date IS NULL)))\n" +
                    "         LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_wwork w1 ON ((w1.work_id = wr1.f_parent) AND (w1.f_status <> 'NVW')))\n" +
                    "         LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation m1 ON ((m1.f_wwork = w1.work_id) AND (m1.f_status <> 'NVM')))\n" +
                    "         LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation_identifier mi1 ON ((((((mi1.f_manifestation = m1.manifestation_id) AND (mi1.f_type = 'ISBN')) AND (mi1.effective_end_date IS NULL)) AND (mi1.identifier IS NOT NULL)) AND (mi1.identifier <> '')) AND (mi1.lead_indicator = true)))\n" +
                    "         LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_relationship wr2 ON (((wr2.f_parent = w.work_id) AND (wr2.f_relationship_type = 'EDI')) AND (wr2.effective_end_date IS NULL)))\n" +
                    "         LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_wwork w2 ON ((w2.work_id = wr2.f_child) AND (w2.f_status = 'WPU')))\n" +
                    "         LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation m2 ON ((m2.f_wwork = w2.work_id) AND (m2.f_status = 'MPU')))\n" +
                    "         LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation_identifier mi2 ON ((((((mi2.f_manifestation = m2.manifestation_id) AND (mi2.f_type = 'ISBN')) AND (mi2.effective_end_date IS NULL)) AND (mi2.identifier IS NOT NULL)) AND (mi2.identifier <> '')) AND (mi2.lead_indicator = true)))\n" +
                    "         GROUP BY w.work_id, w.edition_number, mi.identifier, w.work_title, w1.work_id, w1.edition_number, mi1.identifier, w2.work_id, w2.edition_number, mi2.identifier\n" +
                    "      ) \n" +
                    "      WHERE (((((prev_work_id IS NULL) AND (next_work_id IS NULL)) OR (((prev_work_id IS NOT NULL) AND (prev_lead_isbn IS NOT NULL)) AND (next_work_id IS NULL))) OR (((next_work_id IS NOT NULL) AND (next_lead_isbn IS NOT NULL)) AND (prev_work_id IS NULL))) OR ((((prev_work_id IS NOT NULL) AND (prev_lead_isbn IS NOT NULL)) AND (next_work_id IS NOT NULL)) AND (next_lead_isbn IS NOT NULL)))\n" +
                    "   )  piedi ON (piedi.curr_work_id = piwo.work_id))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        c.ownership ownership\n" +
                    "      , CAST(\"date_parse\"(NULLIF(p.publishedon, ''), '%%d-%%b-%%Y') AS date) firstpubdate\n" +
                    "      , p.isbn13 lead_isbn\n" +
                    "      FROM\n" +
                    "        ("+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product p\n" +
                    "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_content c ON ((c.sourceref = p.sourceref) AND (c.metadeleted = 'N')))\n" +
                    "      WHERE ((p.metadeleted = 'N') AND (p.publishedon <> ''))\n" +
                    "   )  picopd ON (picopd.lead_isbn = pilisbn.identifier))\n" +
                    ") \n" +
                    "SELECT count(*) as sourceCount\n" +
                    "FROM\n" +
                    "  ((packages\n" +
                    "INNER JOIN package_component_relations ON (packages.pkg_epr = package_component_relations.pkgr_owner_epr))\n" +
                    "INNER JOIN package_components ON (package_component_relations.pkgr_component_epr = package_components.pco_epr))\n";

    //  Reports Table Count SQL


    public static String GET_Workflow_p1_INBOUND_COUNT =
    "select count(*) as sourceCount from(\n"+
            "SELECT\n"+
            "  allposs.workflow_id workflow_id\n"+
            ", \"min\"(allposs.snapshot_ts) key_snapshot\n"+
            ", COALESCE(allposs.workflow_type, 'Normal') workflow_type\n"+
            ", allposs.workflow_live_date workflow_live_date\n"+
            ", \"concat\"(\"concat\"(\"date_format\"(allposs.workflow_live_date, '%%Y'), '-'), \"date_format\"(allposs.workflow_live_date, '%%m')) workflow_year_month\n"+
            "FROM\n"+
            "  (\n"+
            "   SELECT\n"+
            "     wflwtp.workflow_id\n"+
            "   , wflwtp.snapshot_ts\n"+
            "   , wflw.workflow_type\n"+
            "   , wflwtp.workflow_live_date\n"+
            "   FROM\n"+
            "     ("+GetCKDLDB.getCKCMMSDataBase()+".ck_workflow_tableau_part wflwtp\n"+
            "   LEFT JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_file_history_workflow_part wflw ON (wflw.workflow_id = wflwtp.workflow_id))\n"+
            "   WHERE (wflwtp.workflow_live_date IS NOT NULL)\n"+
            "   GROUP BY wflwtp.workflow_id, wflwtp.snapshot_ts, wflw.workflow_type, wflwtp.workflow_live_date\n"+
            ")  allposs\n"+
            "GROUP BY allposs.workflow_id, allposs.workflow_type, allposs.workflow_live_date\n"+
            ")";

    public static String GET_Workflow_p2_INBOUND_COUNT =
            "select count(*) as sourceCount from(\n" +
                    "    SELECT\n" +
                    "            workflow_id\n" +
                    "            , key_snapshot\n" +
                    "            , workflow_type\n" +
                    "            , workflow_live_date\n" +
                    "            , workflow_year_month\n" +
                    ", (CASE WHEN (workflow_id = (SELECT \"max\"(workflow_id)\n" +
                    "    FROM\n" +
                    "    "+GetCKDLDB.getCKCMMSDataBase()+".ck_workflow_control_p1 wc2\n" +
                    "    WHERE (wc1.workflow_year_month = wc2.workflow_year_month)\n" +
                    "    GROUP BY workflow_year_month\n" +
                    ")) THEN true ELSE false END) include_flag\n" +
                    "            FROM\n" +
                    "    "+GetCKDLDB.getCKCMMSDataBase()+".ck_workflow_control_p1 wc1\n" +
                    ")";

    public static String GET_WorkFlow_tableu_pkg_Inbound_work =
            "select count(*) as sourceCount from (\n" +
                    "   SELECT\n" +
                    "     ctrl.workflow_type\n" +
                    "   , part.workflow_live_date\n" +
                    "   , part.workflow_id\n" +
                    "   , part.snapshot_ts\n" +
                    "   , part.package_id\n" +
                    "   , part.package_code\n" +
                    "   , part.package_title\n" +
                    "   , part.ck_site_code\n" +
                    "   , part.ck_site_name\n" +
                    "   , part.sub_platform_code\n" +
                    "   , part.sub_platform_name\n" +
                    "   , part.ck_package_type\n" +
                    "   , part.work_id\n" +
                    "   , part.work_type_group\n" +
                    "   , part.work_title\n" +
                    "   , part.ck_alternate_title\n" +
                    "   , part.issn_l\n" +
                    "   , part.lead_isbn\n" +
                    "   , part.edition_number\n" +
                    "   , part.author\n" +
                    "   , part.author_sequence\n" +
                    "   , part.editor\n" +
                    "   , part.editor_seq\n" +
                    "   , part.specialty_name\n" +
                    "   , part.finance_tier\n" +
                    "   , part.search_tier\n" +
                    "   , part.previous_work_id\n" +
                    "   , part.previuos_lead_isbn\n" +
                    "   , part.next_work_id\n" +
                    "   , part.next_lead_isbn\n" +
                    "   , part.ownership\n" +
                    "   , part.first_pub_date\n" +
                    "   FROM\n" +
                    "     ("+GetCKDLDB.getCKCMMSDataBase()+".ck_workflow_tableau_part part\n" +
                    "   INNER JOIN "+GetCKDLDB.getCKCMMSDataBase()+".ck_workflow_control_p2 ctrl ON ((((ctrl.key_snapshot = part.snapshot_ts) AND (ctrl.workflow_id = part.workflow_id)) AND (part.pkg_wk_effective_end_date IS NULL)) AND (ctrl.include_flag = true)))\n" +
                    "   GROUP BY ctrl.workflow_type, part.workflow_live_date, part.workflow_id, part.snapshot_ts, part.package_id, part.package_code, part.package_title, part.ck_site_code, part.ck_site_name, part.sub_platform_code, part.sub_platform_name, part.ck_package_type, part.work_id, part.work_type_group, part.work_title, part.ck_alternate_title, part.issn_l, part.lead_isbn, part.edition_number, part.author, part.author_sequence, part.editor, part.editor_seq, part.specialty_name, part.finance_tier, part.search_tier, part.previous_work_id, part.previuos_lead_isbn, part.next_work_id, part.next_lead_isbn, part.ownership, part.first_pub_date\n" +
                    ")";

    public static String GET_Transaction_workflow_Inbound_Count =
    "WITH\n"+
            "  transactions AS (\n"+
            "   SELECT\n"+
            "     tw.workflow_id\n"+
            "   , tw.workflow_month\n"+
            "   , tw.workflow_year\n"+
            "   , tw.cutoff_date\n"+
            "   , tw.ck_live_date\n"+
            "   , tw.workflow_type\n"+
            "   , tw.workflow_status\n"+
            "   , th.transaction_audit_id\n"+
            "   , th.eph_work_id\n"+
            "   , th.action\n"+
            "   , th.description\n"+
            "   , th.actioned_by\n"+
            "   , th.actioned_on\n"+
            "   , th.reason\n"+
            "   , th.comment\n"+
            "   , th.isEmergency_update\n"+
            "   , th.workflow_code\n"+
            "   , td.transaction_field_change_id\n"+
            "   , td.field\n"+
            "   , td.from_value\n"+
            "   , td.to_value\n"+
            "   FROM\n"+
            "     (("+GetCKDLDB.getCKDataBase()+".ck_transform_file_history_transaction_workflow_part tw\n"+
            "   INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_file_history_transaction_header_part th ON (tw.workflow_id = th.workflow_id))\n"+
            "   INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_file_history_transaction_detail_part td ON (td.transaction_audit_id = th.transaction_audit_id))\n"+
            ") \n"+
            ", packages AS (\n"+
            "   SELECT\n"+
            "     ww.work_id Package_ID\n"+
            "   , ww.work_title Package_Title\n"+
            "   , wid.identifier Package_Code\n"+
            "   , wh.hl3_code CK_Site_Code\n"+
            "   , wh.hl3_name CK_Site_Name\n"+
            "   , wh.hl2_code Sub_platform_code\n"+
            "   , wh.hl2_name Sub_Platform_Name\n"+
            "   , wty.code CK_Package_Type\n"+
            "   FROM\n"+
            "     (((("+GetCKDLDB.getProductDataBase()+".gd_wwork ww\n"+
            "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_work_type wty ON ((wty.code = ww.f_type) AND (wty.roll_up_type = 'CK')))\n"+
            "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_work_status wst ON (wst.code = ww.f_status))\n"+
            "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_identifier wid ON (((wid.f_wwork = ww.work_id) AND (wid.f_type = 'PACKAGE ACRONYM')) AND (wid.effective_end_date IS NULL)))\n"+
            "   LEFT JOIN (\n"+
            "      SELECT\n"+
            "        whl.f_wwork hl_epr\n"+
            "      , whc.code hl3_code\n"+
            "      , whc.name hl3_name\n"+
            "      , whp.code hl2_code\n"+
            "      , whp.name hl2_name\n"+
            "      FROM\n"+
            "        (("+GetCKDLDB.getProductDataBase()+".gd_work_work_hchy_link whl\n"+
            "      INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_hierarchy whc ON ((whl.f_work_hierarchy = whc.work_hierarchy_id) AND (whc.f_type = 'CK')))\n"+
            "      LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_hierarchy whp ON (whc.f_parent_work_hierarchy = whp.work_hierarchy_id))\n"+
            "   )  wh ON (ww.work_id = wh.hl_epr))\n"+
            ") \n"+
            ", package_components AS (\n"+
            "   SELECT\n"+
            "     piwo.work_id Work_ID\n"+
            "   , pity.roll_up_type Work_Type_Group\n"+
            "   , piissnl.identifier ISSN_L\n"+
            "   , pilisbn.identifier Lead_ISBN\n"+
            "   , piid.pkgr_owner_epr Package_ID\n"+
            "   FROM\n"+
            "     (((("+GetCKDLDB.getProductDataBase()+".gd_wwork piwo\n"+
            "   INNER JOIN (\n"+
            "      SELECT DISTINCT\n"+
            "        pkg.f_component pkgr_component_epr\n"+
            "      , pkg.f_package_owner pkgr_owner_epr\n"+
            "      FROM\n"+
            "        (("+GetCKDLDB.getProductDataBase()+".gd_work_rel_package pkg\n"+
            "      INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_wwork pwo ON ((pwo.work_id = pkg.f_package_owner) AND (pkg.effective_end_date IS NULL)))\n"+
            "      INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_work_type pcwty ON ((pcwty.code = pwo.f_type) AND (pcwty.roll_up_type = 'CK')))\n"+
            "   )  piid ON (piwo.work_id = piid.pkgr_component_epr))\n"+
            "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_x_lov_work_type pity ON (pity.code = piwo.f_type))\n"+
            "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_identifier piissnl ON (((piwo.work_id = piissnl.f_wwork) AND (piissnl.f_type = 'ISSN-L')) AND (piissnl.effective_end_date IS NULL)))\n"+
            "   LEFT JOIN (\n"+
            "      SELECT\n"+
            "        gdw.work_id epr\n"+
            "      , gdmid.identifier\n"+
            "      FROM\n"+
            "        (("+GetCKDLDB.getProductDataBase()+".gd_wwork gdw\n"+
            "      INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation gdma ON (gdw.work_id = gdma.f_wwork))\n"+
            "      INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation_identifier gdmid ON ((((gdma.manifestation_id = gdmid.f_manifestation) AND (gdmid.f_type = 'ISBN')) AND (gdmid.lead_indicator = true)) AND (gdmid.effective_end_date IS NULL)))\n"+
            "   )  pilisbn ON (piwo.work_id = pilisbn.epr))\n"+
            ") \n"+
            "select count(*) as sourceCount from(\n"+
            "SELECT DISTINCT\n"+
            "  tn.workflow_id Workflow_ID\n"+
            ", tn.workflow_month Workflow_Month\n"+
            ", tn.workflow_year Workflow_Year\n"+
            ", tn.cutoff_date CutOff_Date\n"+
            ", tn.ck_live_date Workflow_Live_Date\n"+
            ", tn.workflow_type Workflow_Type\n"+
            ", tn.workflow_status Workflow_Status\n"+
            ", tn.transaction_audit_id Transaction_Audit_ID\n"+
            ", tn.eph_work_id Work_ID\n"+
            ", tn.action Action\n"+
            ", tn.description Description\n"+
            ", tn.actioned_by Actioned_By\n"+
            ", tn.actioned_on Actioned_On\n"+
            ", tn.reason Reason\n"+
            ", tn.comment Comment\n"+
            ", tn.isEmergency_update IsEmergency_Update\n"+
            ", tn.workflow_code Workflow_Code\n"+
            ", tn.transaction_field_change_id Transaction_Field_Change_ID\n"+
            ", tn.field Field\n"+
            ", tn.from_value from_value\n"+
            ", tn.to_value To_Value\n"+
            ", pkg_comp.Work_Type_Group\n"+
            ", pkg_comp.ISSN_L\n"+
            ", pkg_comp.Lead_ISBN\n"+
            ", pkg.Package_ID\n"+
            ", pkg.Package_Title\n"+
            ", pkg.Package_Code\n"+
            ", pkg.CK_Site_Code\n"+
            ", pkg.CK_Site_Name\n"+
            ", pkg.Sub_platform_code\n"+
            ", pkg.Sub_Platform_Name\n"+
            ", pkg.CK_Package_Type\n"+
            "FROM\n"+
            "  ((transactions tn\n"+
            "INNER JOIN package_components pkg_comp ON (tn.eph_work_id = pkg_comp.Work_ID))\n"+
            "INNER JOIN packages pkg ON (pkg_comp.Package_ID = pkg.Package_ID))\n"+
            ")";

    public static String GET_Reports_Table_COUNT = "select count(*) as targetCount from "+GetCKDLDB.getCKCMMSDataBase()+".%s";
}


