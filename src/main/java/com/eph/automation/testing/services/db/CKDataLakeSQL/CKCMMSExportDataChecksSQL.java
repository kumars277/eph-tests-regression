package com.eph.automation.testing.services.db.CKDataLakeSQL;

import com.eph.automation.testing.services.db.bcsetlcoresql.GetBcsEtlCoreDLDBUser;

public class CKCMMSExportDataChecksSQL {
    //     GET IDs
    public static String GET_CK_CMMS_WORKFLOW_VIEW_IDs =
            "select workflowid as U_KEY from\n" +
                    "(\n" +
                    "   SELECT\n" +
                    "     '1.0' schemaVersion\n" +
                    "   , TRY(CAST(wf.workflowId AS integer)) workflowId\n" +
                    "   , wf.workflowType workflowType\n" +
                    "   , TRY(CAST(wf.cutoffDate AS varchar)) workflowCutoffDate\n" +
                    "   , TRY(CAST(wf.liveDate AS varchar)) workflowLiveDate\n" +
                    "   FROM\n" +
                    "     "+GetCKDLDB.getCKDataBase()+".ck_workflow_inbound_part wf\n" +
                    "   WHERE (wf.inbound_ts = (SELECT max(inbound_ts)\n" +
                    "FROM\n" +
                    "  "+GetCKDLDB.getCKDataBase()+".ck_workflow_inbound_part\n" +
                    "))\n" +
                    ") limit %s";

    public static String GET_CK_CMMS_WORK1_VIEW_IDs =
            "select currentIdentifier as U_KEY from(\n" +
                    "(\n" +
                    "   SELECT\n" +
                    "     pw.f_component workId\n" +
                    "   , w.work_title workTitle\n" +
                    "   , ckw.alternate_title alternateTitle\n" +
                    "   , TRY(CAST(w.planned_launch_date AS varchar)) plannedLaunchDate\n" +
                    "   , TRY(CAST(w.actual_launch_date AS varchar)) actualLaunchDate\n" +
                    "   , w.f_status workStatus\n" +
                    "   , w.f_type workType\n" +
                    "   , pmc.f_pmg pmg\n" +
                    "   , TRY(CAST(w.edition_number AS integer)) editionNumber\n" +
                    "   , TRY(CAST(w.copyright_year AS integer)) copyrightYear\n" +
                    "   , TRY(CAST(w.planned_discontinue_date AS varchar)) plannedDiscontinuedate\n" +
                    "   , TRY(CAST(w.actual_discontinue_date AS varchar)) actualDiscontinueDate\n" +
                    "   , \"replace\"(ckw.cmms_issn_isbn, '-', '') currentIdentifier\n" +
                    "   , ckw.finance_tier financeTier\n" +
                    "   , ckw.search_tier searchTier\n" +
                    "   , TRY(CAST((CASE WHEN (ckw.pdf_suppression = 'true') THEN true ELSE false END) AS boolean)) pdfSuppression\n" +
                    "   , TRY(CAST((CASE WHEN (ckw.video = 'true') THEN true ELSE false END) AS boolean)) video\n" +
                    "   , TRY(CAST((CASE WHEN (ckw.topic_pages = 'true') THEN true ELSE false END) AS boolean)) topicPages\n" +
                    "   , TRY(CAST((CASE WHEN (ckw.journal_backfiles_only = 'true') THEN true ELSE false END) AS boolean)) journalBackfilesOnly\n" +
                    "   , TRY(CAST(ckw.years_of_coverage AS integer)) journalBackfileYears\n" +
                    "   , ckw.aip journalAip\n" +
                    "   , TRY(CAST(ckw.ck_live_date AS varchar)) workflowLiveDate\n" +
                    "   FROM\n" +
                    "     (((((("+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_rel_package pw\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork w ON (w.work_id = pw.f_component))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p ON (p.work_id = pw.f_package_owner))\n" +
                    "   INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_current_work ckw ON (ckw.eph_work_id = w.work_id))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON ((wt.code = w.f_type) AND (wt.l_end_date IS NULL)))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt2 ON (((wt2.code = p.f_type) AND (wt2.l_end_date IS NULL)) AND (wt2.roll_up_type = 'CK')))\n" +
                    "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_pmc pmc ON ((pmc.code = w.f_pmc) AND (pmc.l_end_date IS NULL)))\n" +
                    "   WHERE (((pw.effective_end_date IS NULL) AND (w.f_status <> 'NVW')) AND (wt.roll_up_type IN ('Book', 'Journal')))\n" +
                    "   GROUP BY pw.f_component, w.work_title, ckw.alternate_title, w.planned_launch_date, w.actual_launch_date, w.f_status, w.f_type, pmc.f_pmg, w.edition_number, w.copyright_year, w.planned_discontinue_date, w.actual_discontinue_date, ckw.cmms_issn_isbn, ckw.finance_tier, ckw.search_tier, ckw.pdf_suppression, ckw.video, ckw.topic_pages, ckw.journal_backfiles_only, ckw.years_of_coverage, ckw.aip, ckw.ck_live_date\n" +
                    "   ORDER BY pw.f_component ASC\n" +
                    ") \n" +
                    ")order by rand() limit %s";


    public static String GET_CK_CMMS_WORK2_IDENTIFIERS_VIEW_IDs =
            "select identifier as U_KEY from(\n" +
                    "(\n" +
                    "   SELECT\n" +
                    "     workId\n" +
                    "   , identifier\n" +
                    "   , endDate\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT\n" +
                    "        pw.f_component workId\n" +
                    "      , wi.s_identifier identifier\n" +
                    "      , TRY(CAST(wi.effective_end_date AS varchar)) endDate\n" +
                    "      FROM\n" +
                    "        (((((("+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_rel_package pw\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork w ON (w.work_id = pw.f_component))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p ON (p.work_id = pw.f_package_owner))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON ((wt.code = w.f_type) AND (wt.l_end_date IS NULL)))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt2 ON (((wt2.code = p.f_type) AND (wt2.l_end_date IS NULL)) AND (wt2.roll_up_type = 'CK')))\n" +
                    "      INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_current_work ckw ON (ckw.eph_work_id = w.work_id))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_identifier wi ON ((wi.f_wwork = w.work_id) AND (wi.f_type = 'ISSN-L')))\n" +
                    "      WHERE ((((pw.effective_end_date IS NULL) AND (w.f_status <> 'NVW')) AND (wt.roll_up_type = 'Journal')) AND (wi.s_identifier <> \"replace\"(ckw.cmms_issn_isbn, '-', '')))\n" +
                    "      GROUP BY pw.f_component, wi.s_identifier, wi.effective_end_date\n" +
                    "UNION       SELECT\n" +
                    "        pw.f_component workId\n" +
                    "      , mi.identifier identifier\n" +
                    "      , TRY(CAST(mi.effective_end_date AS varchar)) endDate\n" +
                    "      FROM\n" +
                    "        ((((((("+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_rel_package pw\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork w ON (w.work_id = pw.f_component))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p ON (p.work_id = pw.f_package_owner))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON ((wt.code = w.f_type) AND (wt.l_end_date IS NULL)))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt2 ON (((wt2.code = p.f_type) AND (wt2.l_end_date IS NULL)) AND (wt2.roll_up_type = 'CK')))\n" +
                    "      INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_current_work ckw ON (ckw.eph_work_id = w.work_id))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_manifestation m ON (m.f_wwork = w.work_id))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_manifestation_identifier mi ON ((mi.f_manifestation = m.manifestation_id) AND (mi.f_type = 'ISBN')))\n" +
                    "      WHERE (((((pw.effective_end_date IS NULL) AND (w.f_status <> 'NVW')) AND (m.f_status <> 'NVM')) AND (wt.roll_up_type = 'Book')) AND (mi.identifier <> ckw.cmms_issn_isbn))\n" +
                    "      GROUP BY pw.f_component, mi.identifier, mi.effective_end_date\n" +
                    "   ) \n" +
                    "   ORDER BY workId ASC, endDate DESC\n" +
                    ") " +
                    ")order by rand() limit %s";

    public static String GET_CK_CMMS_WORK3_SUBJECT_AREAS_VIEW_IDs =
            "select subjectAreaId as U_KEY from (\n" +
                    "(\n" +
                    "   SELECT\n" +
                    "     workId\n" +
                    "   , subjectAreaId\n" +
                    "   , subjectAreaName\n" +
                    "   , endDate\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT\n" +
                    "        pw.f_component workId\n" +
                    "      , TRY(CAST(sa.subject_area_id AS integer)) subjectAreaId\n" +
                    "      , sa.name subjectAreaName\n" +
                    "      , TRY(CAST(sal.effective_end_date AS varchar)) endDate\n" +
                    "      FROM\n" +
                    "        (((((("+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_rel_package pw\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork w ON (w.work_id = pw.f_component))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p ON (p.work_id = pw.f_package_owner))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON ((wt.code = w.f_type) AND (wt.l_end_date IS NULL)))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt2 ON (((wt2.code = p.f_type) AND (wt2.l_end_date IS NULL)) AND (wt2.roll_up_type = 'CK')))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_subject_area_link sal ON (sal.f_wwork = w.work_id))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_subject_area sa ON ((sa.subject_area_id = sal.f_subject_area) AND (sa.f_type = 'CK')))\n" +
                    "      WHERE (((pw.effective_end_date IS NULL) AND (w.f_status <> 'NVW')) AND (wt.roll_up_type IN ('Book', 'Journal')))\n" +
                    "      GROUP BY pw.f_component, sa.subject_area_id, sa.name, sal.effective_end_date\n" +
                    "   ) \n" +
                    "   ORDER BY workId ASC, endDate DESC\n" +
                    ") )order by rand() limit %s";

    public static String GET_CK_CMMS_PACKAGE1_VIEW_IDs =
            "select packageId as U_KEY from(\n" +
                    "(\n" +
                    "   SELECT\n" +
                    "     p.work_id packageId\n" +
                    "   , wh.code packageSiteCode\n" +
                    "   , wi.identifier packageAcronym\n" +
                    "   , p.work_title packageTitle\n" +
                    "   , p.f_status packageStatus\n" +
                    "   , p.f_type packageType\n" +
                    "   , sc.cmms_ck_site_desc cmmsSiteName\n" +
                    "   FROM\n" +
                    "     ((((("+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON (((wt.code = p.f_type) AND (wt.l_end_date IS NULL)) AND (wt.roll_up_type = 'CK')))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_identifier wi ON (((wi.f_wwork = p.work_id) AND (wi.f_type = 'PACKAGE ACRONYM')) AND (wi.effective_end_date IS NULL)))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_work_hchy_link wwh ON ((wwh.f_wwork = p.work_id) AND (wwh.effective_end_date IS NULL)))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_hierarchy wh ON ((((wh.work_hierarchy_id = wwh.f_work_hierarchy) AND (wh.effective_end_date IS NULL)) AND (wh.hierarchy_level = 3)) AND (wh.f_type = 'CK')))\n" +
                    "   INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_site_conversion sc ON (sc.work_hierarchy_code = wh.code))\n" +
                    "   WHERE (p.f_status <> 'NVW')\n" +
                    "   GROUP BY p.work_id, wh.code, wi.identifier, p.work_title, p.f_status, p.f_type, sc.cmms_ck_site_desc\n" +
                    "   ORDER BY wh.code ASC, wi.identifier ASC\n" +
                    ") )order by rand() limit %s";

    public static String GET_CK_CMMS_PACKAGE2_WORKS_VIEW_IDs =
            "select workId as U_KEY from(\n" +
                    "(\n" +
                    "   SELECT\n" +
                    "     workId\n" +
                    "   , packageId\n" +
                    "   , endDate\n" +
                    "   , durableUrl\n" +
                    "   , workflowLiveDate\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT\n" +
                    "        pw.f_component workId\n" +
                    "      , pw.f_package_owner packageId\n" +
                    "      , pw.effective_end_date endDate\n" +
                    "      , pwu.durable_url durableUrl\n" +
                    "      , TRY(CAST(pwu.ck_live_date AS varchar)) workflowLiveDate\n" +
                    "      , \"row_number\"() OVER (PARTITION BY pw.f_package_owner, pw.f_component, pwu.durable_url, pwu.ck_live_date ORDER BY pw.effective_end_date DESC NULLS FIRST) rank\n" +
                    "      FROM\n" +
                    "        ((((("+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_rel_package pw\n" +
                    "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork w ON (w.work_id = pw.f_component))\n" +
                    "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p ON (p.work_id = pw.f_package_owner))\n" +
                    "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON ((wt.code = w.f_type) AND (wt.l_end_date IS NULL)))\n" +
                    "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt2 ON (((wt2.code = p.f_type) AND (wt2.l_end_date IS NULL)) AND (wt2.roll_up_type = 'CK')))\n" +
                    "      INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_current_package_work pwu ON ((pwu.eph_package_id = pw.f_package_owner) AND (pwu.eph_work_id = pw.f_component)))\n" +
                    "      WHERE (((p.f_status <> 'NVW') AND (w.f_status <> 'NVW')) AND (wt.roll_up_type IN ('Book', 'Journal')))\n" +
                    "   ) \n" +
                    "   WHERE (rank = 1)\n" +
                    ") ) order by rand() limit %s";

    //     GET View Records
    public static String GET_CK_CMMS_WORKFLOW_VIEW =
            "select * from\n" +
                    "(\n" +
                    "   SELECT\n" +
                    "     '1.0' schemaVersion\n" +
                    "   , TRY(CAST(wf.workflowId AS integer)) workflowId\n" +
                    "   , wf.workflowType workflowType\n" +
                    "   , TRY(CAST(wf.cutoffDate AS varchar)) workflowCutoffDate\n" +
                    "   , TRY(CAST(wf.liveDate AS varchar)) workflowLiveDate\n" +
                    "   FROM\n" +
                    "     "+GetCKDLDB.getCKDataBase()+".ck_workflow_inbound_part wf\n" +
                    "   WHERE (wf.inbound_ts = (SELECT max(inbound_ts)\n" +
                    "FROM\n" +
                    "  "+GetCKDLDB.getCKDataBase()+".ck_workflow_inbound_part\n" +
                    "))\n" +
                    ")where workflowid in (%s)";

    public static String GET_CK_CMMS_WORK1_VIEW =
            "select * from(\n" +
                    "(\n" +
                    "   SELECT\n" +
                    "     pw.f_component workId\n" +
                    "   , w.work_title workTitle\n" +
                    "   , ckw.alternate_title alternateTitle\n" +
                    "   , TRY(CAST(w.planned_launch_date AS varchar)) plannedLaunchDate\n" +
                    "   , TRY(CAST(w.actual_launch_date AS varchar)) actualLaunchDate\n" +
                    "   , w.f_status workStatus\n" +
                    "   , w.f_type workType\n" +
                    "   , pmc.f_pmg pmg\n" +
                    "   , TRY(CAST(w.edition_number AS integer)) editionNumber\n" +
                    "   , TRY(CAST(w.copyright_year AS integer)) copyrightYear\n" +
                    "   , TRY(CAST(w.planned_discontinue_date AS varchar)) plannedDiscontinuedate\n" +
                    "   , TRY(CAST(w.actual_discontinue_date AS varchar)) actualDiscontinueDate\n" +
                    "   , \"replace\"(ckw.cmms_issn_isbn, '-', '') currentIdentifier\n" +
                    "   , ckw.finance_tier financeTier\n" +
                    "   , ckw.search_tier searchTier\n" +
                    "   , TRY(CAST((CASE WHEN (ckw.pdf_suppression = 'true') THEN true ELSE false END) AS boolean)) pdfSuppression\n" +
                    "   , TRY(CAST((CASE WHEN (ckw.video = 'true') THEN true ELSE false END) AS boolean)) video\n" +
                    "   , TRY(CAST((CASE WHEN (ckw.topic_pages = 'true') THEN true ELSE false END) AS boolean)) topicPages\n" +
                    "   , TRY(CAST((CASE WHEN (ckw.journal_backfiles_only = 'true') THEN true ELSE false END) AS boolean)) journalBackfilesOnly\n" +
                    "   , TRY(CAST(ckw.years_of_coverage AS integer)) journalBackfileYears\n" +
                    "   , ckw.aip journalAip\n" +
                    "   , TRY(CAST(ckw.ck_live_date AS varchar)) workflowLiveDate\n" +
                    "   FROM\n" +
                    "     (((((("+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_rel_package pw\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork w ON (w.work_id = pw.f_component))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p ON (p.work_id = pw.f_package_owner))\n" +
                    "   INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_current_work ckw ON (ckw.eph_work_id = w.work_id))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON ((wt.code = w.f_type) AND (wt.l_end_date IS NULL)))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt2 ON (((wt2.code = p.f_type) AND (wt2.l_end_date IS NULL)) AND (wt2.roll_up_type = 'CK')))\n" +
                    "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_pmc pmc ON ((pmc.code = w.f_pmc) AND (pmc.l_end_date IS NULL)))\n" +
                    "   WHERE (((pw.effective_end_date IS NULL) AND (w.f_status <> 'NVW')) AND (wt.roll_up_type IN ('Book', 'Journal')))\n" +
                    "   GROUP BY pw.f_component, w.work_title, ckw.alternate_title, w.planned_launch_date, w.actual_launch_date, w.f_status, w.f_type, pmc.f_pmg, w.edition_number, w.copyright_year, w.planned_discontinue_date, w.actual_discontinue_date, ckw.cmms_issn_isbn, ckw.finance_tier, ckw.search_tier, ckw.pdf_suppression, ckw.video, ckw.topic_pages, ckw.journal_backfiles_only, ckw.years_of_coverage, ckw.aip, ckw.ck_live_date\n" +
                    "   ORDER BY pw.f_component ASC\n" +
                    ") \n" +
                    ")where currentIdentifier in ('%s')";

    public static String GET_CK_CMMS_WORK2_IDENTIFIERS_VIEW =
            "select * from(\n" +
                    "(\n" +
                    "   SELECT\n" +
                    "     workId\n" +
                    "   , identifier\n" +
                    "   , endDate\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT\n" +
                    "        pw.f_component workId\n" +
                    "      , wi.s_identifier identifier\n" +
                    "      , TRY(CAST(wi.effective_end_date AS varchar)) endDate\n" +
                    "      FROM\n" +
                    "        (((((("+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_rel_package pw\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork w ON (w.work_id = pw.f_component))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p ON (p.work_id = pw.f_package_owner))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON ((wt.code = w.f_type) AND (wt.l_end_date IS NULL)))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt2 ON (((wt2.code = p.f_type) AND (wt2.l_end_date IS NULL)) AND (wt2.roll_up_type = 'CK')))\n" +
                    "      INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_current_work ckw ON (ckw.eph_work_id = w.work_id))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_identifier wi ON ((wi.f_wwork = w.work_id) AND (wi.f_type = 'ISSN-L')))\n" +
                    "      WHERE ((((pw.effective_end_date IS NULL) AND (w.f_status <> 'NVW')) AND (wt.roll_up_type = 'Journal')) AND (wi.s_identifier <> \"replace\"(ckw.cmms_issn_isbn, '-', '')))\n" +
                    "      GROUP BY pw.f_component, wi.s_identifier, wi.effective_end_date\n" +
                    "UNION       SELECT\n" +
                    "        pw.f_component workId\n" +
                    "      , mi.identifier identifier\n" +
                    "      , TRY(CAST(mi.effective_end_date AS varchar)) endDate\n" +
                    "      FROM\n" +
                    "        ((((((("+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_rel_package pw\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork w ON (w.work_id = pw.f_component))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p ON (p.work_id = pw.f_package_owner))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON ((wt.code = w.f_type) AND (wt.l_end_date IS NULL)))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt2 ON (((wt2.code = p.f_type) AND (wt2.l_end_date IS NULL)) AND (wt2.roll_up_type = 'CK')))\n" +
                    "      INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_current_work ckw ON (ckw.eph_work_id = w.work_id))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_manifestation m ON (m.f_wwork = w.work_id))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_manifestation_identifier mi ON ((mi.f_manifestation = m.manifestation_id) AND (mi.f_type = 'ISBN')))\n" +
                    "      WHERE (((((pw.effective_end_date IS NULL) AND (w.f_status <> 'NVW')) AND (m.f_status <> 'NVM')) AND (wt.roll_up_type = 'Book')) AND (mi.identifier <> ckw.cmms_issn_isbn))\n" +
                    "      GROUP BY pw.f_component, mi.identifier, mi.effective_end_date\n" +
                    "   ) \n" +
                    "   ORDER BY workId ASC, endDate DESC\n" +
                    ") " +
                    ")where identifier in ('%s') order by workId desc, identifier desc ";

    public static String GET_CK_CMMS_WORK3_SUBJECT_AREAS_VIEW =
            "select * from (\n" +
                    "(\n" +
                    "   SELECT\n" +
                    "     workId\n" +
                    "   , subjectAreaId\n" +
                    "   , subjectAreaName\n" +
                    "   , endDate\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT\n" +
                    "        pw.f_component workId\n" +
                    "      , TRY(CAST(sa.subject_area_id AS integer)) subjectAreaId\n" +
                    "      , sa.name subjectAreaName\n" +
                    "      , TRY(CAST(sal.effective_end_date AS varchar)) endDate\n" +
                    "      FROM\n" +
                    "        (((((("+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_rel_package pw\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork w ON (w.work_id = pw.f_component))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p ON (p.work_id = pw.f_package_owner))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON ((wt.code = w.f_type) AND (wt.l_end_date IS NULL)))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt2 ON (((wt2.code = p.f_type) AND (wt2.l_end_date IS NULL)) AND (wt2.roll_up_type = 'CK')))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_subject_area_link sal ON (sal.f_wwork = w.work_id))\n" +
                    "      INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_subject_area sa ON ((sa.subject_area_id = sal.f_subject_area) AND (sa.f_type = 'CK')))\n" +
                    "      WHERE (((pw.effective_end_date IS NULL) AND (w.f_status <> 'NVW')) AND (wt.roll_up_type IN ('Book', 'Journal')))\n" +
                    "      GROUP BY pw.f_component, sa.subject_area_id, sa.name, sal.effective_end_date\n" +
                    "   ) \n" +
                    "   ORDER BY workId ASC, endDate DESC\n" +
                    ") )where subjectAreaId in (%s) order by enddate desc, workid desc ";

    public static String GET_CK_CMMS_PACKAGE1_VIEW =
            "select * from(\n" +
                    "(\n" +
                    "   SELECT\n" +
                    "     p.work_id packageId\n" +
                    "   , wh.code packageSiteCode\n" +
                    "   , wi.identifier packageAcronym\n" +
                    "   , p.work_title packageTitle\n" +
                    "   , p.f_status packageStatus\n" +
                    "   , p.f_type packageType\n" +
                    "   , sc.cmms_ck_site_desc cmmsSiteName\n" +
                    "   FROM\n" +
                    "     ((((("+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON (((wt.code = p.f_type) AND (wt.l_end_date IS NULL)) AND (wt.roll_up_type = 'CK')))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_identifier wi ON (((wi.f_wwork = p.work_id) AND (wi.f_type = 'PACKAGE ACRONYM')) AND (wi.effective_end_date IS NULL)))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_work_hchy_link wwh ON ((wwh.f_wwork = p.work_id) AND (wwh.effective_end_date IS NULL)))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_hierarchy wh ON ((((wh.work_hierarchy_id = wwh.f_work_hierarchy) AND (wh.effective_end_date IS NULL)) AND (wh.hierarchy_level = 3)) AND (wh.f_type = 'CK')))\n" +
                    "   INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_site_conversion sc ON (sc.work_hierarchy_code = wh.code))\n" +
                    "   WHERE (p.f_status <> 'NVW')\n" +
                    "   GROUP BY p.work_id, wh.code, wi.identifier, p.work_title, p.f_status, p.f_type, sc.cmms_ck_site_desc\n" +
                    "   ORDER BY wh.code ASC, wi.identifier ASC\n" +
                    ") )where packageId in ('%s') order by packageID desc";

                public static String GET_CK_CMMS_PACKAGE2_WORKS_VIEW =
                        "select * from(\n" +
                                "(\n" +
                                "   SELECT\n" +
                                "     workId\n" +
                                "   , packageId\n" +
                                "   , endDate\n" +
                                "   , durableUrl\n" +
                                "   , workflowLiveDate\n" +
                                "   FROM\n" +
                                "     (\n" +
                                "      SELECT\n" +
                                "        pw.f_component workId\n" +
                                "      , pw.f_package_owner packageId\n" +
                                "      , pw.effective_end_date endDate\n" +
                                "      , pwu.durable_url durableUrl\n" +
                                "      , TRY(CAST(pwu.ck_live_date AS varchar)) workflowLiveDate\n" +
                                "      , \"row_number\"() OVER (PARTITION BY pw.f_package_owner, pw.f_component, pwu.durable_url, pwu.ck_live_date ORDER BY pw.effective_end_date DESC NULLS FIRST) rank\n" +
                                "      FROM\n" +
                                "        ((((("+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_work_rel_package pw\n" +
                                "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork w ON (w.work_id = pw.f_component))\n" +
                                "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_wwork p ON (p.work_id = pw.f_package_owner))\n" +
                                "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt ON ((wt.code = w.f_type) AND (wt.l_end_date IS NULL)))\n" +
                                "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getProdDataBaseGd()+".gd_x_lov_work_type wt2 ON (((wt2.code = p.f_type) AND (wt2.l_end_date IS NULL)) AND (wt2.roll_up_type = 'CK')))\n" +
                                "      INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_transform_current_package_work pwu ON ((pwu.eph_package_id = pw.f_package_owner) AND (pwu.eph_work_id = pw.f_component)))\n" +
                                "      WHERE (((p.f_status <> 'NVW') AND (w.f_status <> 'NVW')) AND (wt.roll_up_type IN ('Book', 'Journal')))\n" +
                                "   ) \n" +
                                "   WHERE (rank = 1)\n" +
                                ") )where workId in ('%s') order by workId desc, durableurl desc, workflowLiveDate desc";

    //    GET Table Records
    public static String GET_CK_CMMS_WORKFLOW_TABLE = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".cmms_workflow_v where workflowid in (%s)";
    public static String GET_CK_CMMS_WORK1 = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where currentIdentifier in ('%s')";
    public static String GET_CK_CMMS_WORK2_IDENTIFIERS = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where identifier in ('%s') order by workId desc, identifier desc";
    public static String GET_CK_CMMS_WORK3_SUBJECT_AREAS = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where subjectAreaId in (%s) order by enddate desc, workid desc";
    public static String GET_CK_CMMS_PACKAGE1 = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where packageId in ('%s') order by packageID desc";
    public static String GET_CK_CMMS_PACKAGE2_WORKS = "select * from " + GetCKDLDB.getCKCMMSDataBase() + ".%s where workId in ('%s') order by workId desc, durableurl desc, workflowLiveDate desc";
}
