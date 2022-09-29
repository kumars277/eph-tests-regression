package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKCMMSInboundCountChecksSQL {

    public static String GET_CMMSINBOUND_VIEW_COUNT = "select count(*) as COUNT from "+GetCKDLDB.getCKDataBase()+".%s";

    public static String GET_CMMSINBOUND_Durable_url1_form_Query_COUNT = "Select count(*) from(\n" +
            "   SELECT\n" +
            "     (CASE WHEN (\"length\"(\"trim\"(t.cmms_issn_isbn)) = 8) THEN 'ISSN-L' ELSE 'ISBN' END) inbound_type\n" +
            "   , CAST(null AS varchar) eph_work_id\n" +
            "   , \"trim\"(t.cmms_issn_isbn) uncorrected_cmms_issn_isbn\n" +
            "   , COALESCE(idc.corrected_identifier, \"trim\"(t.cmms_issn_isbn)) cmms_issn_isbn\n" +
            "   , t.cmms_site_name cmms_site_name\n" +
            "   , t.durable_url durable_url\n" +
            "   , CAST(null AS varchar) u_key\n" +
            "   FROM\n" +
            "     ("+GetCKDLDB.getCKDataBase()+".cmms_durable_url_inbound_part t\n" +
            "   LEFT JOIN "+GetCKDLDB.getCKDataBase()+".ck_identifier_correction idc ON (\"trim\"(t.cmms_issn_isbn) = idc.cmms_issn_isbn))\n" +
            ")";

    public static String GET_CMMSINBOUND_Durable_url2_form_Query_COUNT = "Select count (*) from (\n" +
            "   SELECT\n" +
            "     wh.f_wwork eph_package_id\n" +
            "   , wi.f_wwork eph_work_id\n" +
            "   , t.cmms_issn_isbn cmms_issn_isbn\n" +
            "   , t.durable_url durable_url\n" +
            "   , (CASE WHEN (wi.f_wwork IS NULL) THEN \"concat\"(\"concat\"(wh.f_wwork, '-'), t.cmms_issn_isbn) ELSE \"concat\"(\"concat\"(wh.f_wwork, '-'), wi.f_wwork) END) u_key\n" +
            "   FROM\n" +
            "     (((((("+GetCKDLDB.getCKDataBase()+".cmms_durable_url1_form_v t\n" +
            "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_identifier wi ON ((wi.f_type = 'ISSN-L') AND (wi.s_identifier = t.cmms_issn_isbn)))\n" +
            "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_wwork w ON (w.work_id = wi.f_wwork))\n" +
            "   INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_site_conversion sx ON (sx.cmms_ck_site_desc = t.cmms_site_name))\n" +
            "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_work_hchy_link wh ON ((wh.f_work_hierarchy = CAST(sx.work_hierarchy_id AS integer)) AND (wh.effective_end_date IS NULL)))\n" +
            "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_wwork w1 ON (((w1.work_id = wh.f_wwork) AND (w1.f_type IN ('CKFLEX', 'CKFLEXAG', 'CKSPECPKG'))) AND (w1.f_status <> 'NVW')))\n" +
            "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_identifier u ON ((u.f_type = 'ISSN-L') AND (u.s_identifier = t.uncorrected_cmms_issn_isbn)))\n" +
            "   WHERE (((t.inbound_type = 'ISSN-L') AND (w.f_status <> 'NVW')) AND (wi.effective_end_date IS NULL))\n" +
            "UNION ALL    SELECT\n" +
            "     wh.f_wwork eph_package_id\n" +
            "   , m.f_wwork eph_work_id\n" +
            "   , t.cmms_issn_isbn cmms_issn_isbn\n" +
            "   , t.durable_url durable_url\n" +
            "   , (CASE WHEN (m.f_wwork IS NULL) THEN \"concat\"(\"concat\"(wh.f_wwork, '-'), t.cmms_issn_isbn) ELSE \"concat\"(\"concat\"(wh.f_wwork, '-'), m.f_wwork) END) u_key\n" +
            "   FROM\n" +
            "     ((((((("+GetCKDLDB.getCKDataBase()+".cmms_durable_url1_form_v t\n" +
            "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation_identifier mi ON (((mi.f_type = 'ISBN') AND (mi.s_identifier = t.cmms_issn_isbn)) AND (mi.lead_indicator = true)))\n" +
            "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation m ON (m.manifestation_id = mi.f_manifestation))\n" +
            "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_wwork w ON (w.work_id = m.f_wwork))\n" +
            "   INNER JOIN "+GetCKDLDB.getCKDataBase()+".ck_site_conversion sx ON (sx.cmms_ck_site_desc = t.cmms_site_name))\n" +
            "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_work_work_hchy_link wh ON ((wh.f_work_hierarchy = CAST(sx.work_hierarchy_id AS integer)) AND (wh.effective_end_date IS NULL)))\n" +
            "   INNER JOIN "+GetCKDLDB.getProductDataBase()+".gd_wwork w1 ON (((w1.work_id = wh.f_wwork) AND (w1.f_type IN ('CKFLEX', 'CKFLEXAG', 'CKSPECPKG'))) AND (w1.f_status <> 'NVW')))\n" +
            "   LEFT JOIN "+GetCKDLDB.getProductDataBase()+".gd_manifestation_identifier u ON (((u.f_type = 'ISBN') AND (u.s_identifier = t.uncorrected_cmms_issn_isbn)) AND (u.lead_indicator = true)))\n" +
            "   WHERE (((t.inbound_type = 'ISBN') AND (w.f_status <> 'NVW')) AND (mi.effective_end_date IS NULL))\n" +
            "   ORDER BY u_key ASC\n" +
            ") ";


    public static String GET_CMMSINBOUND_Durable_url3_form_Query_COUNT ="Select count (*)from (\n" +
            "   SELECT\n" +
            "     eph_package_id\n" +
            "   , eph_work_id\n" +
            "   , durable_url\n" +
            "   , u_key\n" +
            "   FROM\n" +
            "     (\n" +
            "      SELECT\n" +
            "        pwu.*\n" +
            "      , \"rank\"() OVER (PARTITION BY pwu.eph_package_id, pwu.eph_work_id ORDER BY pwu.durable_url ASC) rank\n" +
            "      FROM\n" +
            "        "+GetCKDLDB.getCKDataBase()+".cmms_durable_url2_form_v pwu\n" +
            "   ) \n" +
            "   WHERE (rank = 1)\n" +
            "   ORDER BY u_key ASC\n" +
            ") ";
    public static String GET_CMMSINBOUND_Durable_url_Transform_Query_COUNT ="Select count(*) from (\n" +
            "   SELECT\n" +
            "     eph_package_id\n" +
            "   , eph_work_id\n" +
            "   , durable_url\n" +
            "   , u_key\n" +
            "   FROM\n" +
            "     "+GetCKDLDB.getCKDataBase()+".cmms_durable_url3_form_v\n" +
            "   GROUP BY eph_package_id, eph_work_id, durable_url, u_key\n" +
            "   ORDER BY u_key ASC\n" +
            ") ";
}


