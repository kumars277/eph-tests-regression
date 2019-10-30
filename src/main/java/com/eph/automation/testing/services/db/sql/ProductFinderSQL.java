package com.eph.automation.testing.services.db.sql;

public class ProductFinderSQL {
    public static String SELECT_WORK_BY_ID_FOR_SEARCH = "SELECT \"work_id\" as WORK_ID\n" +
            "FROM semarchy_eph_mdm.gd_wwork " +
            "WHERE work_id='%s'";
    public static String EPH_GD_WORK_EXTRACT_FOR_SEARCH = "SELECT \n" +
            "               work_id AS WORK_ID,\n" +
            "               work_title AS WORK_TITLE,\n" +
            "               work_sub_title AS WORK_SUBTITLE,\n" +
            "               volume AS VOLUME,\n" +
            "               f_llanguage AS LANGUAGE_CODE,\n" +
            "               electro_rights_indicator AS ELECTRONIC_RIGHTS_IND,\n" +
            "               f_pmc AS PMC,\n" +
            "               copyright_year as COPYRIGHT_YEAR,\n" +
            "               f_status AS WORK_STATUS,\n" +
            "               f_type AS F_TYPE,\n" +
            "               f_imprint AS IMPRINT,\n" +
            "               edition_number AS EDITION_NUMBER,\n" +
            "               volume AS VOLUME,\n" +
            "               copyright_year AS COPYRIGHT_YEAR\n" +
            "              FROM ephsit.semarchy_eph_mdm.gd_wwork " +
            "  WHERE work_id IN ('%s')";

    public static String SELECT_AVAILABLE_WORK_TYPES_FOR_BOOK = "select distinct f_type as WORK_TYPE from semarchy_eph_mdm.gd_wwork where f_type in ('BKS','MRW','OTH','RBK','SER','TBK')";
    public static String SELECT_AVAILABLE_WORK_TYPES_FOR_JOURNAL = "select distinct f_type as WORK_TYPE from semarchy_eph_mdm.gd_wwork where f_type in ('ABS','JBB','JNL','NWL')";
    public static String SELECT_AVAILABLE_WORK_TYPES_FOR_OTHER = "select distinct f_type as WORK_TYPE from semarchy_eph_mdm.gd_wwork where f_type in ('DMG','MPR')";
    public static String SELECT_WORKID_FOR_WORK_TYPE = "select work_id as WORK_ID from semarchy_eph_mdm.gd_wwork where f_type = '%s' limit 1";
    public static String SELECT_PRODUCTID_TITLE_FOR_SEARCH = "select product_id as PRODUCT_ID, name as PRODUCT_TITLE from semarchy_eph_mdm.gd_product limit 1";

}
