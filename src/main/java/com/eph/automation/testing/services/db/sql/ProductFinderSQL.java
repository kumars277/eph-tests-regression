package com.eph.automation.testing.services.db.sql;

public class ProductFinderSQL {
    public static String SELECT_WORK_BY_ID_FOR_SEARCH = "SELECT \"work_id\" as WORK_ID FROM semarchy_eph_mdm.gd_wwork WHERE work_id='%s'";

    public static String EPH_GD_WORK_EXTRACT_FOR_SEARCH = "SELECT \n" +
            "               work_id AS WORK_ID,\n" +
            "               work_title AS WORK_TITLE,\n" +
            "               work_sub_title AS WORK_SUBTITLE,\n" +
            "               electro_rights_indicator AS ELECTRONIC_RIGHTS_IND,\n" +
            "               volume AS VOLUME,\n" +
            "               copyright_year as COPYRIGHT_YEAR,\n" +
            "               edition_number AS EDITION_NUMBER,\n" +
            "               planned_launch_date AS PLANNED_LAUNCH_DATE,\n"+
            "               f_type AS WORK_TYPE,\n" +
            "               f_status AS WORK_STATUS,\n" +
            "               f_pmc AS PMC,\n" +
            "               f_imprint AS IMPRINT,\n" +
            "               f_legal_ownership AS LEGAL_OWNERSHIP,\n"+
            "               f_llanguage AS LANGUAGE_CODE,\n" +
            "               f_accountable_product\n" +
            "              FROM semarchy_eph_mdm.gd_wwork " +
            "  WHERE work_id IN ('%s')";

    public static String SELECT_AVAILABLE_WORK_TYPES_FOR_BOOK = "select distinct f_type as WORK_TYPE from semarchy_eph_mdm.gd_wwork where f_type in ('BKS','MRW','OTH','RBK','SER','TBK')";
    public static String SELECT_AVAILABLE_WORK_TYPES_FOR_JOURNAL = "select distinct f_type as WORK_TYPE from semarchy_eph_mdm.gd_wwork where f_type in ('ABS','JBB','JNL','NWL')";
    public static String SELECT_AVAILABLE_WORK_TYPES_FOR_OTHER = "select distinct f_type as WORK_TYPE from semarchy_eph_mdm.gd_wwork where f_type in ('DMG','MPR')";
    //updated by Nishant @ Feb 2020
    public static String SELECT_WORKID_FOR_WORK_TYPE = "select work_id as WORK_ID from semarchy_eph_mdm.gd_wwork where f_type = '%s' order by random() limit 1";
    public static String SELECT_PRODUCTID_TITLE_FOR_SEARCH = "select product_id as PRODUCT_ID, name as PRODUCT_TITLE from semarchy_eph_mdm.gd_product order by random() limit 1";

    //created by Nishant @ 15 May 2020
    public static String SELECT_RANDOM_WORK_IDS_FOR_SEARCH = "SELECT \"work_id\" as WORK_ID\n" +
            "FROM semarchy_eph_mdm.gd_wwork \n" +
            "WHERE exists (\n" +
            "SELECT * FROM semarchy_eph_mdm.gd_manifestation\n" +
            "WHERE semarchy_eph_mdm.gd_wwork.work_id = semarchy_eph_mdm.gd_manifestation.f_wwork "+
            "and LENGTH(semarchy_eph_mdm.gd_manifestation.manifestation_key_title)>20)\n" +
            "and LENGTH(work_title)>20\n" +
            "order by random() limit '%s'";

    //created by Nishant @ 9 June 2020
    public static String SELECT_ACCESS_MODEL="select f_access_model ACCESS_MODEL from semarchy_eph_mdm.gd_work_access_model where f_wwork='%s'";
    public static String SELECT_BUSINESS_MODEL="select f_business_model as BUSINESS_MODEL from semarchy_eph_mdm.gd_work_business_model where f_wwork='%s'";
    public static String SELECT_OWNERSHIP_DESCRIPTION="select l_description,f_legal_owner from semarchy_eph_mdm.gd_x_lov_owner_description od \n" +
        "inner join semarchy_eph_mdm.gd_work_legal_owner lo on lo.f_ownership_description=od.code where lo.f_wwork='%S';";
    public static String SELECT_LEGAL_OWNER="select name from semarchy_eph_mdm.gd_legal_owner where legal_owner_id='%s'";
    public static String SELECT_PMC_INFO="select l_description,f_pmg from semarchy_eph_mdm.gd_x_lov_pmc where code='%s'";
    public static String SELECT_PMG_INFO="select l_description from semarchy_eph_mdm.gd_x_lov_pmg where code='%S'";
   public static String SELECT_IMPRINT_INFO="select l_description from semarchy_eph_mdm.gd_x_lov_imprint where code='%S'";
    public static String SELECT_SUBJECT_AREA="select sa.name,sa.f_type,sa.f_parent_subject_area from semarchy_eph_mdm.gd_subject_area sa\n" +
        "inner join semarchy_eph_mdm.gd_work_subject_area_link wsa\n" +
        "on wsa.f_subject_area=sa.subject_area_id where wsa.f_wwork='%S'" +
            "and wsa.f_subject_area not in(select wsa.f_subject_area where wsa.effective_end_date<current_date)";

    public static String SELECT_IDENTIFIER_OF_WORK="select f_type,identifier from semarchy_eph_mdm.gd_work_identifier where f_wwork='%s'" +
            " and identifier not in (select identifier where effective_end_date<current_date)";
}
