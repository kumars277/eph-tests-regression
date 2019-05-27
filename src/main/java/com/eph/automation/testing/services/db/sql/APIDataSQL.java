package com.eph.automation.testing.services.db.sql;

public class APIDataSQL {
    public static String SELECT_RANDOM_PRODUCT_IDS_FOR_SEARCH_BOOKS = "SELECT \"product_id\" as PRODUCT_ID\n" +
            "FROM semarchy_eph_mdm.gd_product\n" +
            "WHERE exists\n" +
            "    (SELECT * \n" +
            "     from semarchy_eph_stg.st_out_notification\n" +
            "     WHERE semarchy_eph_mdm.gd_product.product_id = semarchy_eph_stg.st_out_notification.notification_id and semarchy_eph_stg.st_out_notification.status='PROCESSED')\n" +
            "order by random() limit '%s'";

    public static String SELECT_RANDOM_WORK_IDS_FOR_SEARCH = "SELECT \"work_id\" as WORK_ID\n" +
            "FROM semarchy_eph_mdm.gd_wwork " +
            "WHERE exists (SELECT * FROM semarchy_eph_stg.st_out_notification " +
            "WHERE semarchy_eph_mdm.gd_wwork.work_id = semarchy_eph_stg.st_out_notification.notification_id and semarchy_eph_stg.st_out_notification.status='PROCESSED')" +
            "order by random() limit '%s'";

    public static String SELECT_RANDOM_WORK_INPACKAGE_IDS_FOR_SEARCH = "select f_component from semarchy_eph_mdm.gd_product_rel_package group by f_component order by random() limit '%s';";

    public static String SELECT_RANDOM_PRODUCT_PERSON_ROLES_FOR_SEARCH = "select f_person from semarchy_eph_mdm.gd_product_person_role group by f_person order by random() limit '%s';";

    public static String SELECT_RANDOM_WORK_PERSON_ROLES_FOR_SEARCH = "select f_person from semarchy_eph_mdm.gd_work_person_role group by f_person order by random() limit '%s'";

    public static String SELECT_COUNT_PERSONID_FOR_WORKS = "select count (*) from semarchy_eph_mdm.gd_work_person_role where f_person='%s'";

    public static String SELECT_COUNT_PERSONID_FOR_PRODUCTS = "select count (*) from semarchy_eph_mdm.gd_product_person_role where f_person='%s'";

    public static String EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH = "SELECT \n" +
            "              product_id AS PRODUCT_ID -- Title\n" +
            "              ,name as PRODUCT_NAME\n" +
            "              ,separately_sale_indicator as SEPARATELY_SALEABLE_IND\n" +
            "              ,trial_allowed_indicator as TRIAL_ALLOWED_IND\n" +
            "              ,short_name as PRODUCT_SHORT_NAME\n" +
            "              ,launch_date as FIRST_PUB_DATE\n" +
            "              ,f_type AS F_TYPE\n" +
            "              ,f_status AS F_STATUS\n" +
            "              ,f_revenue_model AS F_REVENUE_MODEL\n" +
            "              ,f_wwork AS F_PRODUCT_WORK\n" +
            "              ,f_manifestation AS F_PRODUCT_MANIFESTATION_TYP\n" +
            "              FROM ephsit.semarchy_eph_mdm.gd_product \n" +
            "  WHERE product_id IN ('%s')";

    public static String EPH_GD_WORK_EXTRACT_FOR_SEARCH = "SELECT \n" +
            "               work_id AS WORK_ID,\n" +
            "               work_title AS WORK_TITLE,\n" +
            "               work_sub_title AS WORK_SUBTITLE,\n" +
            "               volume AS VOLUME,\n" +
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

    public static String EPH_GD_WORK_EXTRACT_AMOUNT_BYPMC ="SELECT COUNT (f_pmc) FROM semarchy_eph_mdm.gd_wwork where f_pmc='%s';";
    public static String EPH_GD_PMG_CODE_EXTRACT_BYPMC ="select f_pmg from semarchy_eph_mdm.gd_x_lov_pmc where code='%s'";
    public static String EPH_GD_PACKAGEID_EXTRACT_BY_PRODUCTID ="select f_package_owner from semarchy_eph_mdm.gd_product_rel_package where f_component='%s' limit 1";
    public static String EPH_GD_WORK_EXTRACT_AMOUNT_BYPMG ="select count (*) from semarchy_eph_mdm.gd_wwork where f_pmc in ( select code from semarchy_eph_mdm.gd_x_lov_pmc where f_pmg in (select code from semarchy_eph_mdm.gd_x_lov_pmg where code='%s'));";
    public static String EPH_GD_PRODUCT_PACKAGE_COUNT_EXTRACT = "select count (*) from semarchy_eph_mdm.gd_product_rel_package where f_package_owner='%s';";
    public static String EPH_GD_WORK_EXTRACT_FOR_SEARCH_BY_MANIFESTATIONID = "SELECT \n" +
            "               work_id AS WORK_ID,\n" +
            "               work_title AS WORK_TITLE,\n" +
            "               work_sub_title AS WORK_SUBTITLE,\n" +
            "               volume AS VOLUME,\n" +
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
            "  WHERE work_id IN (select f_wwork from ephsit.semarchy_eph_mdm.gd_manifestation where manifestation_id in ('%s'))";

    public static final String SELECT_MANIFESTATION_IDS_BY_WORKID = "select \"manifestation_id\" as manifestation_id from semarchy_eph_mdm.gd_manifestation where \"f_wwork\" in ('%s')";

    public static final String SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID = "select F_EVENT  as F_EVENT,\n" +
            "B_CLASSNAME as B_CLASSNAME,\n" +
            "MANIFESTATION_ID as MANIFESTATION_ID,\n" +
            "PMX_SOURCE_REFERENCE as PMX_SOURCE_REFERENCE,\n" +
            "MANIFESTATION_KEY_TITLE as MANIFESTATION_KEY_TITLE,\n" +
            "INTER_EDITION_FLAG as INTER_EDITION_FLAG,\n" +
            "FIRST_PUB_DATE as FIRST_PUB_DATE,\n" +
            "LAST_PUB_DATE as LAST_PUB_DATE, \n" +
            "F_TYPE as F_TYPE,\n" +
            "F_STATUS as F_STATUS, \n" +
            "F_FORMAT_TYPE as F_FORMAT_TYPE, \n" +
            "F_WWORK as F_WWORK\n" +
            "FROM semarchy_eph_mdm.gd_manifestation WHERE MANIFESTATION_ID IN ('%s')";

    public static final String SELECT_GD_MANIFESTATION_IDENTIFIER_BY_MANIFESTATION_ID = "select identifier as identifier, \n" +
            "f_type as f_type " +
            "from semarchy_eph_mdm.gd_manifestation_identifier \n" +
            "where identifier in (select identifier from semarchy_eph_mdm.gd_manifestation_identifier where f_manifestation in ('%s'));";

    public static String getWorkIdentifiersDataFromGD="SELECT \n" +
            " F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS WORK_ID -- WORK IDENTIFIER\n" +
            "  FROM ephsit.semarchy_eph_mdm.gd_work_identifier\n" +
            "  WHERE f_wwork='PARAM1'";

    public static String getWorkIdentifiersDataFromGDByID="SELECT \n" +
            " F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS WORK_ID -- WORK IDENTIFIER\n" +
            "  FROM ephsit.semarchy_eph_mdm.gd_work_identifier\n" +
            "  WHERE identifier='PARAM1'";
}
