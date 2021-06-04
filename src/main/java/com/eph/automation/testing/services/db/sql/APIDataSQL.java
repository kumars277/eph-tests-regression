package com.eph.automation.testing.services.db.sql;

public class APIDataSQL {
    public static  String SELECT_RANDOM_PRODUCT_IDS_FOR_SEARCH_BOOKS = "";
    public static String SELECT_RANDOM_PRODUCT_IDS_FOR_SEARCH = "SELECT \"product_id\" as PRODUCT_ID\n" +
            "FROM semarchy_eph_mdm.gd_product WHERE f_manifestation is not null\n" +
//            "WHERE exists\n" +
//            "    (SELECT * \n" +
//            "     from semarchy_eph_stg.st_out_notification\n" +
//            "     WHERE semarchy_eph_mdm.gd_product.product_id = semarchy_eph_stg.st_out_notification.notification_id and semarchy_eph_stg.st_out_notification.status='PROCESSED')\n" +
            "order by random() limit '%s'";

    public static String EXTRACT_DATA_SUBJECT_AREA_GD_BY_CODE = "select B_CLASSNAME as B_CLASSNAME \n" +
            ",SUBJECT_AREA_ID as SUBJECT_AREA_ID,CODE as SUBJECT_AREA_CODE,NAME as SUBJECT_AREA_NAME"+
            ",F_TYPE as SUBJECT_AREA_TYPE,F_PARENT_SUBJECT_AREA as F_PARENT_SUBJECT_AREA\n" +
            "from semarchy_eph_mdm.gd_subject_area where code='%s'";

    //updated by Nishant @ 16 Oct 2020 for EPHD-2041
    public static String GET_GD_FinnAttr_DATA ="SELECT \n" +
            "  sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,work_fin_attribs_id AS fin_attribs_id -- Title\n" +
            "  ,f_gl_company AS gl_company -- Subtitle\n" +
            "  ,f_gl_cost_resp_centre as cost_resp_centre\n" +
            "  ,f_gl_revenue_resp_centre as revenue_resp_centre\n" +
            "  ,effective_start_date as effective_start_date\n" +
            "  ,f_wwork as work_id\n" +
            "  FROM semarchy_eph_mdm.gd_work_financial_attribs sa\n"+
            "  where f_wwork='%s' and work_fin_attribs_id not in \n" +
            " (select work_fin_attribs_id where effective_end_date <current_date) ";

    public static String SELECT_RANDOM_WORK_IDS_FOR_SEARCH = "SELECT \"work_id\" as WORK_ID\n" +
            "FROM semarchy_eph_mdm.gd_wwork \n" +
            "WHERE exists (\n" +
            "SELECT * FROM semarchy_eph_mdm.gd_manifestation\n" +
            "WHERE semarchy_eph_mdm.gd_wwork.work_id = semarchy_eph_mdm.gd_manifestation.f_wwork "+
            "and LENGTH(semarchy_eph_mdm.gd_manifestation.manifestation_key_title)>20)\n" +
            "and LENGTH(work_title)>20\n" +
            "order by random() limit '%s'";

    public static String SELECT_RANDOM_JOURNAL_IDS_FOR_SEARCH="select work_id from semarchy_eph_mdm.gd_wwork " +
            "where f_type in('ABS','JBB','JNL','NWL')and f_status in('WLA') order by random() limit %S";
    public static String SELECT_RANDOM_EXTENDED_WORK_IDS="SELECT epr_id as WORK_ID FROM eph%s_extended_data_stitch.stch_work_ext_json order by random() limit %S";
    public static String SELECT_RANDOM_EXTENDED_MANIFESTATION_IDS="SELECT epr_id as WORK_ID FROM eph%s_extended_data_stitch.stch_manifestation_ext_json order by random() limit %S";
    //CREATED by Nishant @ 22 Apr 2020
    public static String SELECT_RANDOM_WORK_IDS_WITH_PRODUCT = "select f_wwork as WORK_ID from semarchy_eph_mdm.gd_product where f_wwork is not null order by random() limit %s";
    public static String SELECT_RANDOM_WORK_WITH_MANIFESTATION_WITH_PRODUCT=
            "select f_wwork as WORK_ID FROM semarchy_eph_mdm.gd_manifestation where manifestation_id in (\n" +
            "SELECT f_manifestation from semarchy_eph_mdm.gd_product p where f_manifestation is not null) \n" +
            " order by random() limit %s";

    public static String SELECT_RANDOM_PRODUCT_IDS_WITH_WORK = "select product_id as PRODUCT_ID from semarchy_eph_mdm.gd_product where f_wwork is not null order by random() limit 1";
    public static String SELECT_RANDOM_PRODUCT_IDS_WITH_PERSON = "select f_product as PRODUCT_ID from semarchy_eph_mdm.gd_product_person_role where f_product is not null order by random() limit 1";
    public static String SELECT_RANDOM_PRODUCT_IDS_WITH_IDENTIFIER = "select f_product as PRODUCT_ID from semarchy_eph_mdm.gd_product_identifier where f_product is not null order by random() limit 1";
    public static String SELECT_ACCOUNTABLE_PRODUCT_BY_ACCOUNTABLEID=
        "select accountable_product_id as ACCOUNTABLE_PRODUCT_ID, \n" +
            "gl_product_segment_code as GL_PRODUCT_SEGMENT_CODE,\n" +
            "gl_product_segment_name as GL_PRODUCT_SEGMENT_NAME,\n" +
            "f_gl_product_segment_parent as F_GL_PRODUCT_SEGMENT_PARENT\n" +
            "from semarchy_eph_mdm.gd_accountable_product where accountable_product_id='%s'";

    public static String SELECT_PRODUCTCOUNT_BY_WORKTYPE =
            "select count(p.product_id) from semarchy_eph_mdm.gd_product p,semarchy_eph_mdm.gd_manifestation m,semarchy_eph_mdm.gd_wwork w"+
            " where p.s_name like \'%%CELL%%\' and "+
            " p.f_manifestation = m.manifestation_id and "+
            " w.work_id = m.f_wwork and "+
            " w.f_type='%s'; ";

    public static String SELECT_PRODUCTCOUNT_BY_MANIFESTATIONTYPE =
            "select count(p.product_id) from semarchy_eph_mdm.gd_product p,semarchy_eph_mdm.gd_manifestation m "+
            "where p.s_name like '%%CELL%%' and m.f_type='%S' and m.manifestation_id = p.f_manifestation;";

    //created by Nishant @ 9 Dec 2019
    public static String SELECT_PRODUCTCOUNT_BY_PMC_WITHSEARCH =
            "select count(a.product_id) from "+
                    "(select p.product_id,p.name,w.f_pmc from semarchy_eph_mdm.gd_product p,semarchy_eph_mdm.gd_manifestation m,semarchy_eph_mdm.gd_wwork w "+
                    "where p.f_manifestation = m.manifestation_id and w.work_id = m.f_wwork "+
                    "union "+
                    "select p.product_id,p.name,w.f_pmc from semarchy_eph_mdm.gd_product p "+
                    "inner join semarchy_eph_mdm.gd_wwork w on "+
                    "p.f_wwork=w.work_id)a "+
                    "where upper(a.name) like '%%%s%%' and a.f_pmc='%s'";

    //created by Nishant @ 20 Dec 2019
    public static String SELECT_PRODUCTCOUNT_BY_PMG_WITHSEARCH =
            "select count(a.product_id) from                                                   "+
                    "(                                                                                 "+
                    "	select 	p.product_id,p.name,pmc.f_pmg                                          "+
                    "			from                                                                   "+
                    "				semarchy_eph_mdm.gd_product p,                                     "+
                    "				semarchy_eph_mdm.gd_manifestation m,                               "+
                    "				semarchy_eph_mdm.gd_wwork w,                                       "+
                    "				semarchy_eph_mdm.gd_x_lov_pmc pmc                                  "+
                    "			where                                                                  "+
                    "				p.f_manifestation = m.manifestation_id and                         "+
                    "				w.work_id = m.f_wwork and                                          "+
                    "				pmc.code=w.f_pmc                                                   "+
                    "	union                                                                          "+
                    "	select  p.product_id,p.name,pmc.f_pmg                                          "+
                    "			from                                                                   "+
                    "				semarchy_eph_mdm.gd_product p                                      "+
                    "				inner join semarchy_eph_mdm.gd_wwork w on p.f_wwork=w.work_id      "+
                    "				inner join semarchy_eph_mdm.gd_x_lov_pmc pmc on pmc.code=w.f_pmc   "+
                    ")a                                                                                "+
                    "where upper(a.name) like '%%%s%%' and a.f_pmg='%s';                           ";


    public static String SELECT_PRODUCTCOUNT_BY_PMG =
            "select count(a.product_id) from                                                   "+
                    "(                                                                                 "+
                    "	select 	p.product_id,p.name,pmc.f_pmg                                          "+
                    "			from                                                                   "+
                    "				semarchy_eph_mdm.gd_product p,                                     "+
                    "				semarchy_eph_mdm.gd_manifestation m,                               "+
                    "				semarchy_eph_mdm.gd_wwork w,                                       "+
                    "				semarchy_eph_mdm.gd_x_lov_pmc pmc                                  "+
                    "			where                                                                  "+
                    "				p.f_manifestation = m.manifestation_id and                         "+
                    "				w.work_id = m.f_wwork and                                          "+
                    "				pmc.code=w.f_pmc                                                   "+
                    "	union                                                                          "+
                    "	select  p.product_id,p.name,pmc.f_pmg                                          "+
                    "			from                                                                   "+
                    "				semarchy_eph_mdm.gd_product p                                      "+
                    "				inner join semarchy_eph_mdm.gd_wwork w on p.f_wwork=w.work_id      "+
                    "				inner join semarchy_eph_mdm.gd_x_lov_pmc pmc on pmc.code=w.f_pmc   "+
                    ")a                                                                                "+
                    "where a.f_pmg='%s';                           ";

    public static String SELECT_PRODUCTCOUNT_BY_SEGMENT_CODE =
            "select count(product_id) from semarchy_eph_mdm.gd_product where --f_status <>'NVP' and \n" +
                    "f_manifestation in \n" +
                    "(select manifestation_id from semarchy_eph_mdm.gd_manifestation gm \n" +
                    "inner join semarchy_eph_mdm.gd_wwork gw on gm.f_wwork =gw.work_id \n" +
                    "inner join semarchy_eph_mdm.gd_accountable_product gap on gw.f_accountable_product =gap.accountable_product_id \n" +
                    "where gap.gl_product_segment_code ='%s')\n" +
                    "or\n" +
                    "f_wwork in\n" +
                    "(select work_id from semarchy_eph_mdm.gd_wwork gw \n" +
                    "inner join semarchy_eph_mdm.gd_accountable_product gap \n" +
                    "on gw.f_accountable_product =gap.accountable_product_id \n" +
                    "where gap.gl_product_segment_code ='%s')";

    //by Nishant @ 29 Nov 2019
    public static String EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH = "SELECT \n" +
            "              product_id AS PRODUCT_ID -- Title\n" +
            "              ,b_credate as CREATED\n" +
            "              ,b_upddate as UPDATED\n" +
            "              ,name as PRODUCT_NAME\n" +
            "              ,separately_sale_indicator as SEPARATELY_SALEABLE_IND\n" +
            "              ,trial_allowed_indicator as TRIAL_ALLOWED_IND\n" +
            "              ,short_name as PRODUCT_SHORT_NAME\n" +
            "              ,launch_date as FIRST_PUB_DATE\n" +
            "              ,f_type AS F_TYPE\n" +
            "              ,f_status AS F_STATUS\n" +
            "              ,f_revenue_model AS F_REVENUE_MODEL\n" +
            "              ,f_wwork AS F_PRODUCT_WORK\n" +
            "              ,f_tax_code AS TAX_CODE\n" +
            "              ,f_manifestation AS F_PRODUCT_MANIFESTATION_TYP\n" +
            "              FROM semarchy_eph_mdm.gd_product \n" +
            "  WHERE product_id IN ('%s')";

    //updated by Nishant @ 18 May 2021
    public static String EPH_GD_WORK_EXTRACT_FOR_SEARCH = "SELECT \n" +
            "               work_id AS WORK_ID,\n" +
            "               external_reference AS EXTERNAL_REFERENCE,\n" +
            "               work_title AS WORK_TITLE,\n" +
            "               work_sub_title AS WORK_SUBTITLE,\n" +
            "               electro_rights_indicator AS ELECTRONIC_RIGHTS_IND,\n" +
            "               copyright_year as COPYRIGHT_YEAR,\n" +
            "               edition_number AS EDITION_NUMBER,\n" +
            "               f_type AS WORK_TYPE,\n" +
            "               f_status AS WORK_STATUS,\n" +
            "               f_accountable_product AS f_accountable_product,\n"+
            "               f_pmc AS PMC,\n" +
            "               f_oa_type AS OPEN_ACCESS_TYPE,\n" +
            "               f_imprint AS IMPRINT,\n" +
            "               f_society_ownership AS SOCIETY_OWNERSHIP,\n"+
            "               f_llanguage AS LANGUAGE_CODE,\n" +
            "               f_t_event_type AS F_T_EVENT_TYPE,\n" +
            "               f_subscription_type AS SUBSCRIPTION_TYPE,\n" +
            "               planned_launch_date AS PLANNED_LAUNCH_DATE,\n" +
            "               actual_launch_date AS ACTUAL_LAUNCH_DATE,\n" +
            "               planned_discontinue_date AS PLANNED_DISCONTINUE_DATE,\n" +
            "               actual_discontinue_date AS ACTUAL_DISCONTINUE_DATE,\n" +
            "               f_legal_ownership AS LEGAL_OWNERSHIP,\n"+
            "               volume_name AS BOOK_VOLUME_NAME\n" +
            "              FROM semarchy_eph_mdm.gd_wwork " +
            "  WHERE work_id IN ('%s')";
    //added by Nishant in Nov-Dec 2019
    public static String SELECT_PRODUCT_BY_ID= "SELECT \"product_id\" as PRODUCT_ID FROM semarchy_eph_mdm.gd_product WHERE product_id='%s'";
    public static String SELECT_WORKS_BY_PMC_CODE="SELECT \"work_id\" as WORK_ID FROM semarchy_eph_mdm.gd_wwork WHERE f_pmc in ('%s')";
    public static String SELECT_WORK_BY_ID_FOR_SEARCH = "SELECT \"work_id\" as WORK_ID FROM semarchy_eph_mdm.gd_wwork WHERE work_id='%s'";
    public static String SELECT_MANIFESTATIONS_IDS_BY_WORKS = "select \"manifestation_id\" as MANIFESTATION_ID from semarchy_eph_mdm.gd_manifestation where f_wwork in ('%s');";
    public static String SELECT_MANIFESTATION_IDS_BY_WORKID = "select \"manifestation_id\" as manifestation_id from semarchy_eph_mdm.gd_manifestation where \"f_wwork\" in ('%s')";
    public static String SELECT_COUNT_PRODUCTS_BY_MANIFESTATIONS = "select count (*) from semarchy_eph_mdm.gd_product where f_manifestation in ('%s')";
    public static String SELECT_COUNT_PRODUCTS_BY_WORK = "select count (*) from semarchy_eph_mdm.gd_product where f_wwork in ('%s')";
    public static String SELECT_PRODUCT_SEGMENT_CODE_FROM_WORK = "select gl_product_segment_code from semarchy_eph_mdm.gd_accountable_product where accountable_product_id in('%s');";
    public static String SELECT_PRODUCTCOUNT_BY_PRODUCTSTATUS = "select count(*) from semarchy_eph_mdm.gd_product where s_name like \'%%CELL%%\' and f_status='%s'";
    public static String SELECT_PRODUCTCOUNT_BY_PRODUCTTYPE = "select count(*) from semarchy_eph_mdm.gd_product where s_name like \'%%CELL%%\' and f_type='%s'";
    public static String SELECT_RANDOM_PACKAGE_IDS_FOR_SEARCH = "select product_id from semarchy_eph_mdm.gd_product where f_type ='PKG' and f_status='PAS' group by product_id order by random() limit '%s';";
    public static String SELECT_RANDOM_PRODUCT_FROM_PACKAGE = "select f_component from semarchy_eph_mdm.gd_product_rel_package where f_package_owner in ('%s') group by f_component order by random() limit '1';";
    public static String EPH_GD_PACKAGE_COUNT_BY_PRODUCT_EXTRACT = "select COUNT(*) from semarchy_eph_mdm.gd_product_rel_package where f_component in('%s');";
    public static String EPH_GD_PRODUCT_COUNT_BY_PACKAGE_EXTRACT = "select count (*) from semarchy_eph_mdm.gd_product_rel_package where f_package_owner in ('%s');";
    public static String SELECT_RANDOM_PRODUCT_PERSON_ROLES_FOR_SEARCH = "select f_person from semarchy_eph_mdm.gd_product_person_role group by f_person order by random() limit '%s';";
    public static String SELECT_RANDOM_WORK_PERSON_ROLES_FOR_SEARCH = "select f_person from semarchy_eph_mdm.gd_work_person_role group by f_person order by random() limit '%s'";
    public static String SELECT_COUNT_PERSONID_FOR_WORKS = "select count (*) from semarchy_eph_mdm.gd_work_person_role where f_person='%s'";
    public static String SELECT_COUNT_PERSONID_FOR_PRODUCTS =
            "select count(*) from (select distinct product_id from semarchy_eph_mdm.gd_product where f_manifestation in"
                    +"("
                    +"select m.manifestation_id from semarchy_eph_mdm.gd_manifestation m,semarchy_eph_mdm.gd_wwork w,semarchy_eph_mdm.gd_work_person_role wpr where "
                    +"m.f_wwork=w.work_id and w.work_id=wpr.f_wwork and wpr.f_person='%s'"
                    +")"
                    +"or f_wwork in "
                    +"("
                    +"select w.work_id from semarchy_eph_mdm.gd_wwork w, semarchy_eph_mdm.gd_work_person_role wpr where "
                    +"w.work_id=wpr.f_wwork and wpr.f_person='%s'"
                    +"))s";

    public static String EPH_GD_WORK_EXTRACT_AMOUNT_BYPMC ="SELECT COUNT (f_pmc) FROM semarchy_eph_mdm.gd_wwork where f_pmc='%s';";
    public static String EPH_GD_PMG_CODE_EXTRACT_BYPMC ="select f_pmg from semarchy_eph_mdm.gd_x_lov_pmc where code='%s'";
    public static String EPH_GD_PMG_CODE_WORKS_EXTRACT_BY_PMC ="SELECT \"work_id\" as WORK_ID FROM semarchy_eph_mdm.gd_wwork \n" +
                    "WHERE f_pmc in (select code from semarchy_eph_mdm.gd_x_lov_pmc where f_pmg in (\n" +
                    "select f_pmg from semarchy_eph_mdm.gd_x_lov_pmc where code='%s'));";
    public static String EPH_GD_PACKAGEID_EXTRACT_BY_PRODUCTID ="select f_package_owner from semarchy_eph_mdm.gd_product_rel_package where f_component='%s' limit 1";
    public static String EPH_GD_WORK_EXTRACT_AMOUNT_BYPMG ="select count (*) from semarchy_eph_mdm.gd_wwork where f_pmc in ( select code from semarchy_eph_mdm.gd_x_lov_pmc where f_pmg in (select code from semarchy_eph_mdm.gd_x_lov_pmg where code='%s'));";
    public static String EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_WORKSTATUS  ="select count(work_id) from semarchy_eph_mdm.gd_wwork where upper(work_title) like '%%%s%%' and f_status = '%s';";
    public static String SELECT_GD_WWORK_TYPE_STATUS="select f_type as WORK_TYPE,f_status as WORK_STATUS from semarchy_eph_mdm.gd_wwork where work_id='%s'";
    public static String EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_WORKTYPE  ="select count(work_id) from semarchy_eph_mdm.gd_wwork where upper(work_title) like '%%%s%%' and f_type='%s';";
    public static String EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_MANIFESTATIONTYPE  ="select count(*) from (select distinct w.work_id "+
                    "from semarchy_eph_mdm.gd_wwork w inner join semarchy_eph_mdm.gd_manifestation m "+
                    "on w.work_id=m.f_wwork where upper(w.work_title) like '%%%s%%' and m.f_type='%s') s;";

    public static String EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_PMCCODE="select COUNT(*) from semarchy_eph_mdm.gd_wwork where UPPER(work_title) like '%%%S%%' AND f_pmc='%s';";
    public static String EPH_GD_WORK_EXTRACT_BY_SEARCH_WITH_PMGCODE="select count(w.work_id) from semarchy_eph_mdm.gd_wwork w, semarchy_eph_mdm.gd_x_lov_pmc pmc " +
            "where upper(w.work_title) like '%%%s%%' and pmc.f_pmg='%s' and pmc.code = w.f_pmc;";

    public static String EPH_GD_WORK_EXTRACT_BY_ACCOUNTABLE_PRODUCT  ="select COUNT(work_id) "+
            "from semarchy_eph_mdm.gd_wwork w, semarchy_eph_mdm.gd_accountable_product ap \n" +
            "where w.f_accountable_product = ap.accountable_product_id and ap.gl_product_segment_code='%s'";

    public static String EPH_GD_WORK_EXTRACT_FOR_SEARCH_BY_MANIFESTATIONID = "SELECT \n" +
            "               work_id AS WORK_ID,\n" +
            "               work_title AS WORK_TITLE,\n" +
            "               work_sub_title AS WORK_SUBTITLE,\n" +
            "              -- volume AS VOLUME,\n" +
            "               electro_rights_indicator AS ELECTRONIC_RIGHTS_IND,\n" +
            "               f_pmc AS PMC,\n" +
            "               copyright_year as COPYRIGHT_YEAR,\n" +
            "               f_status AS WORK_STATUS,\n" +
            "               f_type AS WORK_TYPE,\n" +
            "               f_imprint AS IMPRINT,\n" +
            "               f_accountable_product AS f_accountable_product,\n"+
            "               edition_number AS EDITION_NUMBER,\n" +
            "               copyright_year AS COPYRIGHT_YEAR\n" +
            "              FROM semarchy_eph_mdm.gd_wwork " +
            "  WHERE work_id IN (select f_wwork from semarchy_eph_mdm.gd_manifestation where manifestation_id in ('%s'))";

    public static final String SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID = "select F_EVENT  as F_EVENT,\n" +
            "B_CLASSNAME as B_CLASSNAME,\n" +
            "MANIFESTATION_ID as MANIFESTATION_ID,\n" +
            "MANIFESTATION_KEY_TITLE as MANIFESTATION_KEY_TITLE,\n" +
            "INTER_EDITION_FLAG as internationalEditionInd,\n" +
            "FIRST_PUB_DATE as FIRST_PUB_DATE,\n" +
            "LAST_PUB_DATE as LAST_PUB_DATE, \n" +
            "F_TYPE as F_TYPE,\n" +
            "F_STATUS as F_STATUS, \n" +
            "F_FORMAT_TYPE as F_FORMAT_TYPE, \n" +
            "F_WWORK as F_WWORK\n" +
            "FROM semarchy_eph_mdm.gd_manifestation WHERE MANIFESTATION_ID ='%s'";

    public static final String SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_WORKID = "select F_EVENT  as F_EVENT,\n" +
            "B_CLASSNAME as B_CLASSNAME,\n" +
            "MANIFESTATION_ID as MANIFESTATION_ID,\n" +
            "MANIFESTATION_KEY_TITLE as MANIFESTATION_KEY_TITLE,\n" +
            "INTER_EDITION_FLAG as INTER_EDITION_FLAG,\n" +
            "FIRST_PUB_DATE as FIRST_PUB_DATE,\n" +
            "LAST_PUB_DATE as LAST_PUB_DATE, \n" +
            "F_TYPE as F_TYPE,\n" +
            "F_STATUS as F_STATUS, \n" +
            "F_FORMAT_TYPE as F_FORMAT_TYPE, \n" +
            "F_WWORK as F_WWORK\n" +
            "FROM semarchy_eph_mdm.gd_manifestation WHERE F_WWORK ='%s'";

    public static final String SELECT_GD_MANIFESTATION_IDENTIFIER_BY_MANIFESTATION_ID = "select identifier as identifier, \n" +
            "f_type as f_type " +
            "from semarchy_eph_mdm.gd_manifestation_identifier \n" +
            "where identifier in (select identifier from semarchy_eph_mdm.gd_manifestation_identifier where f_manifestation in ('%s'));";

    public static final String SELECT_GD_MANIFESTATION_IDENTIFIER_BY_ID = "select identifier as identifier, f_type as f_type, effective_start_date as effective_start_date " +
            "from semarchy_eph_mdm.gd_manifestation_identifier where identifier in ('%s');";

    public static String getWorkIdentifiersDataFromGD="SELECT \n" +
            " F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS WORK_ID -- WORK IDENTIFIER\n" +
            "  FROM semarchy_eph_mdm.gd_work_identifier\n" +
            "  WHERE f_wwork='PARAM1'";

    public static String getProductIdentifiersDataFromGD="SELECT \n" +
            " F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE \n" +
            " ,F_PRODUCT AS PRODUCT_ID \n" +
            "  FROM semarchy_eph_mdm.gd_product_identifier\n" +
            "  WHERE f_product='PARAM1'";
    public static String getWorkLegelOwnersByWorkId="select " +
            "f_legal_owner, f_ownership_description ,effective_start_date ,effective_end_date " +
            "from semarchy_eph_mdm.gd_work_legal_owner gwlo where f_wwork ='PARAM1'";
    public static String getWorkAccessModelsById = "select f_access_model from semarchy_eph_mdm.gd_work_access_model gwam where f_wwork ='PARAM1'";
    public static String getWorkBusinessModelById = "select f_business_model, effective_start_date , effective_end_date from semarchy_eph_mdm.gd_work_business_model gwam where f_wwork ='PARAM1'";
    public static String getWorkSubjectAreaByWorkId = "select code,name,f_type,f_parent_subject_area " +
            "from semarchy_eph_mdm.gd_subject_area gsa where subject_area_id in\n" +
            "(select f_subject_area from semarchy_eph_mdm.gd_work_subject_area_link gwsal where f_wwork ='PARAM1');";
    //updated by Nishant @ 15 Apr 2020
    public static String getWorkIdentifiersDataFromGDByIdentifier="SELECT \n" +
            " effective_start_date as IDENTIFIER_EFFECTIVE_START_DATE\n" +
            " ,effective_end_date as IDENTIFIER_EFFECTIVE_END_DATE\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS WORK_ID -- WORK IDENTIFIER\n" +
            "  FROM semarchy_eph_mdm.gd_work_identifier\n" +
            "  WHERE identifier='PARAM1'";

    //created by Nishant @ 22 Apr 2020
    public static String SelectProductByManifestationId="SELECT \n" +
            "              p.product_id AS PRODUCT_ID -- Title\n" +
            "              ,p.name as PRODUCT_NAME\n" +
            "              ,p.separately_sale_indicator as SEPARATELY_SALEABLE_IND\n" +
            "              ,p.trial_allowed_indicator as TRIAL_ALLOWED_IND\n" +
            "              ,p.short_name as PRODUCT_SHORT_NAME\n" +
            "              ,p.launch_date as FIRST_PUB_DATE\n" +
            "              ,p.f_type AS F_TYPE\n" +
            "              ,p.f_status AS F_STATUS\n" +
            "              ,p.f_revenue_model AS F_REVENUE_MODEL\n" +
            "              ,p.f_wwork AS F_PRODUCT_WORK\n" +
            "              ,p.f_tax_code AS TAX_CODE\n" +
            "              ,p.f_manifestation AS F_PRODUCT_MANIFESTATION_TYP\n" +
            " from semarchy_eph_mdm.gd_product p inner join semarchy_eph_mdm.gd_manifestation m\n" +
                    "on m.manifestation_id = p.f_manifestation where m.manifestation_id='%s'";

    public static String SelectProductByWorkId="SELECT \n" +
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
            "              ,f_tax_code AS TAX_CODE\n" +
            "              ,f_manifestation AS F_PRODUCT_MANIFESTATION_TYP\n" +
            " from semarchy_eph_mdm.gd_product where f_wwork ='%s'";

    //created by Nishant @ 24 Apr 2020
    public static String selectWorkPersonByworkId ="Select " +
            "work_person_role_id as WORK_PERSON_ROLE_ID,\n" +
            "b_classname as B_CLASSNAME,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE,\n" +
            "f_role as F_ROLE,\n" +
            "f_wwork as F_WWORK,\n" +
            "f_person as F_PERSON,\n" +
            "f_event as F_EVENT" +
            " from semarchy_eph_mdm.gd_work_person_role where f_wwork='%s'";


    public static String SelectPersonDataByPersonId = "select " +
            "person_id as PERSON_ID,\n" +
            "b_classname as B_CLASSNAME,\n" +
            "b_upddate as UPDATED,\n" +
            "given_name\tas PERSON_FIRST_NAME,\n" +
            "family_name\tas PERSON_FAMILY_NAME,\n" +
            "peoplehub_id as PEOPLEHUB_ID\n" +
            " from semarchy_eph_mdm.gd_person where person_id in('%s')";

    public static String SelectLovLanguageDescription = "select l_description from semarchy_eph_mdm.gd_x_lov_language gxll where code = '%s'";
    public static String SelectLovSubscriptionDescription = "select l_description from semarchy_eph_mdm.gd_x_lov_subscription_type where code = '%s'";
    public static String SelectLovsocietyOwnershipValue = "select l_description,roll_up_ownership from semarchy_eph_mdm.gd_x_lov_society_ownership where code = '%s'";
    public static String SelectLovLegalOwnershipValue = "select l_description,roll_up_ownership from semarchy_eph_mdm.gd_x_lov_legal_ownership where code = '%s'";

    public static String SelectLovWorkIdentifierValue = "select l_description from semarchy_eph_mdm.gd_x_lov_identifier_type where code = '%s'";

    public static String SelectLovWorkOwnershipDescriptionValue = "select l_description from semarchy_eph_mdm.gd_x_lov_owner_description where code = '%s'";

    public static String SelectLovWorkLegalOwnerValue = "select name,f_legal_ownership from semarchy_eph_mdm.gd_legal_owner where legal_owner_id = '%s'";

    public static String SelectLovWorkLegalOwnershipValue = "select code,l_description,roll_up_ownership from semarchy_eph_mdm.gd_x_lov_legal_ownership where code = '%s'";
}
