package com.eph.automation.testing.services.db.gdSQLDLSQL;


import com.eph.automation.testing.services.db.JMDataLakeSQL.GetJMDLDBUser;

import static com.eph.automation.testing.services.db.JMDataLakeSQL.GetJMDLDBUser.getJMDataBase;

public class gdTableDLSQL {
    static  String[] databaseEnv = getJMDataBase();
    public static String GET_ACC_PROD_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_accountable_product";
    public static String GET_ACC_PROD_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_accountable_product";
    public static String GET_EVENT_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_event";
    public static String GET_EVENT_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_event";
    public static String GET_LEGAL_OWNER_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_legal_owner";
    public static String GET_LEGAL_OWNER_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_legal_owner";
    public static String GET_MANIF_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_manifestation";
    public static String GET_MANIF_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation";

    public static String GET_MANIF_ID_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_manifestation_identifier";
    public static String GET_MANIF_ID_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation_identifier";
    public static String GET_PERSON_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_person";
    public static String GET_PERSON_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_person";
    public static String GET_PRICE_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_price";
    public static String GET_PRICE_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_price";
    public static String GET_PROD_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product";
    public static String GET_PROD_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_product";
    public static String GET_PROD_FIN_ATTR_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product_financial_attribs";
    public static String GET_PROD_FIN_ATTR_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_product_financial_attribs";
     public static String GET_PROD_IDENTIF_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product_identifier";
    public static String GET_PROD_IDENTIF_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_product_identifier";
    public static String GET_PROD_PERSON_ROLE_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product_person_role";
    public static String GET_PROD_PERSON_ROLE_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_product_person_role";
     public static String GET_PROD_REL_PKG_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product_rel_package";
    public static String GET_PROD_REL_PKG_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_product_rel_package";
    public static String GET_SUB_AREA_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_subject_area";
    public static String GET_SUB_AREA_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_subject_area";
    public static String GET_WORK_ACCESS_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_access_model";
    public static String GET_WORK_ACCESS_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_access_model";
    public static String GET_WORK_BUSINESS_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_business_model";
    public static String GET_WORK_BUSINESS_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_business_model";
    public static String GET_WORK_FIN_ATTR_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_financial_attribs";
    public static String GET_WORK_FIN_ATTR_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_financial_attribs";

    public static String GET_WORK_HERCHY_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_hierarchy";
    public static String GET_WORK_HERCHY_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_hierarchy";
    public static String GET_WORK_IDENTF_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_identifier";
    public static String GET_WORK_IDENTF_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier";
    public static String GET_WORK_LEGAL_OWN_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_legal_owner";
    public static String GET_WORK_LEGAL_OWN_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_legal_owner";
    public static String GET_WORK_METRIC_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_metric";
    public static String GET_WORK_METRIC_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_metric";
    public static String GET_WORK_PERS_ROLE_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_person_role";
    public static String GET_WORK_PERS_ROLE_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_person_role";

    public static String GET_WORK_REL_PKG_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_rel_package";
    public static String GET_WORK_REL_PKG_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_rel_package";

    public static String GET_WORK_REL_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_relationship";
    public static String GET_WORK_REL_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_relationship";


    public static String GET_WORK_SUBJ_AREA_LINK_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_subject_area_link";
    public static String GET_WORK_SUBJ_AREA_LINK_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_subject_area_link";

    public static String GET_WORK_WORK_HCHY_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_work_hchy_link";
    public static String GET_WORK_WORK_HCHY_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_work_work_hchy_link";

    public static String GET_WORK_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_wwork";
    public static String GET_WORK_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_wwork";

    public static String GET_GD_ACC_PROD_IDS ="select external_reference from semarchy_eph_mdm.gd_accountable_product order by random() limit %s";
    public static String GET_GD_EVENT_IDS ="select event_id from semarchy_eph_mdm.gd_event order by random() limit %s";
    public static String GET_GD_LEGAL_OWNER_IDS ="select legal_owner_id from semarchy_eph_mdm.gd_legal_owner order by random() limit %s";
    public static String GET_GD_MANIF_IDS ="select manifestation_id from semarchy_eph_mdm.gd_manifestation order by random() limit %s";
    public static String GET_GD_MANIF_IDENTIF_IDS ="select manif_identifier_id from semarchy_eph_mdm.gd_manifestation_identifier order by random() limit %s";
    public static String GET_GD_PERSON_IDS ="select person_id from semarchy_eph_mdm.gd_person order by random() limit %s";
    public static String GET_GD_PRODUCT_IDS ="select product_id from semarchy_eph_mdm.gd_product order by random() limit %s";
    public static String GET_GD_PRODUCT_FIN_IDS ="select product_fin_attribs_id from semarchy_eph_mdm.gd_product_financial_attribs order by random() limit %s";
    public static String GET_GD_PRODUCT_IDENTIF_IDS ="select product_identifier_id from semarchy_eph_mdm.gd_product_identifier order by random() limit %s";
    public static String GET_GD_PRODUCT_PERSON_ROLE_IDS ="select product_person_role_id from semarchy_eph_mdm.gd_product_person_role order by random() limit %s";
    public static String GET_GD_PRODUCT_REL_PKG_IDS ="select product_rel_pack_id from semarchy_eph_mdm.gd_product_rel_package order by random() limit %s";
    public static String GET_GD_SUBJ_AREA_IDS ="select subject_area_id from semarchy_eph_mdm.gd_subject_area order by random() limit %s";
    public static String GET_GD_WORK_ACCESS_MODEL_IDS ="select work_access_model_id from semarchy_eph_mdm.gd_work_access_model order by random() limit %s";
    public static String GET_GD_WORK_BUSINESS_MODEL_IDS ="select work_business_model_id from semarchy_eph_mdm.gd_work_business_model order by random() limit %s";
    public static String GET_GD_WORK_FIN_ATTR_IDS ="select work_fin_attribs_id from semarchy_eph_mdm.gd_work_financial_attribs order by random() limit %s";
    public static String GET_GD_WORK_IDENTIFIER_IDS ="select work_identifier_id from semarchy_eph_mdm.gd_work_identifier order by random() limit %s";
    public static String GET_GD_WORK_LEGAL_OWNER_IDS ="select work_legal_owner_id from semarchy_eph_mdm.gd_work_legal_owner order by random() limit %s";
    public static String GET_GD_WORK_PERSON_ROLE_IDS ="select work_person_role_id from semarchy_eph_mdm.gd_work_person_role order by random() limit %s";
    public static String GET_GD_WORK_REL_PKG_IDS ="select work_rel_pack_id from semarchy_eph_mdm.gd_work_rel_package order by random() limit %s";
    public static String GET_GD_WORK_RELATIONS_IDS ="select work_relationship_id from semarchy_eph_mdm.gd_work_relationship order by random() limit %s";
    public static String GET_GD_WORK_SUBJ_AREA_LINK_IDS ="select work_subject_area_link_id from semarchy_eph_mdm.gd_work_subject_area_link order by random() limit %s";
    public static String GET_GD_WORK_HCHY_LINK_IDS ="select wrk_wrk_hchy_link_id from semarchy_eph_mdm.gd_work_work_hchy_link order by random() limit %s";
    public static String GET_GD_WORK_IDS ="select work_id from semarchy_eph_mdm.gd_wwork order by random() limit %s";
    public static String GET_GD_WORK_HIER_IDS ="select work_hierarchy_id from semarchy_eph_mdm.gd_work_hierarchy order by random() limit %s";

    public static String GET_GD_ACC_PROD_IDS_DL ="select external_reference from "+GetJMDLDBUser.getProdDataBase()+".gd_accountable_product order by random() limit %s";
    public static String GET_GD_EVENT_IDS_DL ="select event_id from "+GetJMDLDBUser.getProdDataBase()+".gd_event order by random() limit %s";
    public static String GET_GD_LEGAL_OWNER_IDS_DL ="select legal_owner_id from "+GetJMDLDBUser.getProdDataBase()+".gd_legal_owner order by random() limit %s";
    public static String GET_GD_MANIF_IDS_DL ="select manifestation_id from "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation order by random() limit %s";
    public static String GET_GD_MANIF_IDENTIF_IDS_DL ="select manif_identifier_id from "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation_identifier order by random() limit %s";
    public static String GET_GD_PERSON_IDS_DL ="select person_id from "+GetJMDLDBUser.getProdDataBase()+".gd_person order by random() limit %s";
    public static String GET_GD_PRODUCT_IDS_DL ="select product_id from "+GetJMDLDBUser.getProdDataBase()+".gd_product order by random() limit %s";
    public static String GET_GD_PRODUCT_FIN_IDS_DL ="select product_fin_attribs_id from "+GetJMDLDBUser.getProdDataBase()+".gd_product_financial_attribs order by random() limit %s";
    public static String GET_GD_PRODUCT_IDENTIF_IDS_DL ="select product_identifier_id from "+GetJMDLDBUser.getProdDataBase()+".gd_product_identifier order by random() limit %s";
    public static String GET_GD_PRODUCT_PERSON_ROLE_IDS_DL ="select product_person_role_id from "+GetJMDLDBUser.getProdDataBase()+".gd_product_person_role order by random() limit %s";
    public static String GET_GD_PRODUCT_REL_PKG_IDS_DL ="select product_rel_pack_id from "+GetJMDLDBUser.getProdDataBase()+".gd_product_rel_package order by random() limit %s";
    public static String GET_GD_SUBJ_AREA_IDS_DL ="select subject_area_id from "+GetJMDLDBUser.getProdDataBase()+".gd_subject_area order by random() limit %s";
    public static String GET_GD_WORK_ACCESS_MODEL_IDS_DL ="select work_access_model_id from "+GetJMDLDBUser.getProdDataBase()+".gd_work_access_model order by random() limit %s";
    public static String GET_GD_WORK_BUSINESS_MODEL_IDS_DL ="select work_business_model_id from "+GetJMDLDBUser.getProdDataBase()+".gd_work_business_model order by random() limit %s";
    public static String GET_GD_WORK_FIN_ATTR_IDS_DL ="select work_fin_attribs_id from "+GetJMDLDBUser.getProdDataBase()+".gd_work_financial_attribs order by random() limit %s";
    public static String GET_GD_WORK_IDENTIFIER_IDS_DL ="select work_identifier_id from "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier order by random() limit %s";
    public static String GET_GD_WORK_LEGAL_OWNER_IDS_DL ="select work_legal_owner_id from "+GetJMDLDBUser.getProdDataBase()+".gd_work_legal_owner order by random() limit %s";
    public static String GET_GD_WORK_PERSON_ROLE_IDS_DL ="select work_person_role_id from "+GetJMDLDBUser.getProdDataBase()+".gd_work_person_role order by random() limit %s";
    public static String GET_GD_WORK_REL_PKG_IDS_DL ="select work_rel_pack_id from "+GetJMDLDBUser.getProdDataBase()+".gd_work_rel_package order by random() limit %s";
    public static String GET_GD_WORK_RELATIONS_IDS_DL ="select work_relationship_id from "+GetJMDLDBUser.getProdDataBase()+".gd_work_relationship order by random() limit %s";
    public static String GET_GD_WORK_SUBJ_AREA_LINK_IDS_DL ="select work_subject_area_link_id from "+GetJMDLDBUser.getProdDataBase()+".gd_work_subject_area_link order by random() limit %s";
    public static String GET_GD_WORK_HCHY_LINK_IDS_DL ="select wrk_wrk_hchy_link_id from "+GetJMDLDBUser.getProdDataBase()+".gd_work_work_hchy_link order by random() limit %s";
    public static String GET_GD_WORK_IDS_DL ="select work_id from "+GetJMDLDBUser.getProdDataBase()+".gd_wwork order by random() limit %s";
    public static String GET_GD_WORK_HIER_IDS_DL ="select work_hierarchy_id from "+GetJMDLDBUser.getProdDataBase()+".gd_work_hierarchy order by random() limit %s";


    public static String GET_GD_ACCOUNTABLE_PRODUCT = "select * from semarchy_eph_mdm.gd_accountable_product where external_reference in ('%s') order by" +
            " external_reference,b_batchid desc";
    public static String GET_GD_EVENT = "select * from semarchy_eph_mdm.gd_event where event_id in ('%s') order by event_id,b_batchid,b_credate,b_upddate desc";
    public static String GET_GD_LEGAL_OWNER = "select * from semarchy_eph_mdm.gd_legal_owner where legal_owner_id in ('%s') order by legal_owner_id,b_batchid desc";
    public static String GET_GD_MANIFESTATION = "select * from semarchy_eph_mdm.gd_manifestation where manifestation_id in ('%s') order by manifestation_id desc";
    public static String GET_GD_MANIFESTATION_IDENTIFIER = "select * from semarchy_eph_mdm.gd_manifestation_identifier where manif_identifier_id in ('%s') order by manif_identifier_id desc";
    public static String GET_GD_PERSON = "select * from semarchy_eph_mdm.gd_person where person_id in ('%s') order by person_id desc";
    public static String GET_GD_PRODUCT = "select * from semarchy_eph_mdm.gd_product where product_id in ('%s') order by product_id desc";
    public static String GET_GD_PROD_FIN_ATTR = "select * from semarchy_eph_mdm.gd_product_financial_attribs where product_fin_attribs_id in ('%s') order by product_fin_attribs_id desc";
    public static String GET_GD_PROD_IDENTIFIER = "select * from semarchy_eph_mdm.gd_product_identifier where product_identifier_id in ('%s') order by product_identifier_id desc";
    public static String GET_GD_PROD_PERSON_ROLE = "select * from semarchy_eph_mdm.gd_product_person_role where product_person_role_id in ('%s') order by product_person_role_id desc";
    public static String GET_GD_PROD_REL_PKG = "select * from semarchy_eph_mdm.gd_product_rel_package where product_rel_pack_id in ('%s') order by product_rel_pack_id desc";
    public static String GET_GD_SUBJECT_AREA = "select * from semarchy_eph_mdm.gd_subject_area where subject_area_id in ('%s') order by subject_area_id desc";
    public static String GET_GD_WORK_ACCESS_MODEL = "select * from semarchy_eph_mdm.gd_work_access_model where work_access_model_id in ('%s') order by work_access_model_id desc";
    public static String GET_GD_WORK_BUSINESS_MODEL = "select * from semarchy_eph_mdm.gd_work_business_model where work_business_model_id in ('%s') order by work_business_model_id desc";
    public static String GET_GD_WORK_FIN_ATTR = "select * from semarchy_eph_mdm.gd_work_financial_attribs where work_fin_attribs_id in ('%s') order by work_fin_attribs_id desc";
    public static String GET_GD_WORK_HIRERACHY = "select * from semarchy_eph_mdm.gd_work_hierarchy where work_hierarchy_id in ('%s') order by work_hierarchy_id desc";
    public static String GET_GD_WORK_IDENTIFIER = "select * from semarchy_eph_mdm.gd_work_identifier where work_identifier_id in ('%s') order by work_identifier_id desc";
    public static String GET_GD_WORK_LEGAL_OWNER = "select * from semarchy_eph_mdm.gd_work_legal_owner where work_legal_owner_id in ('%s') order by work_legal_owner_id desc";
    public static String GET_GD_WORK_PERSON_ROLE = "select * from semarchy_eph_mdm.gd_work_person_role where work_person_role_id in ('%s') order by work_person_role_id desc";
    public static String GET_GD_WORK_REL_PKG = "select * from semarchy_eph_mdm.gd_work_rel_package where work_rel_pack_id in ('%s') order by work_rel_pack_id desc";
    public static String GET_GD_WORK_RELATIONSHIP = "select * from semarchy_eph_mdm.gd_work_relationship where work_relationship_id in ('%s') order by work_relationship_id desc";
    public static String GET_GD_WORK_SUB_AREA_LINK = "select * from semarchy_eph_mdm.gd_work_subject_area_link where work_subject_area_link_id in ('%s') order by work_subject_area_link_id desc";
    public static String GET_GD_WORK_HCHU_LINK = "select * from semarchy_eph_mdm.gd_work_work_hchy_link where wrk_wrk_hchy_link_id in ('%s') order by wrk_wrk_hchy_link_id desc";
    public static String GET_GD_WWORK = "select * from semarchy_eph_mdm.gd_wwork where work_id in ('%s') order by work_id desc";

    public static String GET_GD_ACCOUNTABLE_PRODUCT_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_accountable_product where external_reference in ('%s')" +
            " order by external_reference,b_batchid desc";
    public static String GET_GD_EVENT_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_event where event_id in (%s) " +
            "order by event_id,b_batchid,b_credate,b_upddate desc";
    public static String GET_GD_LEGAL_OWNER_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_legal_owner where legal_owner_id in (%s) order by legal_owner_id,b_batchid desc";
    public static String GET_GD_MANIFESTATION_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation where manifestation_id in ('%s') order by manifestation_id desc";
    public static String GET_GD_MANIFESTATION_IDENTIFIER_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation_identifier where manif_identifier_id in (%s) order by manif_identifier_id desc";
    public static String GET_GD_PERSON_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_person where person_id in (%s) order by person_id desc";
    public static String GET_GD_PRODUCT_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_product where product_id in ('%s') order by product_id desc";
    public static String GET_GD_PROD_FIN_ATTR_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_product_financial_attribs where product_fin_attribs_id in (%s) order by product_fin_attribs_id desc";
    public static String GET_GD_PROD_IDENTIFIER_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_product_identifier where product_identifier_id in (%s) order by product_identifier_id desc";
    public static String GET_GD_PROD_PERSON_ROLE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_product_person_role where product_person_role_id in (%s) order by product_person_role_id desc";
    public static String GET_GD_PROD_REL_PKG_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_product_rel_package where product_rel_pack_id in (%s) order by product_rel_pack_id desc";
    public static String GET_GD_SUBJECT_AREA_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_subject_area where subject_area_id in (%s) order by subject_area_id desc";
    public static String GET_GD_WORK_ACCESS_MODEL_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_work_access_model where work_access_model_id in (%s) order by work_access_model_id desc";
    public static String GET_GD_WORK_BUSINESS_MODEL_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_work_business_model where work_business_model_id in (%s) order by work_business_model_id desc";
    public static String GET_GD_WORK_FIN_ATTR_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_work_financial_attribs where work_fin_attribs_id in (%s) order by work_fin_attribs_id desc";
    public static String GET_GD_WORK_HIRERACHY_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_work_hierarchy where work_hierarchy_id in (%s) order by work_hierarchy_id desc";
    public static String GET_GD_WORK_IDENTIFIER_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier where work_identifier_id in (%s) order by work_identifier_id desc";
    public static String GET_GD_WORK_LEGAL_OWNER_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_work_legal_owner where work_legal_owner_id in (%s) order by work_legal_owner_id desc";
    public static String GET_GD_WORK_PERSON_ROLE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_work_person_role where work_person_role_id in (%s) order by work_person_role_id desc";
    public static String GET_GD_WORK_REL_PKG_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_work_rel_package where work_rel_pack_id in (%s) order by work_rel_pack_id desc";
    public static String GET_GD_WORK_RELATIONSHIP_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_work_relationship where work_relationship_id in (%s) order by work_relationship_id desc";
    public static String GET_GD_WORK_SUB_AREA_LINK_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_work_subject_area_link where work_subject_area_link_id in (%s) order by work_subject_area_link_id desc";
    public static String GET_GD_WORK_HCHU_LINK_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_work_work_hchy_link where wrk_wrk_hchy_link_id in (%s) order by wrk_wrk_hchy_link_id desc";
    public static String GET_GD_WWORK_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_wwork where work_id in ('%s') order by work_id desc";

    public static String GET_GD_LOV_ACCESS_MODEL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_access_model";
    public static String GET_GD_LOV_ACCESS_MODEL_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_access_model";
    public static String GET_GD_LOV_ACCESS_IDS = "select code from semarchy_eph_mdm.gd_x_lov_access_model order by random() limit %s";
    public static String GET_GD_GD_LOV_ACCESS_MODEL_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_access_model where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_ACCESS_MODEL = "select * from semarchy_eph_mdm.gd_x_lov_access_model where code in ('%s') order by code desc";

    public static String GET_GD_LOV_BUSINESS_MODEL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_business_model";
    public static String GET_GD_LOV_BUSINESS_MODEL_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_business_model";
    public static String GET_GD_LOV_BUSINESS_IDS = "select code from semarchy_eph_mdm.gd_x_lov_business_model order by random() limit %s";
    public static String GET_GD_GD_LOV_BUSINESS_MODEL_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_business_model where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_BUSINESS_MODEL = "select * from semarchy_eph_mdm.gd_x_lov_business_model where code in ('%s') order by code desc";

    public static String GET_GD_LOV_CURRENCY_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_currency";
    public static String GET_GD_LOV_CURRENCY_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_currency";
    public static String GET_GD_LOV_CURRENCY_IDS = "select code from semarchy_eph_mdm.gd_x_lov_currency order by random() limit %s";
    public static String GET_GD_GD_LOV_CURRENCY_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_currency where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_CURRENCY = "select * from semarchy_eph_mdm.gd_x_lov_currency where code in ('%s') order by code desc";

    public static String GET_GD_LOV_ETAX_PROD_CODE_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_etax_product_code";
    public static String GET_GD_LOV_ETAX_PROD_CODE_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_etax_product_code";
    public static String GET_GD_LOV_ETAX_IDS = "select code from semarchy_eph_mdm.gd_x_lov_etax_product_code order by random() limit %s";
    public static String GET_GD_GD_LOV_ETAX_PROD_CODE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_etax_product_code where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_ETAX_PROD_CODE = "select * from semarchy_eph_mdm.gd_x_lov_etax_product_code where code in ('%s') order by code desc";

    public static String GET_GD_LOV_EVENT_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_event_type";
    public static String GET_GD_LOV_EVENT_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_event_type";
    public static String GET_GD_LOV_EVENT_IDS = "select code from semarchy_eph_mdm.gd_x_lov_event_type order by random() limit %s";
    public static String GET_GD_GD_LOV_EVENT_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_event_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_EVENT = "select * from semarchy_eph_mdm.gd_x_lov_event_type where code in ('%s') order by code desc";


    public static String GET_GD_LOV_GL_COMPANY_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_gl_company";
    public static String GET_GD_LOV_GL_COMPANY_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_gl_company";
    public static String GET_GD_LOV_GL_COMPANY_IDS = "select code from semarchy_eph_mdm.gd_x_lov_gl_company order by random() limit %s";
    public static String GET_GD_GD_LOV_GL_COMPANY_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_gl_company where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_GL_COMPANY = "select * from semarchy_eph_mdm.gd_x_lov_gl_company where code in ('%s') order by code desc";


    public static String GET_GD_LOV_GL_PROD_SEG_PARENT_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_gl_prod_seg_parent";
    public static String GET_GD_LOV_GL_PROD_SEG_PARENT_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_gl_prod_seg_parent";
    public static String GET_GD_LOV_GL_PROD_SEG_PARENT_IDS = "select code from semarchy_eph_mdm.gd_x_lov_gl_prod_seg_parent order by random() limit %s";
    public static String GET_GD_GD_LOV_GL_PROD_SEG_PARENT_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_gl_prod_seg_parent where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_GL_PROD_SEG_PARENT = "select * from semarchy_eph_mdm.gd_x_lov_gl_prod_seg_parent where code in ('%s') order by code desc";


    public static String GET_GD_LOV_GL_RESP_CENTER_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_gl_resp_centre";
    public static String GET_GD_LOV_GL_RESP_CENTER_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_gl_resp_centre";
    public static String GET_GD_LOV_GL_RESP_CENTER_IDS = "select code from semarchy_eph_mdm.gd_x_lov_gl_resp_centre order by random() limit %s";
    public static String GET_GD_GD_LOV_GL_RESP_CENTER_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_gl_resp_centre where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_GL_RESP_CENTER = "select * from semarchy_eph_mdm.gd_x_lov_gl_resp_centre where code in ('%s') order by code desc";

    public static String GET_GD_LOV_IDENTIFIER_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_identifier_type";
    public static String GET_GD_LOV_IDENTIFIER_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_identifier_type";
    public static String GET_GD_LOV_IDENTIFIER_IDS = "select code from semarchy_eph_mdm.gd_x_lov_identifier_type order by random() limit %s";
    public static String GET_GD_GD_LOV_IDENTIFIER_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_identifier_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_IDENTIFIER = "select * from semarchy_eph_mdm.gd_x_lov_identifier_type where code in ('%s') order by code desc";

    public static String GET_GD_LOV_IMPRINT_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_imprint";
    public static String GET_GD_LOV_IMPRINT_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_imprint";
    public static String GET_GD_LOV_IMPRINT_IDS = "select code from semarchy_eph_mdm.gd_x_lov_imprint order by random() limit %s";
    public static String GET_GD_GD_LOV_IMPRINT_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_imprint where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_IMPRINT = "select * from semarchy_eph_mdm.gd_x_lov_imprint where code in ('%s') order by code desc";

    public static String GET_GD_LOV_LANGUAGE_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_language";
    public static String GET_GD_LOV_LANGUAGE_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_language";
    public static String GET_GD_LOV_LANGUAGE_IDS = "select code from semarchy_eph_mdm.gd_x_lov_language order by random() limit %s";
    public static String GET_GD_GD_LOV_LANGUAGE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_language where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_LANGUAGE = "select * from semarchy_eph_mdm.gd_x_lov_language where code in ('%s') order by code desc";


    public static String GET_GD_LOV_LEGAL_OWNERSHIP_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_legal_ownership";
    public static String GET_GD_LOV_LEGAL_OWNERSHIP_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_legal_ownership";
    public static String GET_GD_LOV_LEGALOWNER_IDS = "select code from semarchy_eph_mdm.gd_x_lov_legal_ownership order by random() limit %s";
    public static String GET_GD_GD_LOV_LEGAL_OWNERSHIP_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_legal_ownership where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_LEGAL_OWNERSHIP = "select * from semarchy_eph_mdm.gd_x_lov_legal_ownership where code in ('%s') order by code desc";

    public static String GET_GD_LOV_MANIF_STATUS_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_manif_status";
    public static String GET_GD_LOV_MANIF_STATUS_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_manif_status";
    public static String GET_GD_LOV_MANIF_STATUS_IDS = "select code from semarchy_eph_mdm.gd_x_lov_manif_status order by random() limit %s";
    public static String GET_GD_GD_LOV_MANIF_STATUS_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_manif_status where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_MANIF_STATUS = "select * from semarchy_eph_mdm.gd_x_lov_manif_status where code in ('%s') order by code desc";

    public static String GET_GD_LOV_MANIF_TYPE_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_manif_type";
    public static String GET_GD_LOV_MANIF_TYPE_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_manif_type";
    public static String GET_GD_LOV_MANIF_TYPE_IDS = "select code from semarchy_eph_mdm.gd_x_lov_manif_type order by random() limit %s";
    public static String GET_GD_GD_LOV_MANIF_TYPE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_manif_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_MANIF_TYPE = "select * from semarchy_eph_mdm.gd_x_lov_manif_type where code in ('%s') order by code desc";

    public static String GET_GD_LOV_METRIC_TYPE_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_metric_type";
    public static String GET_GD_LOV_METRIC_TYPE_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_metric_type";
    public static String GET_GD_LOV_METRIC_IDS = "select code from semarchy_eph_mdm.gd_x_lov_metric_type order by random() limit %s";
    public static String GET_GD_GD_LOV_METRIC_TYPE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_metric_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_METRIC_TYPE = "select * from semarchy_eph_mdm.gd_x_lov_metric_type where code in ('%s') order by code desc";

    public static String GET_GD_LOV_ORIGIN_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_origin";
    public static String GET_GD_LOV_ORIGIN_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_origin";
    public static String GET_GD_LOV_ORIGIN_IDS = "select code from semarchy_eph_mdm.gd_x_lov_origin order by random() limit %s";
    public static String GET_GD_GD_LOV_ORIGIN_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_origin where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_ORIGIN = "select * from semarchy_eph_mdm.gd_x_lov_origin where code in ('%s') order by code desc";


    public static String GET_GD_LOV_OWNER_DESCRIP_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_owner_description";
    public static String GET_GD_LOV_OWNER_DESCRIP_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_owner_description";
    public static String GET_GD_LOV_OWNER_DESCRIPTION_IDS = "select code from semarchy_eph_mdm.gd_x_lov_owner_description order by random() limit %s";
    public static String GET_GD_GD_LOV_OWNER_DESCRIP_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_owner_description where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_OWNER_DESCRIP = "select * from semarchy_eph_mdm.gd_x_lov_owner_description where code in ('%s') order by code desc";


    public static String GET_GD_LOV_PERSON_ROLE_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_person_role";
    public static String GET_GD_LOV_PERSON_ROLE_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_person_role";
    public static String GET_GD_LOV_PERSON_ROLE_IDS = "select code from semarchy_eph_mdm.gd_x_lov_person_role order by random() limit %s";
    public static String GET_GD_GD_LOV_PERSON_ROLE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_person_role where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_PERSON_ROLE = "select * from semarchy_eph_mdm.gd_x_lov_person_role where code in ('%s') order by code desc";


    public static String GET_GD_LOV_PMC_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_pmc";
    public static String GET_GD_LOV_PMC_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_pmc";
    public static String GET_GD_LOV_PMC_ROLE_IDS = "select code from semarchy_eph_mdm.gd_x_lov_pmc order by random() limit %s";
    public static String GET_GD_GD_LOV_PMC_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_pmc where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_PMC= "select * from semarchy_eph_mdm.gd_x_lov_pmc where code in ('%s') order by code desc";


    public static String GET_GD_LOV_PMG_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_pmg";
    public static String GET_GD_LOV_PMG_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_pmg";
    public static String GET_GD_LOV_PMG_ROLE_IDS = "select code from semarchy_eph_mdm.gd_x_lov_pmg order by random() limit %s";
    public static String GET_GD_GD_LOV_PMG_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_pmg where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_PMG_ROLE = "select * from semarchy_eph_mdm.gd_x_lov_pmg where code in ('%s') order by code desc";



    public static String GET_GD_LOV_PROD_STATUS_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_product_status";
    public static String GET_GD_LOV_PROD_STATUS_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_product_status";
    public static String GET_GD_LOV_PROD_STATUS_IDS = "select code from semarchy_eph_mdm.gd_x_lov_product_status order by random() limit %s";
    public static String GET_GD_GD_LOV_PROD_STATUS_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_product_status where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_PROD_STATUS = "select * from semarchy_eph_mdm.gd_x_lov_product_status where code in ('%s') order by code desc";

    public static String GET_GD_LOV_PROD_TYPE_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_product_type";
    public static String GET_GD_LOV_PROD_TYPE_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_product_type";
    public static String GET_GD_LOV_PROD_TYPE_IDS = "select code from semarchy_eph_mdm.gd_x_lov_product_type order by random() limit %s";
    public static String GET_GD_GD_LOV_PROD_TYPE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_product_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_PROD_TYPE = "select * from semarchy_eph_mdm.gd_x_lov_product_type where code in ('%s') order by code desc";


    public static String GET_GD_LOV_RELATION_TYPE_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_relationship_type";
    public static String GET_GD_LOV_RELATION_TYPE_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_relationship_type";
    public static String GET_GD_LOV_RELATION_TYPE_IDS = "select code from semarchy_eph_mdm.gd_x_lov_relationship_type order by random() limit %s";
    public static String GET_GD_GD_LOV_RELATION_TYPE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_relationship_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_RELATION_TYPE = "select * from semarchy_eph_mdm.gd_x_lov_relationship_type where code in ('%s') order by code desc";


    public static String GET_GD_LOV_REVENUE_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_revenue_account";
    public static String GET_GD_LOV_REVNUE_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_revenue_account";
    public static String GET_GD_LOV_REVNUE_IDS = "select code from semarchy_eph_mdm.gd_x_lov_revenue_account order by random() limit %s";
    public static String GET_GD_GD_LOV_REVENUE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_revenue_account where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_REVENUE = "select * from semarchy_eph_mdm.gd_x_lov_revenue_account where code in ('%s') order by code desc";


    public static String GET_GD_LOV_REVENUE_MODEL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_revenue_model";
    public static String GET_GD_LOV_REVNUE_MODEL_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_revenue_model";
    public static String GET_GD_LOV_REVNUE_MODEL_IDS = "select code from semarchy_eph_mdm.gd_x_lov_revenue_model order by random() limit %s";
    public static String GET_GD_GD_LOV_REVENUE_MODEL_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_revenue_model where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_REVENUE_MODEL = "select * from semarchy_eph_mdm.gd_x_lov_revenue_model where code in ('%s') order by code desc";

    public static String GET_GD_LOV_SUBJ_AREA_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_subject_area_type";
    public static String GET_GD_LOV_SUBJ_AREA_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_subject_area_type";
    public static String GET_GD_LOV_SUBJ_AREA_IDS = "select code from semarchy_eph_mdm.gd_x_lov_subject_area_type order by random() limit %s";
    public static String GET_GD_GD_LOV_SUBJ_AREA_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_subject_area_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_SUBJ_AREA = "select * from semarchy_eph_mdm.gd_x_lov_subject_area_type where code in ('%s') order by code desc";


    public static String GET_GD_LOV_SUB_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_subscription_type";
    public static String GET_GD_LOV_SUB_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_subscription_type";
    public static String GET_GD_LOV_SUB_IDS = "select code from semarchy_eph_mdm.gd_x_lov_subscription_type order by random() limit %s";
    public static String GET_GD_GD_LOV_SUB_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_subscription_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_SUB = "select * from semarchy_eph_mdm.gd_x_lov_subscription_type where code in ('%s') order by code desc";


    public static String GET_GD_LOV_WORK_HCHY_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_work_hchy_type";
    public static String GET_GD_LOV_WORK_HCHY_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_hchy_type";
    public static String GET_GD_LOV_WORK_HCHY_IDS = "select code from semarchy_eph_mdm.gd_x_lov_work_hchy_type order by random() limit %s";
    public static String GET_GD_GD_LOV_WORK_HCHY_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_hchy_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_WORK_HCHY = "select * from semarchy_eph_mdm.gd_x_lov_work_hchy_type where code in ('%s') order by code desc";


    public static String GET_GD_LOV_WORK_STATUS_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_work_status";
    public static String GET_GD_LOV_WORK_STATUS_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_status";
    public static String GET_GD_LOV_WORK_STATUS_IDS = "select code from semarchy_eph_mdm.gd_x_lov_work_status order by random() limit %s";
    public static String GET_GD_GD_LOV_WORK_STATUS_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_status where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_WORK_STATUS = "select * from semarchy_eph_mdm.gd_x_lov_work_status where code in ('%s') order by code desc";


    public static String GET_GD_LOV_WORK_TYPE_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_work_type";
    public static String GET_GD_LOV_WORK_TYPE_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_type";
    public static String GET_GD_LOV_WORK_TYPE_IDS = "select code from semarchy_eph_mdm.gd_x_lov_work_type order by random() limit %s";
    public static String GET_GD_GD_LOV_WORK_TYPE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_WORK_TYPE = "select * from semarchy_eph_mdm.gd_x_lov_work_type where code in ('%s') order by code desc";


    public static String GET_GD_LOV_WORKFOW_SOURCE_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_x_lov_workflow_source";
    public static String GET_GD_LOV_WORKFOW_SOURCE_COUNT_DL = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_workflow_source";
    public static String GET_GD_LOV_WORKFOW_SOURCE_IDS = "select code from semarchy_eph_mdm.gd_x_lov_workflow_source order by random() limit %s";
    public static String GET_GD_GD_LOV_WORKFOW_SOURCE_DL = "select * from "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_workflow_source where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_WORKFOW_SOURCE = "select * from semarchy_eph_mdm.gd_x_lov_workflow_source where code in ('%s') order by code desc";



    public static String GET_ACC_PROD_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_accountable_product";
    public static String GET_EVENT_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_event";
    public static String GET_LEGAL_OWNER_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_legal_owner";
    public static String GET_MANIF_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_manifestation";
    public static String GET_MANIF_ID_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_manifestation_identifier";
    public static String GET_PERSON_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_person";
    public static String GET_PRICE_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_price";
    public static String GET_PROD_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_product";
    public static String GET_PROD_FIN_ATTR_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_product_financial_attribs";
    public static String GET_PROD_IDENTIF_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_product_identifier";
    public static String GET_PROD_PERSON_ROLE_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_product_person_role";
    public static String GET_PROD_REL_PKG_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_product_rel_package";
    public static String GET_SUB_AREA_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_subject_area";
    public static String GET_WORK_ACCESS_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_access_model";
    public static String GET_WORK_BUSINESS_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_business_model";
    public static String GET_WORK_FIN_ATTR_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_financial_attribs";
    public static String GET_WORK_HERCHY_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_hierarchy";
    public static String GET_WORK_IDENTF_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_identifier";
    public static String GET_WORK_LEGAL_OWN_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_legal_owner";
    public static String GET_WORK_METRIC_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_metric";
    public static String GET_WORK_PERS_ROLE_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_person_role";
    public static String GET_WORK_REL_PKG_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_rel_package";
    public static String GET_WORK_REL_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_relationship";
    public static String GET_WORK_SUBJ_AREA_LINK_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_subject_area_link";
    public static String GET_WORK_WORK_HCHY_GD_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_work_hchy_link";
    public static String GET_WORK_PRESENT_PRESENT_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_wwork";
    public static String GET_GD_LOV_ACCESS_MODEL_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_access_model";
    public static String GET_GD_LOV_BUSINESS_MODEL_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_business_model";
    public static String GET_GD_LOV_CURRENCY_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_currency";
    public static String GET_GD_LOV_ETAX_PROD_CODE_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_etax_product_code";
    public static String GET_GD_LOV_EVENT_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_event_type";
    public static String GET_GD_LOV_GL_COMPANY_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_gl_company";
    public static String GET_GD_LOV_GL_PROD_SEG_PARENT_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_gl_prod_seg_parent";
    public static String GET_GD_LOV_GL_RESP_CENTER_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_gl_resp_centre";
    public static String GET_GD_LOV_IDENTIFIER_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_identifier_type";
    public static String GET_GD_LOV_IMPRINT_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_imprint";
    public static String GET_GD_LOV_LANGUAGE_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_language";
    public static String GET_GD_LOV_LEGAL_OWNERSHIP_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_legal_ownership";
    public static String GET_GD_LOV_MANIF_STATUS_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_manif_status";
    public static String GET_GD_LOV_MANIF_TYPE_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_manif_type";
    public static String GET_GD_LOV_METRIC_TYPE_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_metric_type";
    public static String GET_GD_LOV_ORIGIN_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_origin";
    public static String GET_GD_LOV_OWNER_DESCRIP_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_owner_description";
    public static String GET_GD_LOV_PERSON_ROLE_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_person_role";
    public static String GET_GD_LOV_PMC_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_pmc";
    public static String GET_GD_LOV_PMG_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_pmg";
    public static String GET_GD_LOV_PROD_STATUS_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_product_status";
    public static String GET_GD_LOV_PROD_TYPE_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_product_type";
    public static String GET_GD_LOV_RELATION_TYPE_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_relationship_type";
    public static String GET_GD_LOV_REVENUE_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_revenue_account";
    public static String GET_GD_LOV_REVENUE_MODEL_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_revenue_model";
    public static String GET_GD_LOV_SUBJ_AREA_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_subject_area_type";
    public static String GET_GD_LOV_SUB_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_subscription_type";
    public static String GET_GD_LOV_WORK_HCHY_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_work_hchy_type";
    public static String GET_GD_LOV_WORK_STATUS_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_work_status";
    public static String GET_GD_LOV_WORK_TYPE_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_work_type";
    public static String GET_GD_LOV_WORKFOW_SOURCE_PRESENT_COUNT = "select count(*) as Target_Count from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_workflow_source";

    public static String GET_GD_ACCOUNTABLE_PRODUCT_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_accountable_product where external_reference in ('%s') order by" +
            " external_reference,b_batchid desc";
    public static String GET_GD_EVENT_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_event where event_id in ('%s') order by event_id,b_batchid,b_credate,b_upddate desc";
    public static String GET_GD_LEGAL_OWNER_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_legal_owner where legal_owner_id in ('%s') order by legal_owner_id,b_batchid desc";
    public static String GET_GD_MANIFESTATION_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_manifestation where manifestation_id in ('%s') order by manifestation_id desc";
    public static String GET_GD_MANIFESTATION_IDENTIFIER_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_manifestation_identifier where manif_identifier_id in ('%s') order by manif_identifier_id desc";
    public static String GET_GD_PERSON_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_person where person_id in ('%s') order by person_id desc";
    public static String GET_GD_PRODUCT_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_product where product_id in ('%s') order by product_id desc";
    public static String GET_GD_PROD_FIN_ATTR_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_product_financial_attribs where product_fin_attribs_id in ('%s') order by product_fin_attribs_id desc";
    public static String GET_GD_PROD_IDENTIFIER_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_product_identifier where product_identifier_id in ('%s') order by product_identifier_id desc";
    public static String GET_GD_PROD_PERSON_ROLE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_product_person_role where product_person_role_id in ('%s') order by product_person_role_id desc";
    public static String GET_GD_PROD_REL_PKG_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_product_rel_package where product_rel_pack_id in ('%s') order by product_rel_pack_id desc";
    public static String GET_GD_SUBJECT_AREA_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_subject_area where subject_area_id in ('%s') order by subject_area_id desc";
    public static String GET_GD_WORK_ACCESS_MODEL_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_access_model where work_access_model_id in ('%s') order by work_access_model_id desc";
    public static String GET_GD_WORK_BUSINESS_MODEL_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_business_model where work_business_model_id in ('%s') order by work_business_model_id desc";
    public static String GET_GD_WORK_FIN_ATTR_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_financial_attribs where work_fin_attribs_id in ('%s') order by work_fin_attribs_id desc";
    public static String GET_GD_WORK_HIRERACHY_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_hierarchy where work_hierarchy_id in ('%s') order by work_hierarchy_id desc";
    public static String GET_GD_WORK_IDENTIFIER_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_identifier where work_identifier_id in ('%s') order by work_identifier_id desc";
    public static String GET_GD_WORK_LEGAL_OWNER_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_legal_owner where work_legal_owner_id in ('%s') order by work_legal_owner_id desc";
    public static String GET_GD_WORK_PERSON_ROLE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_person_role where work_person_role_id in ('%s') order by work_person_role_id desc";
    public static String GET_GD_WORK_REL_PKG_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_rel_package where work_rel_pack_id in ('%s') order by work_rel_pack_id desc";
    public static String GET_GD_WORK_RELATIONSHIP_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_relationship where work_relationship_id in ('%s') order by work_relationship_id desc";
    public static String GET_GD_WORK_SUB_AREA_LINK_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_subject_area_link where work_subject_area_link_id in ('%s') order by work_subject_area_link_id desc";
    public static String GET_GD_WORK_HCHU_LINK_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_work_work_hchy_link where wrk_wrk_hchy_link_id in ('%s') order by wrk_wrk_hchy_link_id desc";
    public static String GET_GD_WWORK_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_wwork where work_id in ('%s') order by work_id desc";
    public static String GET_GD_GD_LOV_ACCESS_MODEL_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_access_model where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_BUSINESS_MODEL_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_business_model where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_CURRENCY_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_currency where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_ETAX_PROD_CODE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_etax_product_code where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_EVENT_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_event_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_GL_COMPANY_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_gl_company where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_GL_PROD_SEG_PARENT_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_gl_prod_seg_parent where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_GL_RESP_CENTER_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_gl_resp_centre where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_IDENTIFIER_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_identifier_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_IMPRINT_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_imprint where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_LANGUAGE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_language where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_LEGAL_OWNERSHIP_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_legal_ownership where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_MANIF_STATUS_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_manif_status where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_MANIF_TYPE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_manif_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_METRIC_TYPE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_metric_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_ORIGIN_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_origin where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_OWNER_DESCRIP_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_owner_description where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_PERSON_ROLE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_person_role where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_PMC_PRESENT= "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_pmc where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_PMG_ROLE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_pmg where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_PROD_STATUS_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_product_status where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_PROD_TYPE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_product_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_RELATION_TYPE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_relationship_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_REVENUE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_revenue_account where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_REVENUE_MODEL_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_revenue_model where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_SUBJ_AREA_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_subject_area_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_SUB_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_subscription_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_WORK_HCHY_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_work_hchy_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_WORK_STATUS_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_work_status where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_WORK_TYPE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_work_type where code in ('%s') order by code desc";
    public static String GET_GD_GD_LOV_WORKFOW_SOURCE_PRESENT = "select * from "+GetJMDLDBUser.getProdPresentationDB()+".gd_x_lov_workflow_source where code in ('%s') order by code desc";


}



