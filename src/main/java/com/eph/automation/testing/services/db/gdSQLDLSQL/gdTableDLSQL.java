package com.eph.automation.testing.services.db.gdSQLDLSQL;

import com.eph.automation.testing.services.db.EPHDB.GetEPHDB;
import com.eph.automation.testing.services.db.JMDataLakeSQL.GetJMDLDBUser;
import com.eph.automation.testing.services.db.PROMISDataLakeSQL.GetPRMDLDBUser;

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
    public static String GET_PROD_HIER_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product_financial_attribs";
    public static String GET_PROD_HIER_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_product_financial_attribs";
    public static String GET_PROD_LINE_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product_line";
    public static String GET_PROD_LINE_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_product_line";
    public static String GET_PROD_PERSON_ROLE_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product_person_role";
    public static String GET_PROD_PERSON_ROLE_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_product_person_role";
    public static String GET_PROD_HCHY_LINK_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product_product_hchy_link";
    public static String GET_PROD_HCHY_LINK_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_product_product_hchy_link";
    public static String GET_PROD_REL_PKG_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product_rel_package";
    public static String GET_PROD_REL_PKG_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_product_rel_package";
    public static String GET_PROD_REL_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product_relationship";
    public static String GET_PROD_REL_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getProdDataBase()+".gd_product_relationship";
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

}
