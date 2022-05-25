package com.eph.automation.testing.services.db.consumerAppSQL;


import com.eph.automation.testing.services.db.JMDataLakeSQL.GetJMDLDBUser;
import com.eph.automation.testing.services.db.PROMISDataLakeSQL.GetPRMDLDBUser;

public class jdwChecksSQL {
     private jdwChecksSQL() {
         throw new IllegalStateException("Utility class");
     }

       public static String GET_SOURCE_EPH_LOV_COUNT =
              "select count(*) as Source_count from( \n" +
                      "SELECT\n" +
                      "  'ETAX_PRODUCT_CODE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_etax_product_code \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'EVENT_TYPE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_event_type \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'COMPANY' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_company \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'PRODUCT_SEGMENT_PARENT' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_prod_seg_parent \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'RESPONSIBILITY_CENTRE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_resp_centre \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'IDENTIFIER_TYPE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_identifier_type \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'IMPRINT' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_imprint \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'LANGUAGE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_language \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'MANIFESTATION_STATUS' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", \"LOV\".\"roll_up_status\" \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_status \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'MANIFESTATION_TYPE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", \"LOV\".\"roll_up_type\" \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_type \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'ORIGIN' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_origin \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'PERSON_ROLE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_person_role \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'PMC' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", \"LOV\".\"f_pmg\" \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmc \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'PMG' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmg \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'PRODUCT_STATUS' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", \"LOV\".\"roll_up_status\" \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_product_status \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'PRODUCT_TYPE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_product_type \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'RELATIONSHIP_TYPE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_relationship_type \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'REVENUE_MODEL' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_revenue_model \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'SUBJECT_AREA_TYPE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_subject_area_type \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'SUBSCRIPTION_TYPE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_subscription_type \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'WORK_STATUS' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", \"LOV\".\"roll_up_status\" \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'WORK_TYPE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", \"LOV\".\"roll_up_type\" \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type \"LOV\"\n" +
                      "UNION ALL SELECT\n" +
                      "  'WORKFLOW_SOURCE' \"LOV_PARENT\"\n" +
                      ", \"LOV\".\"code\" \"CODE\"\n" +
                      ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                      ", null \"ROLL_UP_VALUE\"\n" +
                      "FROM\n" +
                      "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_workflow_source \"LOV\")\n";

    public static String GET_EXARI_TGT_COUNT =
            "select count(*) as Target_count from "+GetPRMDLDBUser.getProdDataBase()+".jdw_journal_lov_vw";

    public static String GET_RANDOM_ID_JDW =
            "select code as randomIds from( \n" +
                    "SELECT\n" +
                    "  'ETAX_PRODUCT_CODE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_etax_product_code \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'EVENT_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_event_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'COMPANY' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_company \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PRODUCT_SEGMENT_PARENT' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_prod_seg_parent \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'RESPONSIBILITY_CENTRE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_resp_centre \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'IDENTIFIER_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_identifier_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'IMPRINT' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_imprint \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'LANGUAGE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_language \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'MANIFESTATION_STATUS' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"roll_up_status\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_status \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'MANIFESTATION_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"roll_up_type\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'ORIGIN' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_origin \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PERSON_ROLE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_person_role \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PMC' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"f_pmg\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmc \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PMG' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmg \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PRODUCT_STATUS' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"roll_up_status\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_product_status \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PRODUCT_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_product_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'RELATIONSHIP_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_relationship_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'REVENUE_MODEL' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_revenue_model \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'SUBJECT_AREA_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_subject_area_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'SUBSCRIPTION_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_subscription_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'WORK_STATUS' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"roll_up_status\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'WORK_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"roll_up_type\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'WORKFLOW_SOURCE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_workflow_source \"LOV\")order by rand() limit %s\n";

    public static String GET_SOURCE_REC_JDW =
            "select * from( \n" +
                    "SELECT\n" +
                    "  'ETAX_PRODUCT_CODE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_etax_product_code \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'EVENT_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_event_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'COMPANY' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_company \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PRODUCT_SEGMENT_PARENT' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_prod_seg_parent \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'RESPONSIBILITY_CENTRE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_resp_centre \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'IDENTIFIER_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_identifier_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'IMPRINT' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_imprint \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'LANGUAGE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_language \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'MANIFESTATION_STATUS' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"roll_up_status\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_status \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'MANIFESTATION_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"roll_up_type\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'ORIGIN' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_origin \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PERSON_ROLE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_person_role \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PMC' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"f_pmg\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmc \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PMG' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmg \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PRODUCT_STATUS' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"roll_up_status\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_product_status \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'PRODUCT_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_product_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'RELATIONSHIP_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_relationship_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'REVENUE_MODEL' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_revenue_model \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'SUBJECT_AREA_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_subject_area_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'SUBSCRIPTION_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_subscription_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'WORK_STATUS' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"roll_up_status\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'WORK_TYPE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", \"LOV\".\"roll_up_type\" \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type \"LOV\"\n" +
                    "UNION ALL SELECT\n" +
                    "  'WORKFLOW_SOURCE' \"LOV_PARENT\"\n" +
                    ", \"LOV\".\"code\" \"CODE\"\n" +
                    ", \"LOV\".\"l_description\" \"DESCRIPTION\"\n" +
                    ", null \"ROLL_UP_VALUE\"\n" +
                    "FROM\n" +
                    "  "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_workflow_source \"LOV\")where code in ('%s') order by code desc,roll_up_value desc,lov_parent desc\n";

        public static String GET_TARGET_REC_JDW =
                "select * from "+GetPRMDLDBUser.getProdDataBase()+".jdw_journal_lov_vw where code in ('%s') order by code desc,roll_up_value desc,lov_parent desc\n";

}





