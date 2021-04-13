package com.eph.automation.testing.services.db.JMDataLakeSQL;
//  Created by Thomas Kruck on 20/03/20


import static com.eph.automation.testing.services.db.JMDataLakeSQL.GetJMDLDBUser.getJMDataBase;

public class JMtoDataLakeCountChecksSQL {
    static  String[] databaseEnv = getJMDataBase();
    public static String jmf_allocation_change_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_ALLOCATION_CHANGE";
    public static String jmf_application_properties_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_APPLICATION_PROPERTIES";
    public static String jmf_approval_attachment_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_APPROVAL_ATTACHMENT";
    public static String jmf_approval_request_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_APPROVAL_REQUEST";
    public static String jmf_chronicle_scenario_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_CHRONICLE_SCENARIO";
    public static String jmf_chronicle_status_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_CHRONICLE_STATUS";
    public static String jmf_family_resource_details_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_FAMILY_RESOURCE_DETAILS";
    public static String jmf_financial_information_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_FINANCIAL_INFORMATION";
    public static String jmf_legal_information_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_LEGAL_INFORMATION";
    public static String jmf_manifestation_electronic_details_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_MANIFESTATION_ELECTRONIC_DETAILS";
    public static String jmf_manifestation_print_details_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_MANIFESTATION_PRINT_DETAILS";
    public static String jmf_party_in_product_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_PARTY_IN_PRODUCT";
    public static String jmf_product_availability_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_PRODUCT_AVAILABILITY";
    public static String jmf_product_chronicle_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_PRODUCT_CHRONICLE";
    public static String jmf_product_family_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_PRODUCT_FAMILY";
    public static String jmf_product_manifestation_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_PRODUCT_MANIFESTATION";
    public static String jmf_product_subject_area_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_PRODUCT_SUBJECT_AREA";
    public static String jmf_product_work_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_PRODUCT_WORK";
    public static String jmf_production_information_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_PRODUCTION_INFORMATION";
    public static String jmf_review_comment_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_REVIEW_COMMENT";
    public static String jmf_DB_work_business_model_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_WORK_BUSINESS_MODEL";
    public static String jmf_DB_work_access_model_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_WORK_ACCESS_MODEL";
    public static String jmf_DB_work_product_group_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_WORK_PRODUCT_GROUP";
    public static String jmf_DB_product_group_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_PRODUCT_GROUP";
    public static String jmf_DB_pricing_option_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_PRICING_OPTION";
    public static String jmf_DB_bm_pg_options_Count = "select count(*) as count from "+databaseEnv[0]+".JMF_BM_PG_OPTIONS";





    public static String DL_jmf_allocation_change_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_allocation_change where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_allocation_change order by inbound_ts desc limit 1)";
    public static String DL_jmf_application_properties_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_application_properties where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_application_properties order by inbound_ts desc limit 1)";
    public static String DL_jmf_approval_attachment_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_approval_attachment where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_approval_attachment order by inbound_ts desc limit 1)";
    public static String DL_jmf_approval_request_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_approval_request where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_approval_request order by inbound_ts desc limit 1)";
    public static String DL_jmf_chronicle_scenario_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_chronicle_scenario where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_chronicle_scenario order by inbound_ts desc limit 1)";
    public static String DL_jmf_chronicle_status_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_chronicle_status where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_chronicle_status order by inbound_ts desc limit 1)";
    public static String DL_jmf_family_resource_details_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_family_resource_details where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_family_resource_details order by inbound_ts desc limit 1)";
    public static String DL_jmf_financial_information_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_financial_information where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_financial_information order by inbound_ts desc limit 1)";
    public static String DL_jmf_legal_information_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_legal_information where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_legal_information order by inbound_ts desc limit 1)";
    public static String DL_jmf_manifestation_electronic_details_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_manifestation_electronic_details where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_manifestation_electronic_details order by inbound_ts desc limit 1)";
    public static String DL_jmf_manifestation_print_details_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_manifestation_print_details where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_manifestation_print_details order by inbound_ts desc limit 1)";
    public static String DL_jmf_party_in_product_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_party_in_product where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_party_in_product order by inbound_ts desc limit 1)";
    public static String DL_jmf_product_availability_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_product_availability where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_product_availability order by inbound_ts desc limit 1)";
    public static String DL_jmf_product_chronicle_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_product_chronicle where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_product_chronicle order by inbound_ts desc limit 1)";
    public static String DL_jmf_product_family_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_product_family where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_product_family order by inbound_ts desc limit 1)";
    public static String DL_jmf_product_manifestation_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_product_manifestation where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_product_manifestation order by inbound_ts desc limit 1)";
    public static String DL_jmf_product_subject_area_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_product_subject_area where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_product_subject_area order by inbound_ts desc limit 1)";
    public static String DL_jmf_product_work_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_product_work where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_product_work order by inbound_ts desc limit 1)";
    public static String DL_jmf_production_information_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_production_information where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_production_information order by inbound_ts desc limit 1)";
    public static String DL_jmf_review_comment_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_review_comment where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_review_comment order by inbound_ts desc limit 1)";
    public static String DL_jmf_work_business_model_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_work_business_model where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_work_business_model order by inbound_ts desc limit 1)";
    public static String DL_jmf_work_access_model_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_work_access_model where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_work_access_model order by inbound_ts desc limit 1)";
    public static String DL_jmf_work_product_group_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_work_product_group where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_work_product_group order by inbound_ts desc limit 1)";
    public static String DL_jmf_product_group_Count = "select count(*) as count from "+databaseEnv[1]+".jmf_product_group where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_product_group order by inbound_ts desc limit 1)";
    public static String DL_jmf_bm_pg_options = "select count(*) as count from "+databaseEnv[1]+".jmf_bm_pg_options where inbound_ts = (select inbound_ts from "+databaseEnv[1]+".jmf_bm_pg_options order by inbound_ts desc limit 1)";

//    Inbound record count
    public static String jmf_work_business_model_Count = "select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".jmf_work_business_model";
    public static String jmf_work_access_model_Count = "select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".jmf_work_access_model";
    public static String jmf_work_product_group_Count = "select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".jmf_work_productgroup";
    public static String jmf_product_group_Count = "select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".jmf_productgroup";
    public static String jmf_pricing_option_Count = "select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".jmf_pricing_option";
    public static String jmf_bm_pg_options_Count = "select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".jmf_bm_pg_options";

//    Access record count
    public static String jmfAccess_work_business_model_Count = "select count(*) as count from (\n" +
        "SELECT cast(work_business_model_id as integer) work_business_model_id,\n" +
        "       cast(f_product_work as integer) f_work,\n" +
        "       business_model_code,\n" +
        "       business_model_description,\n" +
        "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
        "       inbound_ts\n" +
        "FROM "+ databaseEnv[1]+".jmf_work_business_model\n" +
        "WHERE notified_date is not null) where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_work_business_model order by inbound_ts desc limit 1) ";

    public static String jmfAccess_work_access_model_Count = "select count(*) as count from (\n" +
            "SELECT cast(work_access_model_id as integer) work_access_model_id,\n" +
            "       cast(f_product_work as integer) f_work,\n" +
            "       access_model_code,\n" +
            "       access_model_description,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_work_access_model\n" +
            "WHERE notified_date is not null) where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_work_access_model order by inbound_ts desc limit 1)\n";

    public static String jmfAccess_work_product_group_Count = "select count(*) as count from (SELECT cast(work_product_group_id as integer) work_productgroup_id,\n" +
            "       cast(f_product_work as integer) f_work,\n" +
            "       cast(f_product_group as integer) f_productgroup,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts \n" +
            "FROM "+ databaseEnv[1]+".jmf_work_product_group\n" +
            "WHERE notified_date is not null) where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_work_product_group order by inbound_ts desc limit 1)";

    public static String jmfAccess_product_group_Count = "select count(*) as count from(SELECT cast(product_group_id as integer) productgroup_id,\n" +
            "       product_group_code productgroup_code,\n" +
            "       product_group_description productgroup_description,\n" +
            "       cast(f_pricing_option as integer) f_pricing_option,\n" +
            "       cast(date_parse(effective_to_date, '%%Y-%%m-%%d %%H:%%i:%%s') as date) effective_to_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_product_group) where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_product_group order by inbound_ts desc limit 1)";

    public static String jmfAccess_pricing_option_Count = "select count(*) as count from (SELECT cast(pricing_option_id as integer) pricing_option_id,\n" +
            "       pricing_option_code,\n" +
            "       pricing_option_description,\n" +
            "       cast(date_parse(effective_to_date, '%%Y-%%m-%%d %%H:%%i:%%s') as date) effective_to_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1] +".jmf_pricing_option) where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_pricing_option order by inbound_ts desc limit 1)";

    public static String jmfAccess_bm_pg_options_Count = "select count(*) as count from (SELECT cast(bm_pg_options_id as integer) bm_pg_options_id,\n" +
            "       business_model_code,\n" +
            "       manifestation_formats_code,\n" +
            "       cast(f_product_group as integer) f_productgroup,\n" +
            "       ownership_brand_type,\n" +
            "       jbs_site_ind,\n" +
            "       options,\n" +
            "       cast(date_parse(effective_to_date, '%%Y-%%m-%%d %%H:%%i:%%s') as date) effective_to_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1] +".jmf_bm_pg_options) where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_bm_pg_options order by inbound_ts desc limit 1)";
}
