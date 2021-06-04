package com.eph.automation.testing.services.db.JMDataLakeSQL;

import com.eph.automation.testing.services.db.EPHDB.GetEPHDB;
import com.eph.automation.testing.services.db.PROMISDataLakeSQL.GetPRMDLDBUser;
import static com.eph.automation.testing.services.db.JMDataLakeSQL.GetJMDLDBUser.getJMDataBase;

import com.eph.automation.testing.services.db.sql.GetEPHDBUser;

public class JMETLDataChecksSQL {
    static  String[] databaseEnv = getJMDataBase();
//    JMDB Table Record IDs
    public static String GET_WORK_BUSINESS_MODEL_IDs = "select work_business_model_id as WORK_BUSINESS_MODEL_ID from "+ GetJMDLDBUser.getJMDB()+".JMF_WORK_BUSINESS_MODEL order by rand() limit %s";
    public static String GET_WORK_ACCESS_MODEL_IDs = "select work_access_model_id as WORK_ACCESS_MODEL_ID from "+ GetJMDLDBUser.getJMDB()+".JMF_WORK_ACCESS_MODEL order by rand() limit %s";
    public static String GET_WORK_PRODUCT_GROUP_IDs = "select work_productgroup_id as WORK_PRODUCTGROUP_ID from "+ GetJMDLDBUser.getJMDB()+".JMF_WORK_PRODUCTGROUP order by rand() limit %s";
    public static String GET_PRODUCT_GROUP_IDs = "select productgroup_id as PRODUCTGROUP_ID from "+ GetJMDLDBUser.getJMDB()+".JMF_PRODUCTGROUP order by rand() limit %s";
    public static String GET_PRICING_OPTION_IDs = "select pricing_option_id as PRICING_OPTION_ID from "+ GetJMDLDBUser.getJMDB()+".JMF_PRICING_OPTION order by rand() limit %s";
    public static String GET_BM_PG_OPTIONS_IDs = "select bm_pg_options_id as BM_PG_OPTIONS_ID from "+ GetJMDLDBUser.getJMDB()+".JMF_BM_PG_OPTIONS order by rand() limit %s";

//    JMDB Table Records
    public static String GET_JMDB_WORK_BUSINESS_MODEL = "select * from "+databaseEnv[0]+".JMF_WORK_BUSINESS_MODEL where work_business_model_id in (%s)";
    public static String GET_JMDB_WORK_ACCESS_MODEL = "select * from "+databaseEnv[0]+".JMF_WORK_ACCESS_MODEL where work_access_model_id in (%s)";
    public static String GET_JMDB_WORK_PRODUCT_GROUP = "select * from "+databaseEnv[0]+".JMF_WORK_PRODUCT_GROUP where work_product_group_id in (%s)";
    public static String GET_JMDB_PRODUCT_GROUP = "select * from "+databaseEnv[0]+".JMF_PRODUCT_GROUP where product_group_id in (%s)";
    public static String GET_JMDB_PRICING_OPTION = "select * from "+databaseEnv[0]+".JMF_PRICING_OPTION where pricing_option_id in (%s)";
    public static String GET_JMDB_BM_PG_OPTIONS = "select * from "+databaseEnv[0]+".JMF_BM_PG_OPTIONS where bm_pg_options_id in (%s)";

//    JMInbound Records
    public static String GET_JMINBOUND_WORK_BUSINESS_MODEL = "select * from "+ GetJMDLDBUser.getJMDB()+".JMF_WORK_BUSINESS_MODEL where work_business_model_id in (%s)";
    public static String GET_JMINBOUND_WORK_ACCESS_MODEL = "select * from "+ GetJMDLDBUser.getJMDB()+".JMF_WORK_ACCESS_MODEL where work_access_model_id in (%s)";
    public static String GET_JMINBOUND_WORK_PRODUCT_GROUP = "select * from "+ GetJMDLDBUser.getJMDB()+".JMF_WORK_PRODUCTGROUP where work_productgroup_id in (%s)";
    public static String GET_JMINBOUND_PRODUCT_GROUP = "select * from "+ GetJMDLDBUser.getJMDB()+".JMF_PRODUCTGROUP where productgroup_id in (%s)";
    public static String GET_JMINBOUND_PRICING_OPTION = "select * from "+ GetJMDLDBUser.getJMDB()+".JMF_PRICING_OPTION where pricing_option_id in (%s)";
    public static String GET_JMINBOUND_BM_PG_OPTIONS = "select * from "+ GetJMDLDBUser.getJMDB()+".JMF_BM_PG_OPTIONS where bm_pg_options_id in (%s)";

    //    JM Access table IDs SQL
    public static String GET_JMF_WORK_IDs = "select work_id as WORK_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work order by rand() limit %s";
    public static String GET_JMF_MANIFESTATION_IDs = "select manifestation_id as MANIFESTATION_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_manifestation order by rand() limit %s";
    public static String GET_JMF_WORK_PERSON_ROLE_IDs = "select work_person_role_id as WORK_PERSON_ROLE_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_person_role order by rand() limit %s";
    public static String GET_JMF_WORK_SUBJECT_AREA_IDs = "select work_subject_area_id as WORK_SUBJECT_AREA_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_subject_area limit %s";
    public static String GET_JMF_WORK_CHRONICLE_IDs = "select work_chronicle_id as WORK_CHRONICLE_ID from "+ GetJMDLDBUser.getJMDB2()+".jmf_work_chronicle order by rand() limit %s";
    public static String GET_JMF_PMG_PUBDIR_ALLOCATION_IDs = "select pmg_pubdir_allocation_id as PMG_PUBDIR_ALLOCATION_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_pmg_pubdir_allocation order by rand() limit %s";
    public static String GET_JMF_APPROVAL_ATTACHMENT_IDs = "select approval_attachment_id as APPROVAL_ATTACHMENT_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_approval_attachment order by rand() limit %s";
    public static String GET_JMF_REVIEW_COMMENT_IDs = "select review_comment_id as REVIEW_COMMENT_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_review_comment order by rand() limit %s";
    public static String GET_JMF_APPROVAL_REQUEST_IDs = "select approval_id as APPROVAL_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_approval_request order by rand() limit %s";
    public static String GET_JMF_APPLICATION_PROPERTIES_IDs = "select application_property_key as APPLICATION_PROPERTY_KEY from "+ GetJMDLDBUser.getJMDB()+".jmf_application_properties order by rand() limit %s";
    public static String GET_JMF_CHRONICLE_SCENARIO_IDs = "select chronicle_scenario_code as CHRONICLE_SCENARIO_CODE from "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario order by rand() limit %s";
    public static String GET_JMF_WORK_OWNERSHIP_IDs = "select work_ownership_id as WORK_OWNERSHIP_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_ownership order by rand() limit %s";
    public static String GET_JMF_WORK_BUSINESS_MODEL_IDs = "select work_business_model_id as WORK_BUSINESS_MODEL_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_business_model order by rand() limit %s";
    public static String GET_JMF_WORK_ACCESS_MODEL_IDs = "select work_access_model_id as WORK_ACCESS_MODEL_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_access_model order by rand() limit %s";
    public static String GET_JMF_WORK_PRODUCT_GROUP_IDs = "select work_productgroup_id as WORK_PRODUCTGROUP_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_productgroup order by rand() limit %s";
    public static String GET_JMF_PRODUCT_GROUP_IDs = "select productgroup_id as PRODUCTGROUP_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_productgroup order by rand() limit %s";
    public static String GET_JMF_PRICING_OPTION_IDs = "select pricing_option_id as PRICING_OPTION_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_pricing_option order by rand() limit %s";
    public static String GET_JMF_BM_PG_OPTIONS_IDs = "select bm_pg_options_id as BM_PG_OPTIONS_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_bm_pg_options order by rand() limit %s";


//    JM Access tables SQL
    public static String GET_JMF_WORK = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_work where work_id in (%s)";
    public static String GET_JMF_MANIFESTATION = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_manifestation where manifestation_id in (%s)";
    public static String GET_JMF_WORK_PERSON_ROLE = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_work_person_role where work_person_role_id in (%s)";
    public static String GET_JMF_WORK_SUBJECT_AREA = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_work_subject_area where work_subject_area_id in (%s)";
    public static String GET_JMF_WORK_CHRONICLE = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle where work_chronicle_id in (%s)";
    public static String GET_JMF_PMG_PUBDIR_ALLOCATION = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_pmg_pubdir_allocation where pmg_pubdir_allocation_id in (%s)";
    public static String GET_JMF_APPROVAL_ATTACHMENT = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_approval_attachment where approval_attachment_id in (%s)";
    public static String GET_JMF_REVIEW_COMMENT = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_review_comment where review_comment_id in (%s)";
    public static String GET_JMF_APPROVAL_REQUEST = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_approval_request where approval_id in (%s)";
    public static String GET_JMF_APPLICATION_PROPERTIES = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_application_properties where application_property_key in ('%s')";
    public static String GET_JMF_CHRONICLE_SCENARIO = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario where chronicle_scenario_code in ('%s')";
    public static String GET_JMF_WORK_OWNERSHIP = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_work_ownership where work_ownership_id in (%s)";
    public static String GET_JMF_WORK_BUSINESS_MODEL = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_work_business_model where work_business_model_id in (%s)";
    public static String GET_JMF_WORK_ACCESS_MODEL = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_work_access_model where work_access_model_id in (%s)";
    public static String GET_JMF_WORK_PRODUCTGROUP = "select * from \"+ GetJMDLDBUser.getJMDB()+\".jmf_work_productgroup where work_productgroup_id in (%s)";
    public static String GET_JMF_PRODUCTGROUP = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_productgroup where productgroup_id in (%s)";
    public static String GET_JMF_PRICING_OPTION = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_pricing_option where pricing_option_id in (%s)";
    public static String GET_JMF_BM_PG_OPTIONS = "select * from "+ GetJMDLDBUser.getJMDB()+".jmf_bm_pg_options where bm_pg_options_id in (%s)";

//

// JM Staging Query SQL
    public static String GET_JMF_BM_PG_OPTIONS_QUERY = "select * from (SELECT cast(bm_pg_options_id as integer) bm_pg_options_id,\n" +
            "       business_model_code,\n" +
            "       manifestation_formats_code,\n" +
            "       cast(f_product_group as integer) f_productgroup,\n" +
            "       ownership_brand_type,\n" +
            "       jbs_site_ind,\n" +
            "       options,\n" +
            "       cast(date_parse(effective_to_date, '%%Y-%%m-%%d %%H:%%i:%%s') as date) effective_to_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1] +".jmf_bm_pg_options) where bm_pg_options_id in (%s) and inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_bm_pg_options order by inbound_ts desc limit 1)";

    public static String GET_PRICING_OPTION = "select * from (SELECT cast(pricing_option_id as integer) pricing_option_id,\n" +
            "       pricing_option_code,\n" +
            "       pricing_option_description,\n" +
            "       cast(date_parse(effective_to_date, '%%Y-%%m-%%d %%H:%%i:%%s') as date) effective_to_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1] +".jmf_pricing_option) where pricing_option_id in (%s) and inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_pricing_option order by inbound_ts desc limit 1)";

    public static String GET_WORK_QUERY = "select * from "+ databaseEnv[1] +".jmf_work_v where work_id in (%s)";

    public static String GET_MANIFESTATION_QUERY = "select * from (SELECT m.manifestation_id,\n" +
            "       m.f_work,\n" +
            "       m.manifestation_title,\n" +
            "       case\n" +
            "         when e.manifestation_id is not null then 'E'\n" +
            "         when p.manifestation_id is not null then 'P'\n" +
            "       end as manifestation_type,\n" +
            "       m.issn,\n" +
            "       m.elsevier_journal_number,\n" +
            "       m.subscription_type,\n" +
            "       m.price_categories,\n" +
            "       m.pmx_product_manifestation_id,\n" +
            "       m.eph_manifestation_id,\n" +
            "       e.embargo_times_indicator,\n" +
            "       e.electronic_rights_secured_type,\n" +
            "       e.online_launch_date,\n" +
            "       e.article_in_press_s5_ind,\n" +
            "       e.article_in_press_s100_ind,\n" +
            "       e.article_in_press_s250_ind,\n" +
            "       e.embargo_times_number,\n" +
            "       e.online_last_issue_date,\n" +
            "       p.trim_size,\n" +
            "       p.base_print_run_number,\n" +
            "       p.colour_frequency,\n" +
            "       p.artwork_sensitivity_ind,\n" +
            "       p.mailing_breakdown_europe,\n" +
            "       p.mailing_breakdown_usa,\n" +
            "       p.mailing_breakdown_row,\n" +
            "       p.zero_warehousing_ind,\n" +
            "       p.back_stock_warehouse_location_type,\n" +
            "       p.printer_type,\n" +
            "       p.printer_location_type,\n" +
            "       p.interior_paper_type,\n" +
            "       p.cover_paper_type,\n" +
            "       p.distributor_code,\n" +
            "       p.distributor_location_code,\n" +
            "       p.print_model_code,\n" +
            "       p.separate_print_run_ind,\n" +
            "       p.offprint_pricing_code,\n" +
            "       p.offprint_cover_ind,\n" +
            "       p.free_issues_quantity,\n" +
            "       p.free_offprints_type,\n" +
            "       p.free_paid_colour_offprints_quantity,\n" +
            "       p.colour_printing_currency_code,\n" +
            "       p.colour_artwork_exceptions,\n" +
            "       p.society_owns_labels_ind,\n" +
            "       p.binding_type,\n" +
            "       p.special_bulk_arrangements,\n" +
            "       p.cost_first_printed_colour_unit,\n" +
            "       p.cost_next_printed_colour_unit,\n" +
            "       a.application_code,\n" +
            "       m.notified_date,\n" +
            "       m.inbound_ts\n" +
            "FROM "+ GetJMDLDBUser.getJMDB2()+".jmf_product_manifestation_2 m\n" +
            "LEFT OUTER JOIN "+ GetJMDLDBUser.getJMDB2()+".jmf_manifestation_electronic_details_2 e ON e.manifestation_id = m.manifestation_id\n" +
            "LEFT OUTER JOIN "+ GetJMDLDBUser.getJMDB2()+".jmf_manifestation_print_details_2 p ON p.manifestation_id = m.manifestation_id\n" +
            "LEFT OUTER JOIN (SELECT DISTINCT f_product_manifestation, application_code\n" +
            "                 FROM "+ GetJMDLDBUser.getJMDB2()+".jmf_product_availability_2) a ON a.f_product_manifestation = m.manifestation_id AND a.application_code NOT IN ('SD','CRM')) where manifestation_id in (%s)";

    public static String GET_WORK_PERSON_ROLE_QUERY = "select * from (SELECT cast(party_in_product_id as integer) work_person_role_id,\n" +
            "       cast(f_product_work as integer) f_work,\n" +
            "       party_role_type,\n" +
            "       email_address,\n" +
            "       full_name,\n" +
            "       phone_number,\n" +
            "       address_line_1,\n" +
            "       address_line_2,\n" +
            "       address_line_3,\n" +
            "       city,\n" +
            "       country,\n" +
            "       state,\n" +
            "       post_code,\n" +
            "       organisation_1,\n" +
            "       pmx_party_id,\n" +
            "       peoplehub_id,\n" +
            "       eph_person_id,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1] +".jmf_party_in_product\n" +
            " where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_party_in_product order by inbound_ts desc limit 1)\n" +
            "and notified_date is not null) where work_person_role_id in (%s)";

    public static String GET_WORK_SUBJECT_AREA_QUERY = "select * from (\n" +
            "SELECT cast(product_subject_area_id as integer) work_subject_area_id,\n" +
            "       cast(f_product_work as integer) f_work,\n" +
            "       subject_area_type_code,\n" +
            "       subject_area_priority_code,\n" +
            "       subject_area_code,\n" +
            "       subject_area_name,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_product_subject_area\n" +
            "where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_product_subject_area order by inbound_ts desc limit 1)\n" +
            "and notified_date is not null) where work_subject_area_id in (%s)";

    public static String GET_WORK_CHRONICLE_QUERY = "select * from (SELECT cast(product_chronicle_id as integer) work_chronicle_id,\n" +
            "       date_parse(completed_on, '%%Y-%%m-%%d %%H:%%i:%%s') completed_on,\n" +
            "       distribution_list,\n" +
            "       rename_required_ind,\n" +
            "       journal_renamed_before_launch,\n" +
            "       chronicle_status_code,\n" +
            "       chronicle_scenario_code,\n" +
            "       started_by,\n" +
            "       date_parse(started_on, '%%Y-%%m-%%d %%H:%%i:%%s') started_on,\n" +
            "       updated_by,\n" +
            "       date_parse(updated_on, '%%Y-%%m-%%d %%H:%%i:%%s') updated_on,\n" +
            "       process_instance_id,\n" +
            "       reason_for_change,\n" +
            "       cancelled_by,\n" +
            "       created_by_name,\n" +
            "       rejection_comment,\n" +
            "       date_parse(submission_date, '%%Y-%%m-%%d %%H:%%i:%%s') submission_date,\n" +
            "       date_parse(cancelled_date, '%%Y-%%m-%%d %%H:%%i:%%s') cancelled_date,\n" +
            "       date_parse(rejection_date, '%%Y-%%m-%%d %%H:%%i:%%s') rejection_date,\n" +
            "       cast(version as integer) version,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_product_chronicle\n" +
            "where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_product_chronicle order by inbound_ts desc limit 1)\n" +
            "and notified_date is not null) where work_chronicle_id in (%S)";

    public static String GET_PMG_PUBDIR_ALLOCATION_QUERY = "select * from (SELECT cast(allocation_change_id as integer) pmg_pubdir_allocation_id,\n" +
            "       allocation_type,\n" +
            "       pmg_filter,\n" +
            "       pmc_filter,\n" +
            "       ppc_filter_email as pu_filter_email,\n" +
            "       ppc_filter_name as pu_filter_name,\n" +
            "       pmc_change_ind,\n" +
            "       ppc_change_ind as pu_change_ind,\n" +
            "       pmx_pmgcode,\n" +
            "       pmx_pmg_name,\n" +
            "       pmx_pmg_f_business_unit,\n" +
            "       pmg_bus_ctrl_name,\n" +
            "       pmg_bus_ctrl_email,\n" +
            "       pmg_pubdir_name_old,\n" +
            "       pmg_pubdir_email_old,\n" +
            "       pmg_pubdir_people_hub_id_old as pmg_pubdir_peoplehub_id_old,\n" +
            "       pmg_pubdir_name_new,\n" +
            "       pmg_pubdir_email_new,\n" +
            "       pmg_pubdir_people_hub_id_new as pmg_pubdir_peoplehub_id_new,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       eph_pmg_code,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_allocation_change\n" +
            " where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_allocation_change order by inbound_ts desc limit 1)\n" +
            "and notified_date is not null)where pmg_pubdir_allocation_id in (%s)";

    public static String GET_APPROVAL_ATTACHMENT_QUERY = "select * from (SELECT cast(approval_attachment_id as integer) approval_attachment_id,\n" +
            "       cast(f_approval as integer) f_approval,\n" +
            "       attachment_name,\n" +
            "       attachment,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_approval_attachment\n" +
            "where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_approval_attachment order by inbound_ts desc limit 1)\n" +
            "and notified_date is not null) where approval_attachment_id in (%s)";

    public static String GET_REVIEW_COMMENT_QUERY = "select * from (SELECT cast(review_comment_id as integer) review_comment_id,\n" +
            "       cast(f_approval_id as integer) f_approval_id,\n" +
            "       review_attribute_name,\n" +
            "       review_comment,\n" +
            "       date_parse(review_comment_date, '%%Y-%%m-%%d %%H:%%i:%%s') review_comment_date,\n" +
            "       date_parse(created_on, '%%Y-%%m-%%d %%H:%%i:%%s') created_on,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_review_comment\n" +
            " where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_review_comment order by inbound_ts desc limit 1)\n" +
            "and notified_date is not null) where review_comment_id in (%s) ";

    public static String GET_APPROVAL_REQUEST_QUERY = "select * from (SELECT cast(approval_id as integer) approval_id,\n" +
            "       cast(f_product_chronicle as integer) f_work_chronicle,\n" +
            "       approval_type,\n" +
            "       approver_name,\n" +
            "       approval_given_indicator,\n" +
            "       date_parse(approval_request_date, '%%Y-%%m-%%d %%H:%%i:%%s') approval_request_date,\n" +
            "       date_parse(approval_response_date, '%%Y-%%m-%%d %%H:%%i:%%s') approval_response_date,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_approval_request\n" +
            "where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_approval_request order by inbound_ts desc limit 1)\n" +
            "and notified_date is not null) where approval_id in (%s)";

    public static String GET_APPLICATION_PROPERTIES_QUERY = "select * from (SELECT application_property_key,\n" +
            "       application_property_value,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_application_properties) " +
            "where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_application_properties order by inbound_ts desc limit 1)\n" +
            "and application_property_key in ('%s')\n";

    public static String GET_CHRONICLE_SCENARIO_QUERY = "select * from (SELECT chronicle_scenario_code,\n" +
            "       chronicle_scenario_name,\n" +
            "       chronicle_scenario_description,\n" +
            "       chronicle_scenario_evolutionary_ind,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_chronicle_scenario) " +
            "where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_chronicle_scenario order by inbound_ts desc limit 1)\n" +
            "and chronicle_scenario_code in ('%s')\n";

    public static String GET_WORK_OWNERSHIP_QUERY = "select * from (SELECT cast(ownership_id as integer) work_ownership_id,\n" +
            "       cast(f_product_work as integer) f_work,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       cast(f_legal_owner as integer) f_legal_owner,\n" +
            "       legal_owner_name,\n" +
            "       legal_owner_type,\n" +
            "       journal_ownership_type,\n" +
            "       inbound_ts\n" +
            "FROM journalmaestro_staging_sit.jmf_product_ownership\n" +
            "WHERE notified_date is not null)\n"+
            "where inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_product_ownership order by inbound_ts desc limit 1)\n" +
            "and work_ownership_id in (%s)\n";

    public static String GET_WORK_BUSINESS_MODEL_QUERY = "select * from (\n" +
            "SELECT cast(work_business_model_id as integer) work_business_model_id,\n" +
            "       cast(f_product_work as integer) f_work,\n" +
            "       business_model_code,\n" +
            "       business_model_description,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_work_business_model\n" +
            "WHERE notified_date is not null) where work_business_model_id in (%s) and inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_work_business_model order by inbound_ts desc limit 1) ";

    public static String GET_WORK_ACCESS_MODEL_QUERY = "select * from (\n" +
            "SELECT cast(work_access_model_id as integer) work_access_model_id,\n" +
            "       cast(f_product_work as integer) f_work,\n" +
            "       access_model_code,\n" +
            "       access_model_description,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_work_access_model\n" +
            "WHERE notified_date is not null) where work_access_model_id in (%s) and inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_work_access_model order by inbound_ts desc limit 1)\n";

    public static String GET_WORK_PRODUCTGROUP_QUERY = "select * from (SELECT cast(work_product_group_id as integer) work_productgroup_id,\n" +
            "       cast(f_product_work as integer) f_work,\n" +
            "       cast(f_product_group as integer) f_productgroup,\n" +
            "       date_parse(notified_date, '%%Y-%%m-%%d %%H:%%i:%%s') notified_date,\n" +
            "       inbound_ts \n" +
            "FROM "+ databaseEnv[1]+".jmf_work_product_group\n" +
            "WHERE notified_date is not null) where work_productgroup_id in (%s) and inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_work_product_group order by inbound_ts desc limit 1)";

    public static String GET_PRODUCTGROUP_QUERY = "select * from(SELECT cast(product_group_id as integer) productgroup_id,\n" +
            "       product_group_code productgroup_code,\n" +
            "       product_group_description productgroup_description,\n" +
            "       cast(f_pricing_option as integer) f_pricing_option,\n" +
            "       cast(date_parse(effective_to_date, '%%Y-%%m-%%d %%H:%%i:%%s') as date) effective_to_date,\n" +
            "       inbound_ts\n" +
            "FROM "+ databaseEnv[1]+".jmf_product_group) where productgroup_id in (%s) and inbound_ts = (select inbound_ts from "+ databaseEnv[1]+".jmf_product_group order by inbound_ts desc limit 1)";

//  GET ETL IDs SQL
    public static String GET_JM_SOURCE_REFERENCE_IDs ="select jm_source_reference as JM_SOURCE_REFERENCE from " + GetJMDLDBUser.getJMDB() + ".%s where jm_source_reference not in ('null') order by rand() limit %s";
    public static String GET_JM_SOURCE_REF_NEW_IDs ="select jm_source_ref_new as JM_SOURCE_REF_NEW from " + GetJMDLDBUser.getJMDB() + ".%s where jm_source_ref_new not in ('null') order by rand() limit %s";
    public static String GET_SD_SUBJECT_AREAS_IDs ="select sa_id as SA_ID from "+ GetJMDLDBUser.getJMDB()+".sd_subject_areas_v order by rand() limit %s";
    public static String GET_MANIFESTATION_UPDATES1_IDs ="select w0_chronicle_id as W0_CHRONICLE_ID from " + GetJMDLDBUser.getJMDB() + ".etl_manifestation_updates1_v order by rand() limit %s";
    public static String GET_WORK_LEGAL_OWNER_DQ_IDs ="select work_external_ref as WORK_EXTERNAL_REF from "+ GetJMDLDBUser.getJMDB()+".etl_work_legal_owner_dq_V order by rand() limit %s";
    public static String GET_JM_EXTERNAL_REFERENCE_IDs ="select external_reference as EXTERNAL_REFERENCE from " + GetJMDLDBUser.getJMDB() + ".%s where external_reference not in ('null') order by rand() limit %s";


//  Transformed ETL Query SQL
    public static String GET_ACCOUNTABLE_PRODUCT_DQ_QUERY ="select * from \n" +
        "(\n" +
        "select cs.chronicle_scenario_name as                                       scenario_name,\n" +
        "       wc.chronicle_scenario_code as                                       scenario_code,\n" +
        "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
        "            when 'N' then 'Insert'\n" +
        "            when 'Y' then 'Update'\n" +
        "            else 'Update'\n" +
        "        END) as                                                            upsert,\n" +
        "       'J0'||w.elsevier_journal_number||w.hfm_hierarchy_position_code      jm_source_reference,\n" +
        "       'J0'||w.elsevier_journal_number                                     acc_prod_id,\n" +
        "        w.hfm_hierarchy_position_code as                                   hfm_hierarchy_position_code,\n" +
        "        w.work_title as                                                    work_title,\n" +
        "       'N'  as                                                             dq_err,\n" +
        "        w.notified_date as                                                 notified_date\n" +
        "from  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w\n" +
        "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on wc.work_chronicle_id       = w.work_chronicle_id\n" +
        "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on cs.chronicle_scenario_code = wc.chronicle_scenario_code\n" +
        "where w.work_journey_identifier = 'A1'\n" +
        "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
        "and   w.notified_date is not null\n" +
        ")where jm_source_reference in ('%s') \n" +
        "order by jm_source_reference desc";

    public static String GET_WWORK_DQ_QUERY ="select * from "+ GetJMDLDBUser.getJMDB()+".%s where jm_source_reference in ('%s') order by jm_source_reference desc,scenario_name desc, work_title desc";

    public static String GET_WORK_IDENTIFIER_DQ_QUERY ="select * from (select cs.chronicle_scenario_name as                                      scenario_name,\n" +
            "       wc.chronicle_scenario_code as                                      scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                           upsert,\n" +
            "        CAST (null as varchar) as                                                        jm_source_ref_old, -- for new journals, elsevier journal number is populated on the 'A1' record only.\n" +
            "       'J0'||w.elsevier_journal_number||'-JOURNAL-NUMBER-'||w.elsevier_journal_number as jm_source_ref_new,\n" +
            "        w.eph_work_id as                                                  eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number as                                 work_source_reference,\n" +
            "        CAST ('ELSEVIER JOURNAL NUMBER' as varchar)                       f_type,\n" +
            "        CAST (null as varchar) as                                         identifier_old,\n" +
            "        w.elsevier_journal_number as                                      identifier_new,\n" +
            "        w.notified_date as                                                effective_start_date,\n" +
            "        'N' as                                                            dq_err,\n" +
            "        w.notified_date as                                                notified_date\n" +
            "from  " + GetJMDLDBUser.getJMDB() + ".jmf_work               w\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   w.notified_date is not null\n" +
            "UNION\n" +
            "select cs.chronicle_scenario_name as                                      scenario_name,\n" +
            "       wc.chronicle_scenario_code as                                      scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                           upsert,\n" +
            "        CAST (null as varchar) as                                                     jm_source_ref_old, -- for new journals, acronym is populated on the 'A1' record only.\n" +
            "       'J0'||w.elsevier_journal_number||'-JOURNAL-ACRONYM-'||w.journal_acronym_pts as jm_source_ref_new,\n" +
            "        w.eph_work_id as                                                  eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number as                                 work_source_reference,\n" +
            "        CAST ('JOURNAL ACRONYM' as varchar)                               f_type,\n" +
            "        CAST (null as varchar) as                                         identifier_old,\n" +
            "        w.journal_acronym_pts as                                          identifier_new,\n" +
            "        w.notified_date as                                                effective_start_date,\n" +
            "        'N' as                                                            dq_err,\n" +
            "        w.notified_date as                                                notified_date\n" +
            "from  " + GetJMDLDBUser.getJMDB() + ".jmf_work               w\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   w.notified_date is not null\n" +
            "UNION\n" +
            "select  cs.chronicle_scenario_name as                                     scenario_name,\n" +
            "        wc.chronicle_scenario_code as                                     scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                           upsert,\n" +
            "        CAST (null as varchar) as                                         jm_source_ref_old,\n" +
            "       'J0'||w.elsevier_journal_number||'-ISSN-L-'||w.issn_l as           jm_source_ref_new, -- for new journals ISSN-L is populated on the 'A1' record only.\n" +
            "        w.eph_work_id as                                                  eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number as                                 work_source_reference,\n" +
            "        CAST ('ISSN-L' as varchar)                                        f_type,\n" +
            "        CAST (null as varchar) as                                         identifier_old,\n" +
            "        w.issn_l as                                                       identifier_new,\n" +
            "        w.notified_date as                                                effective_start_date,\n" +
            "        'N' as                                                            dq_err,\n" +
            "        w.notified_date as                                                notified_date\n" +
            "from  " + GetJMDLDBUser.getJMDB() + ".jmf_work               w\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   w.notified_date is not null\n" +
            "UNION\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE - ISSN-L\n" +
            "-- RENAME SCENARIO. FIND OUT IF ISSN-L HAS CHANGED. IF IT HAS, WRITE A NEW WORK_IDENTIFIER RECORD\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "--\n" +
            "select  cs.chronicle_scenario_name as                                                                   scenario_name,\n" +
            "        wc.chronicle_scenario_code as                                                                   scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                                                         upsert,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)||'-ISSN-L-'||w0.issn_l as jm_source_ref_old,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)||'-ISSN-L-'||w1.issn_l as jm_source_ref_new,    -- both jnl Nos should be set; A0 more reliable\n" +
            "        COALESCE(w0.eph_work_id, w1.eph_work_id) as                                                     eph_work_id,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)                           work_source_reference,\n" +
            "        CAST ('ISSN-L' as varchar)                                                                      f_type,               -- \"ISSN-L\"\n" +
            "        w0.issn_l as                                                                                    identifier_old,       --  old ISSN-L value\n" +
            "        w1.issn_l as                                                                                    identifier_new,       --  new ISSN-L value\n" +
            "        w1.notified_date as                                                                             effective_start_date,\n" +
            "       (CASE\n" +
            "            when (w1.eph_work_id             is null and w0.eph_work_id             is null) then 'Y'\n" +
            "            when (w1.elsevier_journal_number is null and w0.elsevier_journal_number is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                                                                                         dq_err,\n" +
            "        COALESCE(w1.notified_date, w0.notified_date) as                                                 notified_date         -- they should both be set the same\n" +
            "from  (((" + GetJMDLDBUser.getJMDB() + ".jmf_work                     w0\n" +
            "         join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on  (w0.work_chronicle_id       = wc.work_chronicle_id))\n" +
            "         join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "         left join " + GetJMDLDBUser.getJMDB() + ".jmf_work           w1 on ((w1.work_chronicle_id       = w0.work_chronicle_id)\n" +
            "                                        and  (w1.work_journey_identifier = 'A1')))\n" +
            "where  w0.work_journey_identifier = 'A0'\n" +
            "and    wc.chronicle_scenario_code = 'RN'\n" +
            "and    w1.notified_date is not null\n" +
            "and    w0.issn_l is not null\n" +
            "and    w1.issn_l is not null\n" +
            "and    w1.issn_l <> w0.issn_l\n" +
            "UNION\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE - PTS ACRONYM\n" +
            "-- RENAME SCENARIO. FIND OUT IF PTS ACRONYM HAS CHANGED. IF IT HAS, WRITE A NEW WORK_IDENTIFIER RECORD FOR \"ACRONYM\".\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "--\n" +
            "select  cs.chronicle_scenario_name as                                                                scenario_name,\n" +
            "        wc.chronicle_scenario_code as                                                                scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                                                      upsert,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)||'-JOURNAL-ACRONYM-'||w0.journal_acronym_pts as jm_source_ref_old,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)||'-JOURNAL-ACRONYM-'||w1.journal_acronym_pts as jm_source_ref_new, -- both jnl Nos should be set; A0 is more reliable in the test data\n" +
            "        COALESCE(w0.eph_work_id, w1.eph_work_id) as                                                  eph_work_id,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number) as                     work_source_reference,\n" +
            "        CAST ('JOURNAL ACRONYM' as varchar)                                                          f_type,                 -- type is \"JOURNAL ACRONYM\"\n" +
            "        w0.journal_acronym_pts as                                                                    identifier_old,         -- old PTS Acronym value\n" +
            "        w1.journal_acronym_pts as                                                                    identifier_new,         -- new PTS Acronym value\n" +
            "        w1.notified_date as                                                                          effective_start_date,\n" +
            "       (CASE\n" +
            "            when (w1.eph_work_id             is null and w0.eph_work_id             is null) then 'Y'\n" +
            "            when (w1.elsevier_journal_number is null and w0.elsevier_journal_number is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                                                                                      dq_err,\n" +
            "        COALESCE(w1.notified_date, w0.notified_date) as                                              notified_date        -- they should both be set the same\n" +
            "from  (((" + GetJMDLDBUser.getJMDB() + ".jmf_work                     w0\n" +
            "         join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on  (w0.work_chronicle_id       = wc.work_chronicle_id))\n" +
            "         join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "         left join " + GetJMDLDBUser.getJMDB() + ".jmf_work           w1 on ((w1.work_chronicle_id       = w0.work_chronicle_id)\n" +
            "                                        and  (w1.work_journey_identifier = 'A1')))\n" +
            "where  w0.work_journey_identifier = 'A0'\n" +
            "and    wc.chronicle_scenario_code = 'RN'\n" +
            "and    w1.notified_date is not null\n" +
            "and    w0.journal_acronym_pts is not null\n" +
            "and    w1.journal_acronym_pts is not null\n" +
            "and    w1.journal_acronym_pts <> w0.journal_acronym_pts\n" +
            ") where jm_source_ref_new in ('%s') order by jm_source_ref_new desc, eph_work_id desc, scenario_name desc";

    public static String GET_WORK_SUBJECT_AREA_DQ_QUERY ="select * from \n" +
            "(select  cs.chronicle_scenario_name as    scenario_name,\n" +
            "        cs.chronicle_scenario_code as    scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "        concat(concat(concat('J0', w.elsevier_journal_number), '-'), wsa.subject_area_code) as jm_source_reference,\n" +
            "        w.eph_work_id                   eph_work_id,\n" +
            "        concat('J0', w.elsevier_journal_number)  work_source_reference,\n" +
            "        CAST (null as integer)          f_subject_area,\n" +
            "        w.elsevier_journal_number as    elsevier_journal_number,\n" +
            "        wsa.subject_area_code as        subject_area_code,\n" +
            "        wsa.subject_area_type_code as   subject_area_type,\n" +
            "        wsa.notified_date as            start_date,\n" +
            "        CAST (null as date)             end_date,\n" +
            "       'N'  as                          dq_err,\n" +
            "        wsa.notified_date as            notified_date\n" +
            "from  " + GetJMDLDBUser.getJMDB() + ".jmf_work_subject_area  wsa\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work               w  on w.work_id = wsa.f_work\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where wsa.subject_area_type_code = 'SD'\n" +
            "and   wsa.notified_date is not null\n" +
            "and   w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')) where jm_source_reference in ('%s') order by jm_source_reference, eph_work_id";

    public static String GET_WORK_PERSON_ROLE_DQ_QUERY ="select * from (select distinct\n" +
            "       scenario_name,\n" +
            "       scenario_code,\n" +
            "       upsert,\n" +
            "       concat(work_source_reference,f_role,lower(to_hex(md5(to_utf8(employee_number_new))))) jm_source_ref_new,\n" +
            "       concat(work_source_reference,f_role,lower(to_hex(md5(to_utf8(employee_number_old))))) jm_source_ref_old,\n" +
            "       eph_work_id,\n" +
            "       work_source_reference,\n" +
            "       f_person,\n" +
            "       lower(to_hex(md5(to_utf8(employee_number_new)))) employee_number_new,\n" +
            "       lower(to_hex(md5(to_utf8(employee_number_old)))) employee_number_old,\n" +
            "       elsevier_journal_number,\n" +
            "       f_role,\n" +
            "       internal_email_old,\n" +
            "       internal_email_new,\n" +
            "       start_date,\n" +
            "       end_date,\n" +
            "       dq_err,\n" +
            "       notified_date\n" +
            "from (\n" +
            "-- PUBLISHERS\n" +
            "SELECT DISTINCT\n" +
            "       cs.chronicle_scenario_name as    scenario_name,\n" +
            "       wc.chronicle_scenario_code as    scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "        CAST (null as varchar) as                                     jm_source_ref_old, -- for new journals there is no 'old'\n" +
            "       'J0'||w.elsevier_journal_number||'-'||wpr.peoplehub_id||'-PU'  jm_source_ref_new,\n" +
            "        w.eph_work_id as                eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number  work_source_reference,\n" +
            "        wpr.eph_person_id as            f_person,\n" +
            "        w.elsevier_journal_number as    elsevier_journal_number,\n" +
            "        CAST (null as varchar) as       employee_number_old, -- for new journals there is no 'old'\n" +
            "        wpr.peoplehub_id as             employee_number_new,\n" +
            "       'PU' as                          f_role,\n" +
            "        CAST (null as varchar) as       internal_email_old,  -- for new journals there is no 'old'\n" +
            "        lower(wpr.email_address) as     internal_email_new,\n" +
            "        wpr.notified_date as            start_date,\n" +
            "        CAST (null as date)             end_date,\n" +
            "       'N'  as                          dq_err,\n" +
            "        wpr.notified_date as            notified_date\n" +
            "from  " + GetJMDLDBUser.getJMDB() + ".jmf_work_person_role   wpr\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work               w  on w.work_id = wpr.f_work\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on wc.work_chronicle_id = w.work_chronicle_id\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on cs.chronicle_scenario_code = wc.chronicle_scenario_code\n" +
            "where wpr.party_role_type in ('PPC', 'PU')\n" +
            "and   wpr.notified_date is not null\n" +
            "and   w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "UNION\n" +
            "-- PUBLISHING DIRECTORS\n" +
            "SELECT DISTINCT\n" +
            "       cs.chronicle_scenario_name as    scenario_name,\n" +
            "       wc.chronicle_scenario_code as    scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "        CAST (null as varchar) as                                         jm_source_ref_old, -- for new journals there is no 'old'\n" +
            "       'J0'||w.elsevier_journal_number||'-'||warp.pd_peoplehub_id||'-PD'  jm_source_ref_new,\n" +
            "        w.eph_work_id as                eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number  work_source_reference,\n" +
            "        wpr.eph_person_id as            f_person,\n" +
            "        w.elsevier_journal_number as    elsevier_journal_number,\n" +
            "        CAST (null as varchar) as       employee_number_old, -- for new journals there is no 'old'\n" +
            "        warp.pd_peoplehub_id as         employee_number_new,\n" +
            "        'PD' as                         f_role,\n" +
            "        CAST (null as varchar) as       internal_email_old,  -- for new journals there is no 'old'\n" +
            "        lower(warp.pd_email) as         internal_email_new,\n" +
            "        wpr.notified_date as            start_date,\n" +
            "        CAST (null as date)             end_date,\n" +
            "       'N'  as                          dq_err,\n" +
            "        wpr.notified_date as            notified_date\n" +
            "from  " + GetJMDLDBUser.getJMDB() + ".jmf_work_person_role   wpr\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work               w  on w.work_id = wpr.f_work\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on wc.work_chronicle_id = w.work_chronicle_id\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on cs.chronicle_scenario_code = wc.chronicle_scenario_code\n" +
            "join " + GetJMDLDBUser.getJMDB() + ".works_attrs_roles_people_v warp on\n" +
            "         (warp.pmgcode = w.pmg_code\n" +
            "      and warp.pmccode = w.pmc_code\n" +
            "      and warp.pd_email is not null\n" +
            "      and warp.pd_email <> 'Not Found')\n" +
            "--    this last join is to pick up PD email and peoplehub_id from works_attrs_roles_people_v (returns active journals)\n" +
            "--    for the given pmgcode and pmccode whilst filtering out missing PDs\n" +
            "where wpr.party_role_type in ('PUBDIR','PD')\n" +
            "and   wpr.notified_date is not null\n" +
            "and   w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "UNION\n" +
            "-- BUSINESS CONTROLLERS\n" +
            "SELECT DISTINCT\n" +
            "       cs.chronicle_scenario_name as    scenario_name,\n" +
            "       wc.chronicle_scenario_code as    scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "        CAST (null as varchar) as                                         jm_source_ref_old,\n" +
            "       'J0'||w.elsevier_journal_number||'-'||warp.bc_peoplehub_id||'-BC'  jm_source_ref_new,\n" +
            "        w.eph_work_id as                eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number  work_source_reference,\n" +
            "        CAST (null as varchar)          f_person, -- eph_person_id\n" +
            "        w.elsevier_journal_number as    elsevier_journal_number,\n" +
            "        CAST (null as varchar) as       employee_number_old, -- for new journals there is no 'old'\n" +
            "        warp.bc_peoplehub_id as         employee_number_new,\n" +
            "        'BC' as                         f_role,\n" +
            "        CAST (null as varchar) as       internal_email_old,  -- for new journals there is no 'old'\n" +
            "        lower(warp.bc_email)            internal_email_new,\n" +
            "        w.notified_date as              start_date,\n" +
            "        CAST (null as date)             end_date,\n" +
            "       'N'  as                          dq_err,\n" +
            "        w.notified_date as              notified_date\n" +
            "from  " + GetJMDLDBUser.getJMDB() + ".jmf_work               w\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on wc.work_chronicle_id = w.work_chronicle_id\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on cs.chronicle_scenario_code = wc.chronicle_scenario_code\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".works_attrs_roles_people_v warp on\n" +
            "         (warp.resp_cen = w.responsibility_centre_code\n" +
            "      and warp.bc_email is not null\n" +
            "      and warp.bc_email <> 'Not Found')\n" +
            "--    this last join is to pick up BC email and peoplehub_id from works_attrs_roles_people_v\n" +
            "--    for the given responsibility_centre_code. JM only holds BC's names.\n" +
            "where w.notified_date is not null\n" +
            "and   w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "UNION\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE PUBLISHER\n" +
            "-- there are two possible sources - (1) jmf_family_resource_details has been drawn up into jmf_work.\n" +
            "--                                  (2) jmf_work_person_role for scenario 'CA'\n" +
            "-- 'CA' CHANGE ALLOCATED RESOURCE for PUBLISHERS\n" +
            "-- both have the same PPC/PU data (checked yesterday) so for future-proofing and alignment with EPH\n" +
            "-- I've taken jmf_work_person_role\n" +
            "-- Notes\n" +
            "--     a) for PMC changes we do still need to access via jmf_work dq.\n" +
            "--     b) warning - the PUBDIR entries on jmf_work_person_role are insert scenarios not updates.\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "--\n" +
            "-- 27-Jan-2021 we now need to send the old and new peoplehub_IDs. For updates, peoplehubid_old/new are on jmf_work table\n" +
            "-- so we have to go to the merged jmf_work, A1 record, to look for pu_peoplehubid_old and pu_peoplehubid_new\n" +
            "-- ditto for pu_email_old and new\n" +
            "--\n" +
            "SELECT DISTINCT\n" +
            "       cs.chronicle_scenario_name as                                       scenario_name,\n" +
            "       wc.chronicle_scenario_code as                                       scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                             upsert,\n" +
            "       'J0'||w1.elsevier_journal_number||'-'||w1.pu_peoplehubid_old||'-PU' as jm_source_ref_old,\n" +
            "       'J0'||w1.elsevier_journal_number||'-'||w1.pu_peoplehubid_new||'-PU' as jm_source_ref_new,\n" +
            "        COALESCE(w1.eph_work_id, w0.eph_work_id) as                         eph_work_id,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number) as work_source_reference,\n" +
            "        wpr.eph_person_id as                                                f_person,\n" +
            "        COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number) as elsevier_journal_number,\n" +
            "        w1.pu_peoplehubid_old as                                            employee_number_old,\n" +
            "        w1.pu_peoplehubid_new as                                            employee_number_new,\n" +
            "       'PU' as                                                              f_role,\n" +
            "        lower(w1.pu_email_old) as                                           internal_email_old,\n" +
            "        lower(w1.pu_email_new) as                                           internal_email_new,\n" +
            "        wpr.notified_date as                                                start_date,\n" +
            "        CAST (null as date)                                                 end_date,\n" +
            "       (CASE\n" +
            "            when (w0.eph_work_id             is null and w1.eph_work_id             is null) then 'Y'\n" +
            "            when (w0.elsevier_journal_number is null and w1.elsevier_journal_number is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                                                             dq_err,\n" +
            "        wpr.notified_date as                                                notified_date\n" +
            "from  " + GetJMDLDBUser.getJMDB() + ".jmf_work_person_role  wpr\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work               w0 on w0.work_id = wpr.f_work\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on wc.work_chronicle_id = w0.work_chronicle_id\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on cs.chronicle_scenario_code = wc.chronicle_scenario_code\n" +
            "left join " + GetJMDLDBUser.getJMDB() + ".jmf_work           w1 on w1.work_chronicle_id       = w0.work_chronicle_id\n" +
            "                               and w1.elsevier_journal_number = w0.elsevier_journal_number\n" +
            "                               and w1.work_journey_identifier = 'A1'\n" +
            "where wpr.party_role_type in ('PPC', 'PU')\n" +
            "and   w0.work_journey_identifier = 'A0'\n" +
            "and   wc.chronicle_scenario_code = 'CA'\n" +
            "and   wpr.notified_date is not null\n" +
            "UNION\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE PUBLISHING DIRECTOR\n" +
            "-- JM updates ALL journals for the new PD assigned to a given PMG. Note: ALL journals, not just active journals.\n" +
            "-- This is why we need to use pmg_pmc_journals_v not works_attrs_roles_people_v.\n" +
            "-- Updates are passed to EPH by WORK PERSON ROLE as if they had come through from jmf_work_person_role\n" +
            "-- (In JMF this is the current model for PU changes, but not yet for PD changes.)\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE EPH  WORK_PERSON_ROLE\n" +
            "--\n" +
            "-- PMGCODE IS NOT IN THE OUTPUT, TO ALLOW THE UNION ALL TO WORK WITH PUBLISHER UPDATES.\n" +
            "-- THE ALTERNATIVE WOULD BE TO ADD PMGCODE TO ALL THE QUERIES.\n" +
            "--\n" +
            "SELECT DISTINCT\n" +
            "       cs.chronicle_scenario_name as       scenario_name,\n" +
            "       cs.chronicle_scenario_code as       scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "             when 'N' then 'Insert'\n" +
            "             when 'Y' then 'Update'\n" +
            "             else 'Update'\n" +
            "        END) as                            upsert,\n" +
            "       'J0'||ppj.jnl_no||'-'||ppa.pmg_pubdir_peoplehub_id_old||'-PD'  jm_source_ref_old,\n" +
            "       'J0'||ppj.jnl_no||'-'||ppa.pmg_pubdir_peoplehub_id_new||'-PD'  jm_source_ref_new,\n" +
            "        ppj.work_id as                     eph_work_id,\n" +
            "       'J0'||ppj.jnl_no                    work_source_reference,\n" +
            "        CAST (null as varchar)             f_person,\n" +
            "        ppj.jnl_no as                      elsevier_journal_number,\n" +
            "        ppa.pmg_pubdir_peoplehub_id_old as employee_number_old,\n" +
            "        ppa.pmg_pubdir_peoplehub_id_new as employee_number_new,\n" +
            "       'PD' as                             f_role,\n" +
            "        lower(ppa.pmg_pubdir_email_old) as internal_email_old,\n" +
            "        lower(ppa.pmg_pubdir_email_new) as internal_email_new,\n" +
            "        CAST(ppa.notified_date as date)    start_date,\n" +
            "        CAST (null as date)                end_date,\n" +
            "       'N'  as                             dq_err,\n" +
            "        ppa.notified_date as               notified_date\n" +
            "from  ((" + GetJMDLDBUser.getJMDB() + ".jmf_pmg_pubdir_allocation   ppa\n" +
            "join    " + GetJMDLDBUser.getJMDB() + ".pmg_pmc_journals_v          ppj on (ppj.pmgcode = ppa.pmx_pmgcode))\n" +
            "join    " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario       cs on (cs.chronicle_scenario_code = ppa.allocation_type))\n" +
            "where    ppa.allocation_type = 'PD'\n" +
            "and      ppa.notified_date is not null\n" +
            "  )\n" +
            "order by notified_date, jm_source_ref_new\n" +
            ") where jm_source_ref_new in ('%s') order by jm_source_ref_new desc, eph_work_id desc";

    public static String GET_MANIFESTATION_UPDATES1_QUERY ="select * from\n" +
            "(select cs.chronicle_scenario_name as               scenario_name,           -- 'Rename'\n" +
            "       wc.chronicle_scenario_code as               scenario_code,           -- 'RN'\n" +
            "      (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                    upsert,                  -- 'Update'\n" +
            "       w0.work_chronicle_id                        w0_chronicle_id,         -- 672, 1339, 1600 etc. journal numbers and work journey identifiers beneath (A0, A1).\n" +
            "       w0.elsevier_journal_number                  w0_journal_number,       -- from A0 work, format 12345\n" +
            "       w0.work_journey_identifier                  w0_journey_identifier,   -- proof that we're looking at the work A0.\n" +
            "       m0.manifestation_id                         m0_manifestation_id,     -- should be unique\n" +
            "       m0.manifestation_type                       m0_manifestation_type,   -- yes, we have E or P\n" +
            "       m0.elsevier_journal_number                  m0_journal_number,       -- format 12345\n" +
            "       w0.issn_l                                   w0_issn_l,               -- from A0 work, format 1234-5678, 1234-567X\n" +
            "       m0.manifestation_title                      m0_manifestation_title,  -- A0, from m0 manifestation\n" +
            "       m0.issn                                     m0_issn,                 -- format 1234-5678, 1234-567X\n" +
            "       w0.eph_work_id                              w0_eph_work_id,          -- from A0 work\n" +
            "       m0.eph_manifestation_id                     m0_eph_manifestation_id, -- from the manifestation pointing to the A0 work\n" +
            "       m0.notified_date                            m0_notified_date         -- should be stamped the same as m1_notified_date\n" +
            "from (((" + GetJMDLDBUser.getJMDB() + ".jmf_manifestation           m0                                      -- the joins and where clause select all rename manifestations 'before', both p & e.\n" +
            "        join " + GetJMDLDBUser.getJMDB() + ".jmf_work               w0 on ((m0.f_work = w0.work_id)\n" +
            "                                       and (w0.work_journey_identifier = 'A0')))\n" +
            "        join " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on ((w0.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                       and (w0.work_journey_identifier = 'A0')))\n" +
            "        join " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on ((wc.chronicle_scenario_code = cs.chronicle_scenario_code)\n" +
            "                                       and (wc.chronicle_scenario_code = 'RN')))\n" +
            "where    m0.notified_date is not null\n" +
            "group by cs.chronicle_scenario_name, wc.chronicle_scenario_code, cs.chronicle_scenario_evolutionary_ind,\n" +
            "         w0.work_chronicle_id, w0.elsevier_journal_number, w0.work_journey_identifier,\n" +
            "         m0.manifestation_id, m0.manifestation_type, m0.elsevier_journal_number, m0.manifestation_title, m0.issn, w0.issn_l, w0.eph_work_id, m0.eph_manifestation_id, m0.notified_date\n" +
            "order by w0.work_chronicle_id, w0.elsevier_journal_number, w0.work_journey_identifier,\n" +
            "         m0.manifestation_type, m0.elsevier_journal_number, m0.notified_date\n" +
            ") where w0_chronicle_id in (%s)";

    public static String GET_MANIFESTATION_IDENTIFIER_DQ_QUERY ="select * from \n" +
            "(select  cs.chronicle_scenario_name as scenario_name,\n" +
            "        wc.chronicle_scenario_code as scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                       upsert,\n" +
            "       CAST (null as varchar) as                                                   jm_source_ref_old,\n" +
            "       m.manifestation_type||'-J0'||w.elsevier_journal_number||'-ISSN-'||m.issn as jm_source_ref_new,\n" +
            "       w.eph_work_id as               eph_work_id,\n" +
            "       m.eph_manifestation_id as      eph_manifestation_id,\n" +
            "       m.manifestation_type||'-J0'||w.elsevier_journal_number as manifestation_source_reference,\n" +
            "       CAST ('ISSN' as varchar)       f_type,\n" +
            "       CAST (null as varchar) as      identifier_old,\n" +
            "       m.issn as                      identifier_new,\n" +
            "       m.notified_date as             effective_start_date,\n" +
            "      'N' as                          dq_err,\n" +
            "       m.notified_date as             notified_date\n" +
            "from  " + GetJMDLDBUser.getJMDB() + ".jmf_manifestation      m\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work               w  on m.f_work = w.work_id\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   m.notified_date is not null\n" +
            "UNION\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE - ISSN - RENAME SCENARIO, PART2.\n" +
            "-- FIND OUT IF THE MANIFESTATION ISSN HAS CHANGED.\n" +
            "-- IF IT HAS, WRITE A NEW RECORD TO EPH_MANIFESTATION_IDENTIFIER\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "--\n" +
            "-- Renames which aren't ISSN renames will be filtered out now, in part 2.\n" +
            "-- These are the fields provided by etl_manifestation_updates1_v:\n" +
            "-- scenario_name,           -- 'Rename'.\n" +
            "-- scenario_code,           -- 'RN'.\n" +
            "-- upsert,                  -- 'Update'\n" +
            "-- w0_chronicle_id          -- 672, 1339, 1600 etc. journal numbers and work journey identifiers beneath (A0, A1).\n" +
            "-- w0_journal_number        -- from A0 work, format 12345\n" +
            "-- w0_journey_identifier,   -- proof that we're looking at the work A0.\n" +
            "-- m0_manifestation_id,     -- should be unique\n" +
            "-- m0_manifestation_type,   -- yes, we have E or P\n" +
            "-- m0_journal_number,       -- format 12345\n" +
            "-- w0_issn_l                -- from A0 work, format 1234-5678, 1234-567X\n" +
            "-- m0_manifestation_title,  -- A0, from m0 manifestation\n" +
            "-- m0_issn,                 -- format 1234-5678, 1234-567X\n" +
            "-- w0_eph_work_id           -- from A0 work\n" +
            "-- m0_eph_manifestation_id  -- from the manifestation pointing to the A0 work\n" +
            "-- m0_notified_date         -- should be stamped the same as m1_notified_date\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "--\n" +
            "select\n" +
            "       mu1.scenario_name as                                               scenario_name,            -- 'Rename'. sent by updates1_v.\n" +
            "       mu1.scenario_code as                                               scenario_code,            -- 'RN'.     sent by updates1_v.\n" +
            "       mu1.upsert                                                         upsert,                   -- 'Update'\n" +
            "       mu1.m0_manifestation_type||'-J0'||mu1.w0_journal_number||'-ISSN-'||mu1.m0_issn as jm_source_ref_old,   -- from 'A0'\n" +
            "       mu1.m0_manifestation_type||'-J0'||mu1.w0_journal_number||'-ISSN-'||m2.issn as     jm_source_ref_new,   -- from 'A1'\n" +
            "       COALESCE(mu1.w0_eph_work_id, w1.eph_work_id)                    as eph_work_id,              -- eph_work_id          from A0/A1\n" +
            "       COALESCE(mu1.m0_eph_manifestation_id, m2.eph_manifestation_id)  as eph_manifestation_id,     -- eph_manifestation_id from A0/A1\n" +
            "       mu1.m0_manifestation_type||'-J0'||mu1.w0_journal_number         as manifestation_source_reference,\n" +
            "       CAST ('ISSN' as varchar)                                           f_type,                   -- \"ISSN\"\n" +
            "       mu1.m0_issn as                                                     identifier_old,           -- old ISSN value, 1234-5678\n" +
            "       m2.issn as                                                         identifier_new,           -- new ISSN value, 1234-5678\n" +
            "       m2.notified_date as                                                effective_start_date,\n" +
            "      (CASE\n" +
            "           when (mu1.w0_eph_work_id          is null and w1.eph_work_id          is null) then 'Y'\n" +
            "           when (mu1.m0_eph_manifestation_id is null and m2.eph_manifestation_id is null) then 'Y'\n" +
            "           else 'N'\n" +
            "       END) as                                                            dq_err,\n" +
            "       m2.notified_date as                                                notified_date\n" +
            "from (((" + GetJMDLDBUser.getJMDB() + ".etl_manifestation_updates1_v mu1\n" +
            "        join " + GetJMDLDBUser.getJMDB() + ".jmf_work                w1 on ((w1.work_chronicle_id       = mu1.w0_chronicle_id)\n" +
            "                                        and (w1.elsevier_journal_number = mu1.w0_journal_number)\n" +
            "                                        and (w1.work_journey_identifier = 'A1')))                   -- we've definitely got one work, the A1.\n" +
            "        join " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle      wc on ((w1.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                        and (w1.work_journey_identifier = 'A1')\n" +
            "                                        and (wc.chronicle_scenario_code = 'RN')))\n" +
            "        join " + GetJMDLDBUser.getJMDB() + ".jmf_manifestation       m2 on ((m2.f_work = w1.work_id)\n" +
            "                                        and (m2.elsevier_journal_number = mu1.m0_journal_number)\n" +
            "                                        and (m2.manifestation_type      = mu1.m0_manifestation_type)))\n" +
            "where m2.notified_date is not null\n" +
            "and   mu1.m0_issn is not null\n" +
            "and   m2.issn is not null\n" +
            "and   m2.issn <> mu1.m0_issn\n" +
            "group by mu1.m0_manifestation_id, mu1.m0_manifestation_type,\n" +
            "         mu1.scenario_name, mu1.scenario_code, mu1.upsert, mu1.w0_journal_number, mu1.w0_eph_work_id,\n" +
            "         mu1.m0_eph_manifestation_id, m2.eph_manifestation_id, mu1.m0_issn, m2.issn, m2.notified_date,\n" +
            "         w1.eph_work_id, m2.eph_manifestation_id\n" +
            ")where jm_source_ref_new in ('%s') order by jm_source_ref_new desc, eph_work_id desc";

    public static String GET_PRODUCT_PART1_QUERY ="select * from \n" +
            "(\n" +
            "select  cs.chronicle_scenario_name as                             scenario_name,\n" +
            "        cs.chronicle_scenario_code as                             scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "             when 'N' then 'Insert'\n" +
            "             when 'Y' then 'Update'\n" +
            "             else 'Update'\n" +
            "        END) as                                                   upsert,\n" +
            "        m.manifestation_type||'-J0'||w.elsevier_journal_number as jm_source_reference,             -- format E-J012345 or P-J012345\n" +
            "        w.eph_work_id as                                          eph_work_id,                     -- format EPR-W-xxxxxx\n" +
            "        m.eph_manifestation_id as                                 eph_manifestation_id,            -- format EPR-M-xxxxxx\n" +
            "        CAST(null as varchar)                                     eph_product_id,                  -- format EPR-xxxxxx the finest grain. set null for new journals.\n" +
            "       (CASE m.manifestation_type\n" +
            "             when 'E' then m.manifestation_title||' (Online)'\n" +
            "             else          m.manifestation_title||' (Print)'\n" +
            "        END) as                                                   base_title,                      -- (JM-manifestation-title with a suffix of (Online) or (Print))\n" +
            "        w.elsevier_journal_number                                 w0_journal_number,\n" +
            "        m.elsevier_journal_number                                 m0_journal_number,\n" +
            "        w.work_chronicle_id as                                    w0_chronicle_id,\n" +
            "        w.work_journey_identifier as                              w0_journey_identifier,\n" +
            "        m.manifestation_type as                                   m0_manifestation_type,           -- (P)rint or (E)lectronic\n" +
            "        CAST (1 as boolean) as                                    separately_saleable_ind,         -- set TRUE for all new journal products.\n" +
            "        CAST (0 as boolean) as                                    trial_allowed_ind,               -- set FALSE for all new journal products.\n" +
            "        CAST(w.launch_date as date)                               launch_date,\n" +
            "       (CASE m.manifestation_type\n" +
            "             when 'P' then 'G003'\n" +
            "             else          'S001'\n" +
            "        END) as                                                   tax_code,                         -- for print set = 'G003', for electronic set = 'S001'\n" +
            "       (CASE when (w.open_accesstype_code = 'SM5')                                          then 'F'\n" +
            "             when (w.open_accesstype_code = 'SM6')                                          then 'S'\n" +
            "             when (w.open_accesstype_code is null and w.manifestation_types_code = 'SM4')   then 'H'\n" +
            "             when (w.open_accesstype_code is null and w.manifestation_types_code = 'SM4SO') then 'N'\n" +
            "             else null\n" +
            "        END) as                                                   open_access_journal_type,        -- build as (F)ull-10, (H)ybrid-11, (S)ubsidised-12, (N)one-2, null.\n" +
            "       (CASE when (COALESCE (w.open_accesstype_code,'XYZ') in ('SM5','SM6')) then 'N'\n" +
            "             else                                                                 'Y'\n" +
            "        END) as                                                   subscription,                    -- set N for (Full/10/SM5 or Subsidised/12/SM6)\n" +
            "       (CASE when (COALESCE (w.open_accesstype_code,'XYZ') not in ('SM5','SM6') and m.manifestation_type = 'P') then 'Y'\n" +
            "             else                                                                                                    'N'\n" +
            "        END) as                                                   bulk_sales,                      -- set Y for Print manifestations which aren't (Full/SM5 or Subsidised/SM6)\n" +
            "       (CASE when (COALESCE (w.open_accesstype_code,'XYZ') not in ('SM5','SM6') and m.manifestation_type = 'E') then 'Y'\n" +
            "             else                                                                                                    'N'\n" +
            "        END) as                                                   back_files,                      -- set Y for Print manifestations which aren't (Full/SM5 or Subsidised/SM6)\n" +
            "       (CASE when (COALESCE (w.open_accesstype_code,'XYZ') in ('SM5','SM6'))              then 'Y'\n" +
            "             when (w.open_accesstype_code is null and w.manifestation_types_code = 'SM4') then 'Y'\n" +
            "             else                                                                              'N'\n" +
            "        END) as                                                   open_access,                     -- set Y when oatc in (F,H,S) else N.\n" +
            "       (CASE m.manifestation_type\n" +
            "             when 'P' then 'Y'\n" +
            "             else 'N'\n" +
            "        END) as                                                   reprints,                        -- yes, print manifestations can be reprinted.\n" +
            "        CAST('Y' as varchar) as                                   author_charges,                  -- set 'Y' for new journal inserts\n" +
            "        CAST('N' as varchar) as                                   one_off_access,                  -- set N for new journals\n" +
            "        CAST('N' as varchar) as                                   packages,                        -- set N for new journals\n" +
            "        CAST('PPL' as varchar) as                                 availability_status,             -- for new journals set to product, planned 'PPL'\n" +
            "        CAST('PPL' as varchar) as                                 work_status,                     -- for new journals set to product, planned 'PPL'\n" +
            "        w.work_title as                                           work_title,                      -- w.work_title\n" +
            "        CAST('JOURNAL' as varchar) as                             work_type,                       -- set 'JOURNAL'.\n" +
            "        lower(wpr.email_address) as                               internal_email_new,              -- to be used by etl_product_person_role_dq_v -- EPHD-2847\n" +
            "        CAST(null as varchar) as                                  internal_email_old,              -- not used for insert\n" +
            "        wpr.peoplehub_id as                                       employee_number_new,             -- to be used by etl_product_person_role_dq_v\n" +
            "        CAST(null as varchar) as                                  employee_number_old,             -- not used for insert\n" +
            "       (CASE when (w.elsevier_journal_number is null) then 'Y'\n" +
            "             when (m.elsevier_journal_number is null) then 'Y'\n" +
            "             when (m.manifestation_type      is null) then 'Y'\n" +
            "             when (wpr.email_address         is null) then 'Y'\n" +
            "             when (wpr.peoplehub_id          is null) then 'Y'                                     -- set 'Y' when any of the essential fields to build JM references are missing\n" +
            "             else                                          'N'                                     -- or when email or peoplehub_id are missing.\n" +
            "        END) as                                                   dq_err,\n" +
            "        m.notified_date as                                        notified_date\n" +
            "from ((((" + GetJMDLDBUser.getJMDB() + ".jmf_manifestation            m\n" +
            "         join  " + GetJMDLDBUser.getJMDB() + ".jmf_work               w   on  (m.f_work   = w.work_id))\n" +
            "    left join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_person_role   wpr on ((wpr.f_work = w.work_id)\n" +
            "                                          and (wpr.party_role_type in ('PPC','PU'))))\n" +
            "         join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc  on  (w.work_chronicle_id        = wc.work_chronicle_id))\n" +
            "         join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs  on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   m.notified_date is not null\n" +
            "--       the grouping was a necessary step for updates, handling both A0 & A1s, to handle duplicate manifestation primary keys in SIT. Harmless to include here, too.\n" +
            "group by cs.chronicle_scenario_name, cs.chronicle_scenario_code, cs.chronicle_scenario_evolutionary_ind,\n" +
            "         w.work_chronicle_id, w.elsevier_journal_number, w.work_journey_identifier, w.open_accesstype_code, w.manifestation_types_code,\n" +
            "         m.manifestation_id, w.launch_date, m.manifestation_type, m.elsevier_journal_number, m.manifestation_title,\n" +
            "         w.eph_work_id, w.work_title, m.eph_manifestation_id, m.notified_date, wpr.email_address, wpr.peoplehub_id\n" +
            "UNION\n" +
            "select  cs.chronicle_scenario_name as                             scenario_name,\n" +
            "        cs.chronicle_scenario_code as                             scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "             when 'N' then 'Insert'\n" +
            "             when 'Y' then 'Update'\n" +
            "             else 'Update'\n" +
            "        END) as                                                   upsert,\n" +
            "        m0.manifestation_type||'-J0'||w0.elsevier_journal_number as jm_source_reference,           -- format E-J012345 or P-J012345\n" +
            "        w0.eph_work_id                                            eph_work_id,                     -- from A0 work\n" +
            "        m0.eph_manifestation_id                                   eph_manifestation_id,            -- from A0 manifestation\n" +
            "        CAST(null as varchar)                                     eph_product_id,                  -- format EPR-xxxxxx the finest grain. JM does not have this detail. set null.\n" +
            "       (CASE m0.manifestation_type\n" +
            "             when 'E' then m0.manifestation_title||' (Online)'\n" +
            "             else          m0.manifestation_title||' (Print)'\n" +
            "        END) as                                                   base_title,                      -- (JM-manifestation-title with a suffix of (Online) or (Print))\n" +
            "        w0.elsevier_journal_number                                w0_journal_number,\n" +
            "        m0.elsevier_journal_number                                m0_journal_number,\n" +
            "        w0.work_chronicle_id as                                   w0_chronicle_id,\n" +
            "        w0.work_journey_identifier as                             w0_journey_identifier,\n" +
            "        m0.manifestation_type as                                  m0_manifestation_type,           -- (P)rint or (E)lectronic\n" +
            "        CAST (null as boolean) as                                 separately_saleable_ind,\n" +
            "        CAST (null as boolean) as                                 trial_allowed_ind,\n" +
            "        CAST (null as date) as                                    launch_date,\n" +
            "        CAST (null as varchar) as                                 tax_code,\n" +
            "        CAST (null as varchar) as                                 open_access_journal_type,\n" +
            "       (CASE when (COALESCE (w0.open_accesstype_code,'XYZ') in ('SM5','SM6')) then 'N'\n" +
            "             else                                                                  'Y'\n" +
            "        END) as                                                   subscription,                    -- set N for (Full/10/SM5 or Subsidised/12/SM6)\n" +
            "       (CASE when (COALESCE (w0.open_accesstype_code,'XYZ') not in ('SM5','SM6') and m0.manifestation_type = 'P') then 'Y'\n" +
            "             else                                                                                                      'N'\n" +
            "        END) as                                                   bulk_sales,                      -- set Y for Print manifestations which aren't (Full/SM5 or Subsidised/SM6)\n" +
            "       (CASE when (COALESCE (w0.open_accesstype_code,'XYZ') not in ('SM5','SM6') and m0.manifestation_type = 'E') then 'Y'\n" +
            "             else                                                                                                      'N'\n" +
            "        END) as                                                   back_files,                      -- set Y for Print manifestations which aren't (Full/SM5 or Subsidised/SM6)\n" +
            "       (CASE when (COALESCE (w0.open_accesstype_code,'XYZ') in ('SM5','SM6'))               then 'Y'\n" +
            "             when (w0.open_accesstype_code is null and w0.manifestation_types_code = 'SM4') then 'Y'\n" +
            "             else                                                                                'N'\n" +
            "        END) as                                                   open_access,                     -- set Y when oatc in (F,H,S) else N.\n" +
            "       (CASE m0.manifestation_type\n" +
            "             when 'P' then 'Y'\n" +
            "             else 'N'\n" +
            "        END) as                                                   reprints,                        -- yes, print manifestations can be reprinted.\n" +
            "        CAST('Y' as varchar) as                                   author_charges,\n" +
            "        CAST('N' as varchar) as                                   one_off_access,                  -- set N for new journals\n" +
            "        CAST('N' as varchar) as                                   packages,                        -- set N for new journals\n" +
            "        CAST(null as varchar) as                                  availability_status,\n" +
            "        CAST(null as varchar) as                                  work_status,                     -- for new journals set to product, planned 'PPL'\n" +
            "        w0.work_title as                                          w0_work_title,                   -- w.work_title\n" +
            "        CAST('JOURNAL' as varchar) as                             work_type,                       -- set 'JOURNAL'.\n" +
            "        CAST(null as varchar) as                                  internal_email_new,               -- set to null as not used for rename\n" +
            "        CAST(null as varchar) as                                  internal_email_old,               -- set to null as not used for rename\n" +
            "        CAST(null as varchar) as                                  employee_number_new,              -- set to null as not used for rename\n" +
            "        CAST(null as varchar) as                                  employee_number_old,              -- set to null as not used for rename\n" +
            "       (CASE when (w0.elsevier_journal_number is null) then 'Y'\n" +
            "             when (m0.elsevier_journal_number is null) then 'Y'\n" +
            "             when (m0.manifestation_type      is null) then 'Y'\n" +
            "             else                                           'N'                                    -- or when email or peoplehub_id are missing?\n" +
            "        END) as                                                   dq_err,\n" +
            "        m0.notified_date as                                       notified_date\n" +
            "--      the joins and where clause select all rename manifestations, A0 'before', both p & e.\n" +
            "--      the grouping is a necessary step to handle duplicate manifestation primary keys in SIT.\n" +
            "from ((((" + GetJMDLDBUser.getJMDB() + ".jmf_manifestation           m0\n" +
            "         join " + GetJMDLDBUser.getJMDB() + ".jmf_work               w0 on ((m0.f_work = w0.work_id)\n" +
            "                                        and (w0.work_journey_identifier = 'A0')))\n" +
            "    left join " + GetJMDLDBUser.getJMDB() + ".jmf_work_person_role  wpr on ((wpr.f_work = w0.work_id)\n" +
            "                                        and (wpr.party_role_type in ('PPC','PU'))))\n" +
            "         join " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc on ((w0.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                        and (w0.work_journey_identifier = 'A0')))\n" +
            "         join " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "where    wc.chronicle_scenario_code = 'RN'\n" +
            "and      m0.notified_date is not null\n" +
            "group by cs.chronicle_scenario_name, cs.chronicle_scenario_code, cs.chronicle_scenario_evolutionary_ind,\n" +
            "         w0.work_chronicle_id, w0.elsevier_journal_number, w0.work_journey_identifier, w0.open_accesstype_code, w0.manifestation_types_code,\n" +
            "         m0.manifestation_id, m0.manifestation_type, m0.elsevier_journal_number, m0.manifestation_title,\n" +
            "         w0.eph_work_id, w0.work_title, m0.eph_manifestation_id, m0.notified_date, wpr.email_address, wpr.peoplehub_id\n" +
            "UNION\n" +
            "select  cs.chronicle_scenario_name as                             scenario_name,\n" +
            "        cs.chronicle_scenario_code as                             scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "             when 'N' then 'Insert'\n" +
            "             when 'Y' then 'Update'\n" +
            "             else 'Update'\n" +
            "        END) as                                                   upsert,\n" +
            "        'J0'||w.elsevier_journal_number as                        jm_source_reference,             -- manifestation-level not available for PU changes\n" +
            "        w.eph_work_id as                                          eph_work_id,                     -- format EPR-W-xxxxxx\n" +
            "        CAST(null as varchar)                                     eph_manifestation_id,            -- manifestation-level not available for PU changes\n" +
            "        CAST(null as varchar)                                     eph_product_id,                  -- product-level not available in JM\n" +
            "        w.work_title as                                           base_title,                      -- should be (JM-manifestation-title with a suffix of (Online) or (Print))\n" +
            "        w.elsevier_journal_number                                 w0_journal_number,\n" +
            "        CAST(null as varchar)                                     m0_journal_number,\n" +
            "        w.work_chronicle_id as                                    w0_chronicle_id,\n" +
            "        w.work_journey_identifier as                              w0_journey_identifier,\n" +
            "        CAST(null as varchar) as                                  m0_manifestation_type,           -- manifestation-level not available for PU changes\n" +
            "        CAST (null as boolean) as                                 separately_saleable_ind,\n" +
            "        CAST (null as boolean) as                                 trial_allowed_ind,\n" +
            "        CAST (null as date)                                       launch_date,\n" +
            "        CAST(null as varchar) as                                  tax_code,\n" +
            "        CAST(null as varchar) as                                  open_access_journal_type,\n" +
            "       (CASE when (COALESCE (w.open_accesstype_code,'XYZ') in ('SM5','SM6')) then 'N'\n" +
            "             else                                                                 'Y'\n" +
            "        END) as                                                   subscription,\n" +
            "        CAST('N' as varchar)                                      bulk_sales,\n" +
            "        CAST('N' as varchar)                                      back_files,\n" +
            "       (CASE when (COALESCE (w.open_accesstype_code,'XYZ') in ('SM5','SM6')) then 'Y'\n" +
            "             else                                                                 'N'\n" +
            "        END) as                                                   open_access,                     -- set Y when oatc in (F,H,S) else N.\n" +
            "        CAST('Y' as varchar)                                      reprints,\n" +
            "        CAST('N' as varchar) as                                   author_charges,                  -- set 'N' for updates.\n" +
            "        CAST('N' as varchar) as                                   one_off_access,\n" +
            "        CAST('N' as varchar) as                                   packages,\n" +
            "        CAST(null as varchar) as                                  availability_status,\n" +
            "        CAST(null as varchar) as                                  work_status,\n" +
            "        w.work_title as                                           work_title,                      -- w.work_title\n" +
            "        CAST('JOURNAL' as varchar) as                             work_type,                       -- set 'JOURNAL'.\n" +
            "        lower(w.pu_email_new) as                                  internal_email_new,               -- to be used by etl_product_person_role_dq_v -- EPHD-2847\n" +
            "        lower(w.pu_email_old) as                                  internal_email_old,               -- to be used by etl_product_person_role_dq_v -- EPHD-2847\n" +
            "        w.pu_peoplehubid_new as                                   employee_number_new,              -- to be used by etl_product_person_role_dq_v\n" +
            "        w.pu_peoplehubid_old as                                   employee_number_old,              -- to be used by etl_product_person_role_dq_v\n" +
            "       (CASE when (w.elsevier_journal_number is null) then 'Y'\n" +
            "             when (w.pu_email_new            is null) then 'Y'\n" +
            "             when (w.pu_peoplehubid_new      is null) then 'Y'                                     -- set 'Y' when any of the essential fields to build JM references are missing\n" +
            "             else                                          'N'                                     -- or when email or peoplehub_id are missing.\n" +
            "        END) as                                                   dq_err,\n" +
            "        w.notified_date as                                        notified_date\n" +
            "from  ((" + GetJMDLDBUser.getJMDB() + ".jmf_work                      w\n" +
            "        join  " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc  on (w.work_chronicle_id        = wc.work_chronicle_id))\n" +
            "        join  " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs  on (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "where   w.work_journey_identifier  = 'A1'\n" +
            "and     wc.chronicle_scenario_code = 'CA'\n" +
            "and     w.notified_date is not null\n" +
            ") where jm_source_reference in ('%s') order by jm_source_reference desc, eph_work_id desc, scenario_name desc, w0_chronicle_id desc";

    public static String GET_PRODUCT_INSERTS_QUERY ="select * from(\n" +
            "with base_data as\n" +
            "(\n" +
            "select\n" +
            "scenario_name,                   -- scenario NAME  New Proprietary, etc\n" +
            "scenario_code,                   -- scenario CODE  NP, NS, AC, MI, etc\n" +
            "upsert,                          -- 'Insert'\n" +
            "jm_source_reference,             -- used to build jm_source_reference (manifestation)\n" +
            "concat('J0',w0_journal_number) as ult_work_ref, --work reference\n" +
            "eph_work_id,                     -- format EPR-W-xxxxxx\n" +
            "eph_manifestation_id,            -- format EPR-M-xxxxxx\n" +
            "eph_product_id,                  -- format EPR-xxxxxx new journals: set null\n" +
            "base_title,                      -- manifestation name with a suffix of (Print) or (Online). JM does not support short title.\n" +
            "w0_journal_number,\n" +
            "m0_journal_number,\n" +
            "w0_chronicle_id,\n" +
            "w0_journey_identifier,\n" +
            "m0_manifestation_type,           -- (P)rint or (E)lectronic\n" +
            "separately_saleable_ind,         -- set TRUE for all new journal products.\n" +
            "trial_allowed_ind,               -- set FALSE for all new journal products.\n" +
            "launch_date,                     -- w.launch_date\n" +
            "tax_code,                        -- yes, hard-code based on manifestation_type\n" +
            "open_access_journal_type,        -- build as (F)ull-10, (H)ybrid-11, (S)ubsidised-12, (N)one-2, null.\n" +
            "subscription,                    -- CASE oa jnl type not in (F,S).\n" +
            "bulk_sales,                      -- CASE oa jnl type not in (F,S) and manifestation type\n" +
            "back_files,                      -- CASE oa jnl type not in (F,S) and manifestation type\n" +
            "open_access,                     -- CASE oa jnl type, set Y when (F,H,S) else N.\n" +
            "reprints,                        -- yes, CASE based on manifestation type\n" +
            "author_charges,                  -- set 'Y' for new journals, 'N' for updates.\n" +
            "one_off_access,                  -- 'N' for new journals\n" +
            "packages,                        -- 'N' for new journals\n" +
            "availability_status,             -- for new journals set to planned available 'PPL'\n" +
            "work_status,                     -- for new journals set to planned available 'PPL'\n" +
            "work_title,                      -- w.product_work_title\n" +
            "work_type,                       -- set 'JOURNAL'\n" +
            "internal_email_old,              -- will be used by etl_product_person_role_dq_v\n" +
            "internal_email_new,              -- will be used by etl_product_person_role_dq_v\n" +
            "employee_number_old,             -- will be used by etl_product_person_role_dq_v\n" +
            "employee_number_new,             -- will be used by etl_product_person_role_dq_v\n" +
            "dq_err,                          -- default 'N', but set 'Y' (yes error) when PU email or PU peoplehub_id are missing.\n" +
            "notified_date\n" +
            "from  " + GetJMDLDBUser.getJMDB() + ".etl_product_part1_v\n" +
            "where notified_date is not null\n" +
            ")\n" +
            "-- select * from base_data\n" +
            ", crosstab_data as\n" +
            "(\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-SUB'      as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    base_title||' Subscription'      as \"name\",\n" +
            "    CASE\n" +
            "        when availability_status = 'PNS' then false\n" +
            "        else separately_saleable_ind\n" +
            "    END as separately_saleable_ind,\n" +
            "    trial_allowed_ind,\n" +
            "    launch_date,\n" +
            "    tax_code,\n" +
            "    'SUB' f_type,\n" +
            "    m0_manifestation_type,\n" +
            "    CASE\n" +
            "        when availability_status = 'PSTB' then 'PST'\n" +
            "        else availability_status\n" +
            "    END as f_status,\n" +
            "    work_type,\n" +
            "    jm_source_reference as manifestation_ref,\n" +
            "    ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where upsert       = 'Insert'\n" +
            "and   subscription = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-JBS' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    base_title||' Bulk Sales'        as \"name\",\n" +
            "    CASE\n" +
            "        when availability_status = 'PNS' then false\n" +
            "        else separately_saleable_ind\n" +
            "    END as separately_saleable_ind,\n" +
            "    trial_allowed_ind,\n" +
            "    launch_date,\n" +
            "    tax_code,\n" +
            "    'JBS' f_type,\n" +
            "    m0_manifestation_type,\n" +
            "    CASE\n" +
            "        when availability_status = 'PSTB' then 'PST'\n" +
            "        else availability_status\n" +
            "    END as f_status,\n" +
            "    work_type,\n" +
            "    jm_source_reference as manifestation_ref,\n" +
            "    ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where upsert     = 'Insert'\n" +
            "and   bulk_sales = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-BKF' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    base_title||' Back Files'        as \"name\",\n" +
            "    CASE\n" +
            "        when availability_status = 'PNS' then false\n" +
            "        else separately_saleable_ind\n" +
            "    END as separately_saleable_ind,\n" +
            "    trial_allowed_ind,\n" +
            "    launch_date,\n" +
            "    tax_code,\n" +
            "    'BKF' f_type,\n" +
            "    m0_manifestation_type,\n" +
            "    CASE\n" +
            "        when availability_status = 'PSTB' then 'PAS'\n" +
            "        else availability_status\n" +
            "    END as f_status,\n" +
            "    work_type,\n" +
            "    jm_source_reference as manifestation_ref,\n" +
            "    ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where upsert     = 'Insert'\n" +
            "and   back_files = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-RPR' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    base_title||' Reprints'          as \"name\",\n" +
            "    CASE\n" +
            "        when availability_status = 'PNS' then false\n" +
            "        else separately_saleable_ind\n" +
            "    END as separately_saleable_ind,\n" +
            "    trial_allowed_ind,\n" +
            "    launch_date,\n" +
            "    tax_code,\n" +
            "    'RPR' f_type,\n" +
            "    m0_manifestation_type,\n" +
            "    CASE\n" +
            "        when availability_status = 'PSTB' then 'PAS'\n" +
            "        else availability_status\n" +
            "    END as f_status,\n" +
            "    work_type,\n" +
            "    jm_source_reference as manifestation_ref,\n" +
            "    ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where upsert   = 'Insert'\n" +
            "and   reprints = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-OOA' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    base_title||' Purchase'          as \"name\",\n" +
            "    CASE\n" +
            "        when availability_status = 'PNS' then false\n" +
            "        else separately_saleable_ind\n" +
            "    END as separately_saleable_ind,\n" +
            "    trial_allowed_ind,\n" +
            "    launch_date,\n" +
            "    tax_code,\n" +
            "   'OOA' f_type,\n" +
            "    m0_manifestation_type,\n" +
            "    CASE\n" +
            "        when availability_status = 'PSTB' then 'PST'\n" +
            "        else availability_status\n" +
            "    END as f_status,\n" +
            "    work_type,\n" +
            "    jm_source_reference as manifestation_ref,\n" +
            "    ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where upsert         = 'Insert'\n" +
            "and   one_off_access = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    \"concat\"('J0',\"w0_journal_number\",'-OAA') as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    cast (null as varchar) as eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    work_title||' Article Publishing Charge' as \"name\",\n" +
            "    CASE\n" +
            "        when availability_status = 'PNS' then false\n" +
            "        else separately_saleable_ind\n" +
            "    END as separately_saleable_ind,\n" +
            "    trial_allowed_ind,\n" +
            "    launch_date,\n" +
            "   'S001' as tax_code,\n" +
            "   'OAA' f_type,\n" +
            "    null m0_manifestation_type,\n" +
            "    CASE\n" +
            "        when work_status = 'PSTB' then 'PST'\n" +
            "        else work_status\n" +
            "    END as f_status,\n" +
            "    work_type,\n" +
            "    cast(null as varchar) as manifestation_ref,\n" +
            "    ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where upsert      = 'Insert'\n" +
            "and   open_access = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    \"concat\"('J0',\"w0_journal_number\",'-JAS') as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    cast (null as varchar) as eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    work_title||' Author Charges' as \"name\",\n" +
            "    CASE\n" +
            "        when availability_status = 'PNS' then false\n" +
            "        else separately_saleable_ind\n" +
            "    END as separately_saleable_ind,\n" +
            "    trial_allowed_ind,\n" +
            "    launch_date,\n" +
            "   'S001' as tax_code,\n" +
            "   'JAS' f_type,\n" +
            "    null as m0_manifestation_type,\n" +
            "    CASE\n" +
            "        when work_status = 'PSTB' then 'PST'\n" +
            "        else work_status\n" +
            "    END as f_status,\n" +
            "    work_type,\n" +
            "    cast(null as varchar) as manifestation_ref,\n" +
            "    ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where upsert         = 'Insert'\n" +
            "and   author_charges = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    substr(jm_source_reference,3,7)||'-PKG' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    work_title                as \"name\",\n" +
            "    CASE\n" +
            "        when availability_status = 'PNS' then false\n" +
            "        else separately_saleable_ind\n" +
            "    END as separately_saleable_ind,\n" +
            "    trial_allowed_ind,\n" +
            "    launch_date,\n" +
            "    null as tax_code,\n" +
            "    'PKG' f_type,\n" +
            "    null as m0_manifestation_type,\n" +
            "    CASE\n" +
            "        when work_status = 'PSTB' then 'PST'\n" +
            "        else work_status\n" +
            "    END as f_status,\n" +
            "    work_type,\n" +
            "    jm_source_reference as manifestation_ref,\n" +
            "    substr(jm_source_reference,3,7) as ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where upsert   = 'Insert'\n" +
            "and   packages = 'Y'\n" +
            ")\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            ",result_data as\n" +
            "(\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_ref as jm_source_reference,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    name,\n" +
            "    CASE when separately_saleable_ind is null then false else separately_saleable_ind END as separately_saleable_ind,\n" +
            "    CASE when trial_allowed_ind   is null then false else trial_allowed_ind END as trial_allowed_ind,\n" +
            "    launch_date,\n" +
            "    f_type,\n" +
            "    f_status,\n" +
            "    case\n" +
            "        when (f_type in ('OOA', 'JAS', 'JBS', 'JBF', 'RPR'))                   then 'ONE'\n" +
            "        when (f_type = 'OAA' or (f_type = 'SUB' and m0_manifestation_type = 'P')) then 'EVE'\n" +
            "        else 'SUB'\n" +
            "    end as f_revenue_model,\n" +
            "    tax_code,\n" +
            "    work_type,\n" +
            "    manifestation_ref,\n" +
            "    ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from\n" +
            "crosstab_data) select * from result_data) where jm_source_reference in ('%s') order by jm_source_reference desc, eph_work_id desc, name desc";

    public static String GET_PRODUCT_UPDATES_QUERY ="select * from (with base_data as\n" +
            "(select\n" +
            "scenario_name,\n" +
            "scenario_code,\n" +
            "upsert,\n" +
            "jm_source_reference,\n" +
            "eph_work_id,\n" +
            "eph_manifestation_id,\n" +
            "eph_product_id,\n" +
            "base_title,\n" +
            "w0_journal_number,\n" +
            "m0_journal_number,\n" +
            "w0_chronicle_id,\n" +
            "w0_journey_identifier,\n" +
            "m0_manifestation_type,\n" +
            "separately_saleable_ind,\n" +
            "trial_allowed_ind,\n" +
            "launch_date,\n" +
            "tax_code,\n" +
            "open_access_journal_type,\n" +
            "subscription,\n" +
            "bulk_sales,\n" +
            "back_files,\n" +
            "open_access,\n" +
            "reprints,\n" +
            "author_charges,\n" +
            "one_off_access,\n" +
            "packages,\n" +
            "availability_status,\n" +
            "work_status,\n" +
            "work_title,\n" +
            "work_type,\n" +
            "-- pu_internal_email,         -- to be used by etl_product_person_role_dq_v\n" +
            "-- pu_employee_number,        -- to be used by etl_product_person_role_dq_v\n" +
            "dq_err,\n" +
            "notified_date\n" +
            "from  " + GetJMDLDBUser.getJMDB() + ".etl_product_part1_v\n" +
            "where notified_date is not null\n" +
            ")\n" +
            "--                                   -- = select * from base_data\n" +
            ", title_renames as\n" +
            "(\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- title renames selection here - ** SELECT scenario Rename/RN Updates only **\n" +
            "-- only populate the field which has changed - in this case product title\n" +
            "-- join base data m0, w0, to w1.\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "select  bd.scenario_name                                                  scenario_name,                 -- 'Rename'\n" +
            "        bd.scenario_code                                                  scenario_code,                 -- 'Rename'\n" +
            "        bd.upsert                                                         upsert,                   -- 'Update'\n" +
            "        bd.jm_source_reference                                            jm_source_reference,\n" +
            "        COALESCE(bd.eph_work_id, w1.eph_work_id) as                       eph_work_id,               -- format EPR-W-XXXXXX new journals: null. The owning work\n" +
            "        COALESCE(bd.eph_manifestation_id, m1.eph_manifestation_id) as     eph_manifestation_id,      -- format EPR-M-xxxxxx new journals: null\n" +
            "        bd.eph_product_id                                                 eph_product_id,            -- null\n" +
            "        bd.w0_journal_number as                                           w0_journal_number,\n" +
            "        bd.m0_journal_number as                                           m0_journal_number,\n" +
            "        bd.w0_chronicle_id                                                w0_chronicle_id,\n" +
            "        bd.w0_journey_identifier                                          w0_journey_identifier,\n" +
            "        bd.m0_manifestation_type                                          m0_manifestation_type,\n" +
            "        bd.base_title as                                                  m0_base_title,                 -- (JM-manifestation-title with a suffix of (Online) or (Print))\n" +
            "       (CASE m1.manifestation_type\n" +
            "             when 'E' then m1.manifestation_title||' (Online)'\n" +
            "             else          m1.manifestation_title||' (Print)'\n" +
            "        END) as                                                           m1_base_title,                 -- (JM-manifestation-title with a suffix of (Online) or (Print))\n" +
            "        bd.separately_saleable_ind                                        separately_saleable_ind,\n" +
            "        bd.trial_allowed_ind                                              trial_allowed_ind,\n" +
            "        bd.launch_date                                                    launch_date,\n" +
            "        bd.tax_code                                                       tax_code,\n" +
            "        bd.open_access_journal_type                                       open_access_journal_type,\n" +
            "        bd.subscription                                                   subscription,\n" +
            "        bd.bulk_sales                                                     bulk_sales,\n" +
            "        bd.back_files                                                     back_files,\n" +
            "        bd.open_access                                                    open_access,\n" +
            "        bd.reprints                                                       reprints,\n" +
            "        bd.author_charges                                                 author_charges,\n" +
            "        bd.one_off_access                                                 one_off_access,\n" +
            "        bd.packages                                                       packages,\n" +
            "        bd.availability_status                                            availability_status,\n" +
            "        bd.work_status                                                    work_status,\n" +
            "        w1.work_title                                                     work_title,\n" +
            "        bd.work_type                                                      work_type,\n" +
            "       (CASE\n" +
            "            when (bd.eph_work_id          is null and w1.eph_work_id          is null) then 'Y'\n" +
            "            when (bd.eph_manifestation_id is null and m1.eph_manifestation_id is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                                                           dq_err,\n" +
            "        m1.notified_date as                                               notified_date\n" +
            "from (((base_data               bd\n" +
            "        join " + GetJMDLDBUser.getJMDB() + ".jmf_work           w1 on ((w1.work_chronicle_id       = bd.w0_chronicle_id)\n" +
            "                                   and (w1.elsevier_journal_number = bd.w0_journal_number)\n" +
            "                                   and (w1.work_journey_identifier = 'A1')))                   -- we've definitely got one work, the A1.\n" +
            "        join " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle wc on ((w1.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                   and (w1.work_journey_identifier = 'A1')\n" +
            "                                   and (wc.chronicle_scenario_code = 'RN')))\n" +
            "        join " + GetJMDLDBUser.getJMDB() + ".jmf_manifestation  m1 on ((m1.f_work = w1.work_id)\n" +
            "                                   and (m1.elsevier_journal_number = bd.m0_journal_number)\n" +
            "                                   and (m1.manifestation_type      = bd.m0_manifestation_type)))\n" +
            "where m1.notified_date is not null\n" +
            "and   bd.scenario_code = 'RN'\n" +
            "and   bd.upsert   = 'Update'\n" +
            "and   bd.base_title is not null\n" +
            "and   m1.manifestation_title    is not null\n" +
            "and   m1.manifestation_title <> bd.base_title\n" +
            "group by bd.m0_manifestation_type, m1.manifestation_type,\n" +
            "         bd.scenario_code, bd.scenario_name, bd.upsert, bd.jm_source_reference,\n" +
            "         bd.m0_journal_number, bd.w0_chronicle_id, bd.w0_journey_identifier,\n" +
            "         bd.eph_work_id, bd.eph_manifestation_id, bd.eph_product_id,\n" +
            "         bd.base_title, m1.manifestation_title, m1.notified_date,\n" +
            "         w1.eph_work_id, bd.w0_journal_number, w1.elsevier_journal_number,\n" +
            "         bd.eph_manifestation_id, m1.eph_manifestation_id,\n" +
            "         bd.separately_saleable_ind, bd.trial_allowed_ind, bd.launch_date, bd.tax_code, bd.open_access_journal_type,\n" +
            "         bd.subscription, bd.bulk_sales, bd.back_files, bd.open_access, bd.reprints, bd.author_charges, bd.one_off_access,bd.packages,\n" +
            "         bd.availability_status, bd.work_status, w1.work_title, bd.work_type\n" +
            "order by bd.eph_work_id, w1.eph_work_id, bd.m0_manifestation_type, m1.manifestation_title, m1.notified_date\n" +
            ")\n" +
            ", crosstab_data as\n" +
            "(\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-SUB' as   jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    m1_base_title||' Subscription' as \"name\",\n" +
            "    CAST (null as boolean) as        separately_saleable_ind,\n" +
            "    CAST (null as boolean) as        trial_allowed_ind,\n" +
            "    CAST (null as date) as           launch_date,\n" +
            "    CAST (null as varchar) as        tax_code,\n" +
            "    'SUB' as                         f_type,\n" +
            "    m0_manifestation_type,\n" +
            "    CAST (null as varchar) as        f_status,\n" +
            "    work_type,\n" +
            "    cast (null as varchar) manifestation_ref,\n" +
            "    cast (null as varchar) as  ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from title_renames\n" +
            "where subscription = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-JBS' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    m1_base_title||' Bulk Sales' as \"name\",\n" +
            "    CAST (null as boolean) as       separately_saleable_ind,\n" +
            "    CAST (null as boolean) as       trial_allowed_ind,\n" +
            "    CAST (null as date) as          launch_date,\n" +
            "    CAST (null as varchar) as       tax_code,\n" +
            "    'JBS' as                        f_type,\n" +
            "    m0_manifestation_type,\n" +
            "    CAST (null as varchar) as       f_status,\n" +
            "    work_type,\n" +
            "    cast (null as varchar) manifestation_ref,\n" +
            "    cast (null as varchar) as  ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from title_renames\n" +
            "where bulk_sales = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-BKF' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    m1_base_title||' Back Files' as \"name\",\n" +
            "    CAST (null as boolean) as       separately_saleable_ind,\n" +
            "    CAST (null as boolean) as       trial_allowed_ind,\n" +
            "    CAST (null as date) as          launch_date,\n" +
            "    CAST (null as varchar) as       tax_code,\n" +
            "    'BKF' as                        f_type,\n" +
            "    m0_manifestation_type,\n" +
            "    CAST (null as varchar) as       f_status,\n" +
            "    work_type,\n" +
            "    cast (null as varchar) manifestation_ref,\n" +
            "    cast (null as varchar) as  ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from title_renames\n" +
            "where back_files = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-RPR' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    m1_base_title||' Reprints' as \"name\",\n" +
            "    CAST (null as boolean) as     separately_saleable_ind,\n" +
            "    CAST (null as boolean) as     trial_allowed_ind,\n" +
            "    CAST (null as date) as        launch_date,\n" +
            "    CAST (null as varchar) as     tax_code,\n" +
            "    'RPR' as                      f_type,\n" +
            "    m0_manifestation_type,\n" +
            "    CAST (null as varchar) as     f_status,\n" +
            "    work_type,\n" +
            "    cast (null as varchar) manifestation_ref,\n" +
            "    cast (null as varchar) as  ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from title_renames\n" +
            "where reprints = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-OOA' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    m1_base_title||' Purchase' as  \"name\",\n" +
            "    CAST (null as boolean) as      separately_saleable_ind,\n" +
            "    CAST (null as boolean) as      trial_allowed_ind,\n" +
            "    CAST (null as date) as         launch_date,\n" +
            "    CAST (null as varchar) as      tax_code,\n" +
            "   'OOA' as                        f_type,\n" +
            "    m0_manifestation_type,\n" +
            "    CAST (null as varchar) as      f_status,\n" +
            "    work_type,\n" +
            "    cast (null as varchar) manifestation_ref,\n" +
            "    cast (null as varchar) as  ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from title_renames\n" +
            "where one_off_access = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    \"concat\"('J0',\"w0_journal_number\", '-OAA') as   jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    cast (null as varchar) as eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    work_title||' Article Publishing Charge' as \"name\",\n" +
            "    CAST (null as boolean) as                       separately_saleable_ind,\n" +
            "    CAST (null as boolean) as                       trial_allowed_ind,\n" +
            "    CAST (null as date) as                          launch_date,\n" +
            "    CAST (null as varchar) as                       tax_code,\n" +
            "   'OAA' as                                         f_type,\n" +
            "    null as                                         m0_manifestation_type,\n" +
            "    CAST (null as varchar) as                       f_status,\n" +
            "    work_type,\n" +
            "    cast (null as varchar) manifestation_ref,\n" +
            "    cast (null as varchar) as  ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from title_renames\n" +
            "where open_access = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    \"concat\"('J0',\"w0_journal_number\",'-JAS') as      jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    cast (null as varchar) as eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    work_title||' Author Charges' as \"name\",\n" +
            "    CAST (null as boolean) as           separately_saleable_ind,\n" +
            "    CAST (null as boolean) as           trial_allowed_ind,\n" +
            "    CAST (null as date) as              launch_date,\n" +
            "    CAST (null as varchar) as           tax_code,\n" +
            "   'JAS' as                             f_type,\n" +
            "    null as                             m0_manifestation_type,\n" +
            "    CAST (null as varchar) as           f_status,\n" +
            "    work_type,\n" +
            "    cast (null as varchar) manifestation_ref,\n" +
            "    cast (null as varchar) as  ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from title_renames\n" +
            "where author_charges = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    substr(jm_source_reference,3,7)||'-PKG' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    work_title             as \"name\",\n" +
            "    CAST (null as boolean) as  separately_saleable_ind,\n" +
            "    CAST (null as boolean) as  trial_allowed_ind,\n" +
            "    CAST (null as date) as     launch_date,\n" +
            "    CAST (null as varchar) as  tax_code,\n" +
            "    'PKG' as                   f_type,\n" +
            "    null as                    m0_manifestation_type,\n" +
            "    CAST (null as varchar) as  f_status,\n" +
            "    work_type,\n" +
            "    cast (null as varchar) manifestation_ref,\n" +
            "    cast (null as varchar) as  ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from title_renames\n" +
            "where packages = 'Y'\n" +
            ")\n" +
            ",result_data as\n" +
            "(\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_ref as jm_source_reference,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    name,\n" +
            "    CASE\n" +
            "        when separately_saleable_ind is null then true\n" +
            "        else separately_saleable_ind\n" +
            "    END as separately_saleable_ind,\n" +
            "    trial_allowed_ind,\n" +
            "    launch_date,\n" +
            "    f_type,\n" +
            "    f_status,\n" +
            "    CAST (null as varchar) as f_revenue_model,\n" +
            "    tax_code,\n" +
            "    work_type,\n" +
            "    manifestation_ref,\n" +
            "    ult_work_ref,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from\n" +
            "crosstab_data\n" +
            ")\n" +
            "select * from result_data) where jm_source_reference in ('%s') order by jm_source_reference desc, name desc, dq_err desc";

    public static String GET_PRODUCT_DQ_QUERY ="select * from("+
            "select * from " + GetJMDLDBUser.getJMDB() + ".etl_product_inserts_v\n" +
            "UNION\n" +
            "select * from " + GetJMDLDBUser.getJMDB() + ".etl_product_updates_v)where jm_source_reference in ('%s') order by jm_source_reference desc, eph_work_id desc, scenario_name desc";

    public static String GET_PRODUCT_PERSON_ROLE_DQ_QUERY ="select * from \n" +
            "(with base_data as\n" +
            "(\n" +
            "select\n" +
            "scenario_name,                   -- scenario NAME 'New Proprietary' etc\n" +
            "scenario_code,                   -- scenario CODE NP, NS, AC, MI, RN, CA\n" +
            "upsert,                          -- 'Insert', or 'Update'\n" +
            "jm_source_reference,             -- format E-J012345 or P-J012345\n" +
            "eph_work_id,                     -- format EPR-W-xxxxxx\n" +
            "eph_manifestation_id,            -- format EPR-M-xxxxxx\n" +
            "eph_product_id,                  -- format EPR-xxxxxx new journals: set null\n" +
            "base_title,                      -- JM-manifestation-title a suffix of (Print) or (Online)\n" +
            "w0_journal_number,\n" +
            "m0_journal_number,\n" +
            "w0_chronicle_id,\n" +
            "w0_journey_identifier,\n" +
            "m0_manifestation_type,           -- (P)rint or (E)lectronic\n" +
            "separately_saleable_ind,         -- set TRUE for all new journal products.\n" +
            "trial_allowed_ind,               -- set FALSE for all new journal products.\n" +
            "launch_date,                     -- w.launch_date\n" +
            "tax_code,                        -- yes, hard-code based on manifestation_type\n" +
            "open_access_journal_type,        -- build as (F)ull-10, (H)ybrid-11, (S)ubsidised-12, (N)one-2, null.\n" +
            "subscription,                    -- CASE oa jnl type not in (F,S).\n" +
            "bulk_sales,                      -- CASE oa jnl type not in (F,S) and manifestation type\n" +
            "back_files,                      -- CASE oa jnl type not in (F,S) and manifestation type\n" +
            "open_access,                     -- CASE oa jnl type, set Y when (F,H,S) else N.\n" +
            "reprints,                        -- yes, CASE based on manifestation type\n" +
            "author_charges,                  -- set 'Y' for new journals, 'N' for updates.\n" +
            "one_off_access,                  -- 'N' for new journals\n" +
            "packages,                        -- 'N' for new journals\n" +
            "availability_status,             -- for new journals set to planned available 'PPL'\n" +
            "work_status,                     -- for new journals set to planned available 'PPL'\n" +
            "work_title,                      -- w.product_work_title\n" +
            "work_type,                       -- set 'JOURNAL'\n" +
            "internal_email_old,              -- for use by etl_product_person_role_dq_v\n" +
            "internal_email_new,              -- ditto\n" +
            "employee_number_old,             -- ditto\n" +
            "employee_number_new,             -- ditto\n" +
            "dq_err,                          -- default is 'N', but is set 'Y' by part1 if PU email or PU peoplehub_id is missing.\n" +
            "notified_date\n" +
            "from  journalmaestro_uat2.etl_product_part1_v\n" +
            "where  (notified_date is not null\n" +
            "and   ((upsert = 'Insert')\n" +
            "    or (upsert = 'Update' and scenario_name = 'Change Allocated Resources')))\n" +
            ")\n" +
            ", crosstab_data as\n" +
            "(\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-SUB-'||employee_number_new||'-PO' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by JM for new products.\n" +
            "   'SUB' as                          f_type,\n" +
            "    lower(internal_email_old) as     internal_email_old,               -- old publisher email@elsevier.com etc.\n" +
            "    lower(internal_email_new) as     internal_email_new,               -- new publisher email@elsevier.com etc.\n" +
            "    employee_number_old as           employee_number_old,              -- old publisher peoplehub_id\n" +
            "    employee_number_new as           employee_number_new,              -- new publisher peoplehub_id\n" +
            "   'PO' as                           PO_role,                          -- hard-code as 'PO'. Leg2 will translate to the role id\n" +
            "    notified_date as                 start_date,\n" +
            "    CAST (null as date)              end_date,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where subscription = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-JBS-'||employee_number_new||'-PO' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by JM for new products.\n" +
            "   'JBS' as                          f_type,\n" +
            "    lower(internal_email_old) as     internal_email_old,               -- old publisher email@elsevier.com etc.\n" +
            "    lower(internal_email_new) as     internal_email_new,               -- new publisher email@elsevier.com etc.\n" +
            "    employee_number_old as           employee_number_old,              -- old publisher peoplehub_id\n" +
            "    employee_number_new as           employee_number_new,              -- new publisher peoplehub_id\n" +
            "   'PO' as                           PO_role,                          -- hard-code as 'PO'. Leg2 will translate to the role id\n" +
            "    notified_date as                 start_date,\n" +
            "    CAST (null as date)              end_date,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where bulk_sales = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-BKF-'||employee_number_new||'-PO' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by JM for new products.\n" +
            "   'BKF' as                          f_type,\n" +
            "    lower(internal_email_old) as     internal_email_old,               -- old publisher email@elsevier.com etc.\n" +
            "    lower(internal_email_new) as     internal_email_new,               -- new publisher email@elsevier.com etc.\n" +
            "    employee_number_old as           employee_number_old,              -- old publisher peoplehub_id\n" +
            "    employee_number_new as           employee_number_new,              -- new publisher peoplehub_id\n" +
            "   'PO' as                           PO_role,                          -- hard-code as 'PO'. Leg2 will translate to the role id\n" +
            "    notified_date as                 start_date,\n" +
            "    CAST (null as date)              end_date,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where back_files = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-RPR-'||employee_number_new||'-PO' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by JM for new products.\n" +
            "   'RPR' as                          f_type,\n" +
            "    lower(internal_email_old) as     internal_email_old,               -- old publisher email@elsevier.com etc.\n" +
            "    lower(internal_email_new) as     internal_email_new,               -- new publisher email@elsevier.com etc.\n" +
            "    employee_number_old as           employee_number_old,              -- old publisher peoplehub_id\n" +
            "    employee_number_new as           employee_number_new,              -- new publisher peoplehub_id\n" +
            "   'PO' as                           PO_role,                          -- hard-code as 'PO'. Leg2 will translate to the role id\n" +
            "    notified_date as                 start_date,\n" +
            "    CAST (null as date)              end_date,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where reprints = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-OOA-'||employee_number_new||'-PO' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by JM for new products.\n" +
            "   'OOA' as                          f_type,\n" +
            "    lower(internal_email_old) as     internal_email_old,               -- old publisher email@elsevier.com etc.\n" +
            "    lower(internal_email_new) as     internal_email_new,               -- new publisher email@elsevier.com etc.\n" +
            "    employee_number_old as           employee_number_old,              -- old publisher peoplehub_id\n" +
            "    employee_number_new as           employee_number_new,              -- new publisher peoplehub_id\n" +
            "   'PO' as                           PO_role,                          -- hard-code as 'PO'. Leg2 will translate to the role id\n" +
            "    notified_date as                 start_date,\n" +
            "    CAST (null as date)              end_date,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where one_off_access = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-OAA-'||employee_number_new||'-PO' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by JM for new products.\n" +
            "   'OAA' as                          f_type,\n" +
            "    lower(internal_email_old) as     internal_email_old,               -- old publisher email@elsevier.com etc.\n" +
            "    lower(internal_email_new) as     internal_email_new,               -- new publisher email@elsevier.com etc.\n" +
            "    employee_number_old as           employee_number_old,              -- old publisher peoplehub_id\n" +
            "    employee_number_new as           employee_number_new,              -- new publisher peoplehub_id\n" +
            "   'PO' as                           PO_role,                          -- hard-code as 'PO'. Leg2 will translate to the role id\n" +
            "    notified_date as                 start_date,\n" +
            "    CAST (null as date)              end_date,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where open_access = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-JAS-'||employee_number_new||'-PO' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by JM for new products.\n" +
            "   'JAS' as                          f_type,\n" +
            "    lower(internal_email_old) as     internal_email_old,               -- old publisher email@elsevier.com etc.\n" +
            "    lower(internal_email_new) as     internal_email_new,               -- new publisher email@elsevier.com etc.\n" +
            "    employee_number_old as           employee_number_old,              -- old publisher peoplehub_id\n" +
            "    employee_number_new as           employee_number_new,              -- new publisher peoplehub_id\n" +
            "   'PO' as                           PO_role,                          -- hard-code as 'PO'. Leg2 will translate to the role id\n" +
            "    notified_date as                 start_date,\n" +
            "    CAST (null as date)              end_date,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where author_charges = 'Y'\n" +
            "UNION\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-PKG-'||employee_number_new||'-PO' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    CAST (null as varchar) as        eph_product_id,                    -- not known by JM for new products.\n" +
            "   'PKG' as                          f_type,\n" +
            "    lower(internal_email_old) as     internal_email_old,               -- old publisher email@elsevier.com etc.\n" +
            "    lower(internal_email_new) as     internal_email_new,               -- new publisher email@elsevier.com etc.\n" +
            "    employee_number_old as           employee_number_old,              -- old publisher peoplehub_id\n" +
            "    employee_number_new as           employee_number_new,              -- new publisher peoplehub_id\n" +
            "   'PO' as                           PO_role,                          -- hard-code as 'PO'. Leg2 will translate to the role id\n" +
            "    notified_date as                 start_date,\n" +
            "    CAST (null as date)              end_date,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from base_data\n" +
            "where packages = 'Y'\n" +
            ")\n" +
            ",result_data as\n" +
            "(\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_ref as jm_source_reference,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    eph_product_id,\n" +
            "    f_type,\n" +
            "    internal_email_old,\n" +
            "    internal_email_new,\n" +
            "    employee_number_old,\n" +
            "    employee_number_new,\n" +
            "    po_role,\n" +
            "    start_date,\n" +
            "    end_date,\n" +
            "    dq_err,\n" +
            "    notified_date\n" +
            "from\n" +
            "crosstab_data\n" +
            ")\n" +
            "select * from result_data) where jm_source_reference in ('%s') order by jm_source_reference desc, eph_work_id desc";

    public static String GET_SD_SUBJECT_AREAS_QUERY ="select * from (SELECT\n" +
            "     subject_area_id sa_id\n" +
            "   , code sa_code\n" +
            "   , name sa_name\n" +
            "   , '1' sa_level\n" +
            "   , f_parent_subject_area sa_parent_id\n" +
            "   , CAST(null AS date) start_date\n" +
            "   , CAST(null AS date) end_date\n" +
            "   FROM\n" +
            "     " + GetPRMDLDBUser.getProdDataBase() + ".gd_subject_area\n" +
            "   WHERE f_type = 'SD'\n" +
            "   AND   f_parent_subject_area IS NULL\n" +
            "UNION ALL\n" +
            "   SELECT\n" +
            "     subject_area_id sa_id\n" +
            "   , code sa_code\n" +
            "   , name sa_name\n" +
            "   , '2' sa_level\n" +
            "   , f_parent_subject_area sa_parent_id\n" +
            "   , CAST(null AS date) start_date\n" +
            "   , CAST(null AS date) end_date\n" +
            "   FROM\n" +
            "     " + GetPRMDLDBUser.getProdDataBase() + ".gd_subject_area\n" +
            "   WHERE f_type = 'SD'\n" +
            "   AND   f_parent_subject_area\n" +
            "           IN (SELECT subject_area_id\n" +
            "               FROM " + GetPRMDLDBUser.getProdDataBase() + ".gd_subject_area\n" +
            "               WHERE f_type = 'SD'\n" +
            "               AND f_parent_subject_area IS NULL)\n" +
            "UNION ALL\n" +
            "   SELECT\n" +
            "     subject_area_id sa_id\n" +
            "   , code sa_code\n" +
            "   , name sa_name\n" +
            "   , '3' sa_level\n" +
            "   , f_parent_subject_area sa_parent_id\n" +
            "   , CAST(null AS date) start_date\n" +
            "   , CAST(null AS date) end_date\n" +
            "   FROM\n" +
            "     " + GetPRMDLDBUser.getProdDataBase() + ".gd_subject_area\n" +
            "   WHERE f_type = 'SD'\n" +
            "   AND   f_parent_subject_area\n" +
            "           IN (SELECT subject_area_id\n" +
            "               FROM " + GetPRMDLDBUser.getProdDataBase() + ".gd_subject_area\n" +
            "               WHERE f_type = 'SD'\n" +
            "               AND f_parent_subject_area\n" +
            "                      IN (SELECT subject_area_id\n" +
            "                          FROM " + GetPRMDLDBUser.getProdDataBase() + ".gd_subject_area\n" +
            "                          WHERE f_type = 'SD'\n" +
            "                          AND f_parent_subject_area IS NULL)\n" +
            "               )) where sa_id in (%s) ";

    public static String GET_MANIFESTATION_DQ_QUERY ="select * from\n" +
            "(\n" +
            "select cs.chronicle_scenario_name as                       scenario_name,\n" +
            "       cs.chronicle_scenario_code as                       scenario_code,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                             upsert,\n" +
            "        m.manifestation_type||'-J0'||w.elsevier_journal_number as jm_source_reference, -- format E-J012345 or P-J012345\n" +
            "        w.eph_work_id as                                    eph_work_id,               -- format EPR-W-XXXXXX new journals: null. The owning work\n" +
            "        m.eph_manifestation_id as                           eph_manifestation_id,      -- format EPR-M-xxxxxx new journals: null\n" +
            "       (CASE m.manifestation_type\n" +
            "            when 'E' then m.manifestation_title||' (Online)'\n" +
            "            else          m.manifestation_title||' (Print)'\n" +
            "        END) as                                             manifestaton_key_title,    -- (title with a suffix of (Online) or (Print))\n" +
            "        w.launch_date as                                    online_launch_date,        \n" +
            "       (CASE m.manifestation_type\n" +
            "            when 'E' then 'JEL'\n" +
            "            else          'JPR'\n" +
            "        END) as                                             f_type,                    -- JPR, JEL\n" +
            "      'MAP'                                                 f_status,                  -- 'MAP' manifestation status stands for Approved (Planned). Set 'MAP' for new journals.\n" +
            "       m.notified_date as                                   effective_start_date,\n" +
            "      'N' as                                                dq_err,\n" +
            "       m.notified_date as                                   notified_date,\n" +
            "      'J0'||w.elsevier_journal_number                       work_source_reference      -- Added New work source reference derived from jmf_work\n" +
            "from  journalmaestro_uat2.jmf_manifestation      m\n" +
            "join  journalmaestro_uat2.jmf_work               w  on m.f_work = w.work_id\n" +
            "join  journalmaestro_uat2.jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  journalmaestro_uat2.jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   m.notified_date is not null\n" +
            "UNION\n" +
            "select  mu1.scenario_name                                                 scenario_name,            -- = scenario_name = 'Rename'\n" +
            "        mu1.scenario_code                                                 scenario_code,            -- = scenario_code = 'RN'\n" +
            "        mu1.upsert                                                        upsert,                   -- 'Update'\n" +
            "        mu1.m0_manifestation_type||'-J0'||mu1.w0_journal_number as        jm_source_reference,      -- format E-J012345 or P-J012345\n" +
            "        COALESCE(mu1.w0_eph_work_id, w1.eph_work_id) as                   eph_work_id,              -- format EPR-W-XXXXXX new journals: null. The owning work\n" +
            "        COALESCE(mu1.m0_eph_manifestation_id, m1.eph_manifestation_id) as eph_manifestation_id,     -- format EPR-M-xxxxxx new journals: null\n" +
            "       (CASE m1.manifestation_type\n" +
            "            when 'E' then m1.manifestation_title||' (Online)'\n" +
            "            else          m1.manifestation_title||' (Print)'\n" +
            "        END) as                                                           manifestation_key_title,  -- (work title with a suffix of (Online) or (Print))\n" +
            "        CAST (null as date) as                                            online_launch_date,\n" +
            "       (CASE m1.manifestation_type\n" +
            "            when 'E' then 'JEL'\n" +
            "            else          'JPR'\n" +
            "        END) as                                                           f_type,                   -- JPR, JEL. EPHD-2858 now set f_type on all updates too.\n" +
            "        CAST (null as varchar) as                                         f_status,                 -- don't change status on renames. leave it null and the current value in EPH gd_manifestation will persist.\n" +
            "        m1.notified_date as                                               effective_start_date,\n" +
            "       (CASE\n" +
            "            when (mu1.w0_eph_work_id          is null and w1.eph_work_id          is null) then 'Y'\n" +
            "            when (mu1.m0_eph_manifestation_id is null and m1.eph_manifestation_id is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                                                           dq_err,\n" +
            "        m1.notified_date as                                               notified_date,\n" +
            "        CAST (null as varchar) as                                         work_source_reference      -- Added a Null Value for new work_source_reference field\n" +
            "from (((journalmaestro_uat2.etl_manifestation_updates1_v mu1\n" +
            "        join journalmaestro_uat2.jmf_work           w1 on ((w1.work_chronicle_id       = mu1.w0_chronicle_id)\n" +
            "                                   and (w1.elsevier_journal_number = mu1.w0_journal_number)\n" +
            "                                   and (w1.work_journey_identifier = 'A1')))                        -- we've definitely got one work, the A1.\n" +
            "        join journalmaestro_uat2.jmf_work_chronicle wc on ((w1.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                   and (w1.work_journey_identifier = 'A1')\n" +
            "                                   and (wc.chronicle_scenario_code = 'RN')))\n" +
            "        join journalmaestro_uat2.jmf_manifestation  m1 on ((m1.f_work = w1.work_id)\n" +
            "                                   and (m1.elsevier_journal_number = mu1.m0_journal_number)\n" +
            "                                   and (m1.manifestation_type      = mu1.m0_manifestation_type)))\n" +
            "where m1.notified_date is not null\n" +
            "and   mu1.m0_manifestation_title is not null\n" +
            "and   m1.manifestation_title    is not null\n" +
            "and   m1.manifestation_title <> mu1.m0_manifestation_title\n" +
            "group by mu1.m0_manifestation_id, mu1.m0_manifestation_type, m1.manifestation_type,\n" +
            "         mu1.scenario_code, mu1.scenario_name, mu1.upsert, mu1.w0_journal_number, mu1.w0_eph_work_id,\n" +
            "         mu1.m0_eph_manifestation_id, m1.eph_manifestation_id, m1.manifestation_title, m1.notified_date,\n" +
            "         w1.eph_work_id, w1.elsevier_journal_number, m1.eph_manifestation_id\n" +
            ")\n" +
            "where jm_source_reference in ('%s') order by jm_source_reference, eph_work_id desc,scenario_name desc, online_launch_date desc";

    public static String GET_WORK_LEGAL_OWNER_DQ_QUERY ="select * from (select 'J0'||w.elsevier_journal_number as                             work_external_ref,\n" +
            "        lo.external_reference as                                      legalowner_external_ref,\n" +
            "--         substr(wo.legal_owner_name,1,30),\n" +
            "        wo.journal_ownership_type as                                  f_ownership_description,\n" +
            "        wo.notified_date as                                           notified_date,\n" +
            "       'J0'||w.elsevier_journal_number||'-'||lo.external_reference as work_legalowner_external_ref,\n" +
            "        CAST (null as integer) as                                     eph_work_id,\n" +
            "        CAST (null as integer) as                                     legal_owner_id\n" +
            "from     " + GetJMDLDBUser.getJMDB() + ".jmf_work_ownership     wo\n" +
            "join     " + GetJMDLDBUser.getJMDB() + ".jmf_work                w on  w.work_id = wo.f_work\n" +
            "join     " + GetJMDLDBUser.getJMDB() + ".legal_owner_v          lo on  CAST(lo.legal_owner_id AS integer) = wo.f_legal_owner\n" +
            "                                   and lo.end_date is null\n" +
            "join     " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle     wc  on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join     " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs  on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where    w.work_journey_identifier = 'A1'\n" +
            "and      wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and      w.notified_date is not null\n" +
            "-- and      w.elsevier_journal_number > '31434'\n" +
            ") where work_external_ref in ('%s') order by work_external_ref desc, legalowner_external_ref desc";

    public static String GET_WORK_BUSINESS_MODEL_DQ_QUERY ="select * from (select  cs.chronicle_scenario_name as                                   scenario_name,\n" +
            "        cs.chronicle_scenario_code as                                   scenario_code,\n" +
            "        'J0'||w.elsevier_journal_number as                               external_work_ref,\n" +
            "       'J0'||w.elsevier_journal_number||'-'||wbm.business_model_code as external_reference,\n" +
            "        CAST (null as integer) as                                       eph_work_id,\n" +
            "        wbm.business_model_code as                                      business_model_code,\n" +
            "        wbm.business_model_description as                               business_model_description,\n" +
            "        wbm.notified_date as                                            notified_date\n" +
            "from    " + GetJMDLDBUser.getJMDB() + ".jmf_work_business_model wbm\n" +
            "join    " + GetJMDLDBUser.getJMDB() + ".jmf_work                  w on w.work_id = wbm.f_work\n" +
            "join    " + GetJMDLDBUser.getJMDB() + ".eph_business_model_v     bm on bm.code   = wbm.business_model_code\n" +
            "join    " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle       wc on wc.work_chronicle_id = w.work_chronicle_id\n" +
            "join    " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs  on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where   w.work_journey_identifier = 'A1'\n" +
            "and     wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and     w.notified_date is not null\n" +
            "-- and     w.elsevier_journal_number > '31434'\n" +
            "order by w.elsevier_journal_number) where external_reference in ('%s') order by external_reference desc, external_work_ref desc ";

    public static String GET_WORK_ACCESS_MODEL_DQ_QUERY="select * from (\n" +
            "select  cs.chronicle_scenario_name as                                 scenario_name,\n" +
            "        cs.chronicle_scenario_code as                                 scenario_code,\n" +
            "       'J0'||w.elsevier_journal_number as                             external_work_ref,\n" +
            "       'J0'||w.elsevier_journal_number||'-'||wam.access_model_code as external_reference,\n" +
            "        CAST (null as integer) as                                     eph_work_id,\n" +
            "        wam.access_model_code as                                      access_model_code,\n" +
            "        wam.access_model_description as                               access_model_description,\n" +
            "        wam.notified_date as                                          notified_date\n" +
            "from    " + GetJMDLDBUser.getJMDB() + ".jmf_work_access_model   wam\n" +
            "join    " + GetJMDLDBUser.getJMDB() + ".jmf_work                  w on w.work_id = wam.f_work\n" +
            "join    " + GetJMDLDBUser.getJMDB() + ".eph_access_model_v       am on am.code   = wam.access_model_code\n" +
            "join    " + GetJMDLDBUser.getJMDB() + ".jmf_work_chronicle       wc on wc.work_chronicle_id = w.work_chronicle_id\n" +
            "join    " + GetJMDLDBUser.getJMDB() + ".jmf_chronicle_scenario cs  on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where   w.work_journey_identifier = 'A1'\n" +
            "and     wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and     w.notified_date is not null\n" +
            "-- and     w.elsevier_journal_number > '31434'\n" +
            "order by w.elsevier_journal_number) where external_reference in ('%s')";

    public static String GET_ACCOUNTABLE_PRODUCT_DQ ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_accountable_product_dq_v where jm_source_reference in ('%s') order by jm_source_reference desc, work_title desc";
    public static String GET_WWORK_DQ ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_wwork_dq where jm_source_reference in ('%s') order by jm_source_reference desc,scenario_name desc, work_title desc";
    public static String GET_WORK_IDENTIFIER_DQ ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_work_identifier_dq_v where jm_source_ref_new in ('%s') order by jm_source_ref_new desc, eph_work_id desc, scenario_name desc";
    public static String GET_WORK_PERSON_ROLE_DQ ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_work_person_role_dq_v where jm_source_ref_new in ('%s') order by jm_source_ref_new desc, eph_work_id desc";
    public static String GET_WORK_SUBJECT_AREA_DQ ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_work_subject_area_dq_v where jm_source_reference in ('%s') order by jm_source_reference, eph_work_id";
    public static String GET_MANIFESTATION_UPDATES1 ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_manifestation_updates1_v where w0_chronicle_id in (%s)";
    public static String GET_MANIFESTATION_IDENTIFIER ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_manifestation_identifier_dq_v where jm_source_ref_new in ('%s') order by jm_source_ref_new desc, eph_work_id desc";
    public static String GET_PRODUCT_PART1 ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_product_part1_v where jm_source_reference in ('%s') order by jm_source_reference desc, eph_work_id desc, scenario_name desc, w0_chronicle_id desc";
    public static String GET_PRODUCT_INSERTS ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_product_inserts_v where jm_source_reference in ('%s') order by jm_source_reference desc, eph_work_id desc, name desc";
    public static String GET_PRODUCT_UPDATES ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_product_updates_v where jm_source_reference in ('%s') order by jm_source_reference desc, name desc, dq_err desc";
    public static String GET_PRODUCT_DQ ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_product_dq_v where jm_source_reference in ('%s') order by jm_source_reference desc, eph_work_id desc, scenario_name desc";
    public static String GET_PRODUCT_PERSON_ROLE_DQ ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_product_person_role_dq_v where jm_source_reference in ('%s') order by jm_source_reference desc, eph_work_id desc";
    public static String GET_SD_SUBJECT_AREAS ="select * from " + GetJMDLDBUser.getJMDB() + ".sd_subject_areas_v where sa_id in (%s)";
    public static String GET_MANIFESTATION_DQ ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_manifestation_dq_v where jm_source_reference in ('%s') order by jm_source_reference, eph_work_id desc,scenario_name desc, online_launch_date desc";
    public static String GET_WORK_LEGAL_OWNER_DQ ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_work_legal_owner_dq_V where work_external_ref in ('%s') order by work_external_ref desc, legalowner_external_ref desc";
    public static String GET_WORK_BUSINESS_MODEL_DQ ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_work_business_model_dq_V where external_reference in ('%s') order by external_reference desc, external_work_ref desc";
    public static String GET_WORK_ACCESS_MODEL_DQ ="select * from " + GetJMDLDBUser.getJMDB() + ".etl_work_access_model_dq_V where external_reference in ('%s') order by external_reference desc, external_work_ref desc";

// Semarchy DB get core ID's

//
    public static String GET_STITCHING_WORK_CORE_EPR_IDs = "select epr_id as EPR_ID from "+ GetEPHDB.getStitchingDB() +".stch_work_core_json order by random() limit %s";
    public static String GET_STITCHING_PRODUCT_CORE_EPR_IDs = "select epr_id as EPR_ID from "+ GetEPHDB.getStitchingDB() +".stch_product_core_json order by random() limit %s";
    public static String GET_GD_WORK_RELATIONSHIP_PARENT_IDs = "select f_parent as F_PARENT from semarchy_eph_mdm.gd_work_relationship order by random() limit %s";
    public static String GET_STITCHING_SubjectArea_EPR_IDs = "select epr_id as EPR_ID from "+ GetEPHDB.getStitchingDB() +".stch_product_core_json where type not in ('OOA') order by random() limit %s";
    public static String GET_GD_MANIFESTATION_IDs = "select f_wwork as F_WWORK from semarchy_eph_mdm.gd_manifestation order by random() limit %s";

    public static String GET_STITCHING_WORK_CORE = "select * from "+ GetEPHDB.getStitchingDB() +".stch_work_core_json where epr_id in ('%s')";
    public static String GET_STITCHING_PRODUCT_CORE = "select * from "+ GetEPHDB.getStitchingDB() +".stch_product_core_json where epr_id in ('%s')";

    public static String GET_SEMARCHY_GD_WWORK = "select * from semarchy_eph_mdm.gd_wwork where work_id in ('%s')";
    public static String GET_SEMARCHY_GD_WORK_IDENTIFIER = "select * from semarchy_eph_mdm.gd_work_identifier where external_reference like ('%s"+"%s"+"%%')";
    public static String GET_SEMARCHY_GD_PRODUCT = "select * from semarchy_eph_mdm.gd_product where product_id in ('%s')";
    public static String GET_SEMARCHY_GD_WORK_PERSON_ROLE = "select * from semarchy_eph_mdm.gd_work_person_role where f_wwork in ('%s') and f_role like ('%s')";
    public static String GET_SEMARCHY_GD_WORK_RELATIONSHIP = "select * from semarchy_eph_mdm.gd_work_relationship where f_parent in ('%s')";
    public static String GET_SEMARCHY_GD_PERSON = "select * from semarchy_eph_mdm.gd_person gp where person_id in (%s)";
    public static String GET_SEMARCHY_GD_SUBJECT_Area = "select * from semarchy_eph_mdm.gd_subject_area where code in ('%s')";
    public static String GET_SEMARCHY_GD_MANIFESTATION = "select * from semarchy_eph_mdm.gd_manifestation where f_wwork in ('%s') and f_type like ('%s')";
    public static String GET_SEMARCHY_GD_MANIFESTATION_IDENTIFIER = "select * from semarchy_eph_mdm.gd_manifestation_identifier where f_manifestation like ('%s')";
    public static String GET_SEMARCHY_GD_ACCOUNTABLE_PRODUCT = "select * from semarchy_eph_mdm.gd_accountable_product where gl_product_segment_code like ('%s')";

//    SemarchySource Table IDs
    public static String GET_ALL_MANIFESTATION_IDENTIFIERS_IDs = "select identifier as IDENTIFIER from "+GetJMDLDBUser.getProdStagingDataBase2()+".%s where delete_flag not in (TRUE) order by rand() limit %s";
    public static String GET_ALL_External_Reference_IDs = "select external_reference as EXTERNAL_REFERENCE from "+GetJMDLDBUser.getProdStagingDataBase2()+".%s where delete_flag not in (TRUE) order by rand() limit %s";
    public static String GET_ALL_WORK_IDENTIFIER_IDs = "select identifier as IDENTIFIER from "+GetJMDLDBUser.getProdStagingDataBase2()+".all_work_identifier_v where delete_flag not in (true) and effective_end_date is null order by rand() limit %s";
    public static String GET_ALL_WORK_SOURCE_REFERENCE_IDs = "select external_reference as EXTERNAL_REFERENCE from "+GetJMDLDBUser.getProdStagingDataBase2()+".%s where delete_flag not in (TRUE) order by rand() limit %s";
    public static String GET_ALL_PRODUCT_IDs = "select external_reference as EXTERNAL_REFERENCE from "+GetJMDLDBUser.getProdStagingDataBase2()+".%s where delete_flag not in (TRUE) order by rand() limit %s";
    public static String GET_ALL_SUBJECT_AREA_IDs = "select external_reference as EXTERNAL_REFERENCE from "+GetJMDLDBUser.getProdStagingDataBase2()+".all_work_subject_areas_v where delete_flag not in (TRUE) order by rand() limit %s";

    public static String GET_ALL_WORK_RELATIONSHIP_IDs = "select s_identifier as S_IDENTIFIER from semarchy_eph_mdm.gd_manifestation_identifier limit %s";
    public static String GET_ALL_WORK_SUBJECT_AREAS_IDs = "select s_identifier as S_IDENTIFIER from semarchy_eph_mdm.gd_manifestation_identifier limit %s";
    public static String GET_ALL_WORK_IDs = "select s_identifier as S_IDENTIFIER from semarchy_eph_mdm.gd_manifestation_identifier limit %s";

//  Get SemarchySource Records
    public static String GET_ALL_MANIFESTATION_IDENTIFIER = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_manifestation_identifiers_v where identifier in ('%s') order by identifier desc";
    public static String GET_ALL_MANIFESTATION = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_manifestation_v where external_reference in ('%s') order by external_reference desc";
    public static String GET_ALL_PERSON = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_person_v where external_reference in ('%s') order by external_reference desc";
    public static String GET_ALL_PRODUCT = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_product_v where external_reference in ('%s') order by external_reference desc, f_status desc";
    public static String GET_ALL_WORK_IDENTIFIER = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_work_identifier_v where identifier in ('%s') order by identifier";
    public static String GET_ALL_WORK_PERSON_ROLE = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_work_person_role_v where external_reference in ('%s') and effective_start_date >(select effective_start_date from "+ GetJMDLDBUser.getProdStagingDataBase2()+".all_work_person_role_v order by effective_start_date limit 1) order by external_reference desc";
    public static String GET_ALL_WORK_RELATIONSHIP = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_work_relationship_v where external_reference in ('%s')";
    public static String GET_ALL_WORK_SUBJECT_AREAS = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_work_subject_areas_v where external_reference in ('%s')";
    public static String GET_ALL_WORK = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_work_v where external_reference in ('%s') order by external_reference desc";
    public static String GET_ALL_ACCOUNTABLE_PRODUCT = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_accountable_product_v where external_reference in ('%s') order by external_reference desc";
    public static String GET_ALL_WORK_ACCESS_MODEL = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_work_access_model_v where external_reference in ('%s') order by external_reference desc";
    public static String GET_ALL_WORK_BUSINESS_MODEL = "select * from "+ GetJMDLDBUser.getProdStagingDataBase2() +".all_work_business_model_v where external_reference in ('%s') order by external_reference";


//Get Semarchy Records
    public static String GET_GD_MANIFESTATION_IDENTIFIER = "select * from semarchy_eph_mdm.gd_manifestation_identifier where identifier in ('%s') order by identifier desc";
    public static String GET_GD_MANIFESTATION = "select * from semarchy_eph_mdm.gd_manifestation where external_reference in ('%s') order by external_reference desc";
    public static String GET_GD_PERSON = "select * from semarchy_eph_mdm.gd_person where external_reference in ('%s') order by external_reference desc";
    public static String GET_GD_PRODUCT = "select * from semarchy_eph_mdm.gd_product where external_reference in ('%s') order by external_reference_core desc, f_status desc";
    public static String GET_GD_WORK_IDENTIFIER = "select * from semarchy_eph_mdm.gd_work_identifier where identifier in ('%s') order by identifier";
    public static String GET_GD_WORK_PERSON_ROLE = "select * from semarchy_eph_mdm.gd_work_person_role where external_reference in ('%s') order by external_reference desc";
    public static String GET_GD_WORK_RELATIONSHIP = "select * from semarchy_eph_mdm.gd_work_relationship where external_reference in ('%s')";
    public static String GET_GD_SUBJECT_AREA = "select * from semarchy_eph_mdm.gd_subject_area where external_reference in ('%s') order by external_reference desc";
    public static String GET_GD_WWORK = "select * from semarchy_eph_mdm.gd_wwork where external_reference in ('%s') order by external_reference desc";
    public static String GET_GD_ACCOUNTABLE_PRODUCT = "select * from semarchy_eph_mdm.gd_accountable_product where external_reference in ('%s') order by external_reference desc";
    public static String GET_GD_WORK_ACCESS_MODEL = "select * from semarchy_eph_mdm.gd_work_access_model where external_reference in ('%s') order by external_reference desc";
    public static String GET_GD_WORK_BUSINESS_MODEL = "select * from semarchy_eph_mdm.gd_work_business_model where external_reference in ('%s') order by external_reference";
}