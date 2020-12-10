package com.eph.automation.testing.services.db.JMDataLakeSQL;

import com.eph.automation.testing.services.db.PROMISDataLakeSQL.GetPRMDLDBUser;

public class JMETLDataChecksSQL {
//    JM Access table IDs SQL
    public static String GET_JMF_WORK_IDs = "select work_id as WORK_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work order by rand() limit %s";
    public static String GET_JMF_MANIFESTATION_IDs = "select manifestation_id as MANIFESTATION_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_manifestation order by rand() limit %s";
    public static String GET_JMF_WORK_PERSON_ROLE_IDs = "select work_person_role_id as WORK_PERSON_ROLE_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_person_role order by rand() limit %s";
    public static String GET_JMF_WORK_SUBJECT_AREA_IDs = "select work_subject_area_id as WORK_SUBJECT_AREA_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_subject_area limit %s";
    public static String GET_JMF_WORK_CHRONICLE_IDs = "select work_chronicle_id as WORK_CHRONICLE_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle order by rand() limit %s";
    public static String GET_JMF_PMG_PUBDIR_ALLOCATION_IDs = "select pmg_pubdir_allocation_id as PMG_PUBDIR_ALLOCATION_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_pmg_pubdir_allocation order by rand() limit %s";
    public static String GET_JMF_APPROVAL_ATTACHMENT_IDs = "select approval_attachment_id as APPROVAL_ATTACHMENT_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_approval_attachment order by rand() limit %s";
    public static String GET_JMF_REVIEW_COMMENT_IDs = "select review_comment_id as REVIEW_COMMENT_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_review_comment order by rand() limit %s";
    public static String GET_JMF_APPROVAL_REQUEST_IDs = "select approval_id as APPROVAL_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_approval_request order by rand() limit %s";
    public static String GET_JMF_APPLICATION_PROPERTIES_IDs = "select application_property_key as APPLICATION_PROPERTY_KEY from "+ GetJMDLDBUser.getJMDB()+".jmf_application_properties order by rand() limit %s";
    public static String GET_JMF_CHRONICLE_SCENARIO_IDs = "select chronicle_scenario_code as CHRONICLE_SCENARIO_CODE from "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario order by rand() limit %s";
    public static String GET_JMF_WORK_OWNERSHIP_IDs = "select work_ownership_id as WORK_OWNERSHIP_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_ownership order by rand() limit %s";


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

// JM Staging Query SQL
static  String[] databaseEnv = GetJMDLDBUser.getJMDataBase();

    public static String GET_WORK_QUERY = "select * from "+ databaseEnv[1] +".jmf_work_v where work_id in (%s)";

//           public static String GET_WORK_QUERY = "select * from (SELECT w.work_id,\n" +
//            "       pf.f_work_chronicle work_chronicle_id,\n" +
//            "       w.elsevier_journal_number,\n" +
//            "       pf.product_journey_identifier work_journey_identifier,\n" +
//            "       w.work_title,\n" +
//            "       w.product_subtitle work_subtitle,\n" +
//            "       w.product_work_title_info work_title_info,\n" +
//            "       w.issn_l,\n" +
//            "       p.journal_acronym_pts,\n" +
//            "       p.journal_acronym_evise,\n" +
//            "       w.journal_acronym_argi,\n" +
//            "       coalesce(w.eph_work_id,\n" +
//            "                xw.epr) as eph_work_id,\n" +
//            "       w.f_product_family family_id,\n" +
//            "       pf.product_family_title family_title,\n" +
//            "       pf.pmx_product_family_id,\n" +
//            "       frd2.family_resource_details_id pmc_family_resource_details_id,\n" +
//            "       frd2.initial_value pmc_old,\n" +
//            "       frd2.new_value pmc_new,\n" +
//            "       frd1.family_resource_details_id pu_family_resource_details_id,\n" +
//            "       frd1.initial_value pu_email_old,\n" +
//            "       frd1.new_value pu_email_new,\n" +
//            "       frd1.peoplehub_id_old pu_peoplehubid_old,\n" +
//            "       frd1.peoplehub_id_new pu_peoplehubid_new,\n" +
//            "       w.internal_elsevier_division,\n" +
//            "       w.manifestation_types_code,\n" +
//            "       w.manifestation_personal_model_type,\n" +
//            "       w.imprint_name,\n" +
//            "       w.pts_journal_indicator,\n" +
//            "       w.year_of_first_issue,\n" +
//            "       w.year_of_last_issue,\n" +
//            "       w.first_volume_name,\n" +
//            "       w.first_issue_name,\n" +
//            "       w.last_volume_name,\n" +
//            "       w.last_issue_name,\n" +
//            "       w.volumes_per_year_quantity,\n" +
//            "       w.issues_per_volume_quantity,\n" +
//            "       w.first_year_volumes_per_year_quantity,\n" +
//            "       w.first_year_issues_per_volume_quantity,\n" +
//            "       w.periodical_timing_desc,\n" +
//            "       w.open_accesstype_code,\n" +
//            "       w.open_access_sponsor_name,\n" +
//            "       w.open_access_fee,\n" +
//            "       w.open_access_fee_short_art,\n" +
//            "       w.open_access_currency_code,\n" +
//            "       w.open_access_discount_ind,\n" +
//            "       w.open_access_discount_period,\n" +
//            "       w.oa_first_year_discount_price_percentage,\n" +
//            "       w.oa_second_year_discount_price_percentage,\n" +
//            "       w.oa_first_year_discount_price,\n" +
//            "       w.oa_second_year_discount_price,\n" +
//            "       w.ddp_eligible_ind,\n" +
//            "       w.webshop_ind,\n" +
//            "       w.medline_ind,\n" +
//            "       w.thomson_reuters_ind,\n" +
//            "       w.scopus_ind,\n" +
//            "       w.embase_ind,\n" +
//            "       w.doaj_ind,\n" +
//            "       w.road_ind,\n" +
//            "       w.pubmedcentral_ind,\n" +
//            "       w.main_language_code,\n" +
//            "       w.english_language_percentage_type,\n" +
//            "       w.open_archive_period,\n" +
//            "       w.delayed_open_archive_ind,\n" +
//            "       w.include_in_collections_ind,\n" +
//            "       w.launch_date,\n" +
//            "       w.publication_schedule_jan,\n" +
//            "       w.publication_schedule_feb,\n" +
//            "       w.publication_schedule_mar,\n" +
//            "       w.publication_schedule_apr,\n" +
//            "       w.publication_schedule_may,\n" +
//            "       w.publication_schedule_jun,\n" +
//            "       w.publication_schedule_jul,\n" +
//            "       w.publication_schedule_aug,\n" +
//            "       w.publication_schedule_sep,\n" +
//            "       w.publication_schedule_oct,\n" +
//            "       w.publication_schedule_nov,\n" +
//            "       w.publication_schedule_dec,\n" +
//            "       w.manifestation_formats_code,\n" +
//            "       w.takeover_year,\n" +
//            "       w.doi_prefix,\n" +
//            "       w.doi_right_assigned_ind,\n" +
//            "       w.society_owned_ind,\n" +
//            "       w.checked_with_oa_team_ind,\n" +
//            "       w.imprint_code,\n" +
//            "       w.discontinue_date,\n" +
//            "       w.pmx_product_work_id,\n" +
//            "       w.removed_from_catalogue_ind,\n" +
//            "       w.transfer_date,\n" +
//            "       p.le_mans_ind,\n" +
//            "       p.society_relationship_type,\n" +
//            "       p.article_number_per_year,\n" +
//            "       p.submission_number_per_year,\n" +
//            "       p.evise_requested_code,\n" +
//            "       p.evise_support_level,\n" +
//            "       p.evise_workflow_type,\n" +
//            "       p.editor_location,\n" +
//            "       p.abp_usage_ind,\n" +
//            "       p.non_standard_production_aspects,\n" +
//            "       p.editorial_production_site,\n" +
//            "       p.production_specification_type,\n" +
//            "       p.typeset_model_type,\n" +
//            "       p.reference_style_type,\n" +
//            "       p.budgeted_page_number_per_issue,\n" +
//            "       p.latex_submission_percentage,\n" +
//            "       p.typesetter_code,\n" +
//            "       p.page_review_charges,\n" +
//            "       p.copy_editing_code,\n" +
//            "       p.history_date_format,\n" +
//            "       p.proof_sent_to_author_ind,\n" +
//            "       p.proof_sent_to_editor_ind,\n" +
//            "       p.editor_email_address,\n" +
//            "       p.e_suite_journal_ind,\n" +
//            "       p.sponsor_accress_required_ind,\n" +
//            "       p.online_publication_date_ind,\n" +
//            "       p.author_feedback_ind,\n" +
//            "       p.send_copyright_form_ind,\n" +
//            "       p.running_order_details,\n" +
//            "       p.flexibility,\n" +
//            "       p.maximum_page_details,\n" +
//            "       p.specific_logo_required_ind,\n" +
//            "       p.additional_deliveries_details,\n" +
//            "       p.mandatory_submission_item_ind,\n" +
//            "       p.doi_statement_ind,\n" +
//            "       p.language_editing_performed_ind,\n" +
//            "       p.language_editing_stage,\n" +
//            "       p.dedicated_journal_url_ind,\n" +
//            "       p.dedicated_journal_url,\n" +
//            "       p.coi_required_ind,\n" +
//            "       p.editorial_system_name,\n" +
//            "       p.typesetter_name,\n" +
//            "       p.editorial_turnaround_time,\n" +
//            "       p.pending_submissions_quantity,\n" +
//            "       p.checked_with_society_team_ind,\n" +
//            "       p.launch_date production_launch_date,\n" +
//            "       p.proposed_evise_rollout_period_date,\n" +
//            "       p.backstock_end_year,\n" +
//            "       p.backstock_end_option,\n" +
//            "       p.jbs_site_ind,\n" +
//            "       p.editorial_production_email_address,\n" +
//            "       p.editorial_production_site_name,\n" +
//            "       p.journal_has_article_nos,\n" +
//            "       p.journal_article_number_type,\n" +
//            "       p.backstock_treatment_note,\n" +
//            "       p.joint_venture_indicator,\n" +
//            "       p.joint_venture_party_name,\n" +
//            "       f.business_controller_name,\n" +
//            "       f.business_unit_code,\n" +
//            "       f.pmg_code,\n" +
//            "       f.pmc_code,\n" +
//            "       f.opco_r12_id,\n" +
//            "       f.opco_r12_code,\n" +
//            "       f.contract_opco_r12_code,\n" +
//            "       f.responsibility_centre_code,\n" +
//            "       f.hfm_hierarchy_position_code,\n" +
//            "       f.city_opco_r12_code,\n" +
//            "       f.country_opco_r12_code,\n" +
//            "       f.opco_r12_name,\n" +
//            "       f.opco_r12_legal_name,\n" +
//            "       f.refund_option,\n" +
//            "       f.reminder_date,\n" +
//            "       f.reminder_option,\n" +
//            "       f.renewal_option,\n" +
//            "       f.renewal_date,\n" +
//            "       f.business_controller_email_address,\n" +
//            "       f.claims_handling_option,\n" +
//            "       f.claims_handling_end_date,\n" +
//            "       f.backissues_handling_option,\n" +
//            "       f.backissues_handling_end_date,\n" +
//            "       l.ownership_brand_type,\n" +
//            "       l.society_contract_date,\n" +
//            "       l.society_contract_expiry_date,\n" +
//            "       l.journal_copyright_owner,\n" +
//            "       l.standard_copyright_statement_ind,\n" +
//            "       l.non_standard_copyright_statement,\n" +
//            "       l.open_access_article_copyright_type,\n" +
//            "       l.article_disclaimer,\n" +
//            "       l.publishing_right_type,\n" +
//            "       l.print_rights_secured_ind,\n" +
//            "       l.exclusive_rights_secured_ind,\n" +
//            "       l.no_exclusive_print_rights_reason,\n" +
//            "       l.electronic_print_rights_secured_ind,\n" +
//            "       l.no_exclusive_electronic_rights_reason,\n" +
//            "       l.rights_restrictions_ind,\n" +
//            "       l.rights_restrictions_list,\n" +
//            "       l.backfile_consent_ind,\n" +
//            "       l.backfile_consent_info,\n" +
//            "       l.back_rights_type,\n" +
//            "       l.back_rights_start_volume_and_issue,\n" +
//            "       l.non_exclusive_electronic_rights_ind,\n" +
//            "       l.post_termination_electronic_rights_type,\n" +
//            "       l.no_post_termination_electronic_rights_reason,\n" +
//            "       l.permission_type,\n" +
//            "       l.permission_handling_email_address,\n" +
//            "       l.society_membership_held_type,\n" +
//            "       l.special_arrangements,\n" +
//            "       l.despatch_method,\n" +
//            "       l.contract_seen_by_els_attorney_ind,\n" +
//            "       l.contract_sent_ind,\n" +
//            "       l.direct_billing_membership,\n" +
//            "       l.direct_billing_membership_with_fee,\n" +
//            "       l.society_maintained_prepayment,\n" +
//            "       l.society_maintained_account,\n" +
//            "       l.automatic_renewal,\n" +
//            "       l.mailing_labels,\n" +
//            "       l.title_included_in_ck,\n" +
//            "       l.soc_agreed_to_ck_content,\n" +
//            "       l.soc_ck_content_agreement_text,\n" +
//            "       l.months_to_keep_transfer_online,\n" +
//            "       l.soc_ck_content_agreement_date,\n" +
//            "       w.notified_date,\n" +
//            "       w.inbound_ts\n" +
//            "FROM "+ databaseEnv[1] +".jmf_product_work_2 w\n" +
//            "JOIN "+ databaseEnv[1] +".jmf_product_family_2 pf ON pf.product_family_id = w.f_product_family\n" +
//            "LEFT OUTER JOIN "+ databaseEnv[1] +".jmf_family_resource_details_2 frd1 ON frd1.f_product_family = pf.product_family_id and frd1.resource_key = 'PPC'\n" +
//            "LEFT OUTER JOIN "+ databaseEnv[1] +".jmf_family_resource_details_2 frd2 ON frd2.f_product_family = pf.product_family_id and frd2.resource_key = 'PMC'\n" +
//            "LEFT OUTER JOIN "+ databaseEnv[1] +".jmf_financial_information_2 f ON f.work_id = w.work_id\n" +
//            "LEFT OUTER JOIN "+ databaseEnv[1] +".jmf_legal_information_2 l ON l.work_id = w.work_id\n" +
//            "LEFT OUTER JOIN "+ databaseEnv[1] +".jmf_production_information_2 p ON p.work_id = w.work_id\n" +
//            "LEFT OUTER JOIN "+ GetJMDLDBUser.getProdStagingDataBase() +".eph_identifier_cross_reference_v xw\n" +
//            "  ON xw.identifier_type = 'ELSEVIER JOURNAL NUMBER'\n" +
//            "  AND xw.record_level = 'Work'\n" +
//            "  AND xw.identifier = w.elsevier_journal_number) where work_id in (%s)";

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

//
    public static String GET_JM_SOURCE_REFERENCE_IDs ="select jm_source_reference as JM_SOURCE_REFERENCE from "+ GetJMDLDBUser.getJMDB()+".%s order by rand() limit %s";
    public static String GET_SD_SUBJECT_AREAS_IDs ="select sa_id as SA_ID from "+ GetJMDLDBUser.getJMDB()+".sd_subject_areas_v order by rand() limit %s";
    public static String GET_MANIFESTATION_UPDATES1_IDs ="select w0_chronicle_id as W0_CHRONICLE_ID from journalmaestro_sit.etl_manifestation_updates1_v order by rand() limit %s";

}