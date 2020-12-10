package com.eph.automation.testing.web.steps.JMDataLake;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JMETL.JMContext;
import com.eph.automation.testing.models.dao.JMDataLake.JMETLObject;
import com.google.common.base.Joiner;
import cucumber.api.java.en.*;
import com.eph.automation.testing.services.db.JMDataLakeSQL.JMETLDataChecksSQL;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

public class JMETLDataChecks {
    private static String sql;
    private static List<String> Ids;

    @Given("^We get the (.*) random JM ids of (.*)$")
    public void getRandomJMIds(String numberOfRecords, String Currenttable) {
//  numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (Currenttable) {
            case "jmf_work":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkIds.stream().map(m -> (Integer) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_manifestation":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_MANIFESTATION_IDs, numberOfRecords);
                List<Map<?, ?>> randomManifestationIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifestationIds.stream().map(m -> (Integer) m.get("MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_person_role":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_PERSON_ROLE_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkPersonRolesIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkPersonRolesIds.stream().map(m -> (Integer) m.get("WORK_PERSON_ROLE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_subject_area":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_SUBJECT_AREA_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkSubjectAreaIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkSubjectAreaIds.stream().map(m -> (Integer) m.get("WORK_SUBJECT_AREA_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_chronicle":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_CHRONICLE_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkChronicleIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkChronicleIds.stream().map(m -> (Integer) m.get("WORK_CHRONICLE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_pmg_pubdir_allocation":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_PMG_PUBDIR_ALLOCATION_IDs, numberOfRecords);
                List<Map<?, ?>> randomPmgPubdirAllocationIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPmgPubdirAllocationIds.stream().map(m -> (Integer) m.get("PMG_PUBDIR_ALLOCATION_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_approval_attachment":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPROVAL_ATTACHMENT_IDs, numberOfRecords);
                List<Map<?, ?>> randomApprovalAttachmentIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomApprovalAttachmentIds.stream().map(m -> (Integer) m.get("APPROVAL_ATTACHMENT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_review_comment":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_REVIEW_COMMENT_IDs, numberOfRecords);
                List<Map<?, ?>> randomReviewCommentIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomReviewCommentIds.stream().map(m -> (Integer) m.get("REVIEW_COMMENT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_approval_request":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPROVAL_REQUEST_IDs, numberOfRecords);
                List<Map<?, ?>> randomApprovalRequestIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomApprovalRequestIds.stream().map(m -> (Integer) m.get("APPROVAL_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_application_properties":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPLICATION_PROPERTIES_IDs, numberOfRecords);
                List<Map<?, ?>> randomApplicationPropertiesIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomApplicationPropertiesIds.stream().map(m -> (String) m.get("APPLICATION_PROPERTY_KEY")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_chronicle_scenario":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_CHRONICLE_SCENARIO_IDs, numberOfRecords);
                List<Map<?, ?>> randomChronicleScenarioIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomChronicleScenarioIds.stream().map(m -> (String) m.get("CHRONICLE_SCENARIO_CODE")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_ownership":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_OWNERSHIP_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkOwnershipIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkOwnershipIds.stream().map(m -> (Integer) m.get("WORK_OWNERSHIP_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the JM Transformed Inbound records from (.*)$")
    public void getRecordsJMInbound(String Currenttable) {
        Log.info("We get the records from JM Access..");
        switch (Currenttable) {
            case "jmf_work":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK, Joiner.on(",").join(Ids));
                break;
            case "jmf_manifestation":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_MANIFESTATION, Joiner.on(",").join(Ids));
                break;
            case "jmf_work_person_role":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_PERSON_ROLE, Joiner.on(",").join(Ids));
                break;
            case "jmf_work_subject_area":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_SUBJECT_AREA, Joiner.on(",").join(Ids));
                break;
            case "jmf_work_chronicle":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_CHRONICLE, Joiner.on(",").join(Ids));
                break;
            case "jmf_pmg_pubdir_allocation":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_PMG_PUBDIR_ALLOCATION, Joiner.on(",").join(Ids));
                break;
            case "jmf_approval_attachment":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPROVAL_ATTACHMENT, Joiner.on(",").join(Ids));
                break;
            case "jmf_review_comment":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_REVIEW_COMMENT, Joiner.on(",").join(Ids));
                break;
            case "jmf_approval_request":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPROVAL_REQUEST, Joiner.on(",").join(Ids));
                break;
            case "jmf_application_properties":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPLICATION_PROPERTIES, Joiner.on("','").join(Ids));
                break;
            case "jmf_chronicle_scenario":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_CHRONICLE_SCENARIO, Joiner.on("','").join(Ids));
                break;
            case "jmf_work_ownership":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_OWNERSHIP, Joiner.on(",").join(Ids));
                break;
        }
        Log.info(sql);
        JMContext.JMObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.AWS_URL);
        System.out.println(JMContext.JMObjectsFromDL.size());
    }

    @Then("^We get the JM Staging Query records from (.*)$")
    public void getRecordsJMStagingQUERY(String CurrenttableQuery) {
        Log.info("We get the Current records..");
        switch (CurrenttableQuery) {
            case "work":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_QUERY, Joiner.on(",").join(Ids));
                break;
            case "manifestation":
                sql = String.format(JMETLDataChecksSQL.GET_MANIFESTATION_QUERY, Joiner.on(",").join(Ids));
                break;
            case "work_person_role":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_PERSON_ROLE_QUERY, Joiner.on(",").join(Ids));
                break;
            case "work_subject_area":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_SUBJECT_AREA_QUERY, Joiner.on(",").join(Ids));
                break;
            case "work_chronicle":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_CHRONICLE_QUERY, Joiner.on(",").join(Ids));
                break;
            case "pmg_pubdir_allocation":
                sql = String.format(JMETLDataChecksSQL.GET_PMG_PUBDIR_ALLOCATION_QUERY, Joiner.on(",").join(Ids));
                break;
            case "approval_attachment":
                sql = String.format(JMETLDataChecksSQL.GET_APPROVAL_ATTACHMENT_QUERY, Joiner.on(",").join(Ids));
                break;
            case "review_comment":
                sql = String.format(JMETLDataChecksSQL.GET_REVIEW_COMMENT_QUERY, Joiner.on(",").join(Ids));
                break;
            case "approval_request":
                sql = String.format(JMETLDataChecksSQL.GET_APPROVAL_REQUEST_QUERY, Joiner.on(",").join(Ids));
                break;
            case "application_properties":
                sql = String.format(JMETLDataChecksSQL.GET_APPLICATION_PROPERTIES_QUERY, Joiner.on("','").join(Ids));
                break;
            case "chronicle_scenario":
                sql = String.format(JMETLDataChecksSQL.GET_CHRONICLE_SCENARIO_QUERY, Joiner.on("','").join(Ids));
                break;
            case "work_ownership":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_OWNERSHIP_QUERY, Joiner.on(",").join(Ids));
                break;
        }
        Log.info(sql);
        JMContext.JMTransformObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JM records in Transformed Inbound and Current of (.*)$")
    public void compareJMTransformInboundtoCurrent(String Currenttable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JMContext.JMObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the JM records in Transformed Inbound and Current ..");
            for (int i = 0; i < JMContext.JMObjectsFromDL.size(); i++) {
                switch (Currenttable) {
                    case "jmf_work":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_id));

                        String[] JMF_WorkColumnName = {"getwork_id","getwork_chronicle_id","getelsevier_journal_number","getwork_journey_identifier","getwork_title","getwork_subtitle","getwork_title_info","getissn_l","getjournal_acronym_pts","getjournal_acronym_evise","getjournal_acronym_argi","geteph_work_id","getfamily_id","getfamily_title","getpmx_product_family_id","getpmc_family_resource_details_id","getpmc_old","getpmc_new","getpu_family_resource_details_id","getpu_email_old","getpu_email_new","getpu_peoplehubid_old","getpu_peoplehubid_new","getinternal_elsevier_division","getmanifestation_types_code","getmanifestation_personal_model_type","getimprint_name","getpts_journal_indicator","getyear_of_first_issue","getyear_of_last_issue","getfirst_volume_name","getfirst_issue_name","getlast_volume_name","getlast_issue_name","getvolumes_per_year_quantity","getissues_per_volume_quantity","getfirst_year_volumes_per_year_quantity","getfirst_year_issues_per_volume_quantity","getperiodical_timing_desc","getopen_accesstype_code","getopen_access_sponsor_name","getopen_access_fee","getopen_access_fee_short_art","getopen_access_currency_code","getopen_access_discount_ind","getopen_access_discount_period","getoa_first_year_discount_price_percentage","getoa_second_year_discount_price_percentage","getoa_first_year_discount_price","getoa_second_year_discount_price","getddp_eligible_ind","getwebshop_ind","getmedline_ind","getthomson_reuters_ind","getscopus_ind","getembase_ind","getdoaj_ind","getroad_ind","getpubmedcentral_ind","getmain_language_code","getenglish_language_percentage_type","getopen_archive_period","getdelayed_open_archive_ind","getinclude_in_collections_ind","getlaunch_date","getpublication_schedule_jan","getpublication_schedule_feb","getpublication_schedule_mar","getpublication_schedule_apr","getpublication_schedule_may","getpublication_schedule_jun","getpublication_schedule_jul","getpublication_schedule_aug","getpublication_schedule_sep","getpublication_schedule_oct","getpublication_schedule_nov","getpublication_schedule_dec","getmanifestation_formats_code","gettakeover_year","getdoi_prefix","getdoi_right_assigned_ind","getsociety_owned_ind","getchecked_with_oa_team_ind","getimprint_code","getdiscontinue_date","getpmx_product_work_id","getremoved_from_catalogue_ind","gettransfer_date","getle_mans_ind","getsociety_relationship_type","getarticle_number_per_year","getsubmission_number_per_year","getevise_requested_code","getevise_support_level","getevise_workflow_type","geteditor_location","getabp_usage_ind","getnon_standard_production_aspects","geteditorial_production_site","getproduction_specification_type","gettypeset_model_type","getreference_style_type","getbudgeted_page_number_per_issue","getlatex_submission_percentage","gettypesetter_code","getpage_review_charges","getcopy_editing_code","gethistory_date_format","getproof_sent_to_author_ind","getproof_sent_to_editor_ind","geteditor_email_address","gete_suite_journal_ind","getsponsor_accress_required_ind","getonline_publication_date_ind","getauthor_feedback_ind","getsend_copyright_form_ind","getrunning_order_details","getflexibility","getmaximum_page_details","getspecific_logo_required_ind","getadditional_deliveries_details","getmandatory_submission_item_ind","getdoi_statement_ind","getlanguage_editing_performed_ind","getlanguage_editing_stage","getdedicated_journal_url_ind","getdedicated_journal_url","getcoi_required_ind","geteditorial_system_name","gettypesetter_name","geteditorial_turnaround_time","getpending_submissions_quantity","getchecked_with_society_team_ind","getproduction_launch_date","getproposed_evise_rollout_period_date","getbackstock_end_year","getbackstock_end_option","getjbs_site_ind","geteditorial_production_email_address","geteditorial_production_site_name","getjournal_has_article_nos","getjournal_article_number_type","getbackstock_treatment_note","getjoint_venture_indicator","getjoint_venture_party_name","getbusiness_controller_name","getbusiness_unit_code","getpmg_code","getpmc_code","getopco_r12_id","getopco_r12_code","getcontract_opco_r12_code","getresponsibility_centre_code","gethfm_hierarchy_position_code","getcity_opco_r12_code","getcountry_opco_r12_code","getopco_r12_name","getopco_r12_legal_name","getrefund_option","getreminder_date","getreminder_option","getrenewal_option","getrenewal_date","getbusiness_controller_email_address","getclaims_handling_option","getclaims_handling_end_date","getbackissues_handling_option","getbackissues_handling_end_date","getownership_brand_type","getsociety_contract_date","getsociety_contract_expiry_date","getjournal_copyright_owner","getstandard_copyright_statement_ind","getnon_standard_copyright_statement","getopen_access_article_copyright_type","getarticle_disclaimer","getpublishing_right_type","getprint_rights_secured_ind","getexclusive_rights_secured_ind","getno_exclusive_print_rights_reason","getelectronic_print_rights_secured_ind","getno_exclusive_electronic_rights_reason","getrights_restrictions_ind","getrights_restrictions_list","getbackfile_consent_ind","getbackfile_consent_info","getback_rights_type","getback_rights_start_volume_and_issue","getnon_exclusive_electronic_rights_ind","getpost_termination_electronic_rights_type","getno_post_termination_electronic_rights_reason","getpermission_type","getpermission_handling_email_address","getsociety_membership_held_type","getspecial_arrangements","getdespatch_method","getcontract_seen_by_els_attorney_ind","getcontract_sent_ind","getdirect_billing_membership","getdirect_billing_membership_with_fee","getsociety_maintained_prepayment","getsociety_maintained_account","getautomatic_renewal","getmailing_labels","gettitle_included_in_ck","getsoc_agreed_to_ck_content","getsoc_ck_content_agreement_text","getmonths_to_keep_transfer_online","getsoc_ck_content_agreement_date","getnotified_date"};

                        for (String strTemp : JMF_WorkColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("WORK_ID => " +  JMContext.JMObjectsFromDL.get(i).getwork_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_manifestation":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getmanifestation_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getmanifestation_id));

                        String[] JMF_ManifestationColumnName = {"getmanifestation_id","getf_work","getmanifestation_title","getmanifestation_type","getissn","getelsevier_journal_number","getsubscription_type","getprice_categories","getpmx_product_manifestation_id","geteph_manifestation_id","getembargo_times_indicator","getelectronic_rights_secured_type","getonline_launch_date","getarticle_in_press_s5_ind","getarticle_in_press_s100_ind","getarticle_in_press_s250_ind","getembargo_times_number","getonline_last_issue_date","gettrim_size","getbase_print_run_number","getcolour_frequency","getartwork_sensitivity_ind","getmailing_breakdown_europe","getmailing_breakdown_usa","getmailing_breakdown_row","getzero_warehousing_ind","getback_stock_warehouse_location_type","getprinter_type","getprinter_location_type","getinterior_paper_type","getcover_paper_type","getdistributor_code","getdistributor_location_code","getprint_model_code","getseparate_print_run_ind","getoffprint_pricing_code","getoffprint_cover_ind","getfree_issues_quantity","getfree_offprints_type","getfree_paid_colour_offprints_quantity","getcolour_printing_currency_code","getcolour_artwork_exceptions","getsociety_owns_labels_ind","getbinding_type","getspecial_bulk_arrangements","getcost_first_printed_colour_unit","getcost_next_printed_colour_unit","getapplication_code","getnotified_date"};

                        for (String strTemp : JMF_ManifestationColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                                Log.info("Manifestation_ID => " +  JMContext.JMObjectsFromDL.get(i).getmanifestation_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                        case "jmf_work_person_role":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_person_role_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_person_role_id));

                            String[] JMF_WorkPersonRoleColumnName = {"getwork_person_role_id","getf_work","getparty_role_type","getemail_address","getfull_name","getphone_number","getaddress_line_1","getaddress_line_2","getaddress_line_3","getcity","getcountry","getstate","getpost_code","getorganisation_1","getpmx_party_id","getpeoplehub_id","geteph_person_id","getnotified_date"};

                            for (String strTemp : JMF_WorkPersonRoleColumnName) {

                                java.lang.reflect.Method method;
                                java.lang.reflect.Method method2;

                                JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                                JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                                method = objectToCompare1.getClass().getMethod(strTemp);
                                method2 = objectToCompare2.getClass().getMethod(strTemp);


                                Log.info("Work_Person_Role_ID => " +  JMContext.JMObjectsFromDL.get(i).getwork_person_role_id() +
                                        " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                            break;
                    case "jmf_work_subject_area":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_subject_area_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_subject_area_id));

                        String[] JMF_WorkSubjectAreaColumnName = {"getwork_subject_area_id","getf_work","getsubject_area_type_code","getsubject_area_priority_code","getsubject_area_code","getsubject_area_name","getnotified_date"};

                        for (String strTemp : JMF_WorkSubjectAreaColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Work_Subject_Area_ID => " +  JMContext.JMObjectsFromDL.get(i).getwork_subject_area_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_work_chronicle":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_chronicle_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_chronicle_id));

                        String[] JMF_WorkChronicleColumnName = {"getwork_chronicle_id","getcompleted_on","getdistribution_list","getrename_required_ind","getjournal_renamed_before_launch","getchronicle_status_code","getchronicle_scenario_code","getstarted_by","getstarted_on","getupdated_by","getupdated_on","getprocess_instance_id","getreason_for_change","getcancelled_by","getcreated_by_name","getrejection_comment","getsubmission_date","getcancelled_date","getrejection_date","getversion","getnotified_date"};

                        for (String strTemp : JMF_WorkChronicleColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Work_Chronicle_ID => " +  JMContext.JMObjectsFromDL.get(i).getwork_chronicle_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_pmg_pubdir_allocation":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getpmg_pubdir_allocation_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getpmg_pubdir_allocation_id));

                        String[] JMF_PmgPubDirAllocationColumnName = {"getpmg_pubdir_allocation_id","getallocation_type","getpmg_filter","getpmc_filter","getpu_filter_email","getpu_filter_name","getpmc_change_ind","getpu_change_ind","getpmx_pmgcode","getpmx_pmg_name","getpmx_pmg_f_business_unit","getpmg_bus_ctrl_name","getpmg_bus_ctrl_email","getpmg_pubdir_name_old","getpmg_pubdir_email_old","getpmg_pubdir_peoplehub_id_old","getpmg_pubdir_name_new","getpmg_pubdir_email_new","getpmg_pubdir_peoplehub_id_new","getnotified_date","geteph_pmg_code"};

                        for (String strTemp : JMF_PmgPubDirAllocationColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("PMG_Pubdir_Allocation_ID => " +  JMContext.JMObjectsFromDL.get(i).getpmg_pubdir_allocation_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_approval_attachment":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapproval_attachment_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapproval_attachment_id));

                        String[] JMF_ApprovalAttachmentColumnName = {"getapproval_attachment_id","getf_approval","getattachment_name","getattachment","getnotified_date"};

                        for (String strTemp : JMF_ApprovalAttachmentColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Approval_Attachment_ID => " +  JMContext.JMObjectsFromDL.get(i).getapproval_attachment_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_review_comment":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getreview_comment_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getreview_comment_id));

                        String[] JMF_ReviewCommentColumnName = {"getreview_comment_id","getf_approval_id","getreview_attribute_name","getreview_comment","getreview_comment_date","getcreated_on","getnotified_date"};

                        for (String strTemp : JMF_ReviewCommentColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Review_Comment_ID => " + JMContext.JMObjectsFromDL.get(i).getreview_comment_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_approval_request":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapproval_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapproval_id));

                        String[] JMF_ApprovalRequestColumnName = {"getapproval_id","getf_work_chronicle","getapproval_type","getapprover_name","getapproval_given_indicator","getapproval_request_date","getapproval_response_date","getnotified_date"};

                        for (String strTemp : JMF_ApprovalRequestColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Approval_ID => " + JMContext.JMObjectsFromDL.get(i).getapproval_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_application_properties":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapplication_property_key)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapplication_property_key));

                        String[] JMF_ApplicationPropertiesColumnName = {"getapplication_property_key","getapplication_property_value","getnotified_date"};

                        for (String strTemp : JMF_ApplicationPropertiesColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Application_Property_key => " + JMContext.JMObjectsFromDL.get(i).getapplication_property_key() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_chronicle_scenario":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getchronicle_scenario_code)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getchronicle_scenario_code));

                        String[] JMF_ChronicleScenarioColumnName = {"getchronicle_scenario_code","getchronicle_scenario_name","getchronicle_scenario_description","getchronicle_scenario_evolutionary_ind","getnotified_date"};

                        for (String strTemp : JMF_ChronicleScenarioColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Chronicle_Scenario_Code => " + JMContext.JMObjectsFromDL.get(i).getchronicle_scenario_code() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_work_ownership":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_ownership_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_ownership_id));

                        String[] JMF_WorkOwnershipColumnName = {"getwork_ownership_id","getf_work","getnotified_date","getf_legal_owner","getlegal_owner_name","getlegal_owner_type","getjournal_ownership_type"};

                        for (String strTemp : JMF_WorkOwnershipColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Work_Ownership_ID => " + JMContext.JMObjectsFromDL.get(i).getwork_ownership_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }

    @Given("^We get the (.*) random JM ETL ids of (.*)$")
    public void getRandomJMETLIds(String numberOfRecords, String Currenttable) {
//  numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (Currenttable) {
            case "etl_manifestation_updates1_v":
                sql = String.format(JMETLDataChecksSQL.GET_MANIFESTATION_UPDATES1_IDs, numberOfRecords);
                List<Map<?, ?>> randomManifestationUpdates1Ids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifestationUpdates1Ids.stream().map(m -> (Integer) m.get("W0_CHRONICLE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "sd_subject_areas_v":
                sql = String.format(JMETLDataChecksSQL.GET_SD_SUBJECT_AREAS_IDs, numberOfRecords);
                List<Map<?, ?>> randomSDSubjectAreasIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomSDSubjectAreasIds.stream().map(m -> (Integer) m.get("SA_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            default:
                sql = String.format(JMETLDataChecksSQL.GET_JM_SOURCE_REFERENCE_IDs, Currenttable, numberOfRecords);
                List<Map<?, ?>> randomAccountableProductDQIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomAccountableProductDQIds.stream().map(m -> (String) m.get("JM_SOURCE_REFERENCE")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }
}


