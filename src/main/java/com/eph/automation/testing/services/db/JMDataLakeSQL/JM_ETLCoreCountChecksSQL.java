package com.eph.automation.testing.services.db.JMDataLakeSQL;

import com.eph.automation.testing.services.db.PROMISDataLakeSQL.GetPRMDLDBUser;

public class JM_ETLCoreCountChecksSQL {


    public static String GET_JMF_ACCOUNTABLE_PRODUCT="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_accountable_product_dq_v";

    public static String GET_JMF_WORK="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_wwork_dq_v";

    public static String GET_JMF_WORK_IDENTIFIER="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_work_identifier_dq_v \n" +
            "\n";

    public static String GET_JMF_WORK_SUBJECT_AREA="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_work_subject_area_dq_v\n";

    public static String GET_JMF_WORK_PERSON_ROLE="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_work_person_role_dq_v";

    public static String GET_JMF_WORK_BUSINESS_MODEL="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_work_business_model_dq_v";

    public static String GET_JMF_MANIFESTATION_UPDATES="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_manifestation_updates1_v";

    public static String GET_JMF_WORK_ACCESS_MODEL="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_work_access_model_dq_v";

    public static String GET_JMF_MANIFESTATION="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_manifestation_dq_v";

    public static String GET_JMF_MANIFESTATION_IDENTIFIER="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_manifestation_identifier_dq_v";


    public static String GET_JMF_PRODUCT_PART="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_product_part1_v";

    public static String GET_JMF_PRODUCT_INSERT="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_product_inserts_v";

    public static String GET_JMF_PRODUCT_UPDATES="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_product_updates_v";

    public static String GET_JMF_PRODUCT="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_product_dq_v";

    public static String GET_JMF_PRODUCT_PERSON_ROLE="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_product_person_role_dq_v";


    public static String GET_JMF_IMPRINT_DATA="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".jm_imprint_data_v";

    public static String GET_JMF_SD_SUBJECT_AREAS="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".sd_subject_areas_v";

    public static String GET_JMF_WORKS_ATTRS_ROLES_PEOPLE="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".works_attrs_roles_people_v";

    public static String GET_JMF_STAGING_ACCOUNTABLE_PRODUCT="select count(*) as COUNT from \n" +
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
            ")";


    public static String GET_JMF_STAGING_WORK=
            "select count(*) as COUNT from( \n" +
                    "WITH\n" +
                    "  coowned_journals AS (\n" +
                    "   SELECT DISTINCT\n" +
                    "     co.f_work\n" +
                    "   , co.legal_owner_type\n" +
                    "   FROM\n" +
                    "     "+ GetJMDLDBUser.getJMDB()+".jmf_work_ownership co\n" +
                    "   WHERE (((co.notified_date IS NOT NULL) AND (co.journal_ownership_type = 'CO')) AND (co.legal_owner_type IN ('SOC', 'COM', 'UNI')))\n" +
                    ") \n" +
                    "SELECT\n" +
                    "  cs.chronicle_scenario_name scenario_name\n" +
                    ", wc.chronicle_scenario_code scenario_code\n" +
                    ", (CASE cs.chronicle_scenario_evolutionary_ind WHEN 'N' THEN 'Insert' WHEN 'Y' THEN 'Update' ELSE 'Update' END) upsert\n" +
                    ", \"concat\"('J0', w.elsevier_journal_number) jm_source_reference\n" +
                    ", w.eph_work_id eph_work_id\n" +
                    ", w.work_title work_title\n" +
                    ", w.work_subtitle work_subtitle\n" +
                    ", CAST(null AS boolean) electro_rights_indicator\n" +
                    ", 0 volume\n" +
                    ", CAST(null AS integer) copyright_year\n" +
                    ", CAST(null AS integer) edition_number\n" +
                    ", w.launch_date planned_launch_date\n" +
                    ", CAST(null AS date) planned_termination_date\n" +
                    ", 'JNL' f_type\n" +
                    ", 'WAP' f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", CAST(null AS varchar) pmc_old\n" +
                    ", w.pmc_code pmc_new\n" +
                    ", w.imprint_code f_imprint\n" +
                    ", w.opco_r12_id opco\n" +
                    ", (CASE WHEN (w.joint_venture_indicator = 'Y') THEN 'JVE' WHEN (fo.legal_owner_type IS NOT NULL) THEN fo.legal_owner_type WHEN (co_soc.legal_owner_type IS NOT NULL) THEN co_soc.legal_owner_type WHEN (co_com.legal_owner_type IS NOT NULL) THEN co_com.legal_owner_type WHEN (co_uni.legal_owner_type IS NOT NULL) THEN co_uni.legal_owner_type ELSE 'ELS' END) f_legal_ownership\n" +
                    ", (CASE WHEN (m.subscription_type = 'Calendar Year') THEN 'FY' WHEN (m.subscription_type = 'Rolling Year') THEN 'RY' WHEN (m.subscription_type = 'Both') THEN 'RY' ELSE null END) subscription_type\n" +
                    ", w.responsibility_centre_code resp_centre\n" +
                    ", COALESCE((CASE WHEN (w.main_language_code = 'English') THEN 'EN' WHEN (w.main_language_code IS NULL) THEN 'EN' WHEN (w.main_language_code = 'French') THEN 'FR' WHEN (w.main_language_code = 'German') THEN 'DE' WHEN (w.main_language_code = 'Spanish') THEN 'ES' WHEN (w.main_language_code = 'Catalan') THEN 'CA' WHEN (w.main_language_code = 'Italian') THEN 'IT' WHEN (w.main_language_code = 'Polish') THEN 'PL' WHEN (w.main_language_code = 'Portuguese') THEN 'PT' END), w.main_language_code) language_code\n" +
                    ", 'N' dq_err\n" +
                    ", w.notified_date notified_date\n" +
                    "FROM\n" +
                    "  (((((((("+ GetJMDLDBUser.getJMDB() +".jmf_work w\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_work_chronicle wc ON (wc.work_chronicle_id = w.work_chronicle_id))\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_chronicle_scenario cs ON (cs.chronicle_scenario_code = wc.chronicle_scenario_code))\n" +
                    "LEFT JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_work_ownership fo ON ((fo.f_work = w.work_id) AND (fo.journal_ownership_type = 'FO')))\n" +
                    "LEFT JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_manifestation m ON ((m.f_work = w.work_id) AND (m.issn = w.issn_l)))\n" +
                    "LEFT JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_work_ownership wo1 ON ((wo1.f_work = w.work_id) AND (wo1.journal_ownership_type = 'FO')))\n" +
                    "LEFT JOIN coowned_journals co_soc ON ((co_soc.f_work = w.work_id) AND (co_soc.legal_owner_type = 'SOC')))\n" +
                    "LEFT JOIN coowned_journals co_com ON ((co_com.f_work = w.work_id) AND (co_com.legal_owner_type = 'COM')))\n" +
                    "LEFT JOIN coowned_journals co_uni ON ((co_uni.f_work = w.work_id) AND (co_uni.legal_owner_type = 'UNI')))\n" +
                    "WHERE (((w.work_journey_identifier = 'A1') AND (wc.chronicle_scenario_code IN ('NP', 'NS', 'AC', 'MI'))) AND (w.notified_date IS NOT NULL))\n" +
                    "UNION SELECT\n" +
                    "  cs.chronicle_scenario_name scenario_name\n" +
                    ", wc.chronicle_scenario_code scenario_code\n" +
                    ", (CASE cs.chronicle_scenario_evolutionary_ind WHEN 'N' THEN 'Insert' WHEN 'Y' THEN 'Update' ELSE 'Update' END) upsert\n" +
                    ", \"concat\"('J0', COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)) jm_source_reference\n" +
                    ", COALESCE(w1.eph_work_id, w0.eph_work_id) eph_work_id\n" +
                    ", w1.work_title work_title\n" +
                    ", CAST(null AS varchar) work_subtitle\n" +
                    ", CAST(null AS boolean) electro_rights_indicator\n" +
                    ", CAST(null AS integer) volume\n" +
                    ", CAST(null AS integer) copyright_year\n" +
                    ", CAST(null AS integer) edition_number\n" +
                    ", CAST(null AS date) planned_launch_date\n" +
                    ", CAST(null AS date) planned_termination_date\n" +
                    ", 'JNL' f_type\n" +
                    ", CAST(null AS varchar) f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", CAST(null AS varchar) pmc_old\n" +
                    ", CAST(null AS varchar) pmc_new\n" +
                    ", CAST(null AS varchar) f_imprint\n" +
                    ", CAST(null AS varchar) opco\n" +
                    ", CAST(null AS varchar) f_legal_ownership\n" +
                    ", CAST(null AS varchar) subscription_type\n" +
                    ", CAST(null AS varchar) resp_centre\n" +
                    ", CAST(null AS varchar) language_code\n" +
                    ", (CASE WHEN ((w0.eph_work_id IS NULL) AND (w1.eph_work_id IS NULL)) THEN 'Y' WHEN ((w0.elsevier_journal_number IS NULL) AND (w1.elsevier_journal_number IS NULL)) THEN 'Y' ELSE 'N' END) dq_err\n" +
                    ", COALESCE(w1.notified_date, w0.notified_date) notified_date\n" +
                    "FROM\n" +
                    "  ((("+ GetJMDLDBUser.getJMDB() +".jmf_work w0\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_work_chronicle wc ON (wc.work_chronicle_id = w0.work_chronicle_id))\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_chronicle_scenario cs ON (cs.chronicle_scenario_code = wc.chronicle_scenario_code))\n" +
                    "LEFT JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_work w1 ON ((w1.work_chronicle_id = w0.work_chronicle_id) AND (w1.work_journey_identifier = 'A1')))\n" +
                    "WHERE ((((((wc.chronicle_scenario_code = 'RN') AND (w0.work_journey_identifier = 'A0')) AND (w1.notified_date IS NOT NULL)) AND (w0.work_title IS NOT NULL)) AND (w1.work_title IS NOT NULL)) AND (w1.work_title <> w0.work_title))\n" +
                    "UNION SELECT\n" +
                    "  cs.chronicle_scenario_name scenario_name\n" +
                    ", wc.chronicle_scenario_code scenario_code\n" +
                    ", (CASE cs.chronicle_scenario_evolutionary_ind WHEN 'N' THEN 'Insert' WHEN 'Y' THEN 'Update' ELSE 'Update' END) upsert\n" +
                    ", \"concat\"('J0', COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)) jm_source_reference\n" +
                    ", COALESCE(w1.eph_work_id, w0.eph_work_id) eph_work_id\n" +
                    ", CAST(null AS varchar) work_title\n" +
                    ", CAST(null AS varchar) work_subtitle\n" +
                    ", CAST(null AS boolean) electro_rights_indicator\n" +
                    ", CAST(null AS integer) volume\n" +
                    ", CAST(null AS integer) copyright_year\n" +
                    ", CAST(null AS integer) edition_number\n" +
                    ", CAST(null AS date) planned_launch_date\n" +
                    ", CAST(null AS date) planned_termination_date\n" +
                    ", 'JNL' f_type\n" +
                    ", CAST(null AS varchar) f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", w1.pmc_old pmc_old\n" +
                    ", w1.pmc_new pmc_new\n" +
                    ", CAST(null AS varchar) f_imprint\n" +
                    ", CAST(null AS varchar) opco\n" +
                    ", CAST(null AS varchar) f_legal_ownership\n" +
                    ", CAST(null AS varchar) subscription_type\n" +
                    ", CAST(null AS varchar) resp_centre\n" +
                    ", CAST(null AS varchar) language_code\n" +
                    ", (CASE WHEN ((w0.eph_work_id IS NULL) AND (w1.eph_work_id IS NULL)) THEN 'Y' WHEN ((w0.elsevier_journal_number IS NULL) AND (w1.elsevier_journal_number IS NULL)) THEN 'Y' ELSE 'N' END) dq_err\n" +
                    ", w1.notified_date notified_date\n" +
                    "FROM\n" +
                    "  ((("+ GetJMDLDBUser.getJMDB() +".jmf_work w0\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_work_chronicle wc ON (wc.work_chronicle_id = w0.work_chronicle_id))\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_chronicle_scenario cs ON (cs.chronicle_scenario_code = wc.chronicle_scenario_code))\n" +
                    "LEFT JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_work w1 ON (((w1.work_chronicle_id = w0.work_chronicle_id) AND (w1.elsevier_journal_number = w0.elsevier_journal_number)) AND (w1.work_journey_identifier = 'A1')))\n" +
                    "WHERE (((((((w0.work_journey_identifier = 'A0') AND (wc.chronicle_scenario_code = 'CA')) AND (w1.pmc_family_resource_details_id IS NOT NULL)) AND (w1.pmc_old IS NOT NULL)) AND (w1.pmc_new IS NOT NULL)) AND (w1.pmc_new <> w1.pmc_old)) AND (w1.notified_date IS NOT NULL))\n" +
                    "UNION SELECT\n" +
                    "  cs.chronicle_scenario_name scenario_name\n" +
                    ", wc.chronicle_scenario_code scenario_code\n" +
                    ", (CASE cs.chronicle_scenario_evolutionary_ind WHEN 'N' THEN 'Insert' WHEN 'Y' THEN 'Update' ELSE 'Update' END) upsert\n" +
                    ", \"concat\"('J0', COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)) jm_source_reference\n" +
                    ", COALESCE(w1.eph_work_id, w0.eph_work_id) eph_work_id\n" +
                    ", CAST(null AS varchar) work_title\n" +
                    ", CAST(null AS varchar) work_subtitle\n" +
                    ", CAST(null AS boolean) electro_rights_indicator\n" +
                    ", CAST(null AS integer) volume\n" +
                    ", CAST(null AS integer) copyright_year\n" +
                    ", CAST(null AS integer) edition_number\n" +
                    ", CAST(null AS date) planned_launch_date\n" +
                    ", w1.discontinue_date planned_termination_date\n" +
                    ", 'JNL' f_type\n" +
                    ", 'WDA' f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", CAST(null AS varchar) pmc_old\n" +
                    ", CAST(null AS varchar) pmc_new\n" +
                    ", CAST(null AS varchar) f_imprint\n" +
                    ", CAST(null AS varchar) opco\n" +
                    ", CAST(null AS varchar) f_legal_ownership\n" +
                    ", CAST(null AS varchar) subscription_type\n" +
                    ", CAST(null AS varchar) resp_centre\n" +
                    ", CAST(null AS varchar) language_code\n" +
                    ", (CASE WHEN ((w0.eph_work_id IS NULL) AND (w1.eph_work_id IS NULL)) THEN 'Y' WHEN ((w0.elsevier_journal_number IS NULL) AND (w1.elsevier_journal_number IS NULL)) THEN 'Y' ELSE 'N' END) dq_err\n" +
                    ", COALESCE(w1.notified_date, w0.notified_date) notified_date\n" +
                    "FROM\n" +
                    "  ((("+ GetJMDLDBUser.getJMDB() +".jmf_work w0\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_work_chronicle wc ON (wc.work_chronicle_id = w0.work_chronicle_id))\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_chronicle_scenario cs ON (cs.chronicle_scenario_code = wc.chronicle_scenario_code))\n" +
                    "LEFT JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_work w1 ON ((w1.work_chronicle_id = w0.work_chronicle_id) AND (w1.work_journey_identifier = 'A1')))\n" +
                    "WHERE (((w0.work_journey_identifier = 'A0') AND (wc.chronicle_scenario_code = 'DC')) AND (w0.notified_date IS NOT NULL))\n" +
                    "UNION SELECT\n" +
                    "  cs.chronicle_scenario_name scenario_name\n" +
                    ", wc.chronicle_scenario_code scenario_code\n" +
                    ", (CASE cs.chronicle_scenario_evolutionary_ind WHEN 'N' THEN 'Insert' WHEN 'Y' THEN 'Update' ELSE 'Update' END) upsert\n" +
                    ", \"concat\"('J0', COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)) jm_source_reference\n" +
                    ", COALESCE(w1.eph_work_id, w0.eph_work_id) eph_work_id\n" +
                    ", CAST(null AS varchar) work_title\n" +
                    ", CAST(null AS varchar) work_subtitle\n" +
                    ", CAST(null AS boolean) electro_rights_indicator\n" +
                    ", CAST(null AS integer) volume\n" +
                    ", CAST(null AS integer) copyright_year\n" +
                    ", CAST(null AS integer) edition_number\n" +
                    ", CAST(null AS date) planned_launch_date\n" +
                    ", w1.transfer_date planned_termination_date\n" +
                    ", 'JNL' f_type\n" +
                    ", (CASE WHEN (\"lower\"(w1.ownership_brand_type) = 'elsevier') THEN 'WVA' WHEN (\"lower\"(w1.ownership_brand_type) = 'third party') THEN 'WTA' WHEN (\"lower\"(w1.ownership_brand_type) = 'society') THEN 'WTA' ELSE 'WVA' END) f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", CAST(null AS varchar) pmc_old\n" +
                    ", CAST(null AS varchar) pmc_new\n" +
                    ", CAST(null AS varchar) f_imprint\n" +
                    ", CAST(null AS varchar) opco\n" +
                    ", CAST(null AS varchar) f_legal_ownership\n" +
                    ", CAST(null AS varchar) subscription_type\n" +
                    ", CAST(null AS varchar) resp_centre\n" +
                    ", CAST(null AS varchar) language_code\n" +
                    ", (CASE WHEN ((w0.eph_work_id IS NULL) AND (w1.eph_work_id IS NULL)) THEN 'Y' WHEN ((w0.elsevier_journal_number IS NULL) AND (w1.elsevier_journal_number IS NULL)) THEN 'Y' ELSE 'N' END) dq_err\n" +
                    ", COALESCE(w1.notified_date, w0.notified_date) notified_date\n" +
                    "FROM\n" +
                    "  ((("+ GetJMDLDBUser.getJMDB() +".jmf_work w0\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_work_chronicle wc ON (wc.work_chronicle_id = w0.work_chronicle_id))\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_chronicle_scenario cs ON (cs.chronicle_scenario_code = wc.chronicle_scenario_code))\n" +
                    "LEFT JOIN "+ GetJMDLDBUser.getJMDB() +".jmf_work w1 ON ((w1.work_chronicle_id = w0.work_chronicle_id) AND (w1.work_journey_identifier = 'A1')))\n" +
                    "WHERE (((w0.work_journey_identifier = 'A0') AND (wc.chronicle_scenario_code = 'TR')) AND (w0.notified_date IS NOT NULL))\n" +
                    "ORDER BY notified_date ASC, jm_source_reference ASC\n)";


    public static String GET_JMF_STAGING_WORK_IDENTIFIER="select count(*) as count from (\n" +
            "\n" +
            "select cs.chronicle_scenario_name as                                      scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                           upsert,\n" +
            "       'J0'||w.elsevier_journal_number||'-JOURNAL-NUMBER' as              jm_source_reference,\n" +
            "        w.eph_work_id as                                                  eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number as                                 work_source_reference,\n" +
            "        CAST ('ELSEVIER JOURNAL NUMBER' as varchar)                       f_type,\n" +
            "        w.elsevier_journal_number as                                      identifier,\n" +
            "        w.notified_date as                                                effective_start_date,\n" +
            "        'N' as                                                            dq_err,\n" +
            "        w.notified_date as                                                notified_date\n" +
            "from  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   w.notified_date is not null\n" +
            " \n" +
            "UNION\n" +
            " \n" +
            "select cs.chronicle_scenario_name as                                      scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                           upsert,\n" +
            "       'J0'||w.elsevier_journal_number||'-JOURNAL-ACRONYM-'||w.journal_acronym_pts as jm_source_reference,\n" +
            "        w.eph_work_id as                                                  eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number as                                 work_source_reference,\n" +
            "        CAST ('JOURNAL ACRONYM' as varchar)                               f_type,\n" +
            "        w.journal_acronym_pts as                                          identifier,\n" +
            "        w.notified_date as                                                effective_start_date,\n" +
            "        'N' as                                                            dq_err,\n" +
            "        w.notified_date as                                                notified_date\n" +
            "from  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   w.notified_date is not null\n" +
            " \n" +
            "UNION\n" +
            " \n" +
            "select  cs.chronicle_scenario_name as                                     scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                           upsert,\n" +
            "       'J0'||w.elsevier_journal_number||'-ISSN-L-'||w.issn_l as           jm_source_reference,\n" +
            "        w.eph_work_id as                                                  eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number as                                 work_source_reference,\n" +
            "        CAST ('ISSN-L' as varchar)                                        f_type,\n" +
            "        w.issn_l as                                                       identifier,\n" +
            "        w.notified_date as                                                effective_start_date,\n" +
            "        'N' as                                                            dq_err,\n" +
            "        w.notified_date as                                                notified_date\n" +
            "from  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   w.notified_date is not null\n" +
            " \n" +
            "UNION\n" +
            " \n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE - ISSN-L\n" +
            "-- RENAME SCENARIO. FIND OUT IF ISSN-L HAS CHANGED. IF IT HAS, WRITE A NEW WORK_IDENTIFIER RECORD\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "--\n" +
            "select  cs.chronicle_scenario_name as                                                                   scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                                                         upsert,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)||'-ISSN-L-'||w1.issn_l as jm_source_reference,  -- both jnl Nos should be set; A0 more reliable\n" +
            "        COALESCE(w0.eph_work_id, w1.eph_work_id) as                                                     eph_work_id,          -- both jnl Nos should be set; A0 more reliable\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number) as                        work_source_reference,\n" +
            "        CAST ('ISSN-L' as varchar)                                                                      f_type,              -- \"ISSN-L\"\n" +
            "        w1.issn_l as                                                                                    identifier,          --  ISSN-L value\n" +
            "        w1.notified_date as                                                                             effective_start_date,\n" +
            "       (CASE\n" +
            "            when (w1.eph_work_id             is null and w0.eph_work_id             is null) then 'Y'\n" +
            "            when (w1.elsevier_journal_number is null and w0.elsevier_journal_number is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                                                                                         dq_err,\n" +
            "        COALESCE(w1.notified_date, w0.notified_date) as                                                 notified_date        -- they should both be set the same\n" +
            "from  ((("+ GetJMDLDBUser.getJMDB()+".jmf_work                     w0\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on  (w0.work_chronicle_id       = wc.work_chronicle_id))\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "         left join "+ GetJMDLDBUser.getJMDB()+".jmf_work           w1 on ((w1.work_chronicle_id       = w0.work_chronicle_id)\n" +
            "                                        and  (w1.work_journey_identifier = 'A1')))\n" +
            "where  w0.work_journey_identifier = 'A0'\n" +
            "and    wc.chronicle_scenario_code = 'RN'\n" +
            "and    w1.notified_date is not null\n" +
            "and    w0.issn_l is not null\n" +
            "and    w1.issn_l is not null\n" +
            "and    w1.issn_l <> w0.issn_l\n" +
            " \n" +
            "UNION\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE - PTS ACRONYM\n" +
            "-- RENAME SCENARIO. FIND OUT IF PTS ACRONYM HAS CHANGED. IF IT HAS, WRITE A NEW WORK_IDENTIFIER RECORD FOR \"ACRONYM\".\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "--\n" +
            "select  cs.chronicle_scenario_name as                                                                scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                                                      upsert,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)||'-JOURNAL-ACRONYM-'||w1.journal_acronym_pts as jm_source_reference, -- both jnl Nos should be set; A0 is more reliable\n" +
            "        COALESCE(w0.eph_work_id, w1.eph_work_id) as                                                  eph_work_id,         -- both jnl Nos should be set; A0 is more reliable\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number) as                     work_source_reference,\n" +
            "        CAST ('JOURNAL ACRONYM' as varchar)                                                          f_type,                 -- type is \"JOURNAL ACRONYM\"\n" +
            "        w1.journal_acronym_pts as                                                                    identifier,             -- the new PTS Acronym value\n" +
            "        w1.notified_date as                                                                          effective_start_date,\n" +
            "       (CASE\n" +
            "            when (w1.eph_work_id             is null and w0.eph_work_id             is null) then 'Y'\n" +
            "            when (w1.elsevier_journal_number is null and w0.elsevier_journal_number is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                                                                                      dq_err,\n" +
            "        COALESCE(w1.notified_date, w0.notified_date) as                                              notified_date        -- they should both be set the same\n" +
            "from  ((("+ GetJMDLDBUser.getJMDB()+".jmf_work                     w0\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on  (w0.work_chronicle_id       = wc.work_chronicle_id))\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "         left join "+ GetJMDLDBUser.getJMDB()+".jmf_work           w1 on ((w1.work_chronicle_id       = w0.work_chronicle_id)\n" +
            "                                        and  (w1.work_journey_identifier = 'A1')))\n" +
            "where  w0.work_journey_identifier = 'A0'\n" +
            "and    wc.chronicle_scenario_code = 'RN'\n" +
            "and    w1.notified_date is not null\n" +
            "and    w0.journal_acronym_pts is not null\n" +
            "and    w1.journal_acronym_pts is not null\n" +
            "and    w1.journal_acronym_pts <> w0.journal_acronym_pts)\n" +
            "\n";
    public static String GET_JMF_STAGING_WORK_SUBJECT_AREA="select count(*) as count from (\n" +
            "select  cs.chronicle_scenario_name as    scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "       'J0'||w.elsevier_journal_number||'-'||wsa.subject_area_code as jm_source_reference,\n" +
            "        w.eph_work_id                   eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number  work_source_reference,\n" +
            "        CAST (null as integer)          f_subject_area,\n" +
            "        w.elsevier_journal_number as    elsevier_journal_number,\n" +
            "        wsa.subject_area_code as        subject_area_code,\n" +
            "        wsa.subject_area_type_code as   subject_area_type,\n" +
            "        wsa.notified_date as            start_date,\n" +
            "        CAST (null as date)             end_date,\n" +
            "       'N'  as                          dq_err,\n" +
            "        wsa.notified_date as            notified_date\n" +
            "from  "+ GetJMDLDBUser.getJMDB()+".jmf_work_subject_area  wsa\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w  on w.work_id = wsa.f_work\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where wsa.subject_area_type_code = 'SD'\n" +
            "and   wsa.notified_date is not null\n" +
            "and   w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "order by wsa.notified_date, jm_source_reference)";

    public static String GET_JMF_STAGING_WORK_PERSON_ROLE="select * from \n" +
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
            "base_title,                      -- jm-manifestation-title a suffix of (Print) or (Online)\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                    -- not known by jm for new products.\n" +
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
            "select * from result_data)";

    public static String GET_JMF_STAGING_WORK_PERSON_ROLE_Count="select count(*) as COUNT from(\n" +
            "(select distinct\n" +
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
            "        CAST (null as varchar) as                                     jm_source_ref_old, \n" +
            "       'J0'||w.elsevier_journal_number||'-'||wpr.peoplehub_id||'-PU'  jm_source_ref_new,\n" +
            "        w.eph_work_id as                eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number  work_source_reference,\n" +
            "        wpr.eph_person_id as            f_person,\n" +
            "        w.elsevier_journal_number as    elsevier_journal_number,\n" +
            "        CAST (null as varchar) as       employee_number_old, \n" +
            "        wpr.peoplehub_id as             employee_number_new,\n" +
            "       'PU' as                          f_role,\n" +
            "        CAST (null as varchar) as       internal_email_old,  \n" +
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
            "        CAST (null as varchar) as                                         jm_source_ref_old, \n" +
            "       'J0'||w.elsevier_journal_number||'-'||warp.pd_peoplehub_id||'-PD'  jm_source_ref_new,\n" +
            "        w.eph_work_id as                eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number  work_source_reference,\n" +
            "        wpr.eph_person_id as            f_person,\n" +
            "        w.elsevier_journal_number as    elsevier_journal_number,\n" +
            "        CAST (null as varchar) as       employee_number_old, \n" +
            "        warp.pd_peoplehub_id as         employee_number_new,\n" +
            "        'PD' as                         f_role,\n" +
            "        CAST (null as varchar) as       internal_email_old,  \n" +
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
            "UNION  \n" +
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
            "        CAST (null as varchar) as       employee_number_old, \n" +
            "        warp.bc_peoplehub_id as         employee_number_new,\n" +
            "        'BC' as                         f_role,\n" +
            "        CAST (null as varchar) as       internal_email_old,  \n" +
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
            "UNION  \n" +
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
            "and   w1.pu_peoplehubid_new is not null  -- EPHD-2877 filter out PMC-change records\n" +
            "and   w1.pu_email_new       is not null  -- EPHD-2877 filter out PMC-change records\n" +
            "and   wpr.notified_date     is not null\n" +
            "UNION  \n" +
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
            "and      ppa.notified_date >= ppj.jnl_created_date\n" +
            "  )\n" +
            "order by notified_date, jm_source_ref_new\n" +
            "))";

    public static String GET_JMF_STAGING_WORK_ACCESS_MODEL_Count=
            "select count(*) as COUNT from(\n" +
           "SELECT\n" +
                    "  cs.chronicle_scenario_name scenario_name\n" +
                    ", cs.chronicle_scenario_code scenario_code\n" +
                    ", \"concat\"('J0', w.elsevier_journal_number) external_work_ref\n" +
                    ", \"concat\"(\"concat\"(\"concat\"('J0', w.elsevier_journal_number), '-'), wam.access_model_code) external_reference\n" +
                    ", CAST(null AS integer) eph_work_id\n" +
                    ", wam.access_model_code access_model_code\n" +
                    ", wam.access_model_description access_model_description\n" +
                    ", wam.notified_date notified_date\n" +
                    ", wam.effective_start_date effective_start_date\n" +
                    ", wam.effective_end_date effective_end_date\n" +
                    "FROM\n" +
                    "  (((("+ GetJMDLDBUser.getJMDB()+".jmf_work_access_model wam\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_work w ON (w.work_id = wam.f_work))\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB()+".eph_access_model_v am ON (am.code = wam.access_model_code))\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle wc ON (wc.work_chronicle_id = w.work_chronicle_id))\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs ON (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
                    "WHERE (((w.work_journey_identifier = 'A1') AND (wc.chronicle_scenario_code IN ('NP', 'NS', 'AC', 'MI'))) OR (((wc.chronicle_scenario_code IN ('CBM')) AND (wam.row_change_type IN ('I', 'U'))) AND (w.notified_date IS NOT NULL)))\n" +
                    "ORDER BY w.elsevier_journal_number ASC)";


    public static String GET_JMF_STAGING_WORK_BUSINESS_MODEL_Count=
            "select count(*) as COUNT from (\n" +
             "SELECT\n" +
                    "  cs.chronicle_scenario_name scenario_name\n" +
                    ", cs.chronicle_scenario_code scenario_code\n" +
                    ", \"concat\"('J0', w.elsevier_journal_number) external_work_ref\n" +
                    ", \"concat\"(\"concat\"(\"concat\"('J0', w.elsevier_journal_number), '-'), wbm.business_model_code) external_reference\n" +
                    ", CAST(null AS integer) eph_work_id\n" +
                    ", wbm.business_model_code business_model_code\n" +
                    ", wbm.business_model_description business_model_description\n" +
                    ", wbm.notified_date notified_date\n" +
                    ", wbm.effective_start_date effective_start_date\n" +
                    ", wbm.effective_end_date effective_end_date\n" +
                    "FROM\n" +
                    "  (((("+ GetJMDLDBUser.getJMDB()+".jmf_work_business_model wbm\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_work w ON (w.work_id = wbm.f_work))\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB()+".eph_business_model_v bm ON (bm.code = wbm.business_model_code))\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle wc ON (wc.work_chronicle_id = w.work_chronicle_id))\n" +
                    "INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs ON (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
                    "WHERE (((w.work_journey_identifier = 'A1') AND (wc.chronicle_scenario_code IN ('NP', 'NS', 'AC', 'MI'))) OR (((wc.chronicle_scenario_code IN ('CBM')) AND (wbm.row_change_type IN ('I', 'U'))) AND (w.notified_date IS NOT NULL)))\n" +
                    "ORDER BY w.elsevier_journal_number ASC)";

    public static String GET_JMF_STAGING_MANIFESTATION_UPDATES="select count(*) as count from (\n" +
            "select cs.chronicle_scenario_name as              scenario,\n" +
            "      (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "\t    END) as                                    upsert,\n" +
            "       w0.work_chronicle_id                        w0_chronicle_id,         -- 672, 1339, 1600 etc. journal numbers and work journey identifiers beneath (A0, A1).\n" +
            "       w0.elsevier_journal_number                  w0_journal_number,       -- from A0 work, format 12345\n" +
            "\t   w0.work_journey_identifier                  w0_journey_identifier,   -- proof that we're looking at the work A0.\n" +
            " \t   m0.manifestation_id                         m0_manifestation_id,     -- should be unique\n" +
            " \t   m0.manifestation_type                       m0_manifestation_type,   -- yes, we have E or P\n" +
            "       m0.elsevier_journal_number                  m0_journal_number,       -- format 12345\n" +
            "       w0.issn_l                                   w0_issn_l,               -- from A0 work, format 1234-5678, 1234-567X\n" +
            "       m0.manifestation_title                      m0_manifestation_title,  -- A0, from m0 manifestation\n" +
            "       m0.issn                                     m0_issn,                 -- format 1234-5678, 1234-567X\n" +
            "       w0.eph_work_id                              w0_eph_work_id,          -- from A0 work\n" +
            "       m0.eph_manifestation_id                     m0_eph_manifestation_id, -- from the manifestation pointing to the A0 work\n" +
            "       m0.notified_date                            m0_notified_date         -- should be stamped the same as m1_notified_date\n" +
            "from ((("+ GetJMDLDBUser.getJMDB()+".jmf_manifestation           m0                                      -- the joins and where clause select all rename manifestations 'before', both p & e.\n" +
            "        join "+ GetJMDLDBUser.getJMDB()+".jmf_work               w0 on ((m0.f_work = w0.work_id)\n" +
            "                                       and (w0.work_journey_identifier = 'A0')))\n" +
            "        join "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on ((w0.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                       and (w0.work_journey_identifier = 'A0')))\n" +
            "        join "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on ((wc.chronicle_scenario_code = cs.chronicle_scenario_code)\n" +
            "\t\t                               and (wc.chronicle_scenario_code = 'RN')))\n" +
            "where    m0.notified_date is not null\n" +
            "group by cs.chronicle_scenario_name, cs.chronicle_scenario_evolutionary_ind,\n" +
            "         w0.work_chronicle_id, w0.elsevier_journal_number, w0.work_journey_identifier,\n" +
            "         m0.manifestation_id, m0.manifestation_type, m0.elsevier_journal_number, m0.manifestation_title, m0.issn, w0.issn_l, w0.eph_work_id, m0.eph_manifestation_id, m0.notified_date\n" +
            "order by w0.work_chronicle_id, w0.elsevier_journal_number, w0.work_journey_identifier,\n" +
            "         m0.manifestation_type, m0.elsevier_journal_number, m0.notified_date\n" +
            ")";


    public static String GET_JMF_STAGING_MANIFESTATION="select count(*) as COUNT from\n" +
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
            "from  "+GetJMDLDBUser.getJMDB()+".jmf_manifestation      m\n" +
            "join  "+GetJMDLDBUser.getJMDB()+".jmf_work               w  on m.f_work = w.work_id\n" +
            "join  "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  "+GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
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
            "from ((("+GetJMDLDBUser.getJMDB()+".etl_manifestation_updates1_v mu1\n" +
            "        join "+GetJMDLDBUser.getJMDB()+".jmf_work           w1 on ((w1.work_chronicle_id       = mu1.w0_chronicle_id)\n" +
            "                                   and (w1.elsevier_journal_number = mu1.w0_journal_number)\n" +
            "                                   and (w1.work_journey_identifier = 'A1')))                        -- we've definitely got one work, the A1.\n" +
            "        join "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle wc on ((w1.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                   and (w1.work_journey_identifier = 'A1')\n" +
            "                                   and (wc.chronicle_scenario_code = 'RN')))\n" +
            "        join "+GetJMDLDBUser.getJMDB()+".jmf_manifestation  m1 on ((m1.f_work = w1.work_id)\n" +
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
            ")";

    public static String GET_JMF_STAGING_MANIFESTATION_IDENTIFIER="select count(*) as count from (select * from \n" +
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
            "from  "+ GetJMDLDBUser.getJMDB()+".jmf_manifestation      m\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w  on m.f_work = w.work_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
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
            "from ((("+ GetJMDLDBUser.getJMDB()+".etl_manifestation_updates1_v mu1\n" +
            "        join "+ GetJMDLDBUser.getJMDB()+".jmf_work                w1 on ((w1.work_chronicle_id       = mu1.w0_chronicle_id)\n" +
            "                                        and (w1.elsevier_journal_number = mu1.w0_journal_number)\n" +
            "                                        and (w1.work_journey_identifier = 'A1')))                   -- we've definitely got one work, the A1.\n" +
            "        join "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle      wc on ((w1.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                        and (w1.work_journey_identifier = 'A1')\n" +
            "                                        and (wc.chronicle_scenario_code = 'RN')))\n" +
            "        join "+ GetJMDLDBUser.getJMDB()+".jmf_manifestation       m2 on ((m2.f_work = w1.work_id)\n" +
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
            "))";


    public static String GET_JMF_STAGING_PRODUCT_PART="select count(*) as count from (" +
            "WITH\n" +
            "  work_business_model AS (\n" +
            "   SELECT\n" +
            "     w.work_id\n" +
            "   , \"max\"((CASE WHEN (wbm.business_model_code = 'SBS') THEN 'Y' ELSE 'N' END)) SBS\n" +
            "   , \"max\"((CASE WHEN (wbm.business_model_code = 'APC') THEN 'Y' ELSE 'N' END)) APC\n" +
            "   , \"max\"((CASE WHEN (wbm.business_model_code = 'SBD') THEN 'Y' ELSE 'N' END)) SBD\n" +
            "   FROM\n" +
            "     ("+ GetJMDLDBUser.getJMDB()+".jmf_work w\n" +
            "   INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_work_business_model wbm ON ((wbm.f_work = w.work_id) AND (wbm.notified_date IS NOT NULL)))\n" +
            "   GROUP BY w.work_id\n" +
            ") \n" +
            "(\n" +
            "   SELECT\n" +
            "     cs.chronicle_scenario_name scenario_name\n" +
            "   , cs.chronicle_scenario_code scenario_code\n" +
            "   , (CASE cs.chronicle_scenario_evolutionary_ind WHEN 'N' THEN 'Insert' WHEN 'Y' THEN 'Update' ELSE 'Update' END) upsert\n" +
            "   , \"concat\"(\"concat\"(m.manifestation_type, '-J0'), w.elsevier_journal_number) jm_source_reference\n" +
            "   , w.eph_work_id eph_work_id\n" +
            "   , m.eph_manifestation_id eph_manifestation_id\n" +
            "   , CAST(null AS varchar) eph_product_id\n" +
            "   , (CASE m.manifestation_type WHEN 'E' THEN \"concat\"(m.manifestation_title, ' (Online)') ELSE \"concat\"(m.manifestation_title, ' (Print)') END) base_title\n" +
            "   , w.elsevier_journal_number w0_journal_number\n" +
            "   , m.elsevier_journal_number m0_journal_number\n" +
            "   , w.work_chronicle_id w0_chronicle_id\n" +
            "   , w.work_journey_identifier w0_journey_identifier\n" +
            "   , m.manifestation_type m0_manifestation_type\n" +
            "   , CAST(1 AS boolean) separately_saleable_ind\n" +
            "   , CAST(0 AS boolean) trial_allowed_ind\n" +
            "   , CAST(w.launch_date AS date) launch_date\n" +
            "   , (CASE m.manifestation_type WHEN 'P' THEN 'G003' ELSE 'S001' END) tax_code\n" +
            "   , (CASE WHEN (wbm.SBS = 'Y') THEN 'Y' ELSE 'N' END) subscription\n" +
            "   , (CASE WHEN ((wbm.SBS = 'Y') AND (m.manifestation_type = 'P')) THEN 'Y' ELSE 'N' END) bulk_sales\n" +
            "   , (CASE WHEN ((wbm.SBS = 'Y') AND (m.manifestation_type = 'E')) THEN 'Y' ELSE 'N' END) back_files\n" +
            "   , (CASE WHEN ((wbm.APC = 'Y') OR (wbm.SBD = 'Y')) THEN 'Y' ELSE 'N' END) open_access\n" +
            "   , (CASE m.manifestation_type WHEN 'P' THEN 'Y' ELSE 'N' END) reprints\n" +
            "   , CAST('Y' AS varchar) author_charges\n" +
            "   , CAST('N' AS varchar) one_off_access\n" +
            "   , CAST('N' AS varchar) packages\n" +
            "   , CAST('PPL' AS varchar) availability_status\n" +
            "   , CAST('PPL' AS varchar) work_status\n" +
            "   , w.work_title work_title\n" +
            "   , CAST('JOURNAL' AS varchar) work_type\n" +
            "   , (CASE WHEN (w.elsevier_journal_number IS NULL) THEN 'Y' WHEN (m.elsevier_journal_number IS NULL) THEN 'Y' WHEN (m.manifestation_type IS NULL) THEN 'Y' ELSE 'N' END) dq_err\n" +
            "   , m.notified_date notified_date\n" +
            "   FROM\n" +
            "     (((("+ GetJMDLDBUser.getJMDB()+".jmf_manifestation m\n" +
            "   INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_work w ON (m.f_work = w.work_id))\n" +
            "   INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle wc ON (w.work_chronicle_id = wc.work_chronicle_id))\n" +
            "   INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs ON (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "   LEFT JOIN work_business_model wbm ON (wbm.work_id = w.work_id))\n" +
            "   WHERE (((w.work_journey_identifier = 'A1') AND (wc.chronicle_scenario_code IN ('NP', 'NS', 'AC', 'MI'))) AND (m.notified_date IS NOT NULL))\n" +
            "   GROUP BY cs.chronicle_scenario_name, cs.chronicle_scenario_code, cs.chronicle_scenario_evolutionary_ind, w.work_chronicle_id, w.elsevier_journal_number, w.work_journey_identifier, wbm.SBS, wbm.APC, wbm.SBD, m.manifestation_id, w.launch_date, m.manifestation_type, m.elsevier_journal_number, m.manifestation_title, w.eph_work_id, w.work_title, m.eph_manifestation_id, m.notified_date\n" +
            "UNION    SELECT\n" +
            "     cs.chronicle_scenario_name scenario_name\n" +
            "   , cs.chronicle_scenario_code scenario_code\n" +
            "   , (CASE cs.chronicle_scenario_evolutionary_ind WHEN 'N' THEN 'Insert' WHEN 'Y' THEN 'Update' ELSE 'Update' END) upsert\n" +
            "   , \"concat\"(\"concat\"(m0.manifestation_type, '-J0'), w0.elsevier_journal_number) jm_source_reference\n" +
            "   , w0.eph_work_id eph_work_id\n" +
            "   , m0.eph_manifestation_id eph_manifestation_id\n" +
            "   , CAST(null AS varchar) eph_product_id\n" +
            "   , (CASE m0.manifestation_type WHEN 'E' THEN \"concat\"(m0.manifestation_title, ' (Online)') ELSE \"concat\"(m0.manifestation_title, ' (Print)') END) base_title\n" +
            "   , w0.elsevier_journal_number w0_journal_number\n" +
            "   , m0.elsevier_journal_number m0_journal_number\n" +
            "   , w0.work_chronicle_id w0_chronicle_id\n" +
            "   , w0.work_journey_identifier w0_journey_identifier\n" +
            "   , m0.manifestation_type m0_manifestation_type\n" +
            "   , CAST(null AS boolean) separately_saleable_ind\n" +
            "   , CAST(null AS boolean) trial_allowed_ind\n" +
            "   , CAST(null AS date) launch_date\n" +
            "   , CAST(null AS varchar) tax_code\n" +
            "   , (CASE WHEN (wbm0.SBS = 'Y') THEN 'Y' ELSE 'N' END) subscription\n" +
            "   , (CASE WHEN ((wbm0.SBS = 'Y') AND (m0.manifestation_type = 'P')) THEN 'Y' ELSE 'N' END) bulk_sales\n" +
            "   , (CASE WHEN ((wbm0.SBS = 'Y') AND (m0.manifestation_type = 'E')) THEN 'Y' ELSE 'N' END) back_files\n" +
            "   , (CASE WHEN ((wbm0.APC = 'Y') OR (wbm0.SBD = 'Y')) THEN 'Y' ELSE 'N' END) open_access\n" +
            "   , (CASE m0.manifestation_type WHEN 'P' THEN 'Y' ELSE 'N' END) reprints\n" +
            "   , CAST('Y' AS varchar) author_charges\n" +
            "   , CAST('N' AS varchar) one_off_access\n" +
            "   , CAST('N' AS varchar) packages\n" +
            "   , CAST(null AS varchar) availability_status\n" +
            "   , CAST(null AS varchar) work_status\n" +
            "   , w0.work_title w0_work_title\n" +
            "   , CAST('JOURNAL' AS varchar) work_type\n" +
            "   , (CASE WHEN (w0.elsevier_journal_number IS NULL) THEN 'Y' WHEN (m0.elsevier_journal_number IS NULL) THEN 'Y' WHEN (m0.manifestation_type IS NULL) THEN 'Y' ELSE 'N' END) dq_err\n" +
            "   , m0.notified_date notified_date\n" +
            "   FROM\n" +
            "     (((("+ GetJMDLDBUser.getJMDB()+".jmf_manifestation m0\n" +
            "   INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_work w0 ON ((m0.f_work = w0.work_id) AND (w0.work_journey_identifier = 'A0')))\n" +
            "   INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle wc ON ((w0.work_chronicle_id = wc.work_chronicle_id) AND (w0.work_journey_identifier = 'A0')))\n" +
            "   INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs ON (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "   LEFT JOIN work_business_model wbm0 ON (wbm0.work_id = w0.work_id))\n" +
            "   WHERE ((wc.chronicle_scenario_code = 'RN') AND (m0.notified_date IS NOT NULL))\n" +
            "   GROUP BY cs.chronicle_scenario_name, cs.chronicle_scenario_code, cs.chronicle_scenario_evolutionary_ind, w0.work_chronicle_id, w0.elsevier_journal_number, w0.work_journey_identifier, wbm0.SBS, wbm0.APC, wbm0.SBD, m0.manifestation_id, m0.manifestation_type, m0.elsevier_journal_number, m0.manifestation_title, w0.eph_work_id, w0.work_title, m0.eph_manifestation_id, m0.notified_date\n" +
            ") ORDER BY upsert ASC, scenario_name ASC, notified_date DESC, jm_source_reference ASC\n)";

    public static String GET_JMF_STAGING_PRODUCT_INSERT="select count(*) as count from (\n" +
            "WITH\n" +
            "  base_data AS (\n" +
            "   SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , jm_source_reference\n" +
            "   , \"concat\"('J0', w0_journal_number) ult_work_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , base_title\n" +
            "   , w0_journal_number\n" +
            "   , m0_journal_number\n" +
            "   , w0_chronicle_id\n" +
            "   , w0_journey_identifier\n" +
            "   , m0_manifestation_type\n" +
            "   , separately_saleable_ind\n" +
            "   , trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , tax_code\n" +
            "   , subscription\n" +
            "   , bulk_sales\n" +
            "   , back_files\n" +
            "   , open_access\n" +
            "   , reprints\n" +
            "   , author_charges\n" +
            "   , one_off_access\n" +
            "   , packages\n" +
            "   , availability_status\n" +
            "   , work_status\n" +
            "   , work_title\n" +
            "   , work_type\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     etl_product_part1_v\n" +
            "   WHERE (notified_date IS NOT NULL)\n" +
            ") \n" +
            ", crosstab_data AS (\n" +
            "   SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(jm_source_reference, '-SUB') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(base_title, ' Subscription') \"name\"\n" +
            "   , (CASE WHEN (availability_status = 'PNS') THEN false ELSE separately_saleable_ind END) separately_saleable_ind\n" +
            "   , trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , tax_code\n" +
            "   , 'SUB' f_type\n" +
            "   , m0_manifestation_type\n" +
             "   , (CASE WHEN (availability_status = 'PSTB') THEN 'PST' ELSE availability_status END) f_status\n" +
            "   , work_type\n" +
            "   , jm_source_reference manifestation_ref\n" +
            "   , ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     base_data\n" +
            "   WHERE ((upsert = 'Insert') AND (subscription = 'Y'))\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(jm_source_reference, '-JBS') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(base_title, ' Bulk Sales') \"name\"\n" +
            "   , (CASE WHEN (availability_status = 'PNS') THEN false ELSE separately_saleable_ind END) separately_saleable_ind\n" +
            "   , trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , tax_code\n" +
            "   , 'JBS' f_type\n" +
            "   , m0_manifestation_type\n" +
            "   , (CASE WHEN (availability_status = 'PSTB') THEN 'PST' ELSE availability_status END) f_status\n" +
            "   , work_type\n" +
            "   , jm_source_reference manifestation_ref\n" +
            "   , ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     base_data\n" +
            "   WHERE ((upsert = 'Insert') AND (bulk_sales = 'Y'))\n" +
              "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(jm_source_reference, '-BKF') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(base_title, ' Back Files') \"name\"\n" +
            "   , (CASE WHEN (availability_status = 'PNS') THEN false ELSE separately_saleable_ind END) separately_saleable_ind\n" +
            "   , trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , tax_code\n" +
            "   , 'BKF' f_type\n" +
            "   , m0_manifestation_type\n" +
            "   , (CASE WHEN (availability_status = 'PSTB') THEN 'PAS' ELSE availability_status END) f_status\n" +
            "   , work_type\n" +
            "   , jm_source_reference manifestation_ref\n" +
            "   , ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     base_data\n" +
            "   WHERE ((upsert = 'Insert') AND (back_files = 'Y'))\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(jm_source_reference, '-RPR') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(base_title, ' Reprints') \"name\"\n" +
            "   , (CASE WHEN (availability_status = 'PNS') THEN false ELSE separately_saleable_ind END) separately_saleable_ind\n" +
            "   , trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , tax_code\n" +
            "   , 'RPR' f_type\n" +
            "   , m0_manifestation_type\n" +
            "   , (CASE WHEN (availability_status = 'PSTB') THEN 'PAS' ELSE availability_status END) f_status\n" +
            "   , work_type\n" +
            "   , jm_source_reference manifestation_ref\n" +
            "   , ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     base_data\n" +
            "   WHERE ((upsert = 'Insert') AND (reprints = 'Y'))\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(jm_source_reference, '-OOA') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(base_title, ' Purchase') \"name\"\n" +
            "   , (CASE WHEN (availability_status = 'PNS') THEN false ELSE separately_saleable_ind END) separately_saleable_ind\n" +
            "   , trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , tax_code\n" +
            "   , 'OOA' f_type\n" +
            "   , m0_manifestation_type\n" +
            "   , (CASE WHEN (availability_status = 'PSTB') THEN 'PST' ELSE availability_status END) f_status\n" +
            "   , work_type\n" +
            "   , jm_source_reference manifestation_ref\n" +
            "   , ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     base_data\n" +
            "   WHERE ((upsert = 'Insert') AND (one_off_access = 'Y'))\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"('J0', \"w0_journal_number\", '-OAA') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , CAST(null AS varchar) eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(work_title, ' Article Publishing Charge') \"name\"\n" +
            "   , (CASE WHEN (availability_status = 'PNS') THEN false ELSE separately_saleable_ind END) separately_saleable_ind\n" +
            "   , trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , 'S011' tax_code\n" +
            "   , 'OAA' f_type\n" +
            "   , null m0_manifestation_type\n" +
            "   , (CASE WHEN (work_status = 'PSTB') THEN 'PST' ELSE work_status END) f_status\n" +
            "   , work_type\n" +
            "   , CAST(null AS varchar) manifestation_ref\n" +
            "   , ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     base_data\n" +
            "   WHERE ((upsert = 'Insert') AND (open_access = 'Y'))\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"('J0', \"w0_journal_number\", '-JAS') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , CAST(null AS varchar) eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(work_title, ' Author Charges') \"name\"\n" +
            "   , (CASE WHEN (availability_status = 'PNS') THEN false ELSE separately_saleable_ind END) separately_saleable_ind\n" +
            "   , trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , 'S001' tax_code\n" +
            "   , 'JAS' f_type\n" +
            "   , null m0_manifestation_type\n" +
            "   , (CASE WHEN (work_status = 'PSTB') THEN 'PST' ELSE work_status END) f_status\n" +
            "   , work_type\n" +
            "   , CAST(null AS varchar) manifestation_ref\n" +
            "   , ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     base_data\n" +
            "   WHERE ((upsert = 'Insert') AND (author_charges = 'Y'))\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(\"substr\"(jm_source_reference, 3, 7), '-PKG') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , work_title \"name\"\n" +
            "   , (CASE WHEN (availability_status = 'PNS') THEN false ELSE separately_saleable_ind END) separately_saleable_ind\n" +
            "   , trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , null tax_code\n" +
            "   , 'PKG' f_type\n" +
            "   , null m0_manifestation_type\n" +
            "   , (CASE WHEN (work_status = 'PSTB') THEN 'PST' ELSE work_status END) f_status\n" +
            "   , work_type\n" +
            "   , jm_source_reference manifestation_ref\n" +
            "   , \"substr\"(jm_source_reference, 3, 7) ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     base_data\n" +
            "   WHERE ((upsert = 'Insert') AND (packages = 'Y'))\n" +
            ") \n" +
            ", result_data AS (\n" +
            "   SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , jm_source_ref jm_source_reference\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , name\n" +
            "   , (CASE WHEN (separately_saleable_ind IS NULL) THEN false ELSE separately_saleable_ind END) separately_saleable_ind\n" +
            "   , (CASE WHEN (trial_allowed_ind IS NULL) THEN false ELSE trial_allowed_ind END) trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , f_type\n" +
            "   , f_status\n" +
            "   , (CASE WHEN (f_type IN ('OOA', 'JAS', 'JBS', 'JBF', 'RPR')) THEN 'ONE' WHEN ((f_type = 'OAA') OR ((f_type = 'SUB') AND (m0_manifestation_type = 'P'))) THEN 'EVE' ELSE 'SUB' END) f_revenue_model\n" +
            "   , tax_code\n" +
            "   , work_type\n" +
            "   , manifestation_ref\n" +
            "   , ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     crosstab_data\n" +
            ") \n" +
            "SELECT *\n" +
            "FROM\n" +
            "  result_data)";


    public static String GET_JMF_STAGING_PRODUCT_UPDATES="select count(*) as count from (\n" +
            "WITH\n" +
            "  base_data AS (\n" +
            "   SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , jm_source_reference\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , base_title\n" +
            "   , w0_journal_number\n" +
            "   , m0_journal_number\n" +
            "   , w0_chronicle_id\n" +
            "   , w0_journey_identifier\n" +
            "   , m0_manifestation_type\n" +
            "   , separately_saleable_ind\n" +
            "   , trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , tax_code\n" +
            "   , subscription\n" +
            "   , bulk_sales\n" +
            "   , back_files\n" +
            "   , open_access\n" +
            "   , reprints\n" +
            "   , author_charges\n" +
            "   , one_off_access\n" +
            "   , packages\n" +
            "   , availability_status\n" +
            "   , work_status\n" +
            "   , work_title\n" +
            "   , work_type\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     "+ GetJMDLDBUser.getJMDB()+".etl_product_part1_v\n" +
            "   WHERE (notified_date IS NOT NULL)\n" +
            ") \n" +
            ", title_renames AS (\n" +
            "   SELECT\n" +
            "     bd.scenario_name scenario_name\n" +
            "   , bd.scenario_code scenario_code\n" +
            "   , bd.upsert upsert\n" +
            "   , bd.jm_source_reference jm_source_reference\n" +
            "   , COALESCE(bd.eph_work_id, w1.eph_work_id) eph_work_id\n" +
            "   , COALESCE(bd.eph_manifestation_id, m1.eph_manifestation_id) eph_manifestation_id\n" +
            "   , bd.eph_product_id eph_product_id\n" +
            "   , bd.w0_journal_number w0_journal_number\n" +
            "   , bd.m0_journal_number m0_journal_number\n" +
            "   , bd.w0_chronicle_id w0_chronicle_id\n" +
            "   , bd.w0_journey_identifier w0_journey_identifier\n" +
            "   , bd.m0_manifestation_type m0_manifestation_type\n" +
            "   , bd.base_title m0_base_title\n" +
            "   , (CASE m1.manifestation_type WHEN 'E' THEN \"concat\"(m1.manifestation_title, ' (Online)') ELSE \"concat\"(m1.manifestation_title, ' (Print)') END) m1_base_title\n" +
            "   , bd.separately_saleable_ind separately_saleable_ind\n" +
            "   , bd.trial_allowed_ind trial_allowed_ind\n" +
            "   , bd.launch_date launch_date\n" +
            "   , bd.tax_code tax_code\n" +
            "   , bd.subscription subscription\n" +
            "   , bd.bulk_sales bulk_sales\n" +
            "   , bd.back_files back_files\n" +
            "   , bd.open_access open_access\n" +
            "   , bd.reprints reprints\n" +
            "   , bd.author_charges author_charges\n" +
            "   , bd.one_off_access one_off_access\n" +
            "   , bd.packages packages\n" +
            "   , bd.availability_status availability_status\n" +
            "   , bd.work_status work_status\n" +
            "   , w1.work_title work_title\n" +
            "   , bd.work_type work_type\n" +
            "   , (CASE WHEN ((bd.eph_work_id IS NULL) AND (w1.eph_work_id IS NULL)) THEN 'Y' WHEN ((bd.eph_manifestation_id IS NULL) AND (m1.eph_manifestation_id IS NULL)) THEN 'Y' ELSE 'N' END) dq_err\n" +
            "   , m1.notified_date notified_date\n" +
            "   FROM\n" +
            "     (((base_data bd\n" +
            "   INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_work w1 ON (((w1.work_chronicle_id = bd.w0_chronicle_id) AND (w1.elsevier_journal_number = bd.w0_journal_number)) AND (w1.work_journey_identifier = 'A1')))\n" +
            "   INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle wc ON (((w1.work_chronicle_id = wc.work_chronicle_id) AND (w1.work_journey_identifier = 'A1')) AND (wc.chronicle_scenario_code = 'RN')))\n" +
            "   INNER JOIN "+ GetJMDLDBUser.getJMDB()+".jmf_manifestation m1 ON (((m1.f_work = w1.work_id) AND (m1.elsevier_journal_number = bd.m0_journal_number)) AND (m1.manifestation_type = bd.m0_manifestation_type)))\n" +
            "   WHERE ((((((m1.notified_date IS NOT NULL) AND (bd.scenario_code = 'RN')) AND (bd.upsert = 'Update')) AND (bd.base_title IS NOT NULL)) AND (m1.manifestation_title IS NOT NULL)) AND (m1.manifestation_title <> bd.base_title))\n" +
            "   GROUP BY bd.m0_manifestation_type, m1.manifestation_type, bd.scenario_code, bd.scenario_name, bd.upsert, bd.jm_source_reference, bd.m0_journal_number, bd.w0_chronicle_id, bd.w0_journey_identifier, bd.eph_work_id, bd.eph_manifestation_id, bd.eph_product_id, bd.base_title, m1.manifestation_title, m1.notified_date, w1.eph_work_id, bd.w0_journal_number, w1.elsevier_journal_number, bd.eph_manifestation_id, m1.eph_manifestation_id, bd.separately_saleable_ind, bd.trial_allowed_ind, bd.launch_date, bd.tax_code, bd.subscription, bd.bulk_sales, bd.back_files, bd.open_access, bd.reprints, bd.author_charges, bd.one_off_access, bd.packages, bd.availability_status, bd.work_status, w1.work_title, bd.work_type\n" +
            "   ORDER BY bd.eph_work_id ASC, w1.eph_work_id ASC, bd.m0_manifestation_type ASC, m1.manifestation_title ASC, m1.notified_date ASC\n" +
            ") \n" +
            ", crosstab_data AS (\n" +
            "   SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(jm_source_reference, '-SUB') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(m1_base_title, ' Subscription') \"name\"\n" +
            "   , CAST(null AS boolean) separately_saleable_ind\n" +
            "   , CAST(null AS boolean) trial_allowed_ind\n" +
            "   , CAST(null AS date) launch_date\n" +
            "   , CAST(null AS varchar) tax_code\n" +
            "   , 'SUB' f_type\n" +
            "   , m0_manifestation_type\n" +
            "   , CAST(null AS varchar) f_status\n" +
            "   , work_type\n" +
            "   , CAST(null AS varchar) manifestation_ref\n" +
            "   , CAST(null AS varchar) ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     title_renames\n" +
            "   WHERE (subscription = 'Y')\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(jm_source_reference, '-JBS') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(m1_base_title, ' Bulk Sales') \"name\"\n" +
            "   , CAST(null AS boolean) separately_saleable_ind\n" +
            "   , CAST(null AS boolean) trial_allowed_ind\n" +
            "   , CAST(null AS date) launch_date\n" +
            "   , CAST(null AS varchar) tax_code\n" +
            "   , 'JBS' f_type\n" +
            "   , m0_manifestation_type\n" +
            "   , CAST(null AS varchar) f_status\n" +
            "   , work_type\n" +
            "   , CAST(null AS varchar) manifestation_ref\n" +
            "   , CAST(null AS varchar) ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     title_renames\n" +
            "   WHERE (bulk_sales = 'Y')\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(jm_source_reference, '-BKF') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(m1_base_title, ' Back Files') \"name\"\n" +
            "   , CAST(null AS boolean) separately_saleable_ind\n" +
            "   , CAST(null AS boolean) trial_allowed_ind\n" +
            "   , CAST(null AS date) launch_date\n" +
            "   , CAST(null AS varchar) tax_code\n" +
            "   , 'BKF' f_type\n" +
            "   , m0_manifestation_type\n" +
            "   , CAST(null AS varchar) f_status\n" +
            "   , work_type\n" +
            "   , CAST(null AS varchar) manifestation_ref\n" +
            "   , CAST(null AS varchar) ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     title_renames\n" +
            "   WHERE (back_files = 'Y')\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(jm_source_reference, '-RPR') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(m1_base_title, ' Reprints') \"name\"\n" +
            "   , CAST(null AS boolean) separately_saleable_ind\n" +
            "   , CAST(null AS boolean) trial_allowed_ind\n" +
            "   , CAST(null AS date) launch_date\n" +
            "   , CAST(null AS varchar) tax_code\n" +
            "   , 'RPR' f_type\n" +
            "   , m0_manifestation_type\n" +
            "   , CAST(null AS varchar) f_status\n" +
            "   , work_type\n" +
            "   , CAST(null AS varchar) manifestation_ref\n" +
            "   , CAST(null AS varchar) ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     title_renames\n" +
            "   WHERE (reprints = 'Y')\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(jm_source_reference, '-OOA') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(m1_base_title, ' Purchase') \"name\"\n" +
            "   , CAST(null AS boolean) separately_saleable_ind\n" +
            "   , CAST(null AS boolean) trial_allowed_ind\n" +
            "   , CAST(null AS date) launch_date\n" +
            "   , CAST(null AS varchar) tax_code\n" +
            "   , 'OOA' f_type\n" +
            "   , m0_manifestation_type\n" +
            "   , CAST(null AS varchar) f_status\n" +
            "   , work_type\n" +
            "   , CAST(null AS varchar) manifestation_ref\n" +
            "   , CAST(null AS varchar) ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     title_renames\n" +
            "   WHERE (one_off_access = 'Y')\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"('J0', \"w0_journal_number\", '-OAA') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , CAST(null AS varchar) eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(work_title, ' Article Publishing Charge') \"name\"\n" +
            "   , CAST(null AS boolean) separately_saleable_ind\n" +
            "   , CAST(null AS boolean) trial_allowed_ind\n" +
            "   , CAST(null AS date) launch_date\n" +
            "   , CAST(null AS varchar) tax_code\n" +
            "   , 'OAA' f_type\n" +
            "   , null m0_manifestation_type\n" +
            "   , CAST(null AS varchar) f_status\n" +
            "   , work_type\n" +
            "   , CAST(null AS varchar) manifestation_ref\n" +
            "   , CAST(null AS varchar) ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     title_renames\n" +
            "   WHERE (open_access = 'Y')\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"('J0', \"w0_journal_number\", '-JAS') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , CAST(null AS varchar) eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , \"concat\"(work_title, ' Author Charges') \"name\"\n" +
            "   , CAST(null AS boolean) separately_saleable_ind\n" +
            "   , CAST(null AS boolean) trial_allowed_ind\n" +
            "   , CAST(null AS date) launch_date\n" +
            "   , CAST(null AS varchar) tax_code\n" +
            "   , 'JAS' f_type\n" +
            "   , null m0_manifestation_type\n" +
            "   , CAST(null AS varchar) f_status\n" +
            "   , work_type\n" +
            "   , CAST(null AS varchar) manifestation_ref\n" +
            "   , CAST(null AS varchar) ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     title_renames\n" +
            "   WHERE (author_charges = 'Y')\n" +
            "UNION    SELECT\n" +
            "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , \"concat\"(\"substr\"(jm_source_reference, 3, 7), '-PKG') jm_source_ref\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , work_title \"name\"\n" +
            "   , CAST(null AS boolean) separately_saleable_ind\n" +
            "   , CAST(null AS boolean) trial_allowed_ind\n" +
            "   , CAST(null AS date) launch_date\n" +
            "   , CAST(null AS varchar) tax_code\n" +
            "   , 'PKG' f_type\n" +
            "   , null m0_manifestation_type\n" +
            "   , CAST(null AS varchar) f_status\n" +
            "   , work_type\n" +
            "   , CAST(null AS varchar) manifestation_ref\n" +
            "   , CAST(null AS varchar) ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     title_renames\n" +
            "   WHERE (packages = 'Y')\n" +
            ") \n" +
            ", result_data AS (\n" +
            "   SELECT\n" +
             "     scenario_name\n" +
            "   , scenario_code\n" +
            "   , upsert\n" +
            "   , jm_source_ref jm_source_reference\n" +
            "   , eph_work_id\n" +
            "   , eph_manifestation_id\n" +
            "   , eph_product_id\n" +
            "   , name\n" +
            "   , (CASE WHEN (separately_saleable_ind IS NULL) THEN true ELSE separately_saleable_ind END) separately_saleable_ind\n" +
            "   , trial_allowed_ind\n" +
            "   , launch_date\n" +
            "   , f_type\n" +
            "   , f_status\n" +
            "   , CAST(null AS varchar) f_revenue_model\n" +
            "   , tax_code\n" +
            "   , work_type\n" +
            "   , manifestation_ref\n" +
            "   , ult_work_ref\n" +
            "   , dq_err\n" +
            "   , notified_date\n" +
            "   FROM\n" +
            "     crosstab_data\n" +
            ") \n" +
            "SELECT *\n" +
            "FROM\n" +
            "  result_data\n)";


    public static String GET_JMF_STAGING_PRODUCT="select count(*) as count from (\n" +
            "\n" +
            "select * from "+ GetJMDLDBUser.getJMDB()+".etl_product_inserts_v\n" +
            "\n" +
            "UNION\n" +
            "\n" +
            "select * from "+ GetJMDLDBUser.getJMDB()+".etl_product_updates_v)";

    public static String GET_JMF_STAGING_PRODUCT_PERSON_ROLE=
            "select count (*) as COUNT from (\n" +
            "with base_data as\n" +
            "(\n" +
            "select\n" +
            "scenario_name,                   -- scenario NAME 'New Proprietary' etc\n" +
            "scenario_code,                   -- scenario CODE NP, NS, AC, MI, RN, CA\n" +
            "upsert,                          -- 'Insert', or 'Update'\n" +
            "jm_source_reference,             -- format E-J012345 or P-J012345\n" +
            "eph_work_id,                     -- format EPR-W-xxxxxx\n" +
            "eph_manifestation_id,            -- format EPR-M-xxxxxx\n" +
            "eph_product_id,                  -- format EPR-xxxxxx new journals: set null\n" +
            "base_title,                      -- jm-manifestation-title a suffix of (Print) or (Online)\n" +
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
            "from  "+ GetJMDLDBUser.getJMDB()+".etl_product_part1_v\n" +
            "where  (notified_date is not null\n" +
            "and   ((upsert = 'Insert')\n" +
            "    or (upsert = 'Update' and scenario_name = 'Change Allocated Resources')))\n" +
            ")\n" +
            "-- we are now processing FOR ALL INSERTS (NP, NS, AC, MI) then for CA UPDATES. Filter out RN Title Updates (not processed here)\n" +
            "-- select * from base_data\n" +
            ", crosstab_data as\n" +
            "(\n" +
            "select\n" +
            "    scenario_name,\n" +
            "    scenario_code,\n" +
            "    upsert,\n" +
            "    jm_source_reference||'-SUB-'||employee_number_new||'-PO' as jm_source_ref,\n" +
            "    eph_work_id,\n" +
            "    eph_manifestation_id,\n" +
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                   -- not known by jm for new products.\n" +
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
            "    CAST (null as varchar) as        eph_product_id,                    -- not known by jm for new products.\n" +
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
            "where packages = 'Y')\n" +
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
            "select * from result_data)\n";


    public static String GET_JMF_STAGING_IMPRINT_DATA="select count(*)as count from (\n" +
            "SELECT\n" +
            "  code\n" +
            ", l_description name\n" +
            ", l_description description\n" +
            "FROM "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_imprint\n" +
            "WHERE valid_for_journals = true)";

    public static String GET_JMF_STAGING_SD_SUBJECT_AREAS="select count(*) as count from (\n" +
            "SELECT\n" +
            "     subject_area_id sa_id\n" +
            "   , code sa_code\n" +
            "   , name sa_name\n" +
            "   , '1' sa_level\n" +
            "   , f_parent_subject_area sa_parent_id\n" +
            "   , CAST(null AS date) start_date\n" +
            "   , CAST(null AS date) end_date\n" +
            "   FROM\n" +
            "     "+GetPRMDLDBUser.getProdDataBase()+".gd_subject_area\n" +
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
            "     "+GetPRMDLDBUser.getProdDataBase()+".gd_subject_area\n" +
            "   WHERE f_type = 'SD'\n" +
            "   AND   f_parent_subject_area\n" +
            "           IN (SELECT subject_area_id\n" +
            "               FROM "+GetPRMDLDBUser.getProdDataBase()+".gd_subject_area\n" +
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
            "     "+GetPRMDLDBUser.getProdDataBase()+".gd_subject_area\n" +
            "   WHERE f_type = 'SD'\n" +
            "   AND   f_parent_subject_area\n" +
            "           IN (SELECT subject_area_id\n" +
            "               FROM "+GetPRMDLDBUser.getProdDataBase()+".gd_subject_area\n" +
            "               WHERE f_type = 'SD'\n" +
            "               AND f_parent_subject_area\n" +
            "                      IN (SELECT subject_area_id\n" +
            "                          FROM "+GetPRMDLDBUser.getProdDataBase()+".gd_subject_area\n" +
            "                          WHERE f_type = 'SD'\n" +
            "                          AND f_parent_subject_area IS NULL))\n" +
            ")\n" +
            "\n";

    public static String GET_JMF_STAGING_WORKS_ATTRS_ROLES_PEOPLE="select count(*) as COUNT from (\n" +
            "   SELECT\n" +
            "     \"w\".\"work_id\"\n" +
            "   , \"w\".\"work_title\" \"WorkTitle\"\n" +
            "   , \"ws\".\"roll_up_status\"\n" +
            "   , COALESCE(\"wi1\".\"identifier\", 'Not Found') \"jnl_no\"\n" +
            "   , COALESCE(\"wi2\".\"identifier\", 'Not Found') \"issn_l\"\n" +
            "   , COALESCE(\"wi3\".\"identifier\", 'Not Found') \"acronym\"\n" +
            "   , \"pmg\".\"code\" \"pmgcode\"\n" +
            "   , \"pmg\".\"l_description\" \"pmgdesc\"\n" +
            "   , \"w\".\"f_pmc\" \"pmccode\"\n" +
            "   , \"pmc\".\"l_description\" \"pmcdesc\"\n" +
            "   , \"f\".\"f_gl_company\" \"opco\"\n" +
            "   , \"gl\".\"l_description\" \"opcodesc\"\n" +
            "   , \"f\".\"f_gl_revenue_resp_centre\" \"resp_cen\"\n" +
            "   , \"p1\".\"person_id\" \"PU_person_id\"\n" +
            "   , \"p1\".\"given_name\" \"PU_given_name\"\n" +
            "   , \"p1\".\"family_name\" \"PU_family_name\"\n" +
            "   , COALESCE(\"p1\".\"email\", 'Not Found') \"PU_email\"\n" +
            "   , \"p1\".\"peoplehub_id\" \"PU_peoplehub_id\"\n" +
            "   , \"p2\".\"person_id\" \"PD_person_id\"\n" +
            "   , \"p2\".\"given_name\" \"PD_given_name\"\n" +
            "   , \"p2\".\"family_name\" \"PD_family_name\"\n" +
            "   , COALESCE(\"p2\".\"email\", 'Not Found') \"PD_email\"\n" +
            "   , \"p2\".\"peoplehub_id\" \"PD_peoplehub_id\"\n" +
            "   , \"p3\".\"person_id\" \"BC_person_id\"\n" +
            "   , \"p3\".\"given_name\" \"BC_given_name\"\n" +
            "   , \"p3\".\"family_name\" \"BC_family_name\"\n" +
            "   , COALESCE(\"p3\".\"email\", 'Not Found') \"BC_email\"\n" +
            "   , \"p3\".\"peoplehub_id\" \"BC_peoplehub_id\"\n" +
            "   FROM "+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON \"ws\".\"code\" = \"w\".\"f_status\" AND \"ws\".\"l_end_date\" IS NULL\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi1 ON \"wi1\".\"f_wwork\" = \"w\".\"work_id\" AND \"wi1\".\"f_type\" = 'ELSEVIER JOURNAL NUMBER' AND \"wi1\".\"effective_end_date\" IS NULL\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi2 ON \"wi2\".\"f_wwork\" = \"w\".\"work_id\" AND \"wi2\".\"f_type\" = 'ISSN-L' AND \"wi2\".\"effective_end_date\" IS NULL\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier wi3 ON \"wi3\".\"f_wwork\" = \"w\".\"work_id\" AND \"wi3\".\"f_type\" = 'JOURNAL ACRONYM' AND \"wi3\".\"effective_end_date\" IS NULL\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmc pmc ON \"pmc\".\"code\" = \"w\".\"f_pmc\" AND \"pmc\".\"l_end_date\" IS NULL\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmg pmg ON \"pmg\".\"code\" = \"pmc\".\"f_pmg\" AND \"pmg\".\"l_end_date\" IS NULL\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_financial_attribs f ON \"f\".\"f_wwork\" = \"w\".\"work_id\" AND \"f\".\"effective_end_date\" IS NULL\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_company gl ON \"gl\".\"code\" = \"f\".\"f_gl_company\" AND \"gl\".\"l_end_date\" IS NULL\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_person_role wp1 ON \"wp1\".\"f_wwork\" = \"w\".\"work_id\" AND \"wp1\".\"f_role\" = 'PU' AND \"wp1\".\"effective_end_date\" IS NULL\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_person p1 ON \"p1\".\"person_id\" = \"wp1\".\"f_person\"\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_person_role wp2 ON \"wp2\".\"f_wwork\" = \"w\".\"work_id\" AND \"wp2\".\"f_role\" = 'PD' AND \"wp2\".\"effective_end_date\" IS NULL\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_person p2 ON \"p2\".\"person_id\" = \"wp2\".\"f_person\"\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_person_role wp3 ON \"wp3\".\"f_wwork\" = \"w\".\"work_id\" AND \"wp3\".\"f_role\" = 'BC' AND \"wp3\".\"effective_end_date\" IS NULL\n" +
            "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_person p3 ON \"p3\".\"person_id\" = \"wp3\".\"f_person\"\n" +
            "   WHERE \"w\".\"f_type\" = 'JNL'\n" +
            "   AND   \"ws\".\"valid_for_journals\" = true\n" +
            "   AND   \"ws\".\"roll_up_status\" IN ('Planned', 'Launched')\n" +
            ")";
}