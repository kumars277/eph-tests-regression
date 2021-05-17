package com.eph.automation.testing.services.db.JMDataLakeSQL;

import com.eph.automation.testing.services.db.PROMISDataLakeSQL.GetPRMDLDBUser;

public class JM_ETLCoreCountChecksSQL {


    public static String GET_JMF_ACCOUNTABLE_PRODUCT="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_accountable_product_dq_v";

    public static String GET_JMF_WORK="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_wwork_dq_v";

    public static String GET_JMF_WORK_IDENTIFIER="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_work_identifier_dq_v \n" +
            "\n";

    public static String GET_JMF_WORK_SUBJECT_AREA="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_work_subject_area_dq_v\n";

    public static String GET_JMF_WORK_PERSON_ROLE="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_work_person_role_dq_v";

    public static String GET_JMF_MANIFESTATION_UPDATES="select count(*) as count from "+ GetJMDLDBUser.getJMDB()+".etl_manifestation_updates1_v";

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

    public static String GET_JMF_STAGING_ACCOUNTABLE_PRODUCT="select count(*) as count from (\n" +
            "select cs.chronicle_scenario_name as                                       scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                            upsert,\n" +
            "       'J0'||w.elsevier_journal_number||'-'||w.hfm_hierarchy_position_code jm_source_reference,\n" +
            "       'J0'||w.elsevier_journal_number                                     acc_prod_id,\n" +
            "        w.hfm_hierarchy_position_code as                                   hfm_hierarchy_position_code,\n" +
            "        w.work_title as                                                    work_title,\n" +
            "       'N'  as                                                             dq_err,\n" +
            "        w.notified_date as                                                 notified_date\n" +
            "from  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   w.notified_date is not null\n" +
            " \n" +
            "UNION\n" +
            " \n" +
            "select cs.chronicle_scenario_name as                                                                  scenario,\n" +
            "      (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "           when 'N' then 'Insert'\n" +
            "           when 'Y' then 'Update'\n" +
            "           else 'Update'\n" +
            "       END) as                                                                                        upsert,\n" +
            "      (CASE\n" +
            "           when (w0.hfm_hierarchy_position_code is null and w1.hfm_hierarchy_position_code is null) then 'J0'||w0.elsevier_journal_number||'-NotFound'\n" +
            "           else 'J0'||w0.elsevier_journal_number||'-'||COALESCE(w1.hfm_hierarchy_position_code,w0.hfm_hierarchy_position_code)\n" +
            "       END) as                                                                                        jm_source_reference,\n" +
            "--              'J0'||w0.elsevier_journal_number||'-'||COALESCE(w1.hfm_hierarchy_position_code,w0.hfm_hierarchy_position_code) as jm_source_reference,\n" +
            "--              test data issue - hfm_hierarchy_position_code doesn't appear to be populated in sit\n" +
            "      'J0'||w0.elsevier_journal_number as                                                             acc_prod_id,\n" +
            "       COALESCE(w1.hfm_hierarchy_position_code, w0.hfm_hierarchy_position_code, 'NotFound') as        hfm_hierarchy_position_code,\n" +
            "       w1.work_title as                                                                               work_title,\n" +
            "      (CASE\n" +
            "           when (w0.elsevier_journal_number     is null and w1.elsevier_journal_number     is null) then 'Y'\n" +
            "           when (w0.hfm_hierarchy_position_code is null and w1.hfm_hierarchy_position_code is null) then 'Y'\n" +
            "           else 'N'\n" +
            "       END) as                                                                                        dq_err,\n" +
            "       COALESCE(w1.notified_date, w0.notified_date) as                                                notified_date\n" +
            "from ((("+ GetJMDLDBUser.getJMDB()+".jmf_work                     w0\n" +
            "        join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on  (w0.work_chronicle_id       = wc.work_chronicle_id))\n" +
            "        join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "        left join "+ GetJMDLDBUser.getJMDB()+".jmf_work           w1 on ((w1.work_chronicle_id       = w0.work_chronicle_id)\n" +
            "                                        and  (w1.work_journey_identifier = 'A1')))\n" +
            "where wc.chronicle_scenario_code = 'RN'\n" +
            "and   w0.work_journey_identifier = 'A0'\n" +
            "and   w1.notified_date is not null\n" +
            "and   w0.work_title is not null\n" +
            "and   w1.work_title is not null\n" +
            "and   w1.work_title <> w0.work_title\n" +
            ")";


    public static String GET_JMF_STAGING_WORK="select count(*) as count from (\n" +
            "select  cs.chronicle_scenario_name as   scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "       'J0'||w.elsevier_journal_number  jm_source_reference,\n" +
            "        w.eph_work_id as                eph_work_id,\n" +
            "        w.work_title as                 work_title,\n" +
            "        w.work_subtitle as              work_subtitle,\n" +
            "        CAST (null as boolean)          electro_rights_indicator,\n" +
            "        0    as                         volume,\n" +
            "        CAST (null as integer)          copyright_year,\n" +
            "        CAST (null as integer)          edition_number,\n" +
            "        w.launch_date as                planned_launch_date,\n" +
            "        CAST (null as date)             planned_termination_date,\n" +
            "        'JNL' as                        f_type,\n" +
            "        'WPL' as                        f_status,\n" +
            "        CAST (null as integer)          f_accountable_product,\n" +
            "        w.pmc_code as                   f_pmc,\n" +
            "       (CASE\n" +
            "            when (w.open_accesstype_code = 'SM5')                                                                                         then 'F'\n" +
            "            when (w.open_accesstype_code = 'SM6')                                                                                         then 'S'\n" +
            "            when (w.open_accesstype_code is null and w.manifestation_types_code = 'SM4')                                                  then 'H'\n" +
            "            when (w.open_accesstype_code is null and w.manifestation_types_code = 'SM4SO')                                                then 'N'\n" +
            "--            when (w.open_accesstype_code is null and w.manifestation_types_code = 'SM4' and w.manifestation_personal_model_type = 'SM2P') then 'H'\n" +
            "--            when (w.open_accesstype_code is null and w.manifestation_types_code is null and w.manifestation_personal_model_type = 'SM2P') then '?'\n" +
            "            ELSE null\n" +
            "        END) as                         f_oa_type,\n" +
            "        w.imprint_code as               f_imprint,\n" +
            "        w.opco_r12_id as                opco,\n" +
            "       (CASE\n" +
            "            when (w.ownership_brand_type = 'Elsevier' and w.society_relationship_type = 'None') then 'ELS-EFO'\n" +
            "            when (w.ownership_brand_type = 'Elsevier' and w.society_relationship_type is null)  then 'ELS-EFO'\n" +
            "            when (w.ownership_brand_type = 'Elsevier' and w.society_relationship_type = 'SCAF') then 'ELS-SCAF'\n" +
            "            when (w.ownership_brand_type = 'Elsevier' and w.society_relationship_type = 'SCCT') then 'ELS-SCCT'\n" +
            "            when (w.ownership_brand_type = 'Society'  and w.society_relationship_type = 'None') then 'SOC-SFO'\n" +
            "            when (w.ownership_brand_type = 'Society'  and w.society_relationship_type is null)  then 'SOC-SFO'\n" +
            "            when (w.ownership_brand_type = 'Society'  and w.society_relationship_type = 'SOW')  then 'SOC-SFO'\n" +
            "            when (w.ownership_brand_type = 'Co-Owned' and w.society_relationship_type = 'SCOW') then 'SOC-CWN'\n" +
            "        END) as                         f_society_ownership,\n" +
            "       (CASE\n" +
            "            when (m.subscription_type = 'Calendar Year') then 'FY'\n" +
            "            when (m.subscription_type = 'Rolling Year')  then 'RY'\n" +
            "            when (m.subscription_type = 'Both')          then 'RY'\n" +
            "            ELSE null\n" +
            "        END) as                         subscription_type,\n" +
            "        w.responsibility_centre_code as resp_centre,\n" +
            "        COALESCE\n" +
            "           ((CASE WHEN (w.main_language_code = 'English')    then 'EN'\n" +
            "                  WHEN (w.main_language_code is null)        then 'EN'\n" +
            "                  WHEN (w.main_language_code = 'French')     then 'FR'\n" +
            "                  WHEN (w.main_language_code = 'German')     then 'DE'\n" +
            "                  WHEN (w.main_language_code = 'Spanish')    then 'ES'\n" +
            "                  WHEN (w.main_language_code = 'Catalan')    then 'CA'\n" +
            "                  WHEN (w.main_language_code = 'Italian')    then 'IT'\n" +
            "                  WHEN (w.main_language_code = 'Polish')     then 'PL'\n" +
            "                  WHEN (w.main_language_code = 'Portuguese') then 'PT'\n" +
            "             END),w.main_language_code) language_code,\n" +
            "       'N'  as                          dq_err,\n" +
            "        w.notified_date as              notified_date\n" +
            "from  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_manifestation      m  on m.f_work = w.work_id and m.issn = w.issn_l\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   w.notified_date is not null\n" +
            "\n" +
            "UNION\n" +
            "--  Title updates to work level\n" +
            "\n" +
            "select  cs.chronicle_scenario_name as   scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number,w0.elsevier_journal_number) as jm_source_reference,\n" +
            "        COALESCE(w1.eph_work_id, w0.eph_work_id) as eph_work_id,\n" +
            "        w1.work_title as                work_title,                 -- the TITLE UPDATE\n" +
            "        CAST (null as varchar) as       work_subtitle,              -- JM does not master subtitle.\n" +
            "        CAST (null as boolean) as       electro_rights_indicator,\n" +
            "        CAST (null as integer) as       volume,                     -- for inserts was set 0\n" +
            "        CAST (null as integer) as       copyright_year,\n" +
            "        CAST (null as integer) as       edition_number,\n" +
            "        CAST (null as date) as          planned_launch_date,        -- for inserts was w.launch_date   as planned_launch_date,\n" +
            "        CAST (null as date) as          planned_termination_date,   -- for inserts will be null\n" +
            "       'JNL' as                         f_type,\n" +
            "        CAST (null as varchar) as       f_status,                   -- set status to WDA (DISCONTINUE APPROVED). for inserts was 'WPL'.\n" +
            "        CAST (null as integer) as       f_accountable_product,\n" +
            "        CAST (null as varchar) as       f_pmc,                      -- for inserts was w.pmc_code      as f_pmc,\n" +
            "        CAST (null as varchar) as       f_oa_type,                  -- for inserts is set F, S, H, N or null.\n" +
            "        CAST (null as varchar) as       f_imprint,                  -- for inserts was w.imprint_code  as f_imprint,\n" +
            "        CAST (null as varchar) as       opco,                       -- for inserts was w.opco_r12_id   as opco,\n" +
            "        CAST (null as varchar) as       f_society_ownership,        -- for inserts sets 'ELS-EFO' etc.\n" +
            "        CAST (null as varchar) as       subscription_type,          -- for inserts, use m.manifestation_type FY/RY\n" +
            "        CAST (null as varchar) as       resp_centre,                -- for inserts, was w.responsibility_centre_code as resp_centre,\n" +
            "        CAST (null as varchar) as       language_code,              -- for inserts, was a translation of w.main_language_code to EN etc.\n" +
            "       (CASE\n" +
            "            when (w0.eph_work_id             is null and w1.eph_work_id             is null) then 'Y'\n" +
            "            when (w0.elsevier_journal_number is null and w1.elsevier_journal_number is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                         dq_err,\n" +
            "        COALESCE(w1.notified_date, w0.notified_date) as notified_date  -- they should both be set the same\n" +
            "from  ((("+ GetJMDLDBUser.getJMDB()+".jmf_work                       w0\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle       wc on  (w0.work_chronicle_id       = wc.work_chronicle_id))\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario   cs on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "         left join "+ GetJMDLDBUser.getJMDB()+".jmf_work             w1 on ((w1.work_chronicle_id       = w0.work_chronicle_id)\n" +
            "                                          and  (w1.work_journey_identifier = 'A1')))\n" +
            "where  wc.chronicle_scenario_code = 'RN'\n" +
            "and    w0.work_journey_identifier = 'A0'\n" +
            "and    w1.notified_date is not null\n" +
            "and    w0.work_title is not null\n" +
            "and    w1.work_title is not null\n" +
            "and    w1.work_title <> w0.work_title\n" +
            "\n" +
            "UNION\n" +
            "\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE PMC  - PMC UPDATES EPH_WWORK\n" +
            "-- this is CAR - Change Allocated Resource - where changes to PMC will go to EPH WWORK.\n" +
            "-- take from JMF_WORK, from the merge with family resource details.\n" +
            "--\n" +
            "-- Change of PMC\n" +
            "--  A number of journals under a given PMG are assigned to a new PMC. Write a wwork update.\n" +
            "--  The new PMC Code will be held on jmf_family_resource_details for the given journal where Resource_Key = 'PMC', in field NEW_VALUE.\n" +
            "--  For info, the old PMC code will be in INITIAL_VALUE.\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "\n" +
            "select  cs.chronicle_scenario_name as   scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number) as jm_source_reference,\n" +
            "        COALESCE(w1.eph_work_id, w0.eph_work_id) as eph_work_id,\n" +
            "        CAST (null as varchar)          work_title,                 -- for inserts was w.work_title    as work_title,\n" +
            "        CAST (null as varchar)          work_subtitle,              -- for inserts was w.work_subtitle as work_subtitle,\n" +
            "        CAST (null as boolean)          electro_rights_indicator,\n" +
            "        CAST (null as integer)          volume,                     -- for inserts was set 0\n" +
            "        CAST (null as integer)          copyright_year,\n" +
            "        CAST (null as integer)          edition_number,\n" +
            "        CAST (null as date)             planned_launch_date,        -- for inserts was w.launch_date   as planned_launch_date,\n" +
            "        CAST (null as date) as          planned_termination_date,   -- the same field is used for both discontinues and transfers (a journal is never both)\n" +
            "        'JNL' as                        f_type,\n" +
            "        CAST (null as varchar)          f_status,                   -- for inserts was 'WPL'           as f_status,\n" +
            "        CAST (null as integer)          f_accountable_product,\n" +
            "        w1.pmc_new as                   f_pmc,                      -- for inserts was w.pmc_code      as f_pmc,\n" +
            "        CAST (null as varchar)          f_oa_type,                  -- for inserts is set F, S, H, N or null.\n" +
            "        CAST (null as varchar)          f_imprint,                  -- for inserts was w.imprint_code  as f_imprint,\n" +
            "        CAST (null as varchar)          opco,                       -- for inserts was w.opco_r12_id   as opco,\n" +
            "        CAST (null as varchar)          f_society_ownership,        -- for inserts sets 'ELS-EFO' etc.\n" +
            "        CAST (null as varchar)          subscription_type,          -- for inserts, use m.manifestation_type FY/RY\n" +
            "        CAST (null as varchar)          resp_centre,                -- for inserts, was w.responsibility_centre_code as resp_centre,\n" +
            "        CAST (null as varchar)          language_code,               -- for inserts, was a translation of w.main_language_code to EN etc.\n" +
            "       (CASE\n" +
            "            when (w0.eph_work_id             is null and w1.eph_work_id             is null) then 'Y'\n" +
            "            when (w0.elsevier_journal_number is null and w1.elsevier_journal_number is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                         dq_err,\n" +
            "        w1.notified_date as             notified_date\n" +
            "from "+ GetJMDLDBUser.getJMDB()+".jmf_work                     w0\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w0.work_chronicle_id       = wc.work_chronicle_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w1 on w1.work_chronicle_id       = w0.work_chronicle_id\n" +
            "                               and w1.elsevier_journal_number = w0.elsevier_journal_number\n" +
            "                               and w1.work_journey_identifier = 'A1'\n" +
            "where   w0.work_journey_identifier = 'A0'\n" +
            "and     wc.chronicle_scenario_code = 'CA'\n" +
            "and     w1.pmc_family_resource_details_id is not null\n" +
            "and     w1.pmc_old is not null\n" +
            "and     w1.pmc_new is not null\n" +
            "and     w1.pmc_new <> w1.pmc_old\n" +
            "and     w1.notified_date is not null\n" +
            "\n" +
            "UNION\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE - DISCONTINUE\n" +
            "-- Discontinue is taken from JMF_WORK\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "--\n" +
            "select  cs.chronicle_scenario_name as   scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number,w0.elsevier_journal_number) as jm_source_reference,\n" +
            "        COALESCE(w1.eph_work_id, w0.eph_work_id) as eph_work_id,\n" +
            "        CAST (null as varchar)          work_title,                 -- for inserts was w.work_title    as work_title,\n" +
            "        CAST (null as varchar)          work_subtitle,              -- for inserts was w.work_subtitle as work_subtitle,\n" +
            "        CAST (null as boolean)          electro_rights_indicator,\n" +
            "        CAST (null as integer)          volume,                     -- for inserts was set 0\n" +
            "        CAST (null as integer)          copyright_year,\n" +
            "        CAST (null as integer)          edition_number,\n" +
            "        CAST (null as date)             planned_launch_date,        -- for inserts was w.launch_date   as planned_launch_date,\n" +
            "        w1.discontinue_date as          planned_termination_date,   -- for inserts will be null\n" +
            "        'JNL' as                        f_type,\n" +
            "        'WDA' as                        f_status,                   -- set status to WDA (DISCONTINUE APPROVED). for inserts it's 'WPL'.\n" +
            "        CAST (null as integer)          f_accountable_product,\n" +
            "        CAST (null as varchar)          f_pmc,                      -- for inserts was w.pmc_code      as f_pmc,\n" +
            "        CAST (null as varchar)          f_oa_type,                  -- for inserts is set F, S, H, N or null.\n" +
            "        CAST (null as varchar)          f_imprint,                  -- for inserts was w.imprint_code  as f_imprint,\n" +
            "        CAST (null as varchar)          opco,                       -- for inserts was w.opco_r12_id   as opco,\n" +
            "        CAST (null as varchar)          f_society_ownership,        -- for inserts sets 'ELS-EFO' etc.\n" +
            "        CAST (null as varchar)          subscription_type,          -- for inserts, use m.manifestation_type FY/RY\n" +
            "        CAST (null as varchar)          resp_centre,                -- for inserts, was w.responsibility_centre_code as resp_centre,\n" +
            "        CAST (null as varchar)          language_code,              -- for inserts, was a translation of w.main_language_code to EN etc.\n" +
            "       (CASE\n" +
            "            when (w0.eph_work_id             is null and w1.eph_work_id             is null) then 'Y'\n" +
            "            when (w0.elsevier_journal_number is null and w1.elsevier_journal_number is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                         dq_err,\n" +
            "        COALESCE(w1.notified_date, w0.notified_date) as notified_date  -- they should both be set the same\n" +
            "from  ((("+ GetJMDLDBUser.getJMDB()+".jmf_work                     w0\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on  (w0.work_chronicle_id       = wc.work_chronicle_id))\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "         left join "+ GetJMDLDBUser.getJMDB()+".jmf_work           w1 on ((w1.work_chronicle_id       = w0.work_chronicle_id)\n" +
            "                                        and  (w1.work_journey_identifier = 'A1')))\n" +
            "where  w0.work_journey_identifier = 'A0'\n" +
            "and    wc.chronicle_scenario_code = 'DC'\n" +
            "and    w0.notified_date is not null\n" +
            "\n" +
            "UNION\n" +
            "\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE - TRANSFER\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "--\n" +
            "select  cs.chronicle_scenario_name as   scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number) as jm_source_reference,\n" +
            "        COALESCE(w1.eph_work_id, w0.eph_work_id) as eph_work_id,\n" +
            "        CAST (null as varchar)          work_title,                 -- for inserts was w.work_title    as work_title,\n" +
            "        CAST (null as varchar)          work_subtitle,              -- for inserts was w.work_subtitle as work_subtitle,\n" +
            "        CAST (null as boolean)          electro_rights_indicator,\n" +
            "        CAST (null as integer)          volume,                     -- for inserts was set 0\n" +
            "        CAST (null as integer)          copyright_year,\n" +
            "        CAST (null as integer)          edition_number,\n" +
            "        CAST (null as date)             planned_launch_date,        -- for inserts was w.launch_date   as planned_launch_date,\n" +
            "        w1.transfer_date as             planned_termination_date,   -- null for inserts, for discontinues w.discontinue_date\n" +
            "        'JNL' as                        f_type,\n" +
            "        'WXA' as                        f_status,                   -- set status to WXA (XFER APPROVED). This may revert to WDA. Inserts 'WPL', Discontinues 'WDA'\n" +
            "        CAST (null as integer)          f_accountable_product,\n" +
            "        CAST (null as varchar)          f_pmc,                      -- for inserts was w.pmc_code      as f_pmc,\n" +
            "        CAST (null as varchar)          f_oa_type,                  -- for inserts is set F, S, H, N or null.\n" +
            "        CAST (null as varchar)          f_imprint,                  -- for inserts was w.imprint_code  as f_imprint,\n" +
            "        CAST (null as varchar)          opco,                       -- for inserts was w.opco_r12_id   as opco,\n" +
            "        CAST (null as varchar)          f_society_ownership,        -- for inserts sets 'ELS-EFO' etc.\n" +
            "        CAST (null as varchar)          subscription_type,          -- for inserts, use m.manifestation_type FY/RY\n" +
            "        CAST (null as varchar)          resp_centre,                -- for inserts, was w.responsibility_centre_code as resp_centre,\n" +
            "        CAST (null as varchar)          language_code,              -- for inserts, was a translation of w.main_language_code to EN etc.\n" +
            "       (CASE\n" +
            "            when (w0.eph_work_id             is null and w1.eph_work_id             is null) then 'Y'\n" +
            "            when (w0.elsevier_journal_number is null and w1.elsevier_journal_number is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                         dq_err,\n" +
            "        COALESCE(w1.notified_date, w0.notified_date) as notified_date  -- they should both be set the same\n" +
            "from  ((("+ GetJMDLDBUser.getJMDB()+".jmf_work                     w0\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on  (w0.work_chronicle_id       = wc.work_chronicle_id))\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "         left join "+ GetJMDLDBUser.getJMDB()+".jmf_work           w1 on ((w1.work_chronicle_id       = w0.work_chronicle_id)\n" +
            "                                        and  (w1.work_journey_identifier = 'A1')))\n" +
            "where  w0.work_journey_identifier = 'A0'\n" +
            "and    wc.chronicle_scenario_code = 'TR'\n" +
            "and    w0.notified_date is not null\n" +
            ")\n" +
            "\n" +
            "\n" +
            "\n";


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

    public static String GET_JMF_STAGING_WORK_PERSON_ROLE="select count(*) as count from (\n" +
            "select distinct\n" +
            "       cs.chronicle_scenario_name as   scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "       'J0'||w.elsevier_journal_number||'-'||wpr.peoplehub_id||'-PU'  jm_source_reference,\n" +
            "        w.eph_work_id as                eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number  work_source_reference,\n" +
            "        wpr.eph_person_id as            f_person,\n" +
            "        wpr.peoplehub_id as             person_source_reference,\n" +
            "        w.elsevier_journal_number as    elsevier_journal_number,\n" +
            "        wpr.peoplehub_id as             employee_number,\n" +
            "       'PU' as                          f_role,\n" +
            "        lower(wpr.email_address) as     internal_email,\n" +
            "        wpr.notified_date as            start_date,\n" +
            "        CAST (null as date)             end_date,\n" +
            "       'N'  as                          dq_err,\n" +
            "        wpr.notified_date as            notified_date\n" +
            "from  "+ GetJMDLDBUser.getJMDB()+".jmf_work_person_role   wpr\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w  on w.work_id = wpr.f_work\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where wpr.party_role_type in ('PPC', 'PU')\n" +
            "and   wpr.notified_date is not null\n" +
            "and   w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "\n" +
            "UNION\n" +
            " \n" +
            "select distinct\n" +
            "       cs.chronicle_scenario_name as   scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "       'J0'||w.elsevier_journal_number||'-'||warp.pd_peoplehub_id||'-PD'  jm_source_reference,\n" +
            "        w.eph_work_id as                eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number  work_source_reference,\n" +
            "        wpr.eph_person_id as            f_person,\n" +
            "        wpr.peoplehub_id as             person_source_reference,\n" +
            "        w.elsevier_journal_number as    elsevier_journal_number,\n" +
            "        warp.pd_peoplehub_id as         employee_number,\n" +
            "        'PD' as                         f_role,\n" +
            "        lower(warp.pd_email) as         internal_email,\n" +
            "        wpr.notified_date as            start_date,\n" +
            "        CAST (null as date)             end_date,\n" +
            "       'N'  as                          dq_err,\n" +
            "        wpr.notified_date as            notified_date\n" +
            "from  "+ GetJMDLDBUser.getJMDB()+".jmf_work_person_role   wpr\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w  on w.work_id = wpr.f_work\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "join "+ GetJMDLDBUser.getJMDB()+".works_attrs_roles_people_v warp on\n" +
            "         (warp.pmgcode = w.pmg_code\n" +
            "      and warp.pmccode = w.pmc_code\n" +
            "      and warp.pd_email is not null\n" +
            "      and warp.pd_email <> 'Not Found')\n" +
            "--    this last join is to pick up PD email and peoplehub_id from works_attrs_roles_people_v (view returns active journals)\n" +
            "--    for the given pmgcode and pmccode whilst filtering out missing PDs\n" +
            "where wpr.party_role_type in ('PUBDIR','PD')\n" +
            "and   wpr.notified_date is not null\n" +
            "and   w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "\n" +
            "UNION\n" +
            " \n" +
            "select distinct\n" +
            "       cs.chronicle_scenario_name as   scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "       'J0'||w.elsevier_journal_number||'-'||warp.bc_peoplehub_id||'-BC'  jm_source_reference,\n" +
            "        w.eph_work_id as                eph_work_id,\n" +
            "       'J0'||w.elsevier_journal_number  work_source_reference,\n" +
            "        CAST (null as varchar)          f_person, -- eph_person_id\n" +
            "        warp.bc_peoplehub_id as         person_source_reference,\n" +
            "        w.elsevier_journal_number as    elsevier_journal_number,\n" +
            "        warp.bc_peoplehub_id as         employee_number,\n" +
            "        'BC' as                         f_role,\n" +
            "        lower(warp.bc_email)            internal_email,\n" +
            "        w.notified_date as              start_date,\n" +
            "        CAST (null as date)             end_date,\n" +
            "       'N'  as                          dq_err,\n" +
            "        w.notified_date as              notified_date\n" +
            "from  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "join  "+ GetJMDLDBUser.getJMDB()+".works_attrs_roles_people_v warp on\n" +
            "         (warp.resp_cen = w.responsibility_centre_code\n" +
            "      and warp.bc_email is not null\n" +
            "      and warp.bc_email <> 'Not Found')\n" +
            "--    this last join is to pick up BC email and peoplehub_id from works_attrs_roles_people_v\n" +
            "--    for the given responsibility_centre_code. JM only holds BC's names.\n" +
            "where w.notified_date is not null\n" +
            "and   w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "\n" +
            "UNION\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE PUBLISHER\n" +
            "-- there are two possible sources - (1) jmf_family_resource_details has been drawn up into jmf_work.\n" +
            "--                                  (2) jmf_work_person_role for scenario 'CA'.\n" +
            "-- both have the same PPC/PU data (checked yesterday) so for future-proofing and alignment with EPH\n" +
            "-- I've taken jmf_work_person_role\n" +
            "-- Notes\n" +
            "--     a) for PMC changes we do still need to access via jmf_work\n" +
            "--     b) warning - the PUBDIR entries on jmf_work_person_role are insert scenarios not updates.\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "--\n" +
            "select distinct\n" +
            "       cs.chronicle_scenario_name as                                       scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "            when 'N' then 'Insert'\n" +
            "            when 'Y' then 'Update'\n" +
            "            else 'Update'\n" +
            "        END) as                                                             upsert,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number)||'-'||wpr.peoplehub_id||'-PU' as jm_source_reference,\n" +
            "        COALESCE(w1.eph_work_id, w0.eph_work_id) as                         eph_work_id,\n" +
            "       'J0'||COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number) as work_source_reference,\n" +
            "        wpr.eph_person_id as                                                f_person,\n" +
            "        wpr.peoplehub_id as                                                 person_source_reference,\n" +
            "        COALESCE(w1.elsevier_journal_number, w0.elsevier_journal_number) as elsevier_journal_number,\n" +
            "        wpr.peoplehub_id as                                                 employee_number,\n" +
            "       'PU' as                                                              f_role,\n" +
            "        wpr.email_address as                                                internal_email,\n" +
            "        wpr.notified_date as                                                start_date,\n" +
            "        CAST (null as date)                                                 end_date,\n" +
            "       (CASE\n" +
            "            when (w0.eph_work_id             is null and w1.eph_work_id             is null) then 'Y'\n" +
            "            when (w0.elsevier_journal_number is null and w1.elsevier_journal_number is null) then 'Y'\n" +
            "            else 'N'\n" +
            "        END) as                                                             dq_err,\n" +
            "        wpr.notified_date as                                                notified_date\n" +
            "from  "+ GetJMDLDBUser.getJMDB()+".jmf_work_person_role     wpr\n" +
            "join "+ GetJMDLDBUser.getJMDB()+".jmf_work               w0 on w0.work_id = wpr.f_work\n" +
            "join "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on w0.work_chronicle_id       = wc.work_chronicle_id\n" +
            "join "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "join "+ GetJMDLDBUser.getJMDB()+".jmf_work               w1 on w1.work_chronicle_id       = w0.work_chronicle_id\n" +
            "                              and w1.elsevier_journal_number = w0.elsevier_journal_number\n" +
            "                              and w1.work_journey_identifier = 'A1'\n" +
            "where wpr.party_role_type in ('PPC', 'PU')\n" +
            "and   w0.work_journey_identifier = 'A0'\n" +
            "and   wc.chronicle_scenario_code = 'CA'\n" +
            "and   wpr.notified_date is not null\n" +
            " \n" +
            "UNION\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE PUBLISHING DIRECTOR\n" +
            "-- JM updates ALL journals for a given PMG. Note: ALL journals, not just active journals.\n" +
            "-- This is why we need to use pmg_pmc_journals_v not works_attrs_roles_people_v.\n" +
            "-- Updates are passed to EPH by WORK PERSON ROLE as if they had come through from jmf_work_person_role\n" +
            "-- (In JMF this is the current model for PU changes, but not yet for PD changes.)\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE EPH  WORK_PERSON_ROLE\n" +
            "--\n" +
            "-- PMGCODE IS NOT IN THE OUTPUT, TO ALLOW THE UNION ALL TO WORK WITH PUBLISHER UPDATES. ALTERNATIVE IS TO ADD PMGCODE TO ALL THE QUERIES.\n" +
            "--\n" +
            "-- THERE IS NO WORK_CHRONICLE FOR PD CHANGES\n" +
            "--\n" +
            " \n" +
            "select distinct\n" +
            "       cs.chronicle_scenario_name as   scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "             when 'N' then 'Insert'\n" +
            "             when 'Y' then 'Update'\n" +
            "             else 'Update'\n" +
            "        END) as                         upsert,\n" +
            "       'J0'||ppj.jnl_no||'-'||ppa.pmg_pubdir_peoplehub_id_new||'-PD'  jm_source_reference,\n" +
            "        ppj.work_id as                  eph_work_id,\n" +
            "       'J0'||ppj.jnl_no                 work_source_reference,\n" +
            "        CAST (null as varchar)          f_person,\n" +
            "        ppa.pmg_pubdir_peoplehub_id_new as person_source_reference,\n" +
            "        ppj.jnl_no as                   elsevier_journal_number,\n" +
            "        ppa.pmg_pubdir_peoplehub_id_new as employee_number,\n" +
            "       'PD' as                          f_role,\n" +
            "        ppa.pmg_pubdir_email_new as     internal_email,\n" +
            "        CAST(ppa.notified_date as date) start_date,\n" +
            "        CAST (null as date)             end_date,\n" +
            "       'N'  as                          dq_err,\n" +
            "        ppa.notified_date as            notified_date\n" +
            "from  (("+ GetJMDLDBUser.getJMDB()+".jmf_pmg_pubdir_allocation   ppa\n" +
            "        join "+ GetJMDLDBUser.getJMDB()+".pmg_pmc_journals_v     ppj on (ppj.pmgcode = ppa.pmx_pmgcode))\n" +
            "        join "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs  on (ppa.allocation_type = cs.chronicle_scenario_code))\n" +
            "where    ppa.allocation_type        = 'PD'\n" +
            "and      ppa.notified_date is not null\n" +
            ")";

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


    public static String GET_JMF_STAGING_MANIFESTATION="select count(*) as count from (select * from \n" +
            "(select cs.chronicle_scenario_name as                       scenario_name,\n" +
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
            "        COALESCE(m.online_launch_date, w.production_launch_date, w.launch_date) as online_launch_date,\n" +
            "       (CASE m.manifestation_type\n" +
            "            when 'E' then 'JEL'\n" +
            "            else          'JPR'\n" +
            "        END) as                                             f_type,                    -- JPR, JEL\n" +
            "      'MAP'                                                 f_status,                  -- 'MAP' manifestation status stands for Approved (Planned). Set 'MAP' for new journals.\n" +
            "       m.notified_date as                                   effective_start_date,\n" +
            "      'N' as                                                dq_err,\n" +
            "       m.notified_date as                                   notified_date,\n" +
            "      'J0'||w.elsevier_journal_number                       work_source_reference      -- Added New work source reference derived from jmf_work\n" +
            "from  journalmaestro_sit2.jmf_manifestation      m\n" +
            "join  journalmaestro_sit2.jmf_work               w  on m.f_work = w.work_id\n" +
            "join  journalmaestro_sit2.jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  journalmaestro_sit2.jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   m.notified_date is not null\n" +
            "UNION\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- RENAMES - only populate the field which has changed - at manifestation level this is TITLE (exception: set f_type too)\n" +
            "-- join base data m0, w0, to w1.\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
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
            "from (((journalmaestro_sit2.etl_manifestation_updates1_v mu1\n" +
            "        join journalmaestro_sit2.jmf_work           w1 on ((w1.work_chronicle_id       = mu1.w0_chronicle_id)\n" +
            "                                   and (w1.elsevier_journal_number = mu1.w0_journal_number)\n" +
            "                                   and (w1.work_journey_identifier = 'A1')))                        -- we've definitely got one work, the A1.\n" +
            "        join journalmaestro_sit2.jmf_work_chronicle wc on ((w1.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                   and (w1.work_journey_identifier = 'A1')\n" +
            "                                   and (wc.chronicle_scenario_code = 'RN')))\n" +
            "        join journalmaestro_sit2.jmf_manifestation  m1 on ((m1.f_work = w1.work_id)\n" +
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
            "))";

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
            "from  journalmaestro_sit2.jmf_manifestation      m\n" +
            "join  journalmaestro_sit2.jmf_work               w  on m.f_work = w.work_id\n" +
            "join  journalmaestro_sit2.jmf_work_chronicle     wc on w.work_chronicle_id        = wc.work_chronicle_id\n" +
            "join  journalmaestro_sit2.jmf_chronicle_scenario cs on wc.chronicle_scenario_code = cs.chronicle_scenario_code\n" +
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
            "from (((journalmaestro_sit2.etl_manifestation_updates1_v mu1\n" +
            "        join journalmaestro_sit2.jmf_work                w1 on ((w1.work_chronicle_id       = mu1.w0_chronicle_id)\n" +
            "                                        and (w1.elsevier_journal_number = mu1.w0_journal_number)\n" +
            "                                        and (w1.work_journey_identifier = 'A1')))                   -- we've definitely got one work, the A1.\n" +
            "        join journalmaestro_sit2.jmf_work_chronicle      wc on ((w1.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                        and (w1.work_journey_identifier = 'A1')\n" +
            "                                        and (wc.chronicle_scenario_code = 'RN')))\n" +
            "        join journalmaestro_sit2.jmf_manifestation       m2 on ((m2.f_work = w1.work_id)\n" +
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


    public static String GET_JMF_STAGING_PRODUCT_PART="select count(*) as count from (\n" +
            "select  cs.chronicle_scenario_name as                             scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "             when 'N' then 'Insert'\n" +
            "             when 'Y' then 'Update'\n" +
            "             else 'Update'\n" +
            "        END) as                                                   upsert,\n" +
            "        m.manifestation_type||'-J0'||m.elsevier_journal_number as jm_source_reference,             -- format E-J012345 or P-J012345\n" +
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
            "       (CASE when (COALESCE (w.open_accesstype_code,'XYZ') not in ('SM5','SM6') and m.manifestation_type = 'P') then 'Y'\n" +
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
            "        CAST('Y' as varchar) as                                   author_charges,                  -- set 'Y' for new journal inserts, 'N' for updates.\n" +
            "        CAST('N' as varchar) as                                   one_off_access,                  -- set N for new journals\n" +
            "        CAST('N' as varchar) as                                   packages,                        -- set N for new journals\n" +
            "        CAST('PPL' as varchar) as                                 availability_status,             -- for new journals set to product, planned 'PPL'\n" +
            "        CAST('PPL' as varchar) as                                 work_status,                     -- for new journals set to product, planned 'PPL'\n" +
            "        w.work_title as                                           work_title,                      -- w.work_title\n" +
            "        CAST('JOURNAL' as varchar) as                             work_type,                       -- set 'JOURNAL'.\n" +
            "        wpr.email_address as                                      pu_internal_email,               -- to be used by etl_product_person_role_dq_v\n" +
            "        wpr.peoplehub_id as                                       pu_employee_number,              -- to be used by etl_product_person_role_dq_v\n" +
            "       (CASE when (w.elsevier_journal_number is null) then 'Y'\n" +
            "             when (m.elsevier_journal_number is null) then 'Y'\n" +
            "             when (m.manifestation_type      is null) then 'Y'\n" +
            "             when (wpr.email_address         is null) then 'Y'\n" +
            "             when (wpr.peoplehub_id          is null) then 'Y'                                     -- set 'Y' when any of the essential fields to build JM references are missing\n" +
            "             else                                          'N'                                     -- or when email or peoplehub_id are missing.\n" +
            "        END) as                                                   dq_err,\n" +
            "        m.notified_date as                                        notified_date\n" +
            "from (((("+ GetJMDLDBUser.getJMDB()+".jmf_manifestation            m\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_work               w   on  (m.f_work   = w.work_id))\n" +
            "    left join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_person_role   wpr on ((wpr.f_work = w.work_id)\n" +
            "                                          and (wpr.party_role_type in ('PPC','PU'))))\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc  on  (w.work_chronicle_id        = wc.work_chronicle_id))\n" +
            "         join  "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs  on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "where w.work_journey_identifier = 'A1'\n" +
            "and   wc.chronicle_scenario_code in ('NP','NS','AC','MI')\n" +
            "and   m.notified_date is not null\n" +
            "--       the grouping was a necessary step for updates, handling both A0 & A1s, to handle duplicate manifestation primary keys in SIT. Harmless to include here, too.\n" +
            "group by cs.chronicle_scenario_name, cs.chronicle_scenario_evolutionary_ind,\n" +
            "         w.work_chronicle_id, w.elsevier_journal_number, w.work_journey_identifier, w.open_accesstype_code, w.manifestation_types_code,\n" +
            "         m.manifestation_id, w.launch_date, m.manifestation_type, m.elsevier_journal_number, m.manifestation_title,\n" +
            "         w.eph_work_id, w.work_title, m.eph_manifestation_id, m.notified_date, wpr.email_address, wpr.peoplehub_id\n" +
            "\n" +
            "\n" +
            "UNION\n" +
            "\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE PRODUCT TITLE - begin with all RN renames at manifestation level.\n" +
            "-- this is the TITLE RENAME select. The join to work_person_role needs to be LEFT because RENAMEs don't affect people.\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "\n" +
            "select  cs.chronicle_scenario_name as                             scenario,\n" +
            "       (CASE cs.chronicle_scenario_evolutionary_ind\n" +
            "             when 'N' then 'Insert'\n" +
            "             when 'Y' then 'Update'\n" +
            "             else 'Update'\n" +
            "        END) as                                                   upsert,\n" +
            "        m0.manifestation_type||'-J0'||m0.elsevier_journal_number as jm_source_reference,           -- format E-J012345 or P-J012345\n" +
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
            "       (CASE when (COALESCE (w0.open_accesstype_code,'XYZ') not in ('SM5','SM6') and m0.manifestation_type = 'P') then 'Y'\n" +
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
            "        CAST('N' as varchar) as                                   author_charges,                  -- set 'N' for journal updates.\n" +
            "        CAST('N' as varchar) as                                   one_off_access,                  -- set N for new journals\n" +
            "        CAST('N' as varchar) as                                   packages,                        -- set N for new journals\n" +
            "        CAST(null as varchar) as                                  availability_status,\n" +
            "        CAST(null as varchar) as                                  work_status,                     -- for new journals set to product, planned 'PPL'\n" +
            "        w0.work_title as                                          w0_work_title,                   -- w.work_title\n" +
            "        CAST('JOURNAL' as varchar) as                             work_type,                       -- set 'JOURNAL'.\n" +
            "        wpr.email_address as                                      pu_internal_email,               -- to be used for product_person_role\n" +
            "        wpr.peoplehub_id as                                       pu_employee_number,              -- to be used for product_person_role\n" +
            "       (CASE when (w0.elsevier_journal_number is null) then 'Y'\n" +
            "             when (m0.elsevier_journal_number is null) then 'Y'\n" +
            "             when (m0.manifestation_type      is null) then 'Y'\n" +
            "             else                                           'N'                                    -- or when email or peoplehub_id are missing?\n" +
            "        END) as                                                   dq_err,\n" +
            "        m0.notified_date as                                       notified_date\n" +
            "--      the joins and where clause select all rename manifestations, A0 'before', both p & e.\n" +
            "--      the grouping is a necessary step to handle duplicate manifestation primary keys in SIT.\n" +
            "from (((("+ GetJMDLDBUser.getJMDB()+".jmf_manifestation           m0\n" +
            "         join "+ GetJMDLDBUser.getJMDB()+".jmf_work               w0 on ((m0.f_work = w0.work_id)\n" +
            "                                        and (w0.work_journey_identifier = 'A0')))\n" +
            "    left join "+ GetJMDLDBUser.getJMDB()+".jmf_work_person_role  wpr on ((wpr.f_work = w0.work_id)\n" +
            "                                        and (wpr.party_role_type in ('PPC','PU'))))\n" +
            "         join "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc on ((w0.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                        and (w0.work_journey_identifier = 'A0')))\n" +
            "         join "+ GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs on  (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "where    wc.chronicle_scenario_code = 'RN'\n" +
            "and      m0.notified_date is not null\n" +
            "group by cs.chronicle_scenario_name, cs.chronicle_scenario_evolutionary_ind,\n" +
            "         w0.work_chronicle_id, w0.elsevier_journal_number, w0.work_journey_identifier, w0.open_accesstype_code, w0.manifestation_types_code,\n" +
            "         m0.manifestation_id, m0.manifestation_type, m0.elsevier_journal_number, m0.manifestation_title,\n" +
            "         w0.eph_work_id, w0.work_title, m0.eph_manifestation_id, m0.notified_date, wpr.email_address, wpr.peoplehub_id\n" +
            "\n" +
            "UNION\n" +
            "\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "-- UPDATE PRODUCT OWNER - begin with all CA Updates\n" +
            "-- ADD a CA select for a change in PPC/PU, which translates at product-level to a change in PO.\n" +
            "--\n" +
            "-- **************** for PU changes JM DOES NOT HAVE MANIFESTATION  **********************\n" +
            "-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "\n" +
            "select  cs.chronicle_scenario_name as                             scenario,\n" +
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
            "        w.pu_email_new as                                         pu_internal_email,               -- to be used by etl_product_person_role_dq_v\n" +
            "        w.pu_peoplehubid_new as                                   pu_employee_number,              -- to be used by etl_product_person_role_dq_v\n" +
            "       (CASE when (w.elsevier_journal_number is null) then 'Y'\n" +
            "             when (w.pu_email_new            is null) then 'Y'\n" +
            "             when (w.pu_peoplehubid_new      is null) then 'Y'                                     -- set 'Y' when any of the essential fields to build JM references are missing\n" +
            "             else                                          'N'                                     -- or when email or peoplehub_id are missing.\n" +
            "        END) as                                                   dq_err,\n" +
            "        w.notified_date as                                        notified_date\n" +
            "from  (("+ GetJMDLDBUser.getJMDB()+".jmf_work                      w\n" +
            "        join  "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle     wc  on (w.work_chronicle_id        = wc.work_chronicle_id))\n" +
            "        join  " + GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario cs  on (wc.chronicle_scenario_code = cs.chronicle_scenario_code))\n" +
            "where   w.work_journey_identifier  = 'A1'\n" +
            "and     wc.chronicle_scenario_code = 'CA'\n" +
            "and     w.notified_date is not null\n" +
            ")";
    public static String GET_JMF_STAGING_PRODUCT_INSERT="select count(*) as count from (\n" +
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
                            "from  journalmaestro_sit2.etl_product_part1_v\n" +
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
                            "crosstab_data) select * from result_data))";


    public static String GET_JMF_STAGING_PRODUCT_UPDATES="select count(*) as count from (\n" +
           "with base_data as\n" +
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
            "from  etl_product_part1_v\n" +
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
            "        join journalmaestro_sit2.jmf_work           w1 on ((w1.work_chronicle_id       = bd.w0_chronicle_id)\n" +
            "                                   and (w1.elsevier_journal_number = bd.w0_journal_number)\n" +
            "                                   and (w1.work_journey_identifier = 'A1')))                   -- we've definitely got one work, the A1.\n" +
            "        join journalmaestro_sit2.jmf_work_chronicle wc on ((w1.work_chronicle_id       = wc.work_chronicle_id)\n" +
            "                                   and (w1.work_journey_identifier = 'A1')\n" +
            "                                   and (wc.chronicle_scenario_code = 'RN')))\n" +
            "        join journalmaestro_sit2.jmf_manifestation  m1 on ((m1.f_work = w1.work_id)\n" +
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
            "select * from result_data))";


    public static String GET_JMF_STAGING_PRODUCT="select count(*) as count from (\n" +
            "\n" +
            "select * from "+ GetJMDLDBUser.getJMDB()+".etl_product_inserts_v\n" +
            "\n" +
            "UNION\n" +
            "\n" +
            "select * from "+ GetJMDLDBUser.getJMDB()+".etl_product_updates_v)";

    public static String GET_JMF_STAGING_PRODUCT_PERSON_ROLE="select count(*) as count from (select * from (with base_data as\n" +
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
            "from  etl_product_part1_v\n" +
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
            ")select * from result_data))";


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

    public static String GET_JMF_STAGING_WORKS_ATTRS_ROLES_PEOPLE="select count(*) as count from (\n" +
            "SELECT\n" +
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
            "   FROM "+ GetPRMDLDBUser.getProdDataBase()+".gd_wwork w\n" +
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
            "   )";
}