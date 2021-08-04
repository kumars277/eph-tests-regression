package com.eph.automation.testing.services.db.jrbisql;


public class JRBIDataLakeCountChecksSQL {
    private JRBIDataLakeCountChecksSQL() {throw new IllegalStateException("Utility class");}

    public static final String GET_JRBI_WORK_SOURCE_COUNT =
         "WITH\n" +
                 "  jrbi_catalogue_max AS (\n" +
                 "   SELECT\n" +
                 "     cr2.epr epr\n" +
                 "   , max(CAST(NULLIF(j.catalogue_volumes_qty, '') AS integer)) catalogue_volumes_qty\n" +
                 "   , max(CAST(NULLIF(j.catalogue_issues_qty, '') AS integer)) catalogue_issues_qty\n" +
                 "   , max(NULLIF(j.catalogue_volume_from, '')) catalogue_volume_from\n" +
                 "   , max(NULLIF(j.catalogue_volume_to, '')) catalogue_volume_to\n" +
                 "   , max(CAST(NULLIF(j.rf_issues_qty, '') AS integer)) rf_issues_qty\n" +
                 "   , max(CAST(NULLIF(j.rf_total_pages_qty, '') AS integer)) rf_total_pages_qty\n" +
                 "   , max(NULLIF(j.rf_fvi, '')) rf_fvi\n" +
                 "   , max(NULLIF(j.rf_lvi, '')) rf_lvi\n" +
                 "   FROM\n" +
                 "     ("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full j\n" +
                 "   INNER JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON (((concat(substr('00000', 1, (5 - length(j.journal_number))), j.journal_number) = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Work')))\n" +
                 "   GROUP BY cr2.epr\n" +
                 ") \n" +
                 ", jrbi_joined AS (\n" +
                 "   SELECT DISTINCT\n" +
                 "     cr2.epr epr\n" +
                 "   , 'jrbi Work Extended' record_type\n" +
                 "   , cr2.work_type work_type\n" +
                 "   , NULLIF(j.primary_site_system, '') primary_site_system\n" +
                 "   , NULLIF(j.primary_site_acronym, '') primary_site_acronym\n" +
                 "   , NULLIF(j.primary_site_support_level, '') primary_site_support_level\n" +
                 "   , NULLIF(j.fulfilment_system, '') fulfilment_system\n" +
                 "   , NULLIF(j.fulfilment_journal_acronym, '') fulfilment_journal_acronym\n" +
                 "   , NULLIF(j.issue_prod_type_code, '') issue_prod_type_code\n" +
                 "   , CAST(NULLIF(j.catalogue_volumes_qty, '') AS integer) catalogue_volumes_qty\n" +
                 "   , CAST(NULLIF(j.catalogue_issues_qty, '') AS integer) catalogue_issues_qty\n" +
                 "   , NULLIF(j.catalogue_volume_from, '') catalogue_volume_from\n" +
                 "   , NULLIF(j.catalogue_volume_to, '') catalogue_volume_to\n" +
                 "   , CAST(NULLIF(j.rf_issues_qty, '') AS integer) rf_issues_qty\n" +
                 "   , CAST(NULLIF(j.rf_total_pages_qty, '') AS integer) rf_total_pages_qty\n" +
                 "   , NULLIF(j.rf_fvi, '') rf_fvi\n" +
                 "   , NULLIF(j.rf_lvi, '') rf_lvi\n" +
                 "   , NULLIF(j.business_unit_desc, '') business_unit_desc\n" +
                 "   , NULLIF(j.journal_prod_site_code, '') journal_prod_site_code\n" +
                 "   FROM\n" +
                 "     ("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full j\n" +
                 "   INNER JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON (((concat(substr('00000', 1, (5 - length(j.journal_number))), j.journal_number) = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Work')))\n" +
                 ") \n" +
                 "SELECT count(*) as Source_Count from (SELECT DISTINCT\n" +
                 "  ji.epr epr\n" +
                 ", ji.record_type record_type\n" +
                 ", ji.work_type work_type\n" +
                 ", ji.primary_site_system primary_site_system\n" +
                 ", ji.primary_site_acronym primary_site_acronym\n" +
                 ", ji.primary_site_support_level primary_site_support_level\n" +
                 ", ji.fulfilment_system fulfilment_system\n" +
                 ", ji.fulfilment_journal_acronym fulfilment_journal_acronym\n" +
                 ", ji.issue_prod_type_code issue_prod_type_code\n" +
                 ", COALESCE(ji.catalogue_volumes_qty, m.catalogue_volumes_qty) catalogue_volumes_qty\n" +
                 ", COALESCE(ji.catalogue_issues_qty, m.catalogue_issues_qty) catalogue_issues_qty\n" +
                 ", COALESCE(ji.catalogue_volume_from, m.catalogue_volume_from) catalogue_volume_from\n" +
                 ", COALESCE(ji.catalogue_volume_to, m.catalogue_volume_to) catalogue_volume_to\n" +
                 ", COALESCE(ji.rf_issues_qty, m.rf_issues_qty) rf_issues_qty\n" +
                 ", COALESCE(ji.rf_total_pages_qty, m.rf_total_pages_qty) rf_total_pages_qty\n" +
                 ", COALESCE(ji.rf_fvi, m.rf_fvi) rf_fvi\n" +
                 ", COALESCE(ji.rf_lvi, m.rf_lvi) rf_lvi\n" +
                 ", ji.business_unit_desc business_unit_desc\n" +
                 ", ji.journal_prod_site_code journal_prod_site_code\n" +
                 "FROM\n" +
                 "  (jrbi_joined ji\n" +
                 "LEFT JOIN jrbi_catalogue_max m ON (ji.epr = m.epr)))\n";

    public static final String GET_JRBI_MANIF_SOURCE_COUNT =
                "select Count(*) as Source_Count from(\n" +
                        "SELECT DISTINCT\n" +
                        "  cr2.epr epr\n" +
                        ", 'jrbi Manifestation Extended' record_type\n" +
                        ", cr2.manifestation_type manifestation_type\n" +
                        ", NULLIF(j.journal_issue_trim_size, '') journal_issue_trim_size\n" +
                        ", NULLIF(j.war_reference, '') war_reference\n" +
                        "FROM\n" +
                        "  ("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full j\n" +
                        "INNER JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON \n" +
                        "((((concat(substr('00000', 1, (5 - length(j.journal_number))), j.journal_number) = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER'))\n" +
                        " AND (cr2.record_level = 'Manifestation')) AND (cr2.manifestation_type = 'JPR'))))\n";

        public static final String GET_JRBI_PERSON_SOURCE_COUNT =
               "select Count(*) as Source_Count from(\n" +
                       "SELECT DISTINCT\n" +
                       "  cr2.epr epr\n" +
                       ", 'jrbi Person Extended' record_type\n" +
                       ", cr2.work_type work_type\n" +
                       ", NULLIF(j.role_code, '') role_code\n" +
                       ", concat(cr2.epr, j.role_code) u_key\n" +
                       ", NULLIF(j.role_description, '') role_description\n" +
                       ", COALESCE(p.given_name, jel.given_name) given_name\n" +
                       ", COALESCE(p.family_name, jel.family_name) family_name\n" +
                       ", COALESCE(p.peoplehub_id, jel.peoplehub_id) peoplehub_id\n" +
                       ", NULLIF(COALESCE(rtrim(ltrim(lower(j.email), ' '), ' '), rtrim(ltrim(lower(jel.email), ' '), ' ')), '') email\n" +
                       "FROM\n" +
                       "  ((("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_person_unpivot_v j\n" +
                       "INNER JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON (((concat(substr('00000', 1, (5 - length(j.journal_number))), j.journal_number) = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Work')))\n" +
                       "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".workday_reference_v p ON (rtrim(ltrim(lower(j.email), ' '), ' ') = rtrim(ltrim(lower(p.email), ' '), ' ')))\n" +
                       "LEFT JOIN "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_email_lookup jel ON (rtrim(ltrim(lower(jel.email), ' '), ' ') = rtrim(ltrim(lower(j.email), ' '), ' ')))\n" +
                       "WHERE ((NULLIF(COALESCE(rtrim(ltrim(lower(j.email), ' '), ' '), rtrim(ltrim(lower(jel.email), ' '), ' ')), '') IS NOT NULL) \n" +
                       "AND (COALESCE(p.peoplehub_id, jel.peoplehub_id) IS NOT NULL)))\n";

    public static final String GET_JRBI_CURRENT_WORK_COUNT = "select count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work\n";
    public static final String GET_JRBI_CURRENT_MANIF_COUNT = "select count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation\n";
    public static final String GET_JRBI_CURRENT_PERSON_COUNT = "select count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person\n";

    public static final String GET_JRBI_PREVIOUS_WORK_COUNT = "select count(*) as Previous_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_work\n";
    public static final String GET_JRBI_PREVIOUS_MANIF_COUNT = "select count(*) as Previous_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation\n";
    public static final String GET_JRBI_PREVIOUS_PERSON_COUNT = "select count(*) as Previous_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person\n";

    public static final String GET_JRBI_DELTA_WORK_COUNT = "SELECT count(*) as Delta_Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_work\n";
    public static final String GET_JRBI_DELTA_MANIF_COUNT = "SELECT count(*) as Delta_Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation\n";
    public static final String GET_JRBI_DELTA_PERSON_COUNT = "SELECT count(*) as Delta_Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person\n";



    public static final String GET_JRBI_COUNT_DIFF_PERSON_HISTORY_AND_DELTA_PERSON =
           "select count(*) as source_count from \n" +
                   "(select A.epr, A.record_type, A.role_code, A.u_key\n" +
                   ", A.role_description, A.given_name, A.family_name, A.peoplehub_id\n" +
                   ", A.email, A.last_updated_date, A.delete_flag, A.transform_ts \n" +
                   "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A \n" +
                   "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person B on A.u_key  = B.u_key \n" +
                   "where B.u_key is null and " +
                    "A.transform_ts = (select max(A.transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A ))\n";

    public static final String GET_JRBI_COUNT_DIFF_MANIF_HISTORY_AND_DELTA_MANIF =
            "select count(*) as source_count from \n" +
                    "(select A.epr, A.journal_issue_trim_size\n" +
                    ", A.record_type, A.war_reference\n" +
                    ", A.last_updated_date, A.transform_ts, A.delete_flag \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation B on A.epr  = B.epr \n" +
                    "where B.epr is null and " +
                    "A.transform_ts = (select max(A.transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part A ))\n";

    public static final String GET_JRBI_COUNT_DIFF_WORK_HISTORY_AND_DELTA_WORK =
           "select count(*) as source_count from \n" +
                   "(select A.epr, A.record_type, A.primary_site_system\n" +
                   ", A.primary_site_acronym, A.primary_site_support_level\n" +
                   ", A.fulfilment_system, A.fulfilment_journal_acronym, A.issue_prod_type_code\n" +
                   ", A.catalogue_volumes_qty, A.catalogue_issues_qty, A.catalogue_volume_from\n" +
                   ", A.catalogue_volume_to, A.rf_issues_qty, A.rf_total_pages_qty, A.rf_fvi\n" +
                   ", A.rf_lvi, A.business_unit_desc, A.last_updated_date, A.delete_flag\n" +
                   ", A.transform_ts from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part A \n" +
                   "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_work B on A.epr  = B.epr \n" +
                   "where B.epr is null and " +
                   "A.transform_ts = (select max(A.transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part A ))\n";

    public static final String GET_JRBI_WORK_EXCL_COUNT =
            "select count(*) as Excl_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_work_excl_delta\n";

    public static final String GET_JRBI_PERSON_EXCL_COUNT =
           "select count(*) as Excl_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_person_excl_delta\n";

    public static final String GET_JRBI_MANIF_EXCL_COUNT =
            "select count(*) as Excl_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_manifestation_excl_delta\n";

    public static final  String GET_JRBI_CURRENT_WORK_HISTORY_COUNT =
            "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part where\n " +
                    "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part) and delete_flag = false\n ";

    public static final  String GET_JRBI_CURRENT_MANIF_HISTORY_COUNT =
            "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part where\n " +
                    "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part) and delete_flag = false\n ";

    public static final  String GET_JRBI_CURRENT_PERSON_HISTORY_COUNT =
            "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part where\n " +
                    "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part) and delete_flag = false\n ";

    public static final  String GET_JRBI_PREVIOUS_WORK_HISTORY_COUNT =
    "select count(*) as Previous_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part where\n " +
            "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part\n " +
            " where transform_ts < (select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part))\n";

    public static final  String GET_JRBI_PREVIOUS_MANIF_HISTORY_COUNT =
            "select count(*) as Previous_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part where\n " +
                    "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part\n " +
                    " where transform_ts < (select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part))\n";

    public static final  String GET_JRBI_PREVIOUS_PERSON_HISTORY_COUNT =
            "select count(*) as Previous_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part where\n " +
                    "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part\n " +
                    " where transform_ts < (select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A))\n";

    public static final  String GET_JRBI_DELTA_WORK_HISTORY_COUNT =
            "select count(*) as Delta_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_work_history_part " +
                    "where delta_ts = (select max(delta_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_work_history_part)\n";

    public static final  String GET_JRBI_DELTA_PERSON_HISTORY_COUNT =
            "select count(*) as Delta_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_person_history_part " +
                    "where delta_ts = (select max(delta_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_person_history_part)\n";
    public static final  String GET_JRBI_DELTA_MANIF_HISTORY_COUNT =
            "select count(*) as Delta_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_manifestation_history_part " +
                    "where delta_ts = (select max(delta_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_manifestation_history_part)\n";

    public static final String GET_JRBI_COUNT_SUM_DELTA_PERSON_AND_PERSON_HISTORY =
            "select count(*) as source_count from \n" +
                    "(select a.epr, a.record_type, a.role_code, \n" +
                    "a.u_key, a.role_description, a.given_name, a.family_name, \n" +
                    "a.peoplehub_id, a.email, a.transform_ts, a.delete_flag, \n" +
                    "a.last_updated_date \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_person_excl_delta \n" +
                    "as a union all select b.epr, b.record_type, b.role_code, b.u_key, \n" +
                    "b.role_description, b.given_name, b.family_name, b.peoplehub_id, \n" +
                    "b.email, b.transform_ts, null as col11, null as col12 \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person as b)\n ";



    public static final String GET_JRBI_WORK_LATEST_COUNT =
            "select count(*) as Target_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_work\n";

    public static final String GET_JRBI_WORK_EXTENDED_COUNT =
            "select count(*) as WORK_EXTENDED_COUNT from "+GetJRBIDLDBUser.getProductExtdb()+".work_extended\n";

    public static final String GET_JRBI_MANIF_EXTENDED_COUNT =
            "select count(*) as MANIF_EXTENDED_COUNT from "+GetJRBIDLDBUser.getProductExtdb()+".manifestation_extended\n";

    public static final String GET_JRBI_PERSON_EXTENDED_COUNT =
            "select count(*) as PERSON_EXTENDED_COUNT from "+GetJRBIDLDBUser.getProductExtdb()+".work_extended_person_role\n";


    public static final String GET_JRBI_COUNT_SUM_DELTA_WORK_AND_WORK_HISTORY =
            "select count(*) as source_count from \n" +
                    "(select a.epr, a.record_type, a.primary_site_system, \n" +
                    "a.primary_site_acronym, a.primary_site_support_level, a.fulfilment_system, a.fulfilment_journal_acronym, \n" +
                    "a.issue_prod_type_code, a.catalogue_volumes_qty, a.catalogue_issues_qty, a.catalogue_volume_from, \n" +
                    "a.catalogue_volume_to, a.rf_issues_qty,a.rf_total_pages_qty, a.rf_fvi,\n" +
                    "a.rf_lvi, a.business_unit_desc, a.last_updated_date, a.delete_flag\n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_work_excl_delta \n" +
                    "as a union all select b.epr, b.record_type, b.primary_site_system, \n" +
                    "b.primary_site_acronym, b.primary_site_support_level, b.fulfilment_system, b.fulfilment_journal_acronym, \n" +
                    "b.issue_prod_type_code, b.catalogue_volumes_qty, b.catalogue_issues_qty, b.catalogue_volume_from, \n" +
                    "b.catalogue_volume_to, b.rf_issues_qty,b.rf_total_pages_qty, b.rf_fvi,\n" +
                    "b.rf_lvi, b.business_unit_desc, null as col11, null as col12 \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_work as b)\n";

    public static final String GET_JRBI_COUNT_SUM_DELTA_MANIF_AND_MANIF_EXCLUDE =
         "select count(*) as source_count from \n" +
                 "(select a.epr, a.record_type, \n" +
                 "a.journal_issue_trim_size, a.war_reference, a.transform_ts, a.last_updated_date, a.delete_flag\n" +
                 "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_manifestation_excl_delta \n" +
                 "as a union all select b.epr, b.record_type, \n" +
                 "b.journal_issue_trim_size, b.war_reference,b.transform_ts,null as col11, null as col12 \n" +
                 "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation as b)\n ";

    public static final String GET_JRBI_MANIF_LATEST_COUNT =
            "select count(*) as Target_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_manifestation\n";

    public static final String GET_JRBI_PERSON_LATEST_COUNT =
            "select count(*) as Target_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_person\n";


    public static final String GET_COUNT_DIFF_CURRENT_PREVIOUS_WORK =
            "select count(*) as source_count from (\n" +
                    "--inserted\n" +
                    "select c.epr,c.record_type, c.work_type, c.primary_site_system,c.primary_site_acronym,c.primary_site_support_level,\n" +
                    "c.fulfilment_system,c.fulfilment_journal_acronym,c.issue_prod_type_code,c.catalogue_volumes_qty,\n" +
                    "c.catalogue_issues_qty,c.catalogue_volume_from,c.catalogue_volume_to,\n" +
                    "c.rf_issues_qty,c.rf_total_pages_qty,c.rf_fvi,c.rf_lvi,c.business_unit_desc,'I' as delta_mode\n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_work p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr,c.record_type, c.work_type, c.primary_site_system,c.primary_site_acronym,c.primary_site_support_level,\n" +
                    "c.fulfilment_system,c.fulfilment_journal_acronym,c.issue_prod_type_code,c.catalogue_volumes_qty,\n" +
                    "c.catalogue_issues_qty,c.catalogue_volume_from,c.catalogue_volume_to,\n" +
                    "c.rf_issues_qty,c.rf_total_pages_qty,c.rf_fvi,c.rf_lvi,c.business_unit_desc,'D' as delta_mode\n" +
                    "FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_work  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr,c.record_type, c.work_type, c.primary_site_system,c.primary_site_acronym,c.primary_site_support_level,\n" +
                    "c.fulfilment_system,c.fulfilment_journal_acronym,c.issue_prod_type_code,c.catalogue_volumes_qty,\n" +
                    "c.catalogue_issues_qty,c.catalogue_volume_from,c.catalogue_volume_to,\n" +
                    "c.rf_issues_qty,c.rf_total_pages_qty,c.rf_fvi,c.rf_lvi,c.business_unit_desc, 'C' as delta_mode\n" +
                    "FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_work  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work p  on c.epr = p.epr\n" +
                    "where (c.record_type != (p.record_type) or\n" +
                    "c.work_type != (p.work_type) or\n" +
                    "c.primary_site_system !=  (p.primary_site_system) or\n" +
                    "c.primary_site_acronym !=  (p.primary_site_acronym) or\n" +
                    "c.primary_site_support_level !=  (p.primary_site_support_level) or\n" +
                    "c.fulfilment_system !=  (p.fulfilment_system) or\n" +
                    "c.fulfilment_journal_acronym !=  (p.fulfilment_journal_acronym) or \n" +
                    "c.issue_prod_type_code !=  (p.issue_prod_type_code) or\n" +
                    "c.catalogue_volumes_qty !=  (p.catalogue_volumes_qty) or\n" +
                    "c.catalogue_issues_qty !=  (p.catalogue_issues_qty) or\n" +
                    "c.catalogue_volume_from !=  (p.catalogue_volume_from) or\n" +
                    "c.catalogue_volume_to !=  (p.catalogue_volume_to) or\n" +
                    "c.rf_issues_qty !=  (p.rf_issues_qty) or\n" +
                    "c.rf_total_pages_qty !=  (p.rf_total_pages_qty) or\n" +
                    "c.rf_fvi !=  (p.rf_fvi) or\n" +
                    "c.rf_lvi !=  (p.rf_lvi) or\n" +
                    "c.business_unit_desc !=  (p.business_unit_desc)))\n";


    public static final String GET_COUNT_DELTA_MANIF =
            "select count(*) as Target_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation\n";

    public static final String GET_COUNT_DIFF_CURRENT_PREVIOUS_PERSON =
            "select count(*) as source_count from (\n" +
                    "--new\n" +
                    "select c.epr, c.record_type, c.role_code , c.role_description ,c.given_name , c.family_name , c.peoplehub_id , c.email FROM "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person p  on c.u_key = p.u_key\n" +
                    "where p.u_key is null\n" +
                    "union all\n" +
                    "-- deleted\n" +
                    "select c.epr, c.record_type, c.role_code , c.role_description ,c.given_name , c.family_name , c.peoplehub_id , c.email FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person p  on c.u_key = p.u_key\n" +
                    "where p.u_key is null\n" +
                    "union all\n" +
                    "--changed\n" +
                    "select c.epr, c.record_type, c.role_code , c.role_description ,c.given_name , c.family_name , c.peoplehub_id , c.email\n" +
                    "FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person p  on c.u_key = p.u_key\n" +
                    "where (c.epr !=(p.epr) or\n" +
                    "c.record_type != (p.record_type) or \n" +
                    "c.role_code !=  (p.role_code) or\n" +
                    "c.role_description !=  (p.role_description) or\n" +
                    "c.given_name !=  (p.given_name) or\n" +
                    "c. family_name !=  (p.family_name) or\n" +
                    "c.peoplehub_id !=  (p.peoplehub_id) or\n" +
                    "c.email !=  (p.email)))\n";


    public static final String GET_COUNT_DIFF_CURRENT_PREVIOUS_MANIF =
            "select count(*) as source_count from (\n" +
                    "select c.epr , c.record_type , c.manifestation_type , c.journal_issue_trim_size , c.war_reference FROM "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr , c.record_type , c.manifestation_type , c.journal_issue_trim_size , c.war_reference  FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr , c.record_type , c.manifestation_type , c.journal_issue_trim_size , c.war_reference FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation p  on c.epr = p.epr\n" +
                    "where (c.epr !=(p.epr) or\n" +
                    "c.record_type != (p.record_type) or \n" +
                    "c.manifestation_type !=  (p.manifestation_type ) or\n" +
               //     "c.journal_prod_site_code !=  (p.journal_prod_site_code ) or\n" +
                    "c.journal_issue_trim_size !=  (p.journal_issue_trim_size ) or\n" +
                    "c.war_reference !=  (p.war_reference )))\n";

    public static final  String GET_JRBI_DUPLICATE_WORK_LATEST_COUNT =
            "select count(*) as Duplicate_Count from (SELECT epr,count(*) FROM "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_work group by epr having count(*)>1)\n";

    public static final  String GET_JRBI_DUPLICATE_MANIF_LATEST_COUNT =
            "select count(*) as Duplicate_Count from (SELECT epr,count(*) FROM "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_manifestation group by epr having count(*)>1)\n";

    public static final  String GET_JRBI_DUPLICATE_PERSON_LATEST_COUNT =
            "select count(*) as Duplicate_Count from (SELECT u_key,count(*) FROM "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_person group by u_key having count(*)>1)\n";

    public static final  String GET_JRBI_ISSN_COUNT_FULL_LOAD =
            "select count(issn) as issnCount from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full where issn is not null";


    public static final String GET_JRBI_FULL_LOAD_DATA_REC =
            "select title as title" +
                    ",issn as issn" +
                    ",journal_number as journal_number" +
                    ",journal_acronym as journal_acronym" +
                    ",journal_email as journal_email" +
                    ",primary_site_system as primary_site_system" +
                    ",primary_site_acronym as primary_site_acronym" +
                    ",primary_site_support_level as primary_site_support_level" +
                    ",pmg_code as pmg_code" +
                    ",pmc_code as pmc_code" +
                    ",business_unit_desc as business_unit_desc" +
                    ",expiry_date as expiry_date" +
                    ",copyright_owner_group as copyright_owner_group" +
                    ",copyright_owner_name as copyright_owner_name" +
                    ",journal_prod_site_code as journal_prod_site_code" +
                    ",opco_code as opco_code" +
                    ",fulfilment_system as fulfilment_system" +
                    ",fulfilment_journal_acronym as fulfilment_journal_acronym" +
                    ",typeset_model_code as typeset_model_code" +
                    ",journal_issue_trim_size as journal_issue_trim_size" +
                    ",issue_publ_type_code as issue_publ_type_code" +
                    ",issue_prod_type_code as issue_prod_type_code" +
                    ",catalogue_volumes_qty as catalogue_volumes_qty" +
                    ",catalogue_issues_qty as catalogue_issues_qty" +
                    ",catalogue_volume_from as catalogue_volume_from" +
                    ",catalogue_volume_to as catalogue_volume_to" +
                    ",rf_issues_qty as rf_issues_qty" +
                    ",rf_total_pages_qty as rf_total_pages_qty" +
                    ",rf_fvi as rf_fvi" +
                    ",rf_lvi as rf_lvi" +
                    ",svp_user_name as svp_user_name" +
                    ",svp_user_email as svp_user_email " +
                    ",pbd_email as pbd_email" +
                    ",pbd_user_email as pbd_user_email" +
                    ",jpu_user_name as jpu_user_name" +
                    ",jpu_user_email as jpu_user_email " +
                    ",pub_user_name as pub_user_name" +
                    ",pub_user_email as pub_user_email" +
                    ",ppc_user_name as ppc_user_name" +
                    ",ppc_user_email as ppc_user_email" +
                    ",jm_user_name as jm_user_name" +
                    ",jm_user_email as jm_user_email" +
                    ",imtuser_name as imtuser_name" +
                    ",imt_user_email as imt_user_email" +
                    ",mm_user_name as mm_user_name" +
                    ",mm_user_email as mm_user_email" +
                    ",csm_user_name as csm_user_name" +
                    ",csm_user_email as csm_user_email" +
                    ",eoc_user_name as eoc_user_name" +
                    ",eoc_user_email as eoc_user_email" +
                    ",epc_user_name as epc_user_name" +
                    ",epc_user_email as epc_user_email" +
                    ",cdm_user_name as cdm_user_name" +
                    ",cdm_user_email as cdm_user_email" +
                    ",adv_user_name as adv_user_name" +
                    ",adv_user_email as adv_user_email" +
                    ",psp_user_name as psp_user_name" +
                    ",psp_user_email as psp_user_email" +
                    ",war_reference as war_reference" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full where issn in ('%s') order by issn desc";

}




