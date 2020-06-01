package com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class JRBIDataLakeCountChecksSQL {

    public static String currentDate(){
        Calendar cal = Calendar.getInstance();
         DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String todayDate= dateFormat.format(cal.getTime());
        return todayDate;

    }

    public static String previousDate(){
        Calendar cal = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        cal.add(Calendar.DATE, -1);
        String yesterdayDate = dateFormat.format(cal.getTime());
        return yesterdayDate;
    }




    public static String GET_JRBI_WORK_SOURCE_COUNT =
            "select Count(*) as Source_Count from(SELECT DISTINCT\n" +
                    "  COALESCE(cr1.epr, cr2.epr) epr\n" +
                    ", 'JRBI Work Extended' record_type\n" +
                    ", j.primary_site_system primary_site_system\n" +
                    ", j.primary_site_acronym primary_site_acronym\n" +
                    ", j.primary_site_support_level primary_site_support_level\n" +
                    ", j.fulfilment_system fulfilment_system\n" +
                    ", j.fulfilment_journal_acronym fulfilment_journal_acronym\n" +
                    ", j.issue_prod_type_code issue_prod_type_code\n" +
                    ", CAST(NULLIF(j.catalogue_volumes_qty, '') AS integer) catalogue_volumes_qty\n" +
                    ", CAST(NULLIF(j.catalogue_issues_qty, '') AS integer) catalogue_issues_qty\n" +
                    ", j.catalogue_volume_from catalogue_volume_from\n" +
                    ", j.catalogue_volume_to catalogue_volume_to\n" +
                    ", CAST(NULLIF(j.rf_issues_qty, '') AS integer) rf_issues_qty\n" +
                    ", CAST(NULLIF(j.rf_total_pages_qty, '') AS integer) rf_total_pages_qty\n" +
                    ", j.rf_fvi rf_fvi\n" +
                    ", j.rf_lvi rf_lvi\n" +
                    ", j.business_unit_desc business_unit_desc\n" +
                    "FROM\n" +
                    "  (("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full j\n" +
                    "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr1 ON (((j.issn = cr1.identifier) AND (cr1.identifier_type = 'ISSN')) AND (cr1.record_level = 'Work')))\n" +
                    "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON (((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Work')))) where epr is not NULL\n";

    public static String GET_JRBI_MANIF_SOURCE_COUNT =
            "select Count(*) as Source_Count from(SELECT DISTINCT\n" +
                    "  COALESCE(cr1.epr, cr2.epr) epr\n" +
                    ", 'JRBI Manifestation' record_type\n" +
                    ", j.journal_prod_site_code journal_prod_site_code\n" +
                    ", j.journal_issue_trim_size journal_issue_trim_size\n" +
                    ", j.war_reference war_reference\n" +
                    "FROM\n" +
                    "  (("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full j\n" +
                    "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr1 ON ((((j.issn = cr1.identifier) AND (cr1.identifier_type = 'ISSN')) AND (cr1.record_level = 'Manifestation')) AND (cr1.manifestation_type = 'JPR')))\n" +
                    "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON ((((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Manifestation')) AND (cr2.manifestation_type = 'JPR')))) where epr is not NULL\n";


        public static String GET_JRBI_PERSON_SOURCE_COUNT =
                "SELECT COUNT(*) as Source_Count FROM (SELECT DISTINCT \n" +
                        "COALESCE(cr1.epr, cr2.epr) epr\n" +
                        ", 'JRBI Person Extended' record_type, j.role_code role_code\n" +
                        ", COALESCE(cr1.epr, cr2.epr)||j.role_code as u_key\n" +
                        ", j.role_description role_description, p.given_name given_name\n" +
                        ", p.family_name family_name, p.peoplehub_id peoplehub_id\n" +
                        ", j.email email \n" +
                        "FROM ((("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_person_unpivot_v j \n" +
                        "LEFT JOIN product_database_sit.gd_person p ON (j.email = p.email)) \n" +
                        "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr1 ON ((((j.issn = cr1.identifier) AND (cr1.identifier_type = 'ISSN')) AND (cr1.record_level = 'n')) AND (cr1.record_level = 'Work'))) \n" +
                        "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON (((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Work'))))where epr is not NULL\n";

    public static String GET_JRBI_CURRENT_WORK_COUNT = "select count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work\n";
    public static String GET_JRBI_CURRENT_MANIF_COUNT = "select count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation\n";
    public static String GET_JRBI_CURRENT_PERSON_COUNT = "select count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person\n";

    public static String GET_JRBI_PREVIOUS_WORK_COUNT = "select count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_work\n";
    public static String GET_JRBI_PREVIOUS_MANIF_COUNT = "select count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation\n";
    public static String GET_JRBI_PREVIOUS_PERSON_COUNT = "select count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person\n";

    public static String GET_JRBI_DELTA_WORK_COUNT = "SELECT count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_work\n";
    public static String GET_JRBI_DELTA_MANIF_COUNT = "SELECT count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation\n";
    public static String GET_JRBI_DELTA_PERSON_COUNT = "SELECT count(*) as Current_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person\n";



    public static String GET_JRBI_COUNT_DIFF_PERSON_HISTORY_AND_DELTA_PERSON =
           "select count(*) as source_count from \n" +
                   "(select A.epr, A.record_type, A.role_code, A.u_key\n" +
                   ", A.role_description, A.given_name, A.family_name, A.peoplehub_id\n" +
                   ", A.email, A.last_updated_date, A.delete_flag, A.transform_ts \n" +
                   "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A \n" +
                   "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person B on A.u_key  = B.u_key \n" +
                   "where B.u_key is null and A.transform_ts like '%"+JRBIDataLakeCountChecksSQL.currentDate()+"%')\n";

    public static String GET_JRBI_COUNT_DIFF_MANIF_HISTORY_AND_DELTA_MANIF =
            "select count(*) as source_count from \n" +
                    "(select A.epr, A.journal_issue_trim_size\n" +
                    ", A.journal_prod_site_code, A.record_type, A.war_reference\n" +
                    ", A.last_updated_date, A.transform_ts, A.delete_flag \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation B on A.epr  = B.epr \n" +
                    "where B.epr is null and A.transform_ts like '%"+JRBIDataLakeCountChecksSQL.currentDate()+"%')\n";

    public static String GET_JRBI_COUNT_DIFF_WORK_HISTORY_AND_DELTA_WORK =
           "select count(*) as source_count from \n" +
                   "(select A.epr, A.record_type, A.primary_site_system\n" +
                   ", A.primary_site_acronym, A.primary_site_support_level\n" +
                   ", A.fulfilment_system, A.fulfilment_journal_acronym, A.issue_prod_type_code\n" +
                   ", A.catalogue_volumes_qty, A.catalogue_issues_qty, A.catalogue_volume_from\n" +
                   ", A.catalogue_volume_to, A.rf_issues_qty, A.rf_total_pages_qty, A.rf_fvi\n" +
                   ", A.rf_lvi, A.business_unit_desc, A.last_updated_date, A.delete_flag\n" +
                   ", A.transform_ts from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part A \n" +
                   "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_work B on A.epr  = B.epr \n" +
                   "where B.epr is null and A.transform_ts like '%"+JRBIDataLakeCountChecksSQL.currentDate()+"%')\n";


    public static String GET_JRBI_WORK_EXCL_COUNT =
            "select count(*) as Target_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_work_excl_delta\n";

    public static String GET_JRBI_PERSON_EXCL_COUNT =
           "select count(*) as Target_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_person_excl_delta\n";

    public static String GET_JRBI_MANIF_EXCL_COUNT =
            "select count(*) as Target_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_manifestation_excl_delta\n";

    public static  String GET_JRBI_CURRENT_WORK_HISTORY_COUNT =
                "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part where transform_ts like '"+JRBIDataLakeCountChecksSQL.currentDate()+"%'\n";

    public static  String GET_JRBI_CURRENT_MANIF_HISTORY_COUNT =
            "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part where transform_ts like '"+JRBIDataLakeCountChecksSQL.currentDate()+"%'\n";

    public static  String GET_JRBI_CURRENT_PERSON_HISTORY_COUNT =
            "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_temp_transform_current_person_history_part where transform_ts like '"+JRBIDataLakeCountChecksSQL.currentDate()+"%'\n";

    public static  String GET_JRBI_PREVIOUS_WORK_HISTORY_COUNT =
            "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part where transform_ts like '"+JRBIDataLakeCountChecksSQL.previousDate()+"%'\n";

    public static  String GET_JRBI_PREVIOUS_MANIF_HISTORY_COUNT =
            "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part where transform_ts like '"+JRBIDataLakeCountChecksSQL.previousDate()+"%'\n";

    public static  String GET_JRBI_PREVIOUS_PERSON_HISTORY_COUNT =
            "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_temp_transform_current_person_history_part where transform_ts like '"+JRBIDataLakeCountChecksSQL.previousDate()+"%'\n";

    public static  String GET_JRBI_DELTA_WORK_HISTORY_COUNT =
            "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_work_history_part where delta_ts like '"+JRBIDataLakeCountChecksSQL.currentDate()+"%'\n";

    public static  String GET_JRBI_DELTA_PERSON_HISTORY_COUNT =
            "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_person_history_part where delta_ts like '"+JRBIDataLakeCountChecksSQL.currentDate()+"%'\n";

    public static  String GET_JRBI_DELTA_MANIF_HISTORY_COUNT =
            "select count(*) as Current_History_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_manifestation_history_part where delta_ts like '"+JRBIDataLakeCountChecksSQL.currentDate()+"%'\n";


    public  static String GET_JRBI_COUNT_SUM_DELTA_PERSON_AND_PERSON_HISTORY =
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

    public static String GET_JRBI_PERSON_LATEST_COUNT =
            "select count(*) as Target_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_person\n";


    public static String GET_JRBI_WORK_LATEST_COUNT =
            "select count(*) as Target_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_work\n";

    public  static String GET_JRBI_COUNT_SUM_DELTA_WORK_AND_WORK_HISTORY =
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

    public  static String GET_JRBI_COUNT_SUM_DELTA_MANIF_AND_MANIF_EXCLUDE =
         "select count(*) as source_count from \n" +
                 "(select a.epr, a.record_type, a.journal_prod_site_code, \n" +
                 "a.journal_issue_trim_size, a.war_reference, a.transform_ts, a.last_updated_date, a.delete_flag\n" +
                 "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_manifestation_excl_delta \n" +
                 "as a union all select b.epr, b.record_type, b.journal_prod_site_code, \n" +
                 "b.journal_issue_trim_size, b.war_reference,b.transform_ts,null as col11, null as col12 \n" +
                 "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation as b)\n ";

    public static String GET_JRBI_MANIF_LATEST_COUNT =
            "select count(*) as Target_Count from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_manifestation\n";

}




