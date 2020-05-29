package com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class JRBIManifestationDataChecksSQL {

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


    public static String GET_EPR_IDS_MANIF_FULLLOAD =
            "select epr as EPR from(SELECT DISTINCT\n" +
            "  COALESCE(cr1.epr, cr2.epr) epr\n" +
            ", 'JRBI Manifestation' record_type\n" +
            ", j.journal_prod_site_code journal_prod_site_code\n" +
            ", j.journal_issue_trim_size journal_issue_trim_size\n" +
            ", j.war_reference war_reference\n" +
            "FROM\n" +
            "  (("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full j\n" +
            "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr1 ON ((((j.issn = cr1.identifier) AND (cr1.identifier_type = 'ISSN')) AND (cr1.record_level = 'Manifestation')) AND (cr1.manifestation_type = 'JPR')))\n" +
            "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON ((((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Manifestation')) AND (cr2.manifestation_type = 'JPR')))) where epr is not NULL\n"+
                    " order by rand() limit %s\n";

    public static String GET_MANIF_RECORDS_FULL_LOAD =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    " from(SELECT DISTINCT\n" +
                    " COALESCE(cr1.epr, cr2.epr) epr\n" +
                    ", 'JRBI Manifestation' record_type\n" +
                    ", j.journal_prod_site_code journal_prod_site_code\n" +
                    ", j.journal_issue_trim_size journal_issue_trim_size\n" +
                    ", j.war_reference war_reference\n" +
                    " FROM\n" +
                    " (("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full j\n" +
                    " LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr1 ON ((((j.issn = cr1.identifier) AND (cr1.identifier_type = 'ISSN')) AND (cr1.record_level = 'Manifestation')) AND (cr1.manifestation_type = 'JPR')))\n" +
                    " LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON ((((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Manifestation')) AND (cr2.manifestation_type = 'JPR'))))\n" +
                    "where EPR in ('%s')";

    public static String GET_CURRENT_MANIF_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation where EPR in ('%s')";


    public static String GET_CURRENT_MANIF_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation order by rand() limit %s\n";


    public static String GET_CURRENT_MANIF_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part where EPR in ('%s') AND " +
                    "transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\'";


    public static String GET_PREVIOUS_MANIF_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation order by rand() limit %s\n";

    public static String GET_DELTA_MANIF_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation order by rand() limit %s\n";

    public static String GET_DELTA_MANIF_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    "type as TYPE" +
                    "delta_mode as DELTA_MODE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation where EPR in ('%s')";

    public static String GET_DELTA_MANIF_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    "type as TYPE" +
                    "delta_mode as DELTA_MODE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_manifestation_history_part where EPR in ('%s') AND " +
                    "delta_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\'";


    public static String GET_PREVIOUS_MANIF_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation where EPR in ('%s')";


    public static String GET_PREVIOUS_MANIF_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part where EPR in ('%s') AND " +
                    "transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.previousDate()+"%%\'";

}




