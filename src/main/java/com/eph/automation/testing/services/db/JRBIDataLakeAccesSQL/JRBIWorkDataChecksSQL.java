package com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class JRBIWorkDataChecksSQL {

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


    public static String GET_EPR_IDS_FULLLOAD =
            "select epr as EPR from(SELECT DISTINCT\n" +
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
                    "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON (((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Work')))) where epr is not NULL\n" +
                    "order by rand() limit %s\n";

    public static String GET_WORK_RECORDS_FULL_LOAD =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",primary_site_system as PRIMARY_SITE_SYSTEM" +
                    ",primary_site_acronym as PRIMARY_SITE_ACRONYM" +
                    ",primary_site_support_level as PRIMARY_SITE_SUPPORT_LEVEL" +
                    ",fulfilment_system as FULFILMENT_SYSTEM" +
                    ",fulfilment_journal_acronym as FULFILMENT_JOURNAL_ACRONYM" +
                    ",issue_prod_type_code as ISSUE_PROD_TYPE_CODE" +
                    ",catalogue_volumes_qty as CATALOGUE_VOLUME_QTY" +
                    ",catalogue_issues_qty as CATALOGUE_ISSUES_QTY" +
                    ",catalogue_volume_from as CATALOGUE_VOLUME_FROM" +
                    ",catalogue_volume_to as CATALOGUE_VOLUME_TO" +
                    ",rf_issues_qty as RF_ISSUES_QTY" +
                    ",rf_total_pages_qty as RF_TOTAL_PAGES_QTY" +
                    ",rf_fvi as RF_FVI" +
                    ",rf_lvi as RF_LVI" +
                    ",business_unit_desc as BUSINESS_UNIT_DESC" +
                    " from(SELECT DISTINCT\n" +
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
                    "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON (((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Work')))) where EPR in ('%s')";


    public static String GET_CURRENT_WORK_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",primary_site_system as PRIMARY_SITE_SYSTEM" +
                    ",primary_site_acronym as PRIMARY_SITE_ACRONYM" +
                    ",primary_site_support_level as PRIMARY_SITE_SUPPORT_LEVEL" +
                    ",fulfilment_system as FULFILMENT_SYSTEM" +
                    ",fulfilment_journal_acronym as FULFILMENT_JOURNAL_ACRONYM" +
                    ",issue_prod_type_code as ISSUE_PROD_TYPE_CODE" +
                    ",catalogue_volumes_qty as CATALOGUE_VOLUME_QTY" +
                    ",catalogue_issues_qty as CATALOGUE_ISSUES_QTY" +
                    ",catalogue_volume_from as CATALOGUE_VOLUME_FROM" +
                    ",catalogue_volume_to as CATALOGUE_VOLUME_TO" +
                    ",rf_issues_qty as RF_ISSUES_QTY" +
                    ",rf_total_pages_qty as RF_TOTAL_PAGES_QTY" +
                    ",rf_fvi as RF_FVI" +
                    ",rf_lvi as RF_LVI" +
                    ",business_unit_desc as BUSINESS_UNIT_DESC" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work where EPR in ('%s')";

    public static String GET_CURRENT_WORK_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",primary_site_system as PRIMARY_SITE_SYSTEM" +
                    ",primary_site_acronym as PRIMARY_SITE_ACRONYM" +
                    ",primary_site_support_level as PRIMARY_SITE_SUPPORT_LEVEL" +
                    ",fulfilment_system as FULFILMENT_SYSTEM" +
                    ",fulfilment_journal_acronym as FULFILMENT_JOURNAL_ACRONYM" +
                    ",issue_prod_type_code as ISSUE_PROD_TYPE_CODE" +
                    ",catalogue_volumes_qty as CATALOGUE_VOLUME_QTY" +
                    ",catalogue_issues_qty as CATALOGUE_ISSUES_QTY" +
                    ",catalogue_volume_from as CATALOGUE_VOLUME_FROM" +
                    ",catalogue_volume_to as CATALOGUE_VOLUME_TO" +
                    ",rf_issues_qty as RF_ISSUES_QTY" +
                    ",rf_total_pages_qty as RF_TOTAL_PAGES_QTY" +
                    ",rf_fvi as RF_FVI" +
                    ",rf_lvi as RF_LVI" +
                    ",business_unit_desc as BUSINESS_UNIT_DESC" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part where EPR in ('%s') AND transform_ts like \'2020-05-28%\'";



    public static String GET_CURRENT_WORK_EPR_ID=
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work order by rand() limit %s\n";






}




