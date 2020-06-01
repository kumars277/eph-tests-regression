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
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part where EPR in ('%s') AND " +
                    "transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\'";

    public static String GET_CURRENT_WORK_EPR_ID=
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work order by rand() limit %s\n";


    public static String GET_PREVIOUS_WORK_EPR_ID=
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_work order by rand() limit %s\n";

    public static String GET_DELTA_WORK_EPR_ID=
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_work order by rand() limit %s\n";


    public static String GET_DELTA_WORK_CURRENT_RECORDS =
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
            ",delta_mode as DELTA_MODE" +
            ",type as TYPE" +
            " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_work where EPR in ('%s')";

    public static String GET_DELTA_WORK_HISTORY_RECORDS =
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
                    ",delta_mode as DELTA_MODE" +
                    ",type as TYPE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_work_history_part where EPR in ('%s') AND \n" +
                    "delta_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\'";


    public static String GET_PREVIOUS_WORK_RECORDS =
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
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_work where EPR in ('%s')";

    public static String GET_PREVIOUS_WORK_HISTORY_RECORDS =
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
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part where EPR in ('%s') AND " +
                    "transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.previousDate()+"%%\'";

    public static String GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_WORK =
            "select epr as EPR from \n" +
                    "(select A.epr, A.record_type, A.primary_site_system\n" +
                    ", A.primary_site_acronym, A.primary_site_support_level, A.fulfilment_system\n" +
                    ", A.fulfilment_journal_acronym, A.issue_prod_type_code, A.catalogue_volumes_qty\n" +
                    ", A.catalogue_issues_qty, A.catalogue_volume_from, A.catalogue_volume_to, A.rf_issues_qty\n" +
                    ", A.rf_total_pages_qty, A.rf_fvi, A.rf_lvi, A.business_unit_desc, A.last_updated_date\n" +
                    ", A.delete_flag, A.transform_ts from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_work B on A.epr  = B.epr \n" +
                    "where B.epr is null and A.transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\')\n" +
                    " order by rand() limit %s\n";

    public static String GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_WORK =
            "select epr as EPR \n" +
                    ",record_type as RECORD_TYPE\n" +
                    ",primary_site_system as PRIMARY_SITE_SYSTEM\n" +
                    ",primary_site_acronym as PRIMARY_SITE_ACRONYM\n" +
                    ",primary_site_support_level as PRIMARY_SITE_SUPPORT_LEVEL\n" +
                    ",fulfilment_system as FULFILMENT_SYSTEM\n" +
                    ",fulfilment_journal_acronym as FULFILMENT_JOURNAL_ACRONYM\n" +
                    ",issue_prod_type_code as ISSUE_PROD_TYPE_CODE\n" +
                    ",catalogue_volumes_qty as CATALOGUE_VOLUME_QTY\n" +
                    ",catalogue_issues_qty as CATALOGUE_ISSUES_QTY\n" +
                    ",catalogue_volume_from as CATALOGUE_VOLUME_FROM\n" +
                    ",catalogue_volume_to as CATALOGUE_VOLUME_TO\n" +
                    ",rf_issues_qty as RF_ISSUES_QTY\n" +
                    ",rf_total_pages_qty as RF_TOTAL_PAGES_QTY\n" +
                    ",rf_fvi as RF_FVI\n" +
                    ",rf_lvi as RF_LVI\n" +
                    ",business_unit_desc as BUSINESS_UNIT_DESC\n" +
                    ",last_updated_date as LAST_UPDATED_DATE\n" +
                    ",delete_flag as DELETE_FLAG\n" +
                    "from \n" +
                    "(select A.epr, A.record_type, A.primary_site_system\n" +
                    ", A.primary_site_acronym, A.primary_site_support_level, A.fulfilment_system\n" +
                    ", A.fulfilment_journal_acronym, A.issue_prod_type_code, A.catalogue_volumes_qty\n" +
                    ", A.catalogue_issues_qty, A.catalogue_volume_from, A.catalogue_volume_to, A.rf_issues_qty\n" +
                    ", A.rf_total_pages_qty, A.rf_fvi, A.rf_lvi, A.business_unit_desc, A.last_updated_date\n" +
                    ", A.delete_flag, A.transform_ts from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_work B on A.epr  = B.epr \n" +
                    "where B.epr is null and A.transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\' AND A.epr in ('%s'))\n";

    public static String GET_RECORDS_FROM_WORK_EXCLUDE =
            "select epr as EPR \n" +
                    ",record_type as RECORD_TYPE\n" +
                    ",primary_site_system as PRIMARY_SITE_SYSTEM\n" +
                    ",primary_site_acronym as PRIMARY_SITE_ACRONYM\n" +
                    ",primary_site_support_level as PRIMARY_SITE_SUPPORT_LEVEL\n" +
                    ",fulfilment_system as FULFILMENT_SYSTEM\n" +
                    ",fulfilment_journal_acronym as FULFILMENT_JOURNAL_ACRONYM\n" +
                    ",issue_prod_type_code as ISSUE_PROD_TYPE_CODE\n" +
                    ",catalogue_volumes_qty as CATALOGUE_VOLUME_QTY\n" +
                    ",catalogue_issues_qty as CATALOGUE_ISSUES_QTY\n" +
                    ",catalogue_volume_from as CATALOGUE_VOLUME_FROM\n" +
                    ",catalogue_volume_to as CATALOGUE_VOLUME_TO\n" +
                    ",rf_issues_qty as RF_ISSUES_QTY\n" +
                    ",rf_total_pages_qty as RF_TOTAL_PAGES_QTY\n" +
                    ",rf_fvi as RF_FVI\n" +
                    ",rf_lvi as RF_LVI\n" +
                    ",business_unit_desc as BUSINESS_UNIT_DESC\n" +
                    ",last_updated_date as LAST_UPDATED_DATE\n" +
                    ",delete_flag as DELETE_FLAG \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_work_excl_delta \n" +
                    "where EPR in ('%s')\n";

    public static String GET_EPR_LATEST_WORK =
            "select epr as EPR from \n" +
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
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_work as b)\n" +
                    " order by rand() limit %s\n";

    public static String GET_JRBI_REC_SUM_DELTA_WORK_AND_WORK_HISTORY =
            "select epr as EPR \n" +
                    ",record_type as RECORD_TYPE\n" +
                    ",primary_site_system as PRIMARY_SITE_SYSTEM\n" +
                    ",primary_site_acronym as PRIMARY_SITE_ACRONYM\n" +
                    ",primary_site_support_level as PRIMARY_SITE_SUPPORT_LEVEL\n" +
                    ",fulfilment_system as FULFILMENT_SYSTEM\n" +
                    ",fulfilment_journal_acronym as FULFILMENT_JOURNAL_ACRONYM\n" +
                    ",issue_prod_type_code as ISSUE_PROD_TYPE_CODE\n" +
                    ",catalogue_volumes_qty as CATALOGUE_VOLUME_QTY\n" +
                    ",catalogue_issues_qty as CATALOGUE_ISSUES_QTY\n" +
                    ",catalogue_volume_from as CATALOGUE_VOLUME_FROM\n" +
                    ",catalogue_volume_to as CATALOGUE_VOLUME_TO\n" +
                    ",rf_issues_qty as RF_ISSUES_QTY\n" +
                    ",rf_total_pages_qty as RF_TOTAL_PAGES_QTY\n" +
                    ",rf_fvi as RF_FVI\n" +
                    ",rf_lvi as RF_LVI\n" +
                    ",business_unit_desc as BUSINESS_UNIT_DESC\n" +
                    ",delete_flag as DELETE_FLAG\n" +
                    ",last_updated_date as LAST_UPDATED_DATE\n" +
                    " from(select a.epr, a.record_type, a.primary_site_system, \n" +
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
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_work as b)\n" +
                    " where EPR in ('%s')\n";

    public static String GET_JRBI_WORK_LATEST_RECORDS =
            "select epr as EPR \n" +
                    ",record_type as RECORD_TYPE\n" +
                    ",primary_site_system as PRIMARY_SITE_SYSTEM\n" +
                    ",primary_site_acronym as PRIMARY_SITE_ACRONYM\n" +
                    ",primary_site_support_level as PRIMARY_SITE_SUPPORT_LEVEL\n" +
                    ",fulfilment_system as FULFILMENT_SYSTEM\n" +
                    ",fulfilment_journal_acronym as FULFILMENT_JOURNAL_ACRONYM\n" +
                    ",issue_prod_type_code as ISSUE_PROD_TYPE_CODE\n" +
                    ",catalogue_volumes_qty as CATALOGUE_VOLUME_QTY\n" +
                    ",catalogue_issues_qty as CATALOGUE_ISSUES_QTY\n" +
                    ",catalogue_volume_from as CATALOGUE_VOLUME_FROM\n" +
                    ",catalogue_volume_to as CATALOGUE_VOLUME_TO\n" +
                    ",rf_issues_qty as RF_ISSUES_QTY\n" +
                    ",rf_total_pages_qty as RF_TOTAL_PAGES_QTY\n" +
                    ",rf_fvi as RF_FVI\n" +
                    ",rf_lvi as RF_LVI\n" +
                    ",business_unit_desc as BUSINESS_UNIT_DESC\n" +
                    ",delete_flag as DELETE_FLAG\n" +
                    ",last_updated_date as LAST_UPDATED_DATE\n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_work\n" +
                    "where EPR in ('%s')\n";

    public static String GET_RANDOM_DELTA_EPR_WORK =
            "select epr as EPR from (\n" +
                    "--inserted\n" +
                    "select c.epr,c.record_type,c.work_type, c.primary_site_system,c.primary_site_acronym,c.primary_site_support_level,\n" +
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
                    "select c.epr,c.record_type,c.work_type, c.primary_site_system,c.primary_site_acronym,c.primary_site_support_level,\n" +
                    "c.fulfilment_system,c.fulfilment_journal_acronym,c.issue_prod_type_code,c.catalogue_volumes_qty,\n" +
                    "c.catalogue_issues_qty,c.catalogue_volume_from,c.catalogue_volume_to,\n" +
                    "c.rf_issues_qty,c.rf_total_pages_qty,c.rf_fvi,c.rf_lvi,c.business_unit_desc, 'C' as delta_mode\n" +
                    "FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_work  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work p  on c.epr = p.epr\n" +
                    "where (c.record_type != (p.record_type) or \n" +
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
                    "c.business_unit_desc !=  (p.business_unit_desc)))\n" +
                    " order by rand() limit %s\n";


    public static String GET_DIFF_REC_PREVIOUS_CURRENT_WORK =
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
                    ",delta_mode as DELTA_MODE" +
                    " from (\n" +
                    "--inserted\n" +
                    "select c.epr,c.record_type,c.work_type, c.primary_site_system,c.primary_site_acronym,c.primary_site_support_level,\n" +
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
                    "select c.epr,c.record_type,c.work_type, c.primary_site_system,c.primary_site_acronym,c.primary_site_support_level,\n" +
                    "c.fulfilment_system,c.fulfilment_journal_acronym,c.issue_prod_type_code,c.catalogue_volumes_qty,\n" +
                    "c.catalogue_issues_qty,c.catalogue_volume_from,c.catalogue_volume_to,\n" +
                    "c.rf_issues_qty,c.rf_total_pages_qty,c.rf_fvi,c.rf_lvi,c.business_unit_desc, 'C' as delta_mode\n" +
                    "FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_work  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_work p  on c.epr = p.epr\n" +
                    "where (c.record_type != (p.record_type) or \n" +
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
                    "c.business_unit_desc !=  (p.business_unit_desc)))\n" +
                    "where EPR in ('%s')\n";
}
