package com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL;

import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.GetJRBIDLDBUser;

public class JRBIDataLakeCountChecksSQL {

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
                    "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON ((((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Manifestation')) AND (cr2.manifestation_type = 'JPR'))))\n";


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
}




