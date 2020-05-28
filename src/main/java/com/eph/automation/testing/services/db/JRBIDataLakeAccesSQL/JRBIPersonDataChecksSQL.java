package com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class JRBIPersonDataChecksSQL {

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


    public static String GET_EPR_IDS_PERSON_FULLLOAD =
            "SELECT epr as EPR FROM (SELECT DISTINCT \n" +
                    "COALESCE(cr1.epr, cr2.epr) epr\n" +
                    ", 'JRBI Person Extended' record_type, j.role_code role_code\n" +
                    ", COALESCE(cr1.epr, cr2.epr)||j.role_code as u_key\n" +
                    ", j.role_description role_description, p.given_name given_name\n" +
                    ", p.family_name family_name, p.peoplehub_id peoplehub_id\n" +
                    ", j.email email \n" +
                    "FROM ((("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_person_unpivot_v j \n" +
                    "LEFT JOIN product_database_sit.gd_person p ON (j.email = p.email)) \n" +
                    "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr1 ON ((((j.issn = cr1.identifier) AND (cr1.identifier_type = 'ISSN')) AND (cr1.record_level = 'n')) AND (cr1.record_level = 'Work')))\n" +
                    "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON (((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Work'))))where epr is not NULL\n" +
                    " order by rand() limit %s\n";

    public static String GET_PERSON_RECORDS_FULL_LOAD =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",role_code as ROLE_CODE" +
                    ",u_key as U_KEY" +
                    ",role_description as ROLE_DESCRIPTION" +
                    ",given_name as GIVEN_NAME" +
                    ",family_name as FAMILY_NAME" +
                    ",peoplehub_id as PEOPLEHUB_ID" +
                    ",email as EMAIL" +
                    " FROM (SELECT DISTINCT \n" +
                    "COALESCE(cr1.epr, cr2.epr) epr\n" +
                    ", 'JRBI Person Extended' record_type, j.role_code role_code\n" +
                    ", COALESCE(cr1.epr, cr2.epr)||j.role_code as u_key\n" +
                    ", j.role_description role_description, p.given_name given_name\n" +
                    ", p.family_name family_name, p.peoplehub_id peoplehub_id\n" +
                    ", j.email email\n" +
                    "FROM ((("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_person_unpivot_v j \n" +
                    "LEFT JOIN product_database_sit.gd_person p ON (j.email = p.email)) \n" +
                    "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr1 ON ((((j.issn = cr1.identifier) AND (cr1.identifier_type = 'ISSN')) AND (cr1.record_level = 'n')) AND (cr1.record_level = 'Work')))\n" +
                     "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON (((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Work'))))\n" +
                    "where EPR in ('%s')";

    public static String GET_CURRENT_PERSON_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",role_code as ROLE_CODE" +
                    ",u_key as U_KEY" +
                    ",role_description as ROLE_DESCRIPTION" +
                    ",given_name as GIVEN_NAME" +
                    ",family_name as FAMILY_NAME" +
                    ",peoplehub_id as PEOPLEHUB_ID" +
                    ",email as EMAIL" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person where EPR in ('%s')";



}




