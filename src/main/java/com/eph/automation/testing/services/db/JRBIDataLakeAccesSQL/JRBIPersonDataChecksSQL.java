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

    public static String GET_DELTA_PERSON_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person order by rand() limit %s\n";

    public static String GET_DELTA_PERSON_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",role_code as ROLE_CODE" +
                    ",u_key as U_KEY" +
                    ",role_description as ROLE_DESCRIPTION" +
                    ",given_name as GIVEN_NAME" +
                    ",family_name as FAMILY_NAME" +
                    ",peoplehub_id as PEOPLEHUB_ID" +
                    ",email as EMAIL" +
                    ",type as TYPE" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person where EPR in ('%s')";

    public static String GET_DELTA_PERSON_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",role_code as ROLE_CODE" +
                    ",u_key as U_KEY" +
                    ",role_description as ROLE_DESCRIPTION" +
                    ",given_name as GIVEN_NAME" +
                    ",family_name as FAMILY_NAME" +
                    ",peoplehub_id as PEOPLEHUB_ID" +
                    ",email as EMAIL" +
                    ",type as TYPE" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_person_history_part where EPR in ('%s') AND " +
                    "delta_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\'";



    public static String GET_CURRENT_PERSON_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person order by rand() limit %s\n";

    public static String GET_PREVIOUS_PERSON_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person order by rand() limit %s\n";

    public static String GET_PREVIOUS_PERSON_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",role_code as ROLE_CODE" +
                    ",u_key as U_KEY" +
                    ",role_description as ROLE_DESCRIPTION" +
                    ",given_name as GIVEN_NAME" +
                    ",family_name as FAMILY_NAME" +
                    ",peoplehub_id as PEOPLEHUB_ID" +
                    ",email as EMAIL" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person where EPR in ('%s')";


    public static String GET_CURRENT_PERSON_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",role_code as ROLE_CODE" +
                    ",u_key as U_KEY" +
                    ",role_description as ROLE_DESCRIPTION" +
                    ",given_name as GIVEN_NAME" +
                    ",family_name as FAMILY_NAME" +
                    ",peoplehub_id as PEOPLEHUB_ID" +
                    ",email as EMAIL" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part where EPR in ('%s') AND " +
                    "transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\'";

    public static String GET_PREVIOUS_PERSON_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",role_code as ROLE_CODE" +
                    ",u_key as U_KEY" +
                    ",role_description as ROLE_DESCRIPTION" +
                    ",given_name as GIVEN_NAME" +
                    ",family_name as FAMILY_NAME" +
                    ",peoplehub_id as PEOPLEHUB_ID" +
                    ",email as EMAIL" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part where EPR in ('%s') AND " +
                    "transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.previousDate()+"%%\'";

    public static String GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_PERSON =
            "select epr as EPR from \n" +
                    "(select A.epr, A.record_type, A.role_code, A.u_key\n" +
                    ", A.role_description, A.given_name, A.family_name, A.peoplehub_id\n" +
                    ", A.email, A.last_updated_date, A.delete_flag, A.transform_ts \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person B on A.u_key  = B.u_key\n" +
                    "where B.u_key is null and A.transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\')\n" +
                    " order by rand() limit %s\n";

    public static String GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_PERSON =
            "select epr as EPR, record_type as RECORDS_TYPE,\n" +
                    "role_code as ROLE_CODE, u_key as U_KEY,\n" +
                    "role_description as ROLE_DESCRIPTION,\n" +
                    "given_name as GIVEN_NAME,\n" +
                    "family_name as FAMILY_NAME,\n" +
                    "peoplehub_id as PEOPLEHUB_ID,\n" +
                    "email as EMAIL,\n" +
                    "last_updated_date as LAST_UPDATED_DATE,\n" +
                    "delete_flag as DELETE_FLAG\n" +
                    "from \n" +
                    "(select A.epr, A.record_type, A.role_code, A.u_key\n" +
                    ", A.role_description, A.given_name, A.family_name, A.peoplehub_id\n" +
                    ", A.email, A.last_updated_date, A.delete_flag, A.transform_ts \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person B on A.u_key  = B.u_key\n" +
                    "where B.u_key is null and A.transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\' AND A.epr in ('%s'))\n";


    public static String GET_RECORDS_FROM_PERSON_EXCLUDE =
            "select epr as EPR, record_type as RECORDS_TYPE,\n" +
                    "role_code as ROLE_CODE, u_key as U_KEY,\n" +
                    "role_description as ROLE_DESCRIPTION,\n" +
                    "given_name as GIVEN_NAME,\n" +
                    "family_name as FAMILY_NAME,\n" +
                    "peoplehub_id as PEOPLEHUB_ID,\n" +
                    "email as EMAIL,\n" +
                    "last_updated_date as LAST_UPDATED_DATE,\n" +
                    "delete_flag as DELETE_FLAG\n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_person_excl_delta \n"+
                    "where EPR in ('%s')\n";


}




