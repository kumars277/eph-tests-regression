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
                    "INNER JOIN "+GetJRBIDLDBUser.getProductGDdb()+".gd_person p ON (j.email = p.email)) \n" +
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
                    "INNER JOIN "+GetJRBIDLDBUser.getProductGDdb()+".gd_person p ON (j.email = p.email)) \n" +
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
                    //"delta_ts like \'%%"+JRBIDataLakeCountChecksSQL.previousDate()+"%%\'";
                    "delta_ts=(select max(delta_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_person_history_part\n " +
                    "where delta_ts < (select max(delta_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_person_history_part))\n";



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
                    //"transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\' " +
                    "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part)\n " +
                    "and delete_flag=false";

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
                    //"transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.previousDate()+"%%\'";
                    "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part\n " +
                    "where transform_ts < (select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part))\n";

    public static String GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_PERSON =
            "select epr as EPR from \n" +
                    "(select A.epr, A.record_type, A.role_code, A.u_key\n" +
                    ", A.role_description, A.given_name, A.family_name, A.peoplehub_id\n" +
                    ", A.email, A.last_updated_date, A.delete_flag, A.transform_ts \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person B on A.u_key  = B.u_key\n" +
                    "where B.u_key is null and " +
                   // "A.transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\')\n" +
                    "A.transform_ts=(select max(A.transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A))\n " +
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
                    "where B.u_key is null and " +
                   // "A.transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\' " +
                    "A.transform_ts=(select max(A.transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A))\n " +
                    "AND A.epr in ('%s'))\n";


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


    public static String GET_EPR_FOR_PERSON_LATEST =
            "select epr as EPR from \n" +
                    "(select a.epr, a.record_type, a.role_code, \n" +
                    "a.u_key, a.role_description, a.given_name, a.family_name, \n" +
                    "a.peoplehub_id, a.email, a.transform_ts, a.delete_flag, \n" +
                    "a.last_updated_date \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_person_excl_delta \n" +
                    "as a union all select b.epr, b.record_type, b.role_code, b.u_key, \n" +
                    "b.role_description, b.given_name, b.family_name, b.peoplehub_id, \n" +
                    "b.email, b.transform_ts, null as col11, null as col12 \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person as b)\n "+
                    "order by rand() limit %s\n";

    public  static String GET_JRBI_REC_SUM_DELTA_PERSON_AND_PERSON_HISTORY =
            "select epr as EPR, record_type as RECORDS_TYPE,\n" +
                    "role_code as ROLE_CODE, u_key as U_KEY,\n" +
                    "role_description as ROLE_DESCRIPTION,\n" +
                    "given_name as GIVEN_NAME,\n" +
                    "family_name as FAMILY_NAME,\n" +
                    "peoplehub_id as PEOPLEHUB_ID,\n" +
                    "email as EMAIL,\n" +
                    "last_updated_date as LAST_UPDATED_DATE,\n" +
                    "delete_flag as DELETE_FLAG\n" +
                    " from(select a.epr, a.record_type, a.role_code, \n" +
                    "a.u_key, a.role_description, a.given_name, a.family_name, \n" +
                    "a.peoplehub_id, a.email, a.transform_ts, a.delete_flag, \n" +
                    "a.last_updated_date \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_person_excl_delta \n" +
                    "as a union all select b.epr, b.record_type, b.role_code, b.u_key, \n" +
                    "b.role_description, b.given_name, b.family_name, b.peoplehub_id, \n" +
                    "b.email, b.transform_ts, null as col11, null as col12 \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person as b)\n "+
                    "where EPR in ('%s')\n";

    public static String GET_JRBI_PERSON_LATEST_RECORDS =
            "select epr as EPR, record_type as RECORDS_TYPE,\n" +
                    "role_code as ROLE_CODE, u_key as U_KEY,\n" +
                    "role_description as ROLE_DESCRIPTION,\n" +
                    "given_name as GIVEN_NAME,\n" +
                    "family_name as FAMILY_NAME,\n" +
                    "peoplehub_id as PEOPLEHUB_ID,\n" +
                    "email as EMAIL,\n" +
                    "last_updated_date as LAST_UPDATED_DATE,\n" +
                    "delete_flag as DELETE_FLAG\n" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_person\n" +
                    " where EPR in ('%s')\n";


    public static String GET_RANDOM_EPR_DELTA_PERSON =
            "select epr as EPR" +
                    " from (\n" +
                    "--new\n" +
                    "select c.epr, c.record_type, c.role_code , c.role_description ,c.given_name , c.family_name , c.peoplehub_id , c.email,'I' as delta_mode FROM jrbi_staging_sit.jrbi_transform_current_person c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person p  on c.u_key = p.u_key\n" +
                    "where p.u_key is null\n" +
                    "union all\n" +
                    "-- deleted\n" +
                    "select c.epr, c.record_type, c.role_code , c.role_description ,c.given_name , c.family_name , c.peoplehub_id , c.email, 'D' as delta_mode FROM  jrbi_staging_sit.jrbi_transform_previous_person  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person p  on c.u_key = p.u_key\n" +
                    "where p.u_key is null\n" +
                    "union all\n" +
                    "--changed\n" +
                    "select c.epr, c.record_type, c.role_code , c.role_description ,c.given_name , c.family_name , c.peoplehub_id , c.email, 'C' as delta_mode\n" +
                    "FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person p  on c.u_key = p.u_key\n" +
                    "where (c.epr !=(p.epr) or\n" +
                    "c.record_type != (p.record_type) or \n" +
                    "c.role_code !=  (p.role_code) or\n" +
                    "c.role_description !=  (p.role_description) or\n" +
                    "c.given_name !=  (p.given_name) or\n" +
                    "c. family_name !=  (p.family_name) or\n" +
                    "c.peoplehub_id !=  (p.peoplehub_id) or\n" +
                    "c.email != (p.email))) order by rand() limit %s\n";


    public static String GET_DIFF_REC_PREVIOUS_CURRENT_PREVIOUS_PERSON =
            "select epr as EPR\n" +
                    ",record_type as RECORD_TYPE\n" +
                    ",role_code as ROLE_CODE\n" +
                    ",u_key as U_KEY\n" +
                    ",role_description as ROLE_DESCRIPTION\n" +
                    ",given_name as GIVEN_NAME\n" +
                    ",family_name as FAMILY_NAME\n" +
                    ",peoplehub_id as PEOPLEHUB_ID\n" +
                    ",email as EMAIL\n" +
                    ",type as TYPE\n" +
                    ",delta_mode as DELTA_MODE from (\n" +
                    "--new\n" +
                    "select c.epr, c.record_type, c.role_code , c.u_key , c.role_description ,c.given_name , c.family_name , c.peoplehub_id , c.email,'NEW' as type,'I' as delta_mode FROM "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person p  on c.u_key = p.u_key\n" +
                    "where p.u_key is null\n" +
                    "union all\n" +
                    "-- deleted\n" +
                    "select c.epr, c.record_type, c.role_code , c. u_key ,c.role_description ,c.given_name , c.family_name , c.peoplehub_id , c.email,'OLD' as type,'D' as delta_mode FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person p  on c.u_key = p.u_key\n" +
                    "where p.u_key is null\n" +
                    "union all\n" +
                    "--changed\n" +
                    "select c.epr, c.record_type, c.role_code , c.u_key , c.role_description ,c.given_name , c.family_name , c.peoplehub_id , c.email,'NEW' as type,'C' as delta_mode\n" +
                    "FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person p  on c.u_key = p.u_key\n" +
                    "where (c.epr !=(p.epr) or\n" +
                    "c.record_type != (p.record_type) or\n" +
                    "c.role_code !=  (p.role_code) or\n" +
                    "c.u_key != (p.u_key ) or\n" +
                    "c.role_description !=  (p.role_description) or\n" +
                    "c.given_name !=  (p.given_name) or\n" +
                    "c. family_name !=  (p.family_name) or\n" +
                    "c.peoplehub_id !=  (p.peoplehub_id) or\n" +
                    "c.email !=  (p.email)))\n" +
                    " where EPR in ('%s')\n";

}




