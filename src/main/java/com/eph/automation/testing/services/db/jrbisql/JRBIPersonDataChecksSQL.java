package com.eph.automation.testing.services.db.jrbisql;

public class JRBIPersonDataChecksSQL {
    private JRBIPersonDataChecksSQL() {throw new IllegalStateException("Utility class");}

    public static final String GET_EPR_IDS_PERSON_FULLLOAD =
       "SELECT epr as EPR from(\n" +
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
               "AND (COALESCE(p.peoplehub_id, jel.peoplehub_id) IS NOT NULL))) order by rand() limit %s\n";

    public static final String GET_PERSON_RECORDS_FULL_LOAD =
                    "select epr as EPR" +
                    ",record_type as recordType" +
                    ",role_code as roleCode" +
                    ",u_key as uKey" +
                    ",role_description as roleDescription" +
                    ",given_name as givenName" +
                    ",family_name as familyName" +
                    ",peoplehub_id as peopleHubId" +
                    ",email as email" +
                    " from(\n" +
                    " SELECT DISTINCT\n" +
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
                    "AND (COALESCE(p.peoplehub_id, jel.peoplehub_id) IS NOT NULL))) where EPR in ('%s')\n";

    public static final String GET_CURRENT_PERSON_RECORDS =
            "select epr as EPR" +
                    ",record_type as recordType" +
                    ",role_code as roleCode" +
                    ",u_key as uKey" +
                    ",role_description as roleDescription" +
                    ",given_name as givenName" +
                    ",family_name as familyName" +
                    ",peoplehub_id as peopleHubId" +
                    ",email as email" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person where EPR in ('%s')";

    public static final String GET_DELTA_PERSON_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person order by rand() limit %s\n";

    public static final String GET_DELTA_PERSON_RECORDS =
            "select epr as EPR" +
                    ",record_type as recordType" +
                    ",role_code as roleCode" +
                    ",u_key as uKey" +
                    ",role_description as roleDescription" +
                    ",given_name as givenName" +
                    ",family_name as familyName" +
                    ",peoplehub_id as peopleHubId" +
                    ",email as email" +
                    ",type as type" +
                    ",delta_mode as deltaMode" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person where EPR in ('%s')";


    public static final String GET_CURRENT_PERSON_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person order by rand() limit %s\n";

    public static final String GET_PREVIOUS_PERSON_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_person order by rand() limit %s\n";


    public static final String GET_CURRENT_PERSON_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as recordType" +
                    ",role_code as roleCode" +
                    ",u_key as uKey" +
                    ",role_description as roleDescription" +
                    ",given_name as givenName" +
                    ",family_name as familyName" +
                    ",peoplehub_id as peopleHubId" +
                    ",email as email" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part where EPR in ('%s') AND " +
                    //"transform_ts like \'%%"+bcsEtlCoreCountChecksSql.currentDate()+"%%\' " +
                    "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part)\n " +
                    "and delete_flag=false";

    public static final String GET_PREVIOUS_PERSON_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as recordType" +
                    ",role_code as roleCode" +
                    ",u_key as uKey" +
                    ",role_description as roleDescription" +
                    ",given_name as givenName" +
                    ",family_name as familyName" +
                    ",peoplehub_id as peopleHubId" +
                    ",email as email" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part where EPR in ('%s') AND " +
                    "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part\n " +
                    "where transform_ts < (select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part))\n";

    public static final String GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_PERSON =
            "select epr as EPR from \n" +
                    "(select A.epr, A.record_type, A.role_code, A.u_key\n" +
                    ", A.role_description, A.given_name, A.family_name, A.peoplehub_id\n" +
                    ", A.email, A.last_updated_date, A.delete_flag, A.transform_ts \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person B on A.u_key  = B.u_key\n" +
                    "where B.u_key is null and " +
                   // "A.transform_ts like \'%%"+bcsEtlCoreCountChecksSql.currentDate()+"%%\')\n" +
                    "A.transform_ts=(select max(A.transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A))\n " +
                    " order by rand() limit %s\n";

    public static final String GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_PERSON =
            "select epr as EPR, record_type as RECORDS_TYPE,\n" +
                    "role_code as roleCode, u_key as uKey,\n" +
                    "role_description as roleDescription,\n" +
                    "given_name as givenName,\n" +
                    "family_name as familyName,\n" +
                    "peoplehub_id as peopleHubId,\n" +
                    "email as email,\n" +
                    "last_updated_date as lastUpdatedDate,\n" +
                    "delete_flag as deleteFlag\n" +
                    "from \n" +
                    "(select A.epr, A.record_type, A.role_code, A.u_key\n" +
                    ", A.role_description, A.given_name, A.family_name, A.peoplehub_id\n" +
                    ", A.email, A.last_updated_date, A.delete_flag, A.transform_ts \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_person B on A.u_key  = B.u_key\n" +
                    "where B.u_key is null and " +
                    "A.transform_ts=(select max(A.transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_person_history_part A)\n " +
                    "AND A.epr in ('%s'))\n";


    public static final String GET_RECORDS_FROM_PERSON_EXCLUDE =
            "select epr as EPR, record_type as RECORDS_TYPE,\n" +
                    "role_code as roleCode, u_key as uKey,\n" +
                    "role_description as roleDescription,\n" +
                    "given_name as givenName,\n" +
                    "family_name as familyName,\n" +
                    "peoplehub_id as peopleHubId,\n" +
                    "email as email,\n" +
                    "last_updated_date as lastUpdatedDate,\n" +
                    "delete_flag as deleteFlag\n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_person_excl_delta \n"+
                    "where EPR in ('%s')\n";


    public static final String GET_EPR_FOR_PERSON_LATEST =
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

    public  static final String GET_JRBI_REC_SUM_DELTA_PERSON_AND_PERSON_EXCL =
            "select epr as EPR, record_type as RECORDS_TYPE,\n" +
                    "role_code as roleCode, u_key as uKey,\n" +
                    "role_description as roleDescription,\n" +
                    "given_name as givenName,\n" +
                    "family_name as familyName,\n" +
                    "peoplehub_id as peopleHubId,\n" +
                    "email as email,\n" +
                    "last_updated_date as lastUpdatedDate,\n" +
                    "delete_flag as deleteFlag\n" +
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

    public static final String GET_JRBI_PERSON_LATEST_RECORDS =
            "select epr as EPR, record_type as RECORDS_TYPE,\n" +
                    "role_code as roleCode, u_key as uKey,\n" +
                    "role_description as roleDescription,\n" +
                    "given_name as givenName,\n" +
                    "family_name as familyName,\n" +
                    "peoplehub_id as peopleHubId,\n" +
                    "email as email,\n" +
                    "last_updated_date as lastUpdatedDate,\n" +
                    "delete_flag as deleteFlag\n" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_person\n" +
                    " where EPR in ('%s') order by role_code,peoplehub_id\n";



    public static final String GET_DIFF_REC_PREVIOUS_CURRENT_PREVIOUS_PERSON =
            "select epr as EPR\n" +
                    ",record_type as recordType\n" +
                    ",role_code as roleCode\n" +
                    ",u_key as uKey\n" +
                    ",role_description as roleDescription\n" +
                    ",given_name as givenName\n" +
                    ",family_name as familyName\n" +
                    ",peoplehub_id as peopleHubId\n" +
                    ",email as email\n" +
                    ",type as type\n" +
                    ",delta_mode as deltaMode from (\n" +
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




