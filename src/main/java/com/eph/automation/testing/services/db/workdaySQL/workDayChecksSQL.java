package com.eph.automation.testing.services.db.workdaySQL;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.sdrmsql.GetSdrmDbUser;

public class workDayChecksSQL {

    public static final String GET_WORKDAY_ORGDATA_IDS_SQL=
            "select sourceref from(\n" +
                    "SELECT\n" +
                    "  emplid sourceref\n" +
                    ", first_name given_name\n" +
                    ", last_name family_name\n" +
                    ", emplid peoplehub_id\n" +
                    ", NULLIF(lower(trim(business_email)), '') email\n" +
                    ", termination_date end_date\n" +
                    " FROM\n" +
                    " "+GetSdrmDbUser.getProdStagingDataBase()+".workday_data_orgdata\n" +
                    "WHERE (NOT (emplid LIKE 'IA%%'))\n" +
                    " order by rand() limit %s)";

    public static final String GET_WORKDAY_ORGDATA_RECS_SQL=
            "select * from(\n" +
                    "SELECT\n" +
                    "  emplid sourceref\n" +
                    ", first_name given_name\n" +
                    ", last_name family_name\n" +
                    ", emplid peoplehub_id\n" +
                    ", NULLIF(lower(trim(business_email)), '') email\n" +
                    ", termination_date end_date\n" +
                    " FROM\n" +
                    " "+GetSdrmDbUser.getProdStagingDataBase()+".workday_data_orgdata\n" +
                    "WHERE (NOT (emplid LIKE 'IA%%')))\n" +
                    " where sourceref in ('%s') order by sourceref desc";

    public static final String GET_WORKDAY_REF_RECS_SQL=
            "select * from "+GetSdrmDbUser.getProdStagingDataBase()+".workday_reference_v where sourceref in ('%s') order by sourceref desc";


    public static final String GET_WORKDAY_ORGDATA_COUNT_SQL=
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "  emplid sourceref\n" +
                    ", first_name given_name\n" +
                    ", last_name family_name\n" +
                    ", emplid peoplehub_id\n" +
                    ", NULLIF(lower(trim(business_email)), '') email\n" +
                    ", termination_date end_date\n" +
                    " FROM\n" +
                    " "+GetSdrmDbUser.getProdStagingDataBase()+".workday_data_orgdata\n" +
                    "WHERE (NOT (emplid LIKE 'IA%'))\n" +
                    ")";

    public static final String GET_WORKDAY_REF_COUNT_SQL=
            "select count(*) as Target_Count from "+GetSdrmDbUser.getProdStagingDataBase()+".workday_reference_v";


    public static final String GET_WORKDAY_INBOUND_COUNT_SQL =
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "   a . sourceref   person_source_ref \n" +
                    ",  a . given_name   person_first_name \n" +
                    ",  a . family_name   person_family_name \n" +
                    ",  a . peoplehub_id \n" +
                    ",  a . email   email_address \n" +
                    ", 'N'  dq_err \n" +
                    "FROM\n" +
                    "  ( "+GetSdrmDbUser.getProdStagingDataBase()+".workday_reference_v   a \n" +
                    "INNER JOIN  "+GetSdrmDbUser.getProdDataBase()+".gd_person   b  ON ((NULLIF( lower ( trim ( a . peoplehub_id )), '') = NULLIF( lower ( trim ( b . peoplehub_id )), '')) AND ( concat ( lower ( trim ( a . given_name )),  lower ( trim ( a . family_name )),  lower ( trim ( a . email ))) <>  concat ( lower ( trim ( b . given_name )),  lower ( trim ( b . family_name )),  lower ( trim ( b . email ))))))\n" +
                    ")";

    public static final String GET_WORKDAY_INBOUND_IDS_SQL =
            "select peoplehub_id from(\n" +
                    "SELECT\n" +
                    "   a . sourceref   person_source_ref \n" +
                    ",  a . given_name   person_first_name \n" +
                    ",  a . family_name   person_family_name \n" +
                    ",  a . peoplehub_id \n" +
                    ",  a . email   email_address \n" +
                    ", 'N'  dq_err \n" +
                    "FROM\n" +
                    "  ( "+GetSdrmDbUser.getProdStagingDataBase()+".workday_reference_v   a \n" +
                    "INNER JOIN  "+GetSdrmDbUser.getProdDataBase()+".gd_person   b  ON ((NULLIF( lower ( trim ( a . peoplehub_id )), '') = NULLIF( lower ( trim ( b . peoplehub_id )), '')) AND ( concat ( lower ( trim ( a . given_name )),  lower ( trim ( a . family_name )),  lower ( trim ( a . email ))) <>  concat ( lower ( trim ( b . given_name )),  lower ( trim ( b . family_name )),  lower ( trim ( b . email ))))))\n" +
                    ")order by peoplehub_id rand() limit %s";


    public static final String GET_WORKDAY_INBOUND_RECS_SQL =
            "select * from(\n" +
                    "SELECT\n" +
                    "   a . sourceref   person_source_ref \n" +
                    ",  a . given_name   person_first_name \n" +
                    ",  a . family_name   person_family_name \n" +
                    ",  a . peoplehub_id \n" +
                    ",  a . email   email_address \n" +
                    ", 'N'  dq_err \n" +
                    "FROM\n" +
                    "  ( "+GetSdrmDbUser.getProdStagingDataBase()+".workday_reference_v   a \n" +
                    "INNER JOIN  "+GetSdrmDbUser.getProdDataBase()+".gd_person   b  ON ((NULLIF( lower ( trim ( a . peoplehub_id )), '') = NULLIF( lower ( trim ( b . peoplehub_id )), '')) AND ( concat ( lower ( trim ( a . given_name )),  lower ( trim ( a . family_name )),  lower ( trim ( a . email ))) <>  concat ( lower ( trim ( b . given_name )),  lower ( trim ( b . family_name )),  lower ( trim ( b . email ))))))\n" +
                    ")where peoplehub_id in ('%s') order by peoplehub_id desc";


    public static final String GET_WORKDAY_CORE_COUNT_SQL =
            "select count(*) as Target_Count from "+GetSdrmDbUser.getProdStagingDataBase()+".eph_ingestion_person_delta_v";

    public static final String GET_WORKDAY_CORE_REC_SQL =
            "select * from "+GetSdrmDbUser.getProdStagingDataBase()+".eph_ingestion_person_delta_v where peoplehub_id in ('%s') order by peoplehub_id desc";

}
