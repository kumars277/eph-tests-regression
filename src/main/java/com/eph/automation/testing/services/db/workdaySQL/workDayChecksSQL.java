package com.eph.automation.testing.services.db.workdaySQL;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.sdrmsql.GetSdrmDbUser;

public class workDayChecksSQL {

    public static final String GET_WORKDAY_INBOUND_IDS_SQL=
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

    public static final String GET_WORKDAY_INBOUND_RECS_SQL=
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


    public static final String GET_WORKDAY_INBOUND_COUNT_SQL=
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


}
