package com.eph.automation.testing.services.db.JMDataLakeSQL;

public class JM_ETLExtendedCountDataChecksSQL {

    public static String GET_JM_EXT_NEW_FULFIL_SYSTEM_COUNT= "select count(*) as Target_Count from "+GetJMDLDBUser.getJMDB()+".jnl_new_fulfilment_system";

    public static String GET_JM_EXT_FULFIL_SYSTEM_COUNT= "select count(*) as Target_Count from "+GetJMDLDBUser.getJMDB()+".jnl_fulfilment_system";

    public static String GET_SOURCE_JM_EXT_FULFIL_SYSTEM_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "  issn\n" +
                    ", application_code\n" +
                    ", TRY(date_parse(last_updated_date, '%d/%m/%Y')) last_updated_date\n" +
                    ", epr_id\n" +
                    ", product_type\n" +
                    ", TRY(date(date_parse(last_updated_date, '%d/%m/%Y'))) availability_start_date\n" +
                    ", 'Available' availability_status\n" +
                    ", false delete_flag\n" +
                    "FROM "+GetJMDLDBUser.getJMDB()+".jnl_fulfilment_system)";

    public static String GET_SOURCE_JM_EXT_FULFIL_SYSTEM_REC =
            "select epr_id as epr_id" +
                    ",issn as issn" +
                    ",application_code as application_code" +
                    ",last_updated_date as last_updated_date" +
                    ",epr_id as epr_id" +
                    ",product_type as product_type" +
                    ",availability_start_date as availability_start_date" +
                    ",availability_status as availability_status" +
                    ",delete_flag as delete_flag" +
                    " from(\n" +
                    "SELECT\n" +
                    "  issn\n" +
                    ", application_code\n" +
                    ", TRY(date_parse(last_updated_date, '%%d/%%m/%%Y')) last_updated_date\n" +
                    ", epr_id\n" +
                    ", product_type\n" +
                    ", TRY(date(date_parse(last_updated_date, '%%d/%%m/%%Y'))) availability_start_date\n" +
                    ", 'Available' availability_status\n" +
                    ", false delete_flag\n" +
                    "FROM "+GetJMDLDBUser.getJMDB()+".jnl_fulfilment_system where epr_id in ('%s') order by epr_id desc)";

    public static String GET_JM_EXT_FULFIL_SYSTEM_REC =
            "select " +
                    "issn as issn" +
                    ",application_code as application_code" +
                    ",last_updated_date as last_updated_date" +
                    ",epr_id as epr_id" +
                    ",product_type as product_type" +
                   // ",availability_start_date as availability_start_date" +
                    //",availability_status as availability_status" +
                    //",delete_flag as delete_flag" +
                    " from "+GetJMDLDBUser.getJMDB()+".jnl_fulfilment_system where epr_id in ('%s') order by epr_id desc";

    public static String GET_SOURCE_JM_EXT_FULFIL_SYSTEM_RAND_ID =
            "select epr_id" +
                    " from(\n" +
                    "SELECT\n" +
                    "  issn\n" +
                    ", application_code\n" +
                    ", TRY(date_parse(last_updated_date, '%%d/%%m/%%Y')) last_updated_date\n" +
                    ", epr_id\n" +
                    ", product_type\n" +
                    ", TRY(date(date_parse(last_updated_date, '%%d/%%m/%%Y'))) availability_start_date\n" +
                    ", 'Available' availability_status\n" +
                    ", false delete_flag\n" +
                    "FROM "+GetJMDLDBUser.getJMDB()+".jnl_fulfilment_system)order by rand() limit %s";

    public static String GET_SOURCE_JM_EXT_NEW_FULFIL_SYSTEM_COUNT =
                "select count(*) as Source_Count from(\n" +
                        "SELECT\n" +
                        "  cr.epr epr_id\n" +
                        ", cr.product_type product_type\n" +
                        ", CAST(current_timestamp AS timestamp) last_updated_date\n" +
                        ", jmm.application_code application_name\n" +
                        ", TRY(date(jmm.notified_date)) availability_start_date\n" +
                        ", CAST('Available' AS varchar) availability_status\n" +
                        ", false delete_flag\n" +
                        "FROM\n" +
                        "  (((("+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle jwc\n" +
                        "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario jcs ON (jwc.chronicle_scenario_code = jcs.chronicle_scenario_code))\n" +
                        "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work jmw ON (jmw.work_chronicle_id = jwc.work_chronicle_id))\n" +
                        "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_manifestation jmm ON (jmw.work_id = jmm.f_work))\n" +
                        "INNER JOIN (\n" +
                        "   SELECT *\n" +
                        "   FROM\n" +
                        "     "+GetJMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v\n" +
                        "   WHERE (((((identifier_type = 'ISSN') AND (record_level = 'Product')) AND (product_type = 'SUB')) AND (manifestation_type = 'JPR')) AND (work_type = 'JNL'))\n" +
                        ")  cr ON (cr.identifier = jmm.issn))\n" +
                        "WHERE ((((jcs.chronicle_scenario_evolutionary_ind = 'N') AND (jmm.manifestation_type = 'P')) AND (jmm.application_code IS NOT NULL)) AND (NOT (cr.epr IN (SELECT epr_id\n" +
                        "FROM\n" +
                        "  "+GetJMDLDBUser.getJMDB()+".jnl_fulfilment_system_v)))))";

    public static String GET_SOURCE_JM_EXT_NEW_FULFIL_SYSTEM_COUNT_RAND_ID =
            "select epr_id from(\n" +
                    "SELECT\n" +
                    "  cr.epr epr_id\n" +
                    ", cr.product_type product_type\n" +
                    ", CAST(current_timestamp AS timestamp) last_updated_date\n" +
                    ", jmm.application_code application_name\n" +
                    ", TRY(date(jmm.notified_date)) availability_start_date\n" +
                    ", CAST('Available' AS varchar) availability_status\n" +
                    ", false delete_flag\n" +
                    "FROM\n" +
                    "  (((("+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle jwc\n" +
                    "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario jcs ON (jwc.chronicle_scenario_code = jcs.chronicle_scenario_code))\n" +
                    "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work jmw ON (jmw.work_chronicle_id = jwc.work_chronicle_id))\n" +
                    "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_manifestation jmm ON (jmw.work_id = jmm.f_work))\n" +
                    "INNER JOIN (\n" +
                    "   SELECT *\n" +
                    "   FROM\n" +
                    "     "+GetJMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v\n" +
                    "   WHERE (((((identifier_type = 'ISSN') AND (record_level = 'Product')) AND (product_type = 'SUB')) AND (manifestation_type = 'JPR')) AND (work_type = 'JNL'))\n" +
                    ")  cr ON (cr.identifier = jmm.issn))\n" +
                    "WHERE ((((jcs.chronicle_scenario_evolutionary_ind = 'N') AND (jmm.manifestation_type = 'P')) AND (jmm.application_code IS NOT NULL)) AND (NOT (cr.epr IN (SELECT epr_id\n" +
                    "FROM\n" +
                    "  "+GetJMDLDBUser.getJMDB()+".jnl_fulfilment_system_v)))) order by rand() limit %s)";

    public static String GET_SOURCE_JM_EXT_NEW_FULFIL_SYSTEM_REC =
            "select epr_id as epr_id" +
                    ",product_type as product_type" +
                    ",last_updated_date as last_updated_date" +
                    ",application_name as application_name" +
                    ",availability_start_date as availability_start_date" +
                    ",availability_status as availability_status" +
                    ",delete_flag as delete_flag" +
                    " from(\n" +
                    "SELECT\n" +
                    "  cr.epr epr_id\n" +
                    ", cr.product_type product_type\n" +
                    ", CAST(current_timestamp AS timestamp) last_updated_date\n" +
                    ", jmm.application_code application_name\n" +
                    ", TRY(date(jmm.notified_date)) availability_start_date\n" +
                    ", CAST('Available' AS varchar) availability_status\n" +
                    ", false delete_flag\n" +
                    "FROM\n" +
                    "  (((("+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle jwc\n" +
                    "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario jcs ON (jwc.chronicle_scenario_code = jcs.chronicle_scenario_code))\n" +
                    "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work jmw ON (jmw.work_chronicle_id = jwc.work_chronicle_id))\n" +
                    "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_manifestation jmm ON (jmw.work_id = jmm.f_work))\n" +
                    "INNER JOIN (\n" +
                    "   SELECT *\n" +
                    "   FROM\n" +
                    "     "+GetJMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v\n" +
                    "   WHERE (((((identifier_type = 'ISSN') AND (record_level = 'Product')) AND (product_type = 'SUB')) AND (manifestation_type = 'JPR')) AND (work_type = 'JNL'))\n" +
                    ")  cr ON (cr.identifier = jmm.issn))\n" +
                    "WHERE ((((jcs.chronicle_scenario_evolutionary_ind = 'N') AND (jmm.manifestation_type = 'P')) AND (jmm.application_code IS NOT NULL)) AND (NOT (cr.epr IN (SELECT epr_id\n" +
                    "FROM\n" +
                    "  "+GetJMDLDBUser.getJMDB()+".jnl_fulfilment_system_v))))) where epr_id in ('%s') order by epr_id desc";


    public static String GET_JM_EXT_NEW_FULFIL_SYSTEM_REC=
            "select epr_id as epr_id" +
                    ",product_type as product_type" +
                    ",last_updated_date as last_updated_date" +
                    ",application_name as application_name" +
                    ",availability_start_date as availability_start_date" +
                    ",availability_status as availability_status" +
                    ",delete_flag as delete_flag" +
                    " from "+GetJMDLDBUser.getJMDB()+".jnl_new_fulfilment_system where epr_id in ('%s') order by epr_id desc";

    public static String GET_JM_EXT_FULFIL_SYSTEM_Single_REC =
            "select " +
                    "issn as issn" +
                    ",application_code as application_code" +
                    ",last_updated_date as last_updated_date" +
                    ",epr_id as epr_id" +
                    ",product_type as product_type" +
                    // ",availability_start_date as availability_start_date" +
                    //",availability_status as availability_status" +
                    //",delete_flag as delete_flag" +
                    " from "+GetJMDLDBUser.getJMDB()+".jnl_fulfilment_system where " +
                    "epr_id in ('EPR-10GW8S')";

 }




