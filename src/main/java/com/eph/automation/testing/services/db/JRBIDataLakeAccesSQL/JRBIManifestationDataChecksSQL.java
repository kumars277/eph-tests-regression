package com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class JRBIManifestationDataChecksSQL {

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


    public static String GET_EPR_IDS_MANIF_FULLLOAD =
            "select epr as EPR from(SELECT DISTINCT\n" +
            "  COALESCE(cr1.epr, cr2.epr) epr\n" +
            ", 'JRBI Manifestation Extended' record_type\n" +
            ", COALESCE(cr1.manifestation_type,cr2.manifestation_type) manifestation_type\n" +
            ", j.journal_prod_site_code journal_prod_site_code\n" +
            ", j.journal_issue_trim_size journal_issue_trim_size\n" +
            ", j.war_reference war_reference\n" +
            "FROM\n" +
            "  (("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full j\n" +
            "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr1 ON ((((j.issn = cr1.identifier) AND (cr1.identifier_type = 'ISSN')) AND (cr1.record_level = 'Manifestation')) AND (cr1.manifestation_type = 'JPR')))\n" +
            "LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON ((((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Manifestation')) AND (cr2.manifestation_type = 'JPR')))) where epr is not NULL\n"+
                    " order by rand() limit %s\n";

    public static String GET_MANIF_RECORDS_FULL_LOAD =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",manifestation_type as MANIFESTATION_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    " from(SELECT DISTINCT\n" +
                    " COALESCE(cr1.epr, cr2.epr) epr\n" +
                    ", 'JRBI Manifestation Extended' record_type\n" +
                    ", COALESCE(cr1.manifestation_type,cr2.manifestation_type) manifestation_type\n" +
                    ", j.journal_prod_site_code journal_prod_site_code\n" +
                    ", j.journal_issue_trim_size journal_issue_trim_size\n" +
                    ", j.war_reference war_reference\n" +
                    " FROM\n" +
                    " (("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full j\n" +
                    " LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr1 ON ((((j.issn = cr1.identifier) AND (cr1.identifier_type = 'ISSN')) AND (cr1.record_level = 'Manifestation')) AND (cr1.manifestation_type = 'JPR')))\n" +
                    " LEFT JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON ((((j.journal_number = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER')) AND (cr2.record_level = 'Manifestation')) AND (cr2.manifestation_type = 'JPR'))))\n" +
                    "where EPR in ('%s')";

    public static String GET_CURRENT_MANIF_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",manifestation_type as MANIFESTATION_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation where EPR in ('%s')";


    public static String GET_CURRENT_MANIF_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation order by rand() limit %s\n";


    public static String GET_CURRENT_MANIF_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",manifestation_type as MANIFESTATION_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part where EPR in ('%s') AND " +
                  //  "transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\' and delete_flag=false";
                    "transform_ts = (select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part)\n";

    public static String GET_PREVIOUS_MANIF_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation order by rand() limit %s\n";

    public static String GET_DELTA_MANIF_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation order by rand() limit %s\n";

    public static String GET_DELTA_MANIF_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",manifestation_type as MANIFESTATION_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    ",type as TYPE" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation where EPR in ('%s')";

    public static String GET_DELTA_MANIF_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",manifestation_type as MANIFESTATION_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    ",type as TYPE" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_manifestation_history_part where EPR in ('%s') AND " +
                   // "delta_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\'";
                    "delta_ts = (select max(delta_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_manifestation_history_part)\n";

    public static String GET_PREVIOUS_MANIF_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",manifestation_type as MANIFESTATION_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation where EPR in ('%s')";


    public static String GET_PREVIOUS_MANIF_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",manifestation_type as MANIFESTATION_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part where EPR in ('%s') AND " +
                    //"transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.previousDate()+"%%\'";
                    "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part\n " +
                    " where transform_ts < (select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part))\n";


    public static String GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_MANIF =
            "select epr as EPR from \n" +
                    "(select A.epr, A.journal_issue_trim_size \n" +
                    ",A.journal_prod_site_code, A.record_type\n" +
                    ", A.war_reference, A.last_updated_date\n" +
                    ", A.transform_ts, A.delete_flag \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation B on A.epr  = B.epr \n" +
                    "where B.epr is null and " +
                    //"A.transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\') " +
                    "A.transform_ts = (select max(A.transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_delta_manifestation_history_part A))"+
                    "order by rand() limit %s\n";

    public static String GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_MANIF =
            "select epr as EPR,\n" +
                    "record_type as RECORD_TYPE,\n" +
                    "manifestation_type as MANIFESTATION_TYPE,\n" +
                    "journal_prod_site_code as JOURNAL_PROD_SITE,\n" +
                    "journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE,\n" +
                    "war_reference as WAR_REFERENCE,\n" +
                    "last_updated_date as LAST_UPDATED_DATE,\n" +
                    "delete_flag as DELETE_FLAG \n" +
                    "from \n" +
                    "(select A.epr, A.journal_issue_trim_size \n" +
                    ",A.journal_prod_site_code, A.record_type\n" +
                    ", A.manifestation_type\n" +
                    ", A.war_reference, A.last_updated_date\n" +
                    ", A.transform_ts, A.delete_flag \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation B on A.epr  = B.epr \n" +
                    "where B.epr is null and " +
                   // "A.transform_ts like \'%%"+JRBIDataLakeCountChecksSQL.currentDate()+"%%\' " +
                    "A.transform_ts = (select max(A.transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part A)" +
                    "AND A.epr in ('%s'))\n";

    public static String GET_RECORDS_FROM_MANIF_EXCLUDE =
            "select epr as EPR,\n" +
                    "record_type as RECORD_TYPE,\n" +
                    "manifestation_type as MANIFESTATION_TYPE,\n" +
                    "journal_prod_site_code as JOURNAL_PROD_SITE,\n" +
                    "journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE,\n" +
                    "war_reference as WAR_REFERENCE,\n" +
                    "last_updated_date as LAST_UPDATED_DATE,\n" +
                    "delete_flag as DELETE_FLAG\n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_manifestation_excl_delta where EPR in ('%s')\n";

    public  static String GET_JRBI_EPR_MANIF_LATEST =
            "select epr as EPR from \n" +
                    "(select a.epr, a.record_type,a.manifestation_type, a.journal_prod_site_code, \n" +
                    "a.journal_issue_trim_size, a.war_reference, a.transform_ts, a.last_updated_date, a.delete_flag\n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_manifestation_excl_delta \n" +
                    "as a union all select b.epr, b.record_type,b.manifestation_type, b.journal_prod_site_code, \n" +
                    "b.journal_issue_trim_size, b.war_reference,b.transform_ts,null as col11, null as col12 \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation as b)\n "+
                    "order by rand() limit %s\n";

    public static String GET_JRBI_REC_SUM_DELTA_MANIF_AND_MANIF_EXCLUDE =
            "select epr as EPR,\n" +
                    "record_type as RECORD_TYPE,\n" +
                    "manifestation_type as MANIFESTATION_TYPE,\n" +
                    "journal_prod_site_code as JOURNAL_PROD_SITE,\n" +
                    "journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE,\n" +
                    "war_reference as WAR_REFERENCE,\n" +
                    "last_updated_date as LAST_UPDATED_DATE,\n" +
                    "delete_flag as DELETE_FLAG\n" +
                    " from (select a.epr, a.record_type,a.manifestation_type, a.journal_prod_site_code, \n" +
                    "a.journal_issue_trim_size, a.war_reference, a.transform_ts, a.last_updated_date, a.delete_flag\n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_manifestation_excl_delta \n" +
                    "as a union all select b.epr, b.record_type,b.manifestation_type, b.journal_prod_site_code, \n" +
                    "b.journal_issue_trim_size, b.war_reference,b.transform_ts,null as col11, null as col12 \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation as b) where EPR in ('%s')\n";

    public static String GET_JRBI_MANIF_LATEST_RECORDS =
            "select epr as EPR,\n" +
                    "record_type as RECORD_TYPE,\n" +
                    "manifestation_type as MANIFESTATION_TYPE,\n" +
                    "journal_prod_site_code as JOURNAL_PROD_SITE,\n" +
                    "journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE,\n" +
                    "war_reference as WAR_REFERENCE,\n" +
                    "last_updated_date as LAST_UPDATED_DATE,\n" +
                    "delete_flag as DELETE_FLAG\n" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_manifestation where EPR in ('%s')\n";

    public static String GET_JRBI_MANIF_EXTENDED_RECORDS =
            "select epr_id as EPR_ID,\n" +
                    "manifestation_type as MANIFESTATION_TYPE,\n" +
                    "journal_prod_site_code as JOURNAL_PROD_SITE,\n" +
                    "journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE,\n" +
                    "war_reference as WAR_REFERENCE,\n" +
                    "last_updated_date as LAST_UPDATED_DATE,\n" +
                    "delete_flag as DELETE_FLAG\n" +
                    " from "+GetJRBIDLDBUser.getProductExtdb()+".manifestation_extended where EPR_ID in ('%s')\n";


    public static String GET_RANDOM_EPR_DELTA_PERSON =
                    "select epr as EPR from (\n" +
                    "select c.epr , c.record_type , c.manifestation_type, c.journal_prod_site_code , c.journal_issue_trim_size , c.war_reference FROM "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr , c.record_type , c.manifestation_type, c.journal_prod_site_code , c.journal_issue_trim_size , c.war_reference  FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr , c.record_type , c.manifestation_type, c.journal_prod_site_code , c.journal_issue_trim_size , c.war_reference FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation p  on c.epr = p.epr\n" +
                    "where (c.epr !=(p.epr) or\n" +
                    "c.record_type != (p.record_type) or \n" +
                    "c.manifestation_type !=  (p.manifestation_type ) or\n" +
                    "c.journal_prod_site_code !=  (p.journal_prod_site_code ) or\n" +
                    "c.journal_issue_trim_size !=  (p.journal_issue_trim_size ) or\n" +
                    "c.war_reference !=  (p.war_reference ))) order by rand() limit %s\n";

    public static String GET_DIFF_REC_PREVIOUS_CURRENT_PREVIOUS_MANIF =
            "select epr as EPR" +
                    ",record_type as RECORD_TYPE" +
                    ",manifestation_type as MANIFESTATION_TYPE" +
                    ",journal_prod_site_code as JOURNAL_PROD_SITE" +
                    ",journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE" +
                    ",war_reference as WAR_REFERENCE" +
                    ",type as TYPE" +
                    ",delta_mode as DELTA_MODE" +
                    " from (\n" +
                    "select c.epr , c.record_type , c.manifestation_type, c.journal_prod_site_code , c.journal_issue_trim_size , c.war_reference,'NEW' as type,'I' as delta_mode\n " +
                    "FROM "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr , c.record_type , c.manifestation_type, c.journal_prod_site_code , c.journal_issue_trim_size , c.war_reference,'OLD' as type,'D' as delta_mode\n  " +
                    "FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr , c.record_type , c.manifestation_type, c.journal_prod_site_code , c.journal_issue_trim_size , c.war_reference,'NEW' as type,'C' as delta_mode\n " +
                    "FROM  jrbi_staging_sit.jrbi_transform_previous_manifestation c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation p  on c.epr = p.epr\n" +
                    "where (c.epr !=(p.epr) or\n" +
                    "c.record_type != (p.record_type) or \n" +
                    "c.manifestation_type !=  (p.manifestation_type ) or\n" +
                    "c.journal_prod_site_code !=  (p.journal_prod_site_code ) or\n" +
                    "c.journal_issue_trim_size !=  (p.journal_issue_trim_size ) or\n" +
                    "c.war_reference !=  (p.war_reference ))) where EPR in ('%s')\n";

    public static String Get_ProductDB_Manif_Extended_Records =
            "select epr_id as EPR,\n" +
                    "manifestation_type as MANIFESTATION_TYPE,\n" +
                    "journal_prod_site_code as JOURNAL_PROD_SITE,\n" +
                    "journal_issue_trim_size as JOURNAL_ISSUE_TRIM_SIZE,\n" +
                    "war_reference as WAR_REFERENCE,\n" +
                    "delete_flag as DELETE_FLAG\n" +
                    " from "+GetJRBIDLDBUser.getProductExtendedDatabase()+".manifestation_extended where epr_id in ('%s')\n";

}




