package com.eph.automation.testing.services.db.jrbisql;

public class JRBIManifestationDataChecksSQL {
   private JRBIManifestationDataChecksSQL() {throw new IllegalStateException("Utility class");}

    public static final String GET_EPR_IDS_MANIF_FULLLOAD =
            "select epr as EPR from (\n" +
                    "SELECT DISTINCT\n" +
                    "  cr2.epr epr\n" +
                    ", 'jrbi Manifestation Extended' record_type\n" +
                    ", cr2.manifestation_type manifestation_type\n" +
                    ", NULLIF(j.journal_issue_trim_size, '') journal_issue_trim_size\n" +
                    ", NULLIF(j.war_reference, '') war_reference\n" +
                    "FROM\n" +
                    "  ("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full j\n" +
                    "INNER JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON \n" +
                    "((((concat(substr('00000', 1, (5 - length(j.journal_number))), j.journal_number) = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER'))\n" +
                    " AND (cr2.record_level = 'Manifestation')) AND (cr2.manifestation_type = 'JPR')))) order by rand() limit %s\n";

    public static final String GET_MANIF_RECORDS_FULL_LOAD =
            "select epr as EPR" +
                    ",record_type as recordType" +
                    ",manifestation_type as manifestationType" +
                    ",journal_issue_trim_size as journal_issue_trim_size" +
                    ",war_reference as warReference" +
                    " from(\n" +
                    "SELECT DISTINCT\n" +
                    "  cr2.epr epr\n" +
                    ", 'jrbi Manifestation Extended' record_type\n" +
                    ", cr2.manifestation_type manifestation_type\n" +
                    ", NULLIF(j.journal_issue_trim_size, '') journal_issue_trim_size\n" +
                    ", NULLIF(j.war_reference, '') war_reference\n" +
                    "FROM\n" +
                    "  ("+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_journal_data_full j\n" +
                    "INNER JOIN "+GetJRBIDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v cr2 ON \n" +
                    "((((concat(substr('00000', 1, (5 - length(j.journal_number))), j.journal_number) = cr2.identifier) AND (cr2.identifier_type = 'ELSEVIER JOURNAL NUMBER'))\n" +
                    " AND (cr2.record_level = 'Manifestation')) AND (cr2.manifestation_type = 'JPR')))) where EPR in ('%s')";

    public static final String GET_CURRENT_MANIF_RECORDS =
            "select epr as EPR" +
                    ",record_type as recordType" +
                    ",manifestation_type as manifestationType" +
                    ",journal_issue_trim_size as journal_issue_trim_size" +
                    ",war_reference as warReference" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation where EPR in ('%s')";


    public static final String GET_CURRENT_MANIF_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation order by rand() limit %s\n";

    public static final String GET_CURRENT_MANIF_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as recordType" +
                    ",manifestation_type as manifestationType" +
                    ",journal_issue_trim_size as journal_issue_trim_size" +
                    ",war_reference as warReference" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part where EPR in ('%s') AND " +
                    "transform_ts = (select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part) and delete_flag=false \n";

 public static final String GET_MANIF_FILE_RECORDS =
         "select epr as EPR" +
                 ",record_type as recordType" +
                 ",manifestation_type as manifestationType" +
                 ",journal_issue_trim_size as journal_issue_trim_size" +
                 ",war_reference as warReference" +
                 " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_manifestation_file_history_part where EPR in ('%s') AND " +
                 "transform_ts = (select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_manifestation_file_history_part)\n";

    public static final String GET_PREVIOUS_MANIF_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation order by rand() limit %s\n";

    public static final String GET_DELTA_MANIF_EPR_ID =
            "select epr as EPR from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation order by rand() limit %s\n";

    public static final String GET_DELTA_MANIF_RECORDS =
            "select epr as EPR" +
                    ",record_type as recordType" +
                    ",manifestation_type as manifestationType" +
                    ",journal_issue_trim_size as journal_issue_trim_size" +
                    ",war_reference as warReference" +
                    ",type as type" +
                    ",delta_mode as deltaMode" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation where EPR in ('%s')";


    public static final String GET_PREVIOUS_MANIF_RECORDS =
            "select epr as EPR" +
                    ",record_type as recordType" +
                    ",manifestation_type as manifestationType" +
                    ",journal_issue_trim_size as journal_issue_trim_size" +
                    ",war_reference as warReference" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation where EPR in ('%s')";


    public static final String GET_PREVIOUS_MANIF_HISTORY_RECORDS =
            "select epr as EPR" +
                    ",record_type as recordType" +
                    ",manifestation_type as manifestationType" +
                    ",journal_issue_trim_size as journal_issue_trim_size" +
                    ",war_reference as warReference" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part where EPR in ('%s') AND " +
                    "transform_ts=(select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part\n " +
                    " where transform_ts < (select max(transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part))\n";


    public static final String GET_EPR_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_MANIF =
            "select epr as EPR from \n" +
                    "(select A.epr, A.journal_issue_trim_size \n" +
                    ", A.record_type\n" +
                    ", A.war_reference, A.last_updated_date\n" +
                    ", A.transform_ts, A.delete_flag \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation B on A.epr  = B.epr \n" +
                    "where B.epr is null and " +
                    //"A.transform_ts like \'%%"+BcsEtlCoreCountChecksSql.currentDate()+"%%\') " +
                    "A.transform_ts = (select max(A.transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part A))"+
                    "order by rand() limit %s\n";

    public static final String GET_RECORDS_FROM_DIFF_OF_DELTA_AND_CURRENT_HISTORY_MANIF =
            "select epr as EPR,\n" +
                    "record_type as recordType,\n" +
                    "manifestation_type as manifestationType,\n" +
                    "journal_issue_trim_size as journal_issue_trim_size,\n" +
                    "war_reference as warReference,\n" +
                    "last_updated_date as lastUpdatedDate,\n" +
                    "delete_flag as deleteFlag \n" +
                    "from \n" +
                    "(select A.epr, A.journal_issue_trim_size \n" +
                    ", A.record_type\n" +
                    ", A.manifestation_type\n" +
                    ", A.war_reference, A.last_updated_date\n" +
                    ", A.transform_ts, A.delete_flag \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part A \n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation B on A.epr  = B.epr \n" +
                    "where B.epr is null and " +
                    "A.transform_ts = (select max(A.transform_ts) from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation_history_part A)" +
                    "AND A.epr in ('%s'))\n";

    public static final String GET_RECORDS_FROM_MANIF_EXCLUDE =
            "select epr as EPR,\n" +
                    "record_type as recordType,\n" +
                    "manifestation_type as manifestationType,\n" +
                    "journal_issue_trim_size as journal_issue_trim_size,\n" +
                    "war_reference as warReference,\n" +
                    "last_updated_date as lastUpdatedDate,\n" +
                    "delete_flag as deleteFlag\n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_manifestation_excl_delta where EPR in ('%s')\n";

    public static final String GET_JRBI_EPR_MANIF_LATEST =
            "select epr as EPR from \n" +
                    "(select a.epr, a.record_type,a.manifestation_type, \n" +
                    "a.journal_issue_trim_size, a.war_reference, a.transform_ts, a.last_updated_date, a.delete_flag\n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_manifestation_excl_delta \n" +
                    "as a union all select b.epr, b.record_type,b.manifestation_type, \n" +
                    "b.journal_issue_trim_size, b.war_reference,b.transform_ts,null as col11, null as col12 \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation as b)\n "+
                    "order by rand() limit %s\n";

    public static final String GET_JRBI_REC_SUM_DELTA_MANIF_AND_MANIF_EXCLUDE =
            "select epr as EPR,\n" +
                    "record_type as recordType,\n" +
                    "manifestation_type as manifestationType,\n" +
                    "journal_issue_trim_size as journal_issue_trim_size,\n" +
                    "war_reference as warReference,\n" +
                    "last_updated_date as lastUpdatedDate,\n" +
                    "delete_flag as deleteFlag\n" +
                    " from (select a.epr, a.record_type,a.manifestation_type, \n" +
                    "a.journal_issue_trim_size, a.war_reference, a.transform_ts, a.last_updated_date, a.delete_flag\n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_history_manifestation_excl_delta \n" +
                    "as a union all select b.epr, b.record_type,b.manifestation_type, \n" +
                    "b.journal_issue_trim_size, b.war_reference,b.transform_ts,null as col11, null as col12 \n" +
                    "from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_delta_current_manifestation as b) where EPR in ('%s')\n";

    public static final String GET_JRBI_MANIF_LATEST_RECORDS =
            "select epr as EPR,\n" +
                    "record_type as recordType,\n" +
                    "manifestation_type as manifestationType,\n" +
                    "journal_issue_trim_size as journal_issue_trim_size,\n" +
                    "war_reference as warReference,\n" +
                    "last_updated_date as lastUpdatedDate,\n" +
                    "delete_flag as deleteFlag\n" +
                    " from "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_manifestation where EPR in ('%s')\n";


    public static final String GET_RANDOM_EPR_DELTA_PERSON =
            "select epr as EPR from (\n" +
                    "select c.epr , c.record_type , c.manifestation_type , c.journal_issue_trim_size , c.war_reference FROM "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr , c.record_type , c.manifestation_type , c.journal_issue_trim_size , c.war_reference  FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr , c.record_type , c.manifestation_type , c.journal_issue_trim_size , c.war_reference FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation p  on c.epr = p.epr\n" +
                    "where (c.epr !=(p.epr) or\n" +
                    "c.record_type != (p.record_type) or \n" +
                    "c.manifestation_type !=  (p.manifestation_type ) or\n" +
                    "c.journal_issue_trim_size !=  (p.journal_issue_trim_size ) or\n" +
                    "c.war_reference !=  (p.war_reference ))) order by rand() limit %s\n";

    public static final String GET_DIFF_REC_PREVIOUS_CURRENT_PREVIOUS_MANIF =
            "select epr as EPR" +
                    ",record_type as recordType" +
                    ",manifestation_type as manifestationType" +
                    ",journal_issue_trim_size as journal_issue_trim_size" +
                    ",war_reference as warReference" +
                    ",type as type" +
                    ",delta_mode as deltaMode" +
                    " from (\n" +
                    "select c.epr , c.record_type , c.manifestation_type , c.journal_issue_trim_size , c.war_reference,'NEW' as type,'I' as delta_mode\n " +
                    "FROM "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr , c.record_type , c.manifestation_type , c.journal_issue_trim_size , c.war_reference,'OLD' as type,'D' as delta_mode\n  " +
                    "FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation  c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation p  on c.epr = p.epr\n" +
                    "where p.epr is null\n" +
                    "union all\n" +
                    "select c.epr , c.record_type , c.manifestation_type , c.journal_issue_trim_size , c.war_reference,'NEW' as type,'C' as delta_mode\n " +
                    "FROM  "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_previous_manifestation c\n" +
                    "left join "+GetJRBIDLDBUser.getJRBIDataBase()+".jrbi_transform_current_manifestation p  on c.epr = p.epr\n" +
                    "where (c.epr !=(p.epr) or\n" +
                    "c.record_type != (p.record_type) or \n" +
                    "c.manifestation_type !=  (p.manifestation_type ) or\n" +
                    "c.journal_issue_trim_size !=  (p.journal_issue_trim_size ) or\n" +
                    "c.war_reference !=  (p.war_reference ))) where EPR in ('%s')\n";

}




