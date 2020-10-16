package com.eph.automation.testing.services.db.BCS_ETLCoreSQL;


public class BCS_ETLCoreCountChecksSQL {


    public static String GET_BCS_ETL_CORE_ACC_PROD_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_current_v";

    public static String GET_BCS_ETL_CORE_ACC_PROD_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part  where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part)";

    public static String GET_BCS_ETL_CORE_ACC_PROD_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part  where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part)";


    public static String GET_BCS_ETL_CORE_MANIF_CURR_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_current_v";

    public static String GET_BCS_ETL_CORE_MANIF_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_part) ";

    public static String GET_BCS_ETL_CORE_MANIF_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_transform_file_history_part) ";


     public static String GET_BCS_ETL_CORE_PERSON_CURR_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_current_v";
    public static String GET_BCS_ETL_CORE_PERSON_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part) ";
    public static String GET_BCS_ETL_CORE_PERSON_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_transform_file_history_part) ";


    public static String GET_BCS_ETL_CORE_PRODUCT_CURR_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_current_v";

    public static String GET_BCS_ETL_CORE_PRODUCT_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_part) ";
    public static String GET_BCS_ETL_CORE_PRODUCT_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_transform_file_history_part) ";



    public static String GET_BCS_ETL_CORE_WRK_PERS_CURR_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_current_v";

    public static String GET_BCS_ETL_CORE_WRK_PERS_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_part) ";
    public static String GET_BCS_ETL_CORE_WRK_PERS_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_transform_file_history_part) ";



    public static String GET_BCS_ETL_CORE_WRK_RELT_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_current_v";

    public static String GET_BCS_ETL_CORE_WRK_RELT_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_part) ";
    public static String GET_BCS_ETL_CORE_WRK_RELT_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_transform_file_history_part) ";


    public static String GET_BCS_ETL_CORE_WRK_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_current_v";
    public static String GET_BCS_ETL_CORE_WRK_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_part) ";
    public static String GET_BCS_ETL_CORE_WRK_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_transform_file_history_part) ";


    public static String GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_current_v";

    public static String GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_part) ";
    public static String GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_transform_file_history_part) ";


    public static String GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_current_v";
    public static String GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part) ";
    public static String GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part) ";


    public static String GET_MANIF_IDENTIF_INBOUND_CURRENT_COUNT =
            "select count(*) as Source_Count from( \n" +
                    "SELECT \n" +
                    "     sourceref \n" +
                    "   , isbn13 identifier \n" +
                    "   , 'ISBN' identifier_type \n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product \n" +
                    "   WHERE (isbn13 <> '') \n" +
                    " UNION ALL SELECT \n" +
                    "     sourceref \n" +
                    "   , seriesissn identifier \n" +
                    "   , 'ISSN' identifier_type \n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content\n" +
                    "   WHERE (seriesissn <> ''))";

    public static String GET_WRK_IDENTIF_INBOUND_CURRENT_COUNT =
            "WITH \n" +
                    "  work_id AS ( \n" +
                    "   SELECT DISTINCT workmasterprojectno \n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily \n" +
                    ")\n" +
                    "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "  sourceref \n" +
                    ", piidack identifier \n" +
                    ", 'DAC-Key' identifier_type \n" +
                    "FROM \n" +
                    "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content \n" +
                    "INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                    "WHERE (piidack <> '') \n" +
                    "UNION ALL SELECT\n" +
                    "  sourceref \n" +
                    ", doi identifier \n" +
                    ", 'DOI' identifier_type \n" +
                    "FROM \n" +
                    "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                    "INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                    "WHERE (doi <> '')\n" +
                    "UNION ALL SELECT\n" +
                    "  sourceref \n" +
                    ", seriesissn identifier \n" +
                    ", 'ISSN-L' identifier_type \n" +
                    "FROM \n" +
                    "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content \n" +
                    "INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                    "WHERE (seriesissn <> '')\n" +
                    "UNION ALL SELECT\n" +
                    "  sourceref \n" +
                    ", orderno identifier \n" +
                    ", 'PPM-PART' identifier_type \n" +
                    "FROM \n" +
                    "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product \n" +
                    "INNER JOIN work_id ON (product.sourceref = work_id.workmasterprojectno))\n" +
                    "WHERE (orderno <> ''))";

    public static String GET_WRK_INBOUND_CURRENT_COUNT =
            "SELECT count(*)as Source_Count \n" +
                    "FROM \n" +
                    "  ( \n" +
                    "   SELECT DISTINCT\n" +
                    "     NULLIF(content.sourceref, '') sourceref \n" +
                    "   , NULLIF(content.title, '') title \n" +
                    "   , NULLIF(content.subtitle, '') subtitle \n" +
                    "   , NULLIF(content.volumeno, '') volumeno \n" +
                    "   , content.copyrightyear copyrightyear \n" +
                    "   , content.editionno editionno \n" +
                    "   , substring(split_part(NULLIF(content.subgroup, ''), ' | ', 1), 2, 3) pmc \n" +
                    "   , COALESCE(NULLIF(worktypecode.ephcode, ''), 'UNK') work_type \n" +
                    "   , COALESCE(NULLIF(status.ephworkcode, ''), 'UNK') statuscode \n" +
                    "   , split_part(NULLIF(content.imprint, ''), ' | ', 1) imprintcode \n" +
                    "   , NULLIF(cast(\"opcocode\".\"11icode\" as varchar), '') te_opco \n" +
                    "   , NULLIF(cast(opcocode.r12code as varchar), '') opco \n" +
                    "   , NULLIF(cast(temapping.mapped_r12_sgmnt_value as varchar), '') resp_centre \n" +
                    "   , substring(split_part(NULLIF(content.division, ''), ' | ', 1), 2, 3) pmg \n" +
                    "   , NULLIF(languagecode.ephcode, '') languagecode \n" +
                    "   , CAST(NULL AS boolean) electro_rights_indicator \n" +
                    "   , CAST(NULL AS varchar) f_oa_journal_type \n" +
                    "   , CAST(NULL AS varchar) f_society_ownership \n" +
                    "   , CAST(NULL AS varchar) subscription_type \n" +
                    "   , date_parse(NULLIF(content.metamodifiedon, ''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
                    "   FROM \n" +
                    "     (((((("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content \n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily ON (content.sourceref = versionfamily.workmasterprojectno)) \n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification ON ((content.sourceref = classification.sourceref) AND (classification.classificationcode = 'DCDFAC | Accounting class'))) \n" +
                    "   LEFT JOIN ( \n" +
                    "      SELECT DISTINCT \n" +
                    "        masterstatus.sourceref \n" +
                    "      , statuscode.ephworkcode \n" +
                    "      FROM \n" +
                    "        ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode \n" +
                    "      INNER JOIN ( \n" +
                    "         SELECT \n" +
                    "           workmasterprojectno sourceref \n" +
                    "         , min(priority) statuspriority \n" +
                    "         FROM \n" +
                    "           (("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily \n" +
                    "         INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation ON (versionfamily.sourceref = sublocation.sourceref))\n" +
                    "         INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (split_part(sublocation.status, ' | ', 1) = statuscode.ppmcode))\n" +
                    "         GROUP BY workmasterprojectno \n" +
                    "      )  masterstatus ON (statuscode.priority = masterstatus.statuspriority))\n" +
                    "   )  status ON (content.sourceref = status.sourceref))\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".opcocode opcocode ON (content.ownership = opcocode.ppmcode)\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".temapping temapping ON ((\"opcocode\".\"11icode\" = temapping.r11_sgmnt_c_value) and (substring(split_part(NULLIF(content.division, ''), ' | ', 1), 2, 3) = temapping.r11_sgmnt_a_value) and (temapping.r11_sgmnt_b_value = '0') and (NULLIF(temapping.r11_sgmnt_d_value,'') is null) and (temapping.mapping_rule_number = 2)))\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".languagecode languagecode ON (split_part(content.language, ' | ', 1) = languagecode.ppmcode))\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".worktypecode worktypecode ON (split_part(classification.value, ' | ', 1) = worktypecode.ppmcode))\n" +
                    ")  A \n" +
                    "WHERE (A.sourceref IS NOT NULL)";

    public static String GET_WRK_RELT_INBOUND_CURRENT_COUNT =
               "select count(*) as Source_Count from( \n" +
                       "SELECT \n" +
                       "     concat(concat(CAST(sourceref AS varchar), split_part(relationtype, ' | ', 1)), CAST(projectno AS varchar)) sourceref \n" +
                       "   , sourceref parentref \n" +
                       "   , projectno childref \n" +
                       "   , relationtypecode.ephcode relationtyperef \n" +
                       "   , date_parse(metamodifiedon,'%d-%b-%Y %H:%i:%s') modifiedon \n" +
                       "   , 'N' dq_err \n" +
                       "   FROM \n" +
                       "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_relations \n" +
                       "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".relationtypecode ON \n" +
                       "   (split_part(relationtype, ' | ', 1) = relationtypecode.ppmcode)))";

       public static String GET_WRK_PERSON_INBOUND_CURRENT_COUNT =
               "SELECT count(*) as Source_Count \n" +
                       "FROM \n" +
                       "  ( \n" +
                       "   SELECT DISTINCT\n" +
                       "     NULLIF(sourceref, '') worksourceref \n" +
                       "   , NULLIF(CAST(businesspartnerid AS varchar), '') personsourceref \n" +
                       "   , NULLIF(rolecode.ephcode, '') roletype \n" +
                       "   , concat(concat(NULLIF(sourceref, ''), NULLIF(rolecode.ephcode, '')), NULLIF(CAST(businesspartnerid AS varchar), '')) u_key \n" +
                       "   , NULLIF(CAST(sequence AS varchar), '') sequence \n" +
                       "   , NULLIF(CAST(locationid AS varchar), '') deduplicator \n" +
                       "   , date_parse(NULLIF(metamodifiedon, ''), '%d-%b-%Y %H:%i:%s') modifiedon \n" +
                       "   , 'N' dq_err \n" +
                       "   FROM \n" +
                       "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators \n" +
                       "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(copyrightholdertype, ' | ', 1) = rolecode.ppmcode)) \n" +
                       "UNION    SELECT \n" +
                       "     NULLIF(sourceref, '') worksourceref \n" +
                       "   , NULLIF(personid, '') personsourceref \n" +
                       "   , NULLIF(rolecode.ephcode, '') roletype \n" +
                       "   , concat(concat(sourceref, rolecode.ephcode), personid) u_key \n" +
                       "   , (CASE WHEN (substr(split_part(NULLIF(responsibility, ''), ' | ', 1), -1, 1) = '2') THEN '2' ELSE '1' END) sequence\n" +
                       "   , '0' deduplicator\n" +
                       "   , date_parse(NULLIF(metamodifiedon, ''), '%d-%b-%Y %H:%i:%s') modifiedon \n" +
                       "   , 'N' dq_err \n" +
                       "   FROM \n" +
                       "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_responsibilities \n" +
                       "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(responsibility, ' | ', 1) = rolecode.ppmcode))\n" +
                       ")  A \n" +
                       "WHERE (((A.worksourceref IS NOT NULL) AND (A.personsourceref IS NOT NULL)) AND (A.roletype IS NOT NULL))";


       public static String GET_PRODUCT_INBOUND_CURRENT_COUNT =
               "SELECT count(*) as Source_Count \n" +
                       "FROM \n" +
                       "  ( \n" +
                       "   SELECT DISTINCT \n" +
                       "     NULLIF(product.sourceref, '') sourceref \n" +
                       "   , split_part (NULLIF(product.binding, ''), ' | ', 1) bindingcode \n" +
                       "   , concat(NULLIF(product.sourceref, ''), split_part(NULLIF(product.binding, ''), ' | ', 1)) u_key \n" +
                       "   , concat(NULLIF(content.title, ''), ' ', split_part(NULLIF(product.binding, ''), ' | ', 2), ' OOA') name \n" +
                       "   , NULLIF(content.shorttitle, '') shorttitle \n" +
                       "   , CAST(date_parse((CASE WHEN ((publishedon = '') AND (pubdateplanned = '')) THEN CAST(null AS varchar) ELSE (CASE WHEN (publishedon = '') THEN pubdateplanned ELSE publishedon END) END), '%d-%b-%Y') AS date) launchdate \n" +
                       "   , NULLIF(taxcode.ephcode, '') taxcode \n" +
                       "   , COALESCE(NULLIF(status.ephproductcode, ''), 'UNK') status \n" +
                       "   , NULLIF(product.sourceref, '') manifestationref \n" +
                       "   , NULLIF(workcontent.sourceref, '') worksource \n" +
                       "   , NULLIF(workcontent.work_type, '') work_type \n" +
                       "   , CAST(null AS boolean) separately_sale_indicator \n" +
                       "   , CAST(null AS boolean) trial_allowed_indicator \n" +
                       "   , CAST(null AS varchar) f_work_source_ref \n" +
                       "   , 'OOA' product_type \n" +
                       "   , 'ONE' f_revenue_model \n" +
                       "   , 'N' dq_err \n" +
                       "   FROM \n" +
                       "     (((((("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product \n" +
                       "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content ON (product.sourceref = content.sourceref))\n" +
                       "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_fullversionfamily fullversionfamily ON ((product.sourceref = fullversionfamily.sourceref) AND (workmaster = 'YES')))\n" +
                       "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_current_v workcontent ON (fullversionfamily.projectno = workcontent.sourceref))\n" +
                       "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification ON ((product.sourceref = classification.sourceref) AND (classificationcode = 'DCDFAC | Accounting class')))\n" +
                       "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".taxcode ON (split_part(classification.value, ' | ', 1) = taxcode.ppmcode))\n" +
                       "   LEFT JOIN (\n" +
                       "      SELECT DISTINCT\n" +
                       "        sourceref \n" +
                       "      , ephproductcode \n" +
                       "      FROM \n" +
                       "        (( \n" +
                       "         SELECT\n" +
                       "           sourceref \n" +
                       "         , min (priority) statuspriority \n" +
                       "         FROM ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation\n" +
                       "         INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (split_part(status, ' | ', 1) = ppmcode))\n" +
                       "         GROUP BY sourceref \n" +
                       "      )  masterstatus \n" +
                       "      INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (statuspriority = priority))\n" +
                       "   )  status ON (product.sourceref = status.sourceref))\n" +
                       ")  A \n" +
                       "WHERE ((A.sourceref IS NOT NULL) AND (A.bindingcode IS NOT NULL))";

       public static String GET_PERSON_INBOUND_CURRENT_COUNT =
               "SELECT count(*) as Source_Count \n" +
                       "FROM \n" +
                       "  ( \n" +
                       "   SELECT DISTINCT \n" +
                       "     businesspartnerid sourceref \n" +
                       "   , (CASE WHEN (isperson = 'N') THEN NULLIF(department, '') ELSE NULLIF(firstname, '') END) firstname \n" +
                       "   , (CASE WHEN (isperson = 'N') THEN NULLIF(institution, '') ELSE NULLIF(lastname, '') END) familyname \n" +
                       "   , CAST(null AS varchar) peoplehub_id \n" +
                       "   , CAST(null AS varchar) email_address \n" +
                       "   , 'N' dq_err \n" +
                       "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators \n" +
                       ")  A \n" +
                       " WHERE (A.sourceref IS NOT NULL)";

       public static String GET_MANIF_INBOUND_CURRENT_COUNT =
               "select count(*) as Source_Count from( \n" +
                       "SELECT DISTINCT product.sourceref, content.title, (CASE WHEN (intedition.classificationcode IS NULL) \n" +
                       "THEN false ELSE true END) intereditionflag, cast((date_parse(COALESCE(NULLIF(firstactual,''),\n" +
                       "NULLIF(firstplanned,'')),'%d-%b-%Y')) as date ) firstpublisheddate, product.binding, \n" +
                       "manifestationtypecode.ephcode manifestation_type, manifestationstatus.ephmanifestationcode status \n" +
                       "   , workprod.workmasterprojectno work_id, CAST(NULL AS timestamp)last_pub_date, 'N' dq_err \n" +
                       "   from ((((((("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product \n" +
                       "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content ON (product.sourceref = content.sourceref))\n" +
                       "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily workprod ON ((product.sourceref = workprod.sourceref) AND (workprod.workmasterprojectno IS NOT NULL)))\n" +
                       "   LEFT JOIN (\n" +
                       "      SELECT sourceref, classificationcode \n" +
                       "      FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification \n" +
                       "      WHERE (classificationcode LIKE 'PARELIE%') \n" +
                       "   ) intedition ON (product.sourceref = intedition.sourceref)) \n" +
                       "   LEFT JOIN (\n" +
                       "      SELECT sourceref, min(plannedpubdate) firstplanned \n" +
                       "      FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation \n" +
                       "      WHERE (plannedpubdate <> '') GROUP BY sourceref \n" +
                       "   ) planneddates ON (product.sourceref = planneddates.sourceref))\n" +
                       "   LEFT JOIN (\n" +
                       "      select sourceref, min(pubdateactual) firstactual \n" +
                       "      FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation \n" +
                       "      WHERE (pubdateactual <> '') GROUP BY sourceref \n" +
                       "   )  actualdates ON (product.sourceref = actualdates.sourceref))\n" +
                       "   LEFT JOIN (\n" +
                       "      SELECT distinct sourceref, ephmanifestationcode \n" +
                       "      FROM ((SELECT sourceref, min(priority) statuspriority \n" +
                       "         FROM ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation\n" +
                       "         INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (split_part(status, ' | ', 1) = ppmcode))\n" +
                       "         GROUP BY sourceref)  masterstatus\n" +
                       "      INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (statuspriority = priority)) \n" +
                       "   )  manifestationstatus ON (product.sourceref = manifestationstatus.sourceref))\n" +
                       "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".manifestationtypecode ON (split_part(product.versiontype, ' | ', 1) = manifestationtypecode.ppmcode))\n" +
                       ")";

       public static String GET_ACC_PROD_INBOUND_CURRENT_COUNT =
               "SELECT count(*) as Source_Count\n" +
                       " FROM(( \n" +
                       "      SELECT DISTINCT \n" +
                       "      NULLIF(sourceref, '')sourceref \n" +
                       "      , NULLIF(CAST(accountableproduct AS varchar), '') accountableproduct \n" +
                       "      , NULLIF(accountablename, '') accountablename \n" +
                       "      , NULLIF(accountableparent, '') accountableparent \n" +
                       "      , concat (NULLIF(sourceref, ''), NULLIF(accountableparent, '')) u_key \n" +
                       "      , 'N' dq_err \n" +
                       "      FROM( "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification \n" +
                       "      LEFT JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".worktypecode ON (split_part(classification.value, ' | ', 1) = ppmcode))\n" +
                       "      WHERE (classification.classificationcode = 'DCDFAC | Accounting class'))) A \n" +
                       "WHERE ((A.sourceref IS NOT NULL) AND (A.accountableparent IS NOT NULL))";



    public static String GET_ACC_PROD_DELTA_CURR_COUNT =
               "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_accountable_product";

    public static String GET_MANIF_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation";

    public static String GET_PERSON_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_person";

    public static String GET_PRODUCT_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_product";

    public static String GET_WORK_PERSON_ROLE_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_person_role";

    public static String GET_WORK_RELATION_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_relationship";

    public static String GET_WORK_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work";

    public static String GET_WORK_IDENTIF_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_identifier";

    public static String GET_MANIF_IDENTIF_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation_identifier";

    public static String GET_ACC_PROD_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_accountable_product_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_accountable_product_part)";

    public static String GET_MANIF_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_part " +
                    "where delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_part)";

    public static String GET_PERSON_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_person_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_person_part) ";

    public static String GET_PRODUCT_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_product_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_product_part) ";

    public static String GET_WORK_PERSON_ROLE_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_person_role_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_person_role_part) ";

    public static String GET_WORK_RELATION_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_relationship_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_relationship_part) ";

    public static String GET_WORK_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_part where" +
                    " delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_part) ";

    public static String GET_WORK_IDENTIF_DELTA_HIST_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_identifier_part where" +
                    " delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_identifier_part) ";

    public static String GET_MANIF_IDENTIF_DELTA_HIST_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_identifier_part " +
                    "where delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_identifier_part)";


    public static String GET_ACC_PROD_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_accountable_product d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_part c ))";


    public static String GET_MANIF_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_part c ))";

    public static String GET_PERSON_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_person d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part c ))";


    public static String GET_PRODUCT_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_product d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_part c ))";

    public static String GET_WRK_PERS_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_person_role d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_part c ))";

    public static String GET_WRK_RELT_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_relationship d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_part c ))";

    public static String GET_WRK_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_part c ))";

    public static String GET_WRK_IDENTIF_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_identifier d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_part c ))";

    public static String GET_MANIF_IDENTIF_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation_identifier d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part c ))";

    public static String GET_ACC_PROD_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_excl_delta";

    public static String GET_MANIF_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_excl_delta";

    public static String GET_PERSON_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_excl_delta";

    public static String GET_PRODUCT_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_excl_delta";

    public static String GET_WRK_PERS_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_excl_delta";

    public static String GET_WRK_RELT_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_excl_delta";

    public static String GET_WRK_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_excl_delta";

    public static String GET_WRK_IDENTIF_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_excl_delta";

    public static String GET_MANIF_IDENTIF_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_excl_delta";

    public static String GET_ACC_PROD_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_latest";

    public static String GET_MANIF_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_latest";

    public static String GET_PERSON_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_latest";

    public static String GET_PRODUCT_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_latest";

    public static String GET_WRK_PERS_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_latest";

    public static String GET_WRK_RELT_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_latest";

    public static String GET_WRK_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_latest";

    public static String GET_WRK_IDENTIF_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_latest";

    public static String GET_MANIF_IDENTIF_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_latest";


    public static String GET_ACC_PROD_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_excl_delta as c union all \n" +
                    "select d.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_accountable_product as d)";

    public static String GET_MANIF_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_excl_delta as c union all \n" +
                    "select d.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation as d)";

    public static String GET_PERSON_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_excl_delta\n" +
                    "as c union all \n" +
                    "select d.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_person as d)";

    public static String GET_PRODUCT_SUM_DELTACURR_EXCL_COUNT=
           "select count(*) as source_count from \n" +
                   "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_excl_delta\n" +
                   "as c union all \n" +
                   "select d.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_product as d)";

    public static String GET_WRK_PERS_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_excl_delta as c \n" +
                    "union all \n" +
                    "select d.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_person_role as d)";

    public static String GET_WRK_RELT_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_excl_delta\n" +
                    "as c union all \n" +
                    "select d.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_relationship as d)";

    public static String GET_WRK_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_excl_delta\n" +
                    "as c union all \n" +
                    "select d.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work as d)";

    public static String GET_WRK_IDENTIF_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_excl_delta as c union all \n" +
                    "select d.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_identifier as d)";


    public static String GET_MANIF_IDENTIF_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_excl_delta\n" +
                    "as c union all \n" +
                    "select d.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation_identifier as d)";


    public static String GET_DUPLICATES_LATEST_ACC_PROD_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_latest " +
                    "group by u_key having count(*)>1)";


    public static String GET_DUPLICATES_LATEST_MANIF_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_latest " +
                    "group by sourceref having count(*)>1)";


    public static String GET_DUPLICATES_LATEST_PROD_COUNT =
            "select count(*) as Duplicate_Count from " +
                    "(SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_latest " +
                    "group by u_key having count(*)>1)";

    public static String GET_DUPLICATES_LATEST_PERSON_COUNT =
            "select count(*) as Duplicate_Count from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_latest group by sourceref having count(*)>1)";

    public static String  GET_DUPLICATES_LATEST_WORK_RELT_COUNT=
        "select count(*) as Duplicate_Count " +
                "from (SELECT count(*) " +
                "FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_latest " +
                "group by u_key having count(*)>1)";

    public static String GET_DUPLICATES_LATEST_WORK_PERS_COUNT =
            "select count(*) as Duplicate_Count from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_latest group by u_key having count(*)>1)";

    public static String GET_DUPLICATES_LATEST_WORK_COUNT =
            "select count(*) as Duplicate_Count from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_latest group by sourceref having count(*)>1)";

    public static String GET_DUPLICATES_WORK_IDENTIFIER_COUNT =
            "select count(*) as Duplicate_Count from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_latest group by sourceref,identifier,identifier_type having count(*)>1)";

    public static String GET_DUPLICATES_MANIF_IDENTIFIER_COUNT =
            "select count(*) as Duplicate_Count from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_latest group by sourceref,identifier,identifier_type having count(*)>1)";

    public static String GET_PERSON_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select sourceref, u_key, firstname, familyname, peoplehub_id, email_address, dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select sourceref, u_key, firstname, familyname, peoplehub_id, email_address, dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 0) <> coalesce(prev.sourceref,0 ) or\n" +
                    "            coalesce(crr.u_key, 0) <> coalesce(prev.u_key, 0) or\n" +
                    "            coalesce (crr.firstname, 'na') <> coalesce (prev.firstname, 'na') or\n" +
                    "            coalesce (crr.familyname, 'na') <> coalesce (prev.familyname, 'na') or\n" +
                    "            coalesce (crr.peoplehub_id, 'na') <> coalesce (prev.peoplehub_id, 'na') or\n" +
                    "            coalesce (crr.email_address, 'na') <> coalesce (prev.email_address, 'na') or\n" +
                    "            coalesce (crr.dq_err, 'na') <> coalesce (prev.dq_err, 'na')))";

    public static String GET_ACC_PROD_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select sourceref,accountableproduct, accountablename, accountableparent, u_key, dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select sourceref, accountableproduct, accountablename, accountableparent, u_key, dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'na') <> coalesce(prev.sourceref,'na' ) or\n" +
                    "            coalesce(crr.accountableproduct, 'na') <> coalesce(prev.accountableproduct, 'na') or\n" +
                    "            coalesce (crr.accountablename, 'na') <> coalesce (prev.accountablename, 'na') or\n" +
                    "            coalesce (crr.accountableparent, 'na') <> coalesce (prev.accountableparent, 'na') or\n" +
                    "            coalesce (crr.u_key, 'na') <> coalesce (prev.u_key, 'na') or\n" +
                    "            coalesce (crr.dq_err, 'na') <> coalesce (prev.dq_err, 'na')))";

    public static String GET_MANIF_DIFF_TRANSFORM_FILE_COUNT =
    " with crr_dataset as(\n"+
            "  select sourceref,u_key, title, intereditionflag, firstpublisheddate, binding,manifestation_type,status,work_id,last_pub_date,dq_err\n"+
            "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_transform_file_history_part\n"+
            "  where transform_file_ts = (select max(transform_file_ts ) \n"+
            "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_transform_file_history_part)\n"+
            "  ),\n"+
            "  prev_dataset as (\n"+
            "  select sourceref,u_key, title, intereditionflag, firstpublisheddate, binding,manifestation_type,status,work_id,last_pub_date,dq_err\n"+
            "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_transform_file_history_part\n"+
            "  where transform_file_ts \n"+
            "  = (select distinct transform_file_ts from \n"+
            "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n"+
            "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_transform_file_history_part dhap)where rn = 2))                                  \n"+
            "select count(*) as source_count from( \n"+
            "    select crr.u_key as UKEY\n"+
            "    from crr_dataset crr\n"+
            "        left join prev_dataset prev on crr.u_key = prev.u_key\n"+
            "    where prev.u_key is null\n"+
            "    union\n"+
            "    select  prev.u_key as UKEY \n"+
            "    from prev_dataset prev\n"+
            "        left join crr_dataset crr on crr.u_key = prev.u_key\n"+
            "    where crr.u_key is null\n"+
            "    union\n"+
            "    select crr.u_key as UKEY\n"+
            "    from crr_dataset crr\n"+
            "        join prev_dataset prev on crr.u_key = prev.u_key\n"+
            "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n"+
            "            coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n"+
            "            coalesce (crr.title, 'null') <> coalesce (prev.title, 'null') or\n"+
            "            coalesce (crr.intereditionflag, false) <> coalesce (prev.intereditionflag, false) or\n"+
            "            coalesce (crr.firstpublisheddate, current_date) <> coalesce (prev.firstpublisheddate, current_date) or\n"+
            " coalesce (crr.binding, 'null') <> coalesce (prev.binding, 'null') or\n"+
            " coalesce (crr.manifestation_type, 'null') <> coalesce (prev.manifestation_type, 'null') or\n"+
            " coalesce (crr.status, 'null') <> coalesce (prev.status, 'null') or\n"+
            " coalesce (crr.work_id, 'null') <> coalesce (prev.work_id, 'null') or\n"+
            " coalesce (crr.last_pub_date, current_date) <> coalesce (prev.last_pub_date, current_date) or\n"+
            " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))";



    public static String GET_PRODUCT_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "            select sourceref,bindingcode, u_key, name, shorttitle, launchdate,taxcode,status,manifestationref,worksource,work_type,separately_sale_indicator,trial_allowed_indicator,f_work_source_ref,product_type,f_revenue_model,dq_err\n" +
                    "            from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_transform_file_history_part\n" +
                    "                    where transform_file_ts = (select max(transform_file_ts )\n" +
                    "    from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_transform_file_history_part)\n" +
                    "            ),\n" +
                    "    prev_dataset as (\n" +
                    "            select sourceref,bindingcode, u_key, name, shorttitle, launchdate,taxcode,status,manifestationref,worksource,work_type,separately_sale_indicator,trial_allowed_indicator,f_work_source_ref,product_type,f_revenue_model,dq_err\n" +
                    "            from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_transform_file_history_part\n" +
                    "                    where transform_file_ts\n" +
                    "                    = (select distinct transform_file_ts from\n" +
                    "                    (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn\n" +
                    "    from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_transform_file_history_part dhap)where rn = 2))\n" +
                    "    select count(*) as source_count from(\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "    left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY\n" +
                    "    from prev_dataset prev\n" +
                    "    left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "    join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n" +
                    "    coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
                    "    coalesce (crr.bindingcode, 'null') <> coalesce (prev.bindingcode, 'null') or\n" +
                    "    coalesce (crr.name, 'null') <> coalesce (prev.name, 'null') or\n" +
                    "    coalesce (crr.shorttitle, 'null') <> coalesce (prev.shorttitle, 'null') or\n" +
                    "    coalesce (crr.taxcode, 'null') <> coalesce (prev.taxcode, 'null') or\n" +
                    "    coalesce (crr.launchdate, current_date) <> coalesce (prev.launchdate, current_date) or\n" +
                    "    coalesce (crr.manifestationref, 'null') <> coalesce (prev.manifestationref, 'null') or\n" +
                    "    coalesce (crr.status, 'null') <> coalesce (prev.status, 'null') or\n" +
                    "    coalesce (crr.worksource, 'null') <> coalesce (prev.worksource, 'null') or\n" +
                    "    coalesce (crr.work_type, 'null') <> coalesce (prev.work_type, 'null') or\n" +
                    "    coalesce (crr.separately_sale_indicator, false) <> coalesce (prev.separately_sale_indicator, false) or\n" +
                    "    coalesce (crr.trial_allowed_indicator, false) <> coalesce (prev.trial_allowed_indicator, false) or\n" +
                    "    coalesce (crr.f_work_source_ref, 'null') <> coalesce (prev.f_work_source_ref, 'null') or\n" +
                    "    coalesce (crr.product_type, 'null') <> coalesce (prev.product_type, 'null') or\n" +
                    "    coalesce (crr.f_revenue_model, 'null') <> coalesce (prev.f_revenue_model, 'null') or\n" +
                    "    coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))";

    public static String GET_WRK_PERS_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select worksourceref,personsourceref, roletype, u_key, sequence, deduplicator,modifiedon,dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select worksourceref,personsourceref, roletype, u_key, sequence, deduplicator,modifiedon,dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.worksourceref, 'null') <> coalesce(prev.worksourceref,'null' ) or\n" +
                    "            coalesce(crr.personsourceref, 'null') <> coalesce(prev.personsourceref, 'null') or\n" +
                    "            coalesce (crr.roletype, 'null') <> coalesce (prev.roletype, 'null') or\n" +
                    "            coalesce (crr.u_key, 'null') <> coalesce (prev.u_key, 'null') or\n" +
                    " coalesce (crr.sequence, 'null') <> coalesce (prev.sequence, 'null') or\n" +
                    " coalesce (crr.deduplicator, 'null') <> coalesce (prev.deduplicator, 'null') or\n" +
                    " coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))";

    public static String GET_WRK_RELT_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select u_key,parentref, childref, relationtyperef, modifiedon, dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select u_key,parentref, childref, relationtyperef, modifiedon, dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.u_key, 'null') <> coalesce(prev.u_key,'null' ) or\n" +
                    " coalesce(crr.parentref, 'null') <> coalesce(prev.parentref, 'null') or\n" +
                    " coalesce (crr.childref, 'null') <> coalesce (prev.childref, 'null') or\n" +
                    " coalesce (crr.relationtyperef, 'null') <> coalesce (prev.relationtyperef, 'null') or\n" +
                    " coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))";

    public static String GET_WRK_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select sourceref,u_key, title, subtitle, volumeno, copyrightyear,editionno,pmc,work_type,statuscode,imprintcode,te_opco,opco,resp_centre,pmg,languagecode,electro_rights_indicator,f_oa_journal_type,f_society_ownership,subscription_type,modifiedon\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "   select sourceref,u_key, title, subtitle, volumeno, copyrightyear,editionno,pmc,work_type,statuscode,imprintcode,te_opco,opco,resp_centre,pmg,languagecode,electro_rights_indicator,f_oa_journal_type,f_society_ownership,subscription_type,modifiedon\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n" +
                    "            coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
                    "            coalesce (crr.title, 'null') <> coalesce (prev.title, 'null') or\n" +
                    "            coalesce (crr.subtitle, 'null') <> coalesce (prev.subtitle, 'null') or\n" +
                    " coalesce (crr.volumeno, 'null') <> coalesce (prev.volumeno, 'null') or\n" +
                    " coalesce (crr.copyrightyear, 0) <> coalesce (prev.copyrightyear, 0) or\n" +
                    "            coalesce (crr.editionno, 0) <> coalesce (prev.editionno, 0) or\n" +
                    " coalesce (crr.pmc, 'null') <> coalesce (prev.pmc, 'null') or\n" +
                    " coalesce (crr.work_type, 'null') <> coalesce (prev.work_type, 'null') or\n" +
                    " coalesce (crr.statuscode, 'null') <> coalesce (prev.statuscode, 'null') or\n" +
                    " coalesce (crr.imprintcode, 'null') <> coalesce (prev.imprintcode, 'null') or\n" +
                    " coalesce (crr.te_opco, 'null') <> coalesce (prev.te_opco, 'null') or\n" +
                    " coalesce (crr.opco, 'null') <> coalesce (prev.opco, 'null') or\n" +
                    " coalesce (crr.resp_centre, 'null') <> coalesce (prev.resp_centre, 'null') or\n" +
                    " coalesce (crr.pmg, 'null') <> coalesce (prev.pmg, 'null') or\n" +
                    " coalesce (crr.languagecode, 'null') <> coalesce (prev.languagecode, 'null') or\n" +
                    " coalesce (crr.electro_rights_indicator, false) <> coalesce (prev.electro_rights_indicator, false) or\n" +
                    " coalesce (crr.f_oa_journal_type, 'null') <> coalesce (prev.f_oa_journal_type, 'null') or\n" +
                    " coalesce (crr.f_society_ownership, 'null') <> coalesce (prev.f_society_ownership, 'null') or\n" +
                    " coalesce (crr.subscription_type, 'null') <> coalesce (prev.subscription_type, 'null') or\n" +
                    " coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date)))";

    public static String GET_WRK_IDENTIF_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select sourceref,u_key, identifier, identifier_type\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "   select sourceref,u_key, identifier, identifier_type\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n" +
                    "            coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
                    "            coalesce (crr.identifier, 'null') <> coalesce (prev.identifier, 'null') or\n" +
                    "            coalesce (crr.identifier_type, 'null') <> coalesce (prev.identifier_type, 'null')))";


    public static String GET_MANIF_IDENTIF_DIFF_TRANSFORM_FILE_COUNT =

            " with crr_dataset as(\n" +
                    "  select sourceref,u_key, identifier, identifier_type\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "   select sourceref,u_key, identifier, identifier_type\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n" +
                    "            coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
                    "            coalesce (crr.identifier, 'null') <> coalesce (prev.identifier, 'null') or\n" +
                    "            coalesce (crr.identifier_type, 'null') <> coalesce (prev.identifier_type, 'null')))";


}




