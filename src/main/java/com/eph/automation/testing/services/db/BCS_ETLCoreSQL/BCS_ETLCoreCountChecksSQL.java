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
                    "(select c.sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_relationship d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
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

}




